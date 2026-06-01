////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "connector/duckdb_physical_search_insert.h"

#include <duckdb/common/allocator.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "catalog/catalog.h"
#include "catalog/column_expr.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/key_utils.hpp"
#include "connector/search_table_marker.h"
#include "connector/search_table_sink_writer.h"
#include "pg/connection_context.h"
#include "query/local_table_changes.h"
#include "query/transaction.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

// All operator state for the parallel INSERT pipeline. One instance per
// query::Transaction-bound INSERT; rebuilt per query, not reused across
// transactions.
struct SearchInsertGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<TableShard> table_shard;
  // Borrowed -- lives on the connection's query::Transaction, which outlives
  // this state (the sdb-side commit happens after Finalize and owns the
  // per-thread iresearch trxs via _parallel_search_transactions).
  query::Transaction* sdb_txn = nullptr;
  // The kSearch shard, downcast once here. Each sink thread opens its own
  // IndexWriter::Transaction from it in GetLocalSinkState (one segment per
  // thread); table_shard's shared_ptr keeps it alive.
  search::SearchTableShard* search_shard = nullptr;

  // Catalog column ids in input-chunk order (skips the synthetic
  // generated-PK column). Used both as the SearchTableSinkWriter
  // SwitchColumn key and as the WAL marker's column_ids payload.
  std::vector<catalog::Column::Id> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;

  // PK encoding scaffolding (matches duckdb_physical_insert's setup,
  // but without conflict-resolver / index-writer fanout).
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  std::string table_key;

  // Set for tables without an explicit PRIMARY KEY -- Sink reserves a
  // contiguous range from this sequence per chunk; Finalize walks the
  // reservations via a cursor when assigning per-row PKs. Null for
  // explicit-PK tables.
  std::shared_ptr<catalog::Sequence> generated_pk_seq;

  std::shared_lock<std::shared_mutex> table_lock;

  // Serialises the two cross-thread mutations on query::Transaction:
  // registering a per-thread collection (GetLocalSinkState) and handing off
  // the per-thread trx + summing insert_count (Combine). Both are
  // once-per-thread, so contention is negligible.
  std::mutex combine_mu;
  duckdb::idx_t insert_count = 0;

  // CTAS bookkeeping. ctas_mode=false for plain INSERT/COPY. On the
  // failure path (txn abort before Finalize clears the tombstone) the
  // dtor drops the half-created table -- DropTable -> DropArtifacts
  // (kSearch) wipes its iresearch directory.
  bool ctas_mode = false;
  bool ctas_finalized = false;
  ObjectId ctas_database_id;
  std::string ctas_database_name;
  std::string ctas_schema_name;
  std::string ctas_table_name;

  ~SearchInsertGlobalState() override {
    if (ctas_mode && !ctas_finalized && !ctas_table_name.empty()) {
      try {
        auto& catalog = SerenedServer::Instance()
                          .getFeature<catalog::CatalogFeature>()
                          .Global();
        std::ignore = catalog.DropTable(ctas_database_name, ctas_schema_name,
                                        ctas_table_name, true);
      } catch (...) {
      }
    }
  }
};

struct SearchInsertSourceState : duckdb::GlobalSourceState {
  bool finished = false;
};

// Per-sink-thread state. Each thread owns one iresearch segment (its own
// IndexWriter::Transaction) and writes its chunks directly into it during
// Sink. The transaction is handed to query::Transaction at Combine and
// committed on the shared tick at txn commit -- NOT here, so it must survive
// Combine (we must not let its destructor auto-commit, which would assign an
// unrelated _tick-derived tick).
struct SearchInsertLocalState : duckdb::LocalSinkState {
  // search_trx must outlive `sink` (the writer holds a reference to it).
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<SearchTableSinkWriter> sink;
  // This thread's in-flight buffer. Owned by query::Transaction
  // (LocalTableChangesEntry::insert_collections); referenced here. Used for
  // WAL-marker emission at Finalize and (future) the RYOW scan overlay.
  duckdb::ColumnDataCollection* collection = nullptr;
  duckdb::idx_t insert_count = 0;
  // Reused per-row PK scratch buffer.
  std::string pk_buffer;
};

// CTAS-mode helper: creates the target table (tombstoned) from the bound
// CREATE TABLE info and fills the CTAS bookkeeping on `state`. Returns the
// freshly-created catalog Table, or nullptr when IF NOT EXISTS hit an
// existing relation (caller returns a null sink state). Mirrors
// SereneDBPhysicalCTAS::GetGlobalSinkState's creation block -- the only
// part of CTAS that differs from a plain search insert.
std::shared_ptr<catalog::Table> CreateCtasTable(
  SearchInsertGlobalState& state, duckdb::BoundCreateTableInfo& info,
  duckdb::SchemaCatalogEntry& schema) {
  auto& schema_entry = schema.Cast<SereneDBSchemaEntry>();
  auto database_id = schema_entry.GetDatabaseId();
  auto& create_info = info.Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();

  catalog::CreateTableOptions options;
  options.name = table_info.table;
  for (auto& col : table_info.columns.Logical()) {
    catalog::Column sdb_col{{}, catalog::NextId(), col.Name(), col.Type()};
    if (col.Generated()) {
      sdb_col.generated_type = catalog::Column::GeneratedType::kStored;
      sdb_col.expr =
        std::make_shared<ColumnExpr>(col.GeneratedExpression().Copy());
    } else if (col.HasDefaultValue()) {
      sdb_col.expr = std::make_shared<ColumnExpr>(col.DefaultValue().Copy());
    }
    options.columns.push_back(std::move(sdb_col));
  }
  // CTAS has no PK/UNIQUE constraints -- pk_columns stays empty, so the
  // Table ctor wires up a generated PK sequence (handled by the
  // generated-PK reservation path in Sink/Finalize).
  ApplyColumnModes(options.columns, table_info.options);
  ApplyStorageKind(options, table_info.options);
  SDB_ASSERT(options.storage == catalog::StorageKind::kSearch,
             "SereneDBSearchInsert CTAS mode used for non-search storage");

  auto& catalog_impl =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  const bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;
  op_options.create_with_tombstone = true;

  auto r = catalog_impl.CreateTable(database_id, schema.name,
                                    std::move(options), op_options);
  if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
    if (if_not_exists) {
      return nullptr;
    }
    throw duckdb::CatalogException("relation \"%s\" already exists",
                                   table_info.table);
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  // Fetch the new (tombstoned) table from a fresh global snapshot -- the
  // connection's snapshot predates the create.
  auto snapshot = catalog_impl.GetCatalogSnapshot();
  auto catalog_table =
    snapshot->GetTable(database_id, schema.name, std::string{table_info.table});
  SDB_ASSERT(catalog_table);
  auto database = snapshot->GetDatabase(database_id);
  SDB_ASSERT(database);

  state.ctas_mode = true;
  state.ctas_database_id = database_id;
  state.ctas_database_name = database->GetName();
  state.ctas_schema_name = schema.name;
  state.ctas_table_name = table_info.table;
  return catalog_table;
}

// Removes the CTAS tombstone so the populated table becomes visible. No-op
// for plain INSERT/COPY. Runs on every Finalize success path (including
// the zero-row CTAS case -- an empty SELECT still creates an empty table).
void RemoveCtasTombstoneIfNeeded(SearchInsertGlobalState& state) {
  if (!state.ctas_mode) {
    return;
  }
  SDB_IF_FAILURE("crash_before_remove_tombstone") { SDB_IMMEDIATE_ABORT(); }
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  auto r = catalog.RemoveTombstone(
    state.ctas_database_id, state.ctas_schema_name, state.ctas_table_name);
  if (!r.ok()) {
    throw duckdb::InternalException("Failed to remove tombstone: %s",
                                    std::string{r.errorMessage()});
  }
  state.ctas_finalized = true;
}

}  // namespace

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(std::move(table)) {}

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  duckdb::SchemaCatalogEntry& schema, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _ctas_info(std::move(info)),
    _ctas_schema(&schema) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchInsert::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  // Resolve the target table: pre-existing (insert mode) or created here
  // (CTAS mode). In CTAS mode the connection snapshot predates the new
  // table, so we both look up via a fresh global snapshot (inside
  // CreateCtasTable) and drop the connection snapshot afterwards so this
  // statement -- and later ones -- re-fetch a snapshot that sees it.
  std::shared_ptr<catalog::Table> table;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  if (_ctas_info) {
    table = CreateCtasTable(*state, *_ctas_info, *_ctas_schema);
    if (!table) {
      return nullptr;  // IF NOT EXISTS hit an existing relation.
    }
    conn_ctx.DropCatalogSnapshot();
    auto& catalog_impl =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
    snapshot = catalog_impl.GetCatalogSnapshot();
  } else {
    table = _table;
    snapshot = conn_ctx.EnsureCatalogSnapshot();
  }

  state->table_id = table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  state->table_shard = snapshot->GetTableShard(state->table_id);
  SDB_ASSERT(state->table_shard);
  SDB_ASSERT(state->table_shard->GetStorage() == catalog::StorageKind::kSearch,
             "SereneDBSearchInsert dispatched against a non-search shard");
  state->table_lock = std::shared_lock{state->table_shard->GetTableLock()};

  // No explicit PK -> resolve the generated-PK sequence object once here;
  // Sink reserves per-chunk ranges from it. Mirrors duckdb_physical_insert.
  // CTAS tables always take this path (CTAS never declares a PK).
  if (table->PKColumns().empty()) {
    state->generated_pk_seq =
      snapshot->GetObject<catalog::Sequence>(table->GetGeneratedPkSeqId());
    SDB_ASSERT(state->generated_pk_seq);
  }

  // Build column metadata in chunk order. The DuckDB plan binds the
  // chunk to non-generated columns of the table in declaration order.
  state->column_ids.reserve(table->Columns().size());
  state->chunk_types.reserve(table->Columns().size());
  for (const auto& col : table->Columns()) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->column_ids.push_back(col.GetId());
    state->chunk_types.push_back(col.type);
  }

  state->pk_columns = duckdb_primary_key::BuildPKColumns(*table);

  state->sdb_txn = &conn_ctx;
  state->search_shard =
    &basics::downCast<search::SearchTableShard>(*state->table_shard);

  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchInsert::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto* gstate =
    sink_state ? &sink_state->Cast<SearchInsertGlobalState>() : nullptr;
  // Guards the CTAS IF-NOT-EXISTS path where GetGlobalSinkState returned
  // nullptr: hand back a plain LocalSinkState; Sink/Combine no-op on it.
  if (gstate == nullptr || gstate->search_shard == nullptr) {
    return duckdb::make_uniq<duckdb::LocalSinkState>();
  }

  auto lstate = duckdb::make_uniq<SearchInsertLocalState>();
  lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
    gstate->search_shard->GetTransaction());
  lstate->sink = std::make_unique<SearchTableSinkWriter>(*lstate->search_trx);

  // Register this thread's collection on query::Transaction so it outlives
  // the local state (read single-threaded at Finalize for marker emission;
  // future RYOW overlay). unique_ptr keeps the pointer stable across vector
  // growth.
  auto collection = std::make_unique<duckdb::ColumnDataCollection>(
    duckdb::BufferManager::GetBufferManager(context.client),
    gstate->chunk_types);
  {
    std::lock_guard<std::mutex> lock(gstate->combine_mu);
    auto& entry = gstate->sdb_txn->GetLocalTableChanges()[gstate->table_id];
    entry.insert_collections.push_back(std::move(collection));
    lstate->collection = entry.insert_collections.back().get();
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate = dynamic_cast<SearchInsertLocalState*>(&input.local_state);

  const auto num_rows = chunk.size();
  if (num_rows == 0 || lstate == nullptr || !lstate->sink) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  // Write this chunk straight into the thread's own iresearch segment: one
  // Init/SwitchColumn/Write/Finish cycle per chunk. Init's batch_size must
  // be the exact row count -- iresearch pre-allocates that many DocContext
  // slots per Insert(false, batch_size), and the segment's docs_count
  // reflects the allocation, not the NextDocument calls.
  auto& sink = *lstate->sink;
  sink.Init(num_rows);
  for (duckdb::idx_t col_idx = 0; col_idx < gstate.column_ids.size();
       ++col_idx) {
    sink.SwitchColumn(gstate.column_ids[col_idx], gstate.chunk_types[col_idx],
                      chunk.data[col_idx], num_rows);
  }

  // Per-row PK. Explicit-PK reads the key from chunk columns
  // (pk_columns/pk_formats); generated-PK reserves a contiguous range from
  // the sequence -- this thread's range is disjoint from every other
  // thread's because ReserveWriteUnsafe is atomic.
  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);
  const bool uses_generated_pk = gstate.generated_pk_seq != nullptr;
  const uint64_t pk_base =
    uses_generated_pk ? gstate.generated_pk_seq->ReserveWriteUnsafe(num_rows)
                      : 0;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    const uint64_t generated_pk = uses_generated_pk ? pk_base + row : 0;
    lstate->pk_buffer.clear();
    duckdb_primary_key::MakeColumnKey(
      pk_formats, gstate.pk_columns, row, generated_pk, gstate.table_key,
      [](std::string_view) {}, lstate->pk_buffer);
    // The PK field indexes only the row-key portion (bytes after the
    // table_id + column_id prefix); pass it directly.
    sink.Write(key_utils::ExtractRowKey(lstate->pk_buffer));
  }
  sink.Finish();
  // Retain the chunk in this thread's collection for WAL-marker emission at
  // Finalize (and future RYOW). Lock-free -- the collection is this thread's.
  lstate->collection->Append(chunk);
  lstate->insert_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchInsert::Combine(
  duckdb::ExecutionContext& /*context*/,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate = dynamic_cast<SearchInsertLocalState*>(&input.local_state);
  if (lstate == nullptr || !lstate->search_trx) {
    return duckdb::SinkCombineResultType::FINISHED;
  }

  // The writer holds a reference to search_trx -- destroy it first (its
  // Document was already released by the last Sink's Finish).
  lstate->sink.reset();

  // A thread that received no rows has an empty transaction (no segment was
  // ever acquired). Discard it instead of handing it off: destructing it is
  // a no-op (mirrors CREATE INDEX's unconditional reset of per-thread trxs),
  // and its empty collection is skipped at Finalize. Only segments that hold
  // rows are committed on tick.
  if (lstate->insert_count == 0) {
    lstate->search_trx.reset();
    return duckdb::SinkCombineResultType::FINISHED;
  }

  // Hand the populated segment to query::Transaction. We do NOT commit it
  // here: the commit-on-tick (Commit(post_commit_seq)) happens in
  // Transaction::Commit. Letting the destructor auto-commit would advance
  // the writer's internal _tick and assign an unrelated tick, breaking the
  // single-RefreshCommit-publishes-all-segments invariant.
  std::lock_guard<std::mutex> lock(gstate.combine_mu);
  gstate.insert_count += lstate->insert_count;
  gstate.sdb_txn->AddParallelSearchTransaction(std::move(lstate->search_trx));
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  if (gstate.insert_count == 0) {
    // Empty SELECT into CTAS still materialises an (empty) table.
    RemoveCtasTombstoneIfNeeded(gstate);
    return duckdb::SinkFinalizeType::READY;
  }

  // The iresearch writes already happened in the parallel Sink phase; the
  // per-thread trxs were handed to query::Transaction at Combine and are
  // committed on the shared tick by query::Transaction::Commit. All Combines
  // have run (event-DAG barrier), so every per-thread collection is
  // registered and fully populated.
  //
  // Emit the WAL marker(s) here, single-threaded: PutLogData mutates the
  // shared rocksdb WriteBatch and is NOT concurrency-safe, so marker
  // emission must stay out of the parallel Sink phase. One marker set per
  // per-thread collection (EmitInsertsForBuffer picks CDC vs per-chunk by
  // size); recovery replays them and re-feeds the rows into iresearch.
  auto& local_changes = gstate.sdb_txn->GetLocalTableChanges();
  auto it = local_changes.find(gstate.table_id);
  if (it != local_changes.end()) {
    for (auto& collection : it->second.insert_collections) {
      if (collection && collection->Count() > 0) {
        search_table_marker::EmitInsertsForBuffer(
          *gstate.sdb_txn, gstate.table_id, gstate.column_ids, *collection);
      }
    }
    // Collections consumed; drop them so a later statement in the same txn
    // starts fresh. (RYOW would instead retain them with an emitted
    // watermark; deferred.)
    local_changes.erase(it);
  }

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.UpdateNumRows(gstate.table_id, gstate.insert_count);

  // CTAS: reveal the now-populated table. Must run after the data is
  // staged so the table never becomes visible empty mid-commit.
  RemoveCtasTombstoneIfNeeded(gstate);
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SearchInsertSourceState>();
}

duckdb::SourceResultType SereneDBSearchInsert::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchInsertSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SearchInsertGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
