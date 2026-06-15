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
#include <optional>
#include <shared_mutex>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/log.h"
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
#include "connector/search_sink_writer.hpp"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/transaction.h"
#include "search/search_table_changes.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

struct SearchInsertGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<TableShard> table_shard;
  query::Transaction* sdb_txn = nullptr;
  search::SearchTableShard* search_shard = nullptr;
  std::vector<catalog::Column::Id> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  std::string table_key;
  std::shared_ptr<catalog::Sequence> generated_pk_seq;
  std::shared_lock<std::shared_mutex> table_lock;

  std::mutex combine_mu;
  duckdb::idx_t insert_count = 0;
  // Bulk path only: each combining sink thread appends its sealed chunk's
  // seg_id here (under combine_mu); Finalize hands the whole statement's
  // segments to the manifest in one batched AddReferences call.
  std::vector<uint64_t> bulk_seg_ids;

  // CTAS bookkeeping.
  bool ctas_mode = false;
  bool ctas_finalized = false;
  ObjectId ctas_database_id;
  std::string ctas_database_name;
  std::string ctas_schema_name;
  std::string ctas_table_name;

  ~SearchInsertGlobalState() override {
    if (ctas_mode && !ctas_finalized && !ctas_table_name.empty()) {
      try {
        auto& catalog = catalog::CatalogFeature::instance().Global();
        auto r = catalog.DropTable(ctas_database_name, ctas_schema_name,
                                   ctas_table_name, true);
        if (!r.ok()) {
          SDB_WARN(SEARCH, "CTAS rollback: failed to drop half-created table '",
                   ctas_table_name, "': ", r.errorMessage());
        }
      } catch (const std::exception& e) {
        SDB_WARN(SEARCH, "CTAS rollback: failed to drop half-created table '",
                 ctas_table_name, "': ", e.what());
      } catch (...) {
        SDB_WARN(SEARCH, "CTAS rollback: failed to drop half-created table '",
                 ctas_table_name, "' (unknown exception)");
      }
    }
  }
};

struct SearchInsertSourceState : duckdb::GlobalSourceState {
  bool finished = false;
};

// Per-sink-thread state. A bulk thread owns a fresh trx (one segment per
// thread); an inline (single-threaded) statement reuses the shard's serial trx
// so consecutive statements coalesce into one segment. Either way the trx
// commits on the shared tick at txn commit, not here.
struct SearchInsertLocalState : duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  bool bulk = false;
  std::optional<search::SearchDbWal::ChunkWriter> chunk_writer;
  duckdb::idx_t insert_count = 0;
};

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
  // CTAS has no PK/UNIQUE constraints, so the Table ctor wires up a generated
  // PK sequence.
  ApplyColumnModes(options.columns, table_info.options);
  ApplyStorageKind(options, table_info.options);
  SDB_ASSERT(options.storage == catalog::StorageKind::kSearch,
             "SereneDBSearchInsert CTAS mode used for non-search storage");

  auto& catalog_impl = catalog::CatalogFeature::instance().Global();
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
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_DUPLICATE_TABLE),
      ERR_MSG("relation \"", table_info.table, "\" already exists"));
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
  auto& catalog = catalog::CatalogFeature::instance().Global();
  auto r = catalog.RemoveTombstone(
    state.ctas_database_id, state.ctas_schema_name, state.ctas_table_name);
  if (!r.ok()) {
    SDB_THROW(std::move(r));
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

  std::shared_ptr<catalog::Table> table;
  std::shared_ptr<const catalog::Snapshot> snapshot;
  if (_ctas_info) {
    table = CreateCtasTable(*state, *_ctas_info, *_ctas_schema);
    if (!table) {
      return nullptr;  // IF NOT EXISTS hit an existing relation.
    }
    conn_ctx.DropCatalogSnapshot();
    auto& catalog_impl = catalog::CatalogFeature::instance().Global();
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

  if (table->PKColumns().empty()) {
    state->generated_pk_seq =
      snapshot->GetObject<catalog::Sequence>(table->GetGeneratedPkSeqId());
    SDB_ASSERT(state->generated_pk_seq);
  }

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
  lstate->bulk = context.pipeline && context.pipeline->GetMaxThreads() > 1;

  if (lstate->bulk) {
    lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
      gstate->search_shard->GetTransaction());
    lstate->sink =
      MakeSearchTableInsertSink(*lstate->search_trx, gstate->column_ids);
  }
  // Inline (single-threaded): the serial trx + sink are created lazily in Sink,
  // and each chunk is coalesced into the ordered manifest's current insert run
  // there -- no per-statement collection is pre-created.
  return lstate;
}

duckdb::SinkResultType SereneDBSearchInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate = dynamic_cast<SearchInsertLocalState*>(&input.local_state);

  const auto num_rows = chunk.size();
  if (num_rows == 0 || lstate == nullptr) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  if (!lstate->sink) {
    SDB_ASSERT(!lstate->bulk);
    auto& trx = gstate.sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
      gstate.table_shard,
      [&] { return gstate.search_shard->GetTransaction(); });
    lstate->sink = MakeSearchTableInsertSink(trx, gstate.column_ids);
  }

  const bool uses_generated_pk = gstate.generated_pk_seq != nullptr;
  const uint64_t pk_base =
    uses_generated_pk ? gstate.generated_pk_seq->ReserveWriteUnsafe(num_rows)
                      : 0;
  WriteChunkToSearchSink(*lstate->sink, chunk, gstate.column_ids,
                         gstate.pk_columns, gstate.table_key, uses_generated_pk,
                         pk_base);

  if (lstate->bulk) {
    if (!lstate->chunk_writer) {
      lstate->chunk_writer.emplace(gstate.search_shard->NewChunkWriter());
    }
    lstate->chunk_writer->Append(chunk, pk_base);
  } else {
    gstate.sdb_txn->SearchTxn().AddInlineInsertChunk(
      gstate.table_shard,
      duckdb::BufferManager::GetBufferManager(context.client),
      gstate.chunk_types, chunk, uses_generated_pk, pk_base);
  }
  lstate->insert_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchInsert::Combine(
  duckdb::ExecutionContext& /*context*/,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate = dynamic_cast<SearchInsertLocalState*>(&input.local_state);
  if (lstate == nullptr) {
    return duckdb::SinkCombineResultType::FINISHED;  // CTAS IF-NOT-EXISTS no-op
  }
  lstate->sink.reset();

  // No rows: nothing to hand off. Discard a bulk thread's empty owned trx;
  if (lstate->insert_count == 0) {
    lstate->search_trx.reset();
    return duckdb::SinkCombineResultType::FINISHED;
  }

  search::SearchDbWal::PendingChunk pending;
  if (lstate->bulk) {
    SDB_ASSERT(lstate->chunk_writer,
               "bulk sink thread with rows but no chunk writer");
    pending = lstate->chunk_writer->Finish();
  }

  std::lock_guard<std::mutex> lock(gstate.combine_mu);
  gstate.insert_count += lstate->insert_count;
  if (lstate->bulk) {
    gstate.bulk_seg_ids.push_back(pending.SegId());
    gstate.sdb_txn->SearchTxn().AddParallelSearchTransaction(
      gstate.table_shard, std::move(lstate->search_trx), std::move(pending));
  }
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

  // All parallel sinks have combined: hand the bulk statement's chunk-file refs
  // to the manifest in one batched call (resolves the insert run once).
  if (!gstate.bulk_seg_ids.empty()) {
    gstate.sdb_txn->SearchTxn().AddReferences(gstate.table_shard,
                                              gstate.bulk_seg_ids);
  }

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.UpdateNumRows(gstate.table_id, gstate.insert_count);
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
