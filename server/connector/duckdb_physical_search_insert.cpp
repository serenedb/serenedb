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
#include <duckdb/common/types/column/column_data_collection.hpp>
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
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/column_expr.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_schema_entry.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"
#include "search/search_table_changes.h"

namespace sdb::connector {
namespace {

struct SearchInsertGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;
  std::vector<catalog::ColumnId> column_ids;
  duckdb::vector<duckdb::LogicalType> chunk_types;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  std::shared_ptr<catalog::SequenceCounter> generated_pk_seq;
  std::shared_lock<std::shared_mutex> table_lock;

  std::mutex combine_mu;
  duckdb::idx_t insert_count = 0;
  // RETURNING only: the inserted rows, merged out of the sink threads.
  std::optional<duckdb::ColumnDataCollection> returned;

  std::vector<search::SearchDbWal::PendingChunk> bulk_chunks;

  bool ctas_mode = false;
  bool ctas_finalized = false;
  std::string ctas_database_name;
  std::string ctas_schema_name;
  std::string ctas_table_name;

  ~SearchInsertGlobalState() override {
    if (ctas_mode && !ctas_finalized && !ctas_table_name.empty()) {
      try {
        catalog::GetCatalog().DropTable(
          catalog::NoAccessCheck(), ctas_database_name, ctas_schema_name,
          ctas_table_name, /*cascade=*/true, /*missing_ok=*/true);
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
  duckdb::ColumnDataScanState scan;
};

struct SearchInsertLocalState : duckdb::LocalSinkState {
  std::unique_ptr<irs::IndexWriter::Transaction> search_trx;
  std::unique_ptr<SearchSinkInsertBaseImpl> sink;
  bool bulk = false;
  bool no_op = false;
  std::optional<search::SearchDbWal::ChunkWriter> chunk_writer;
  duckdb::idx_t insert_count = 0;
  // RETURNING only: collected per sink thread so a parallel insert does not
  // serialise on one collection, and merged on Combine.
  std::optional<duckdb::ColumnDataCollection> returned;
};

SearchWriteTarget CtasWriteTarget(const catalog::CreateTableInfo& table,
                                  const SereneDBTableEntry& entry) {
  SearchWriteTarget target;
  target.table_id = table.GetId();
  target.data = entry.GetSearchData();
  const auto& columns = table.columns;
  target.column_ids.reserve(columns.LogicalColumnCount());
  target.chunk_types.reserve(columns.LogicalColumnCount());
  for (const auto& col : columns.Logical()) {
    target.column_ids.emplace_back(col.CatalogOid());
    target.chunk_types.push_back(col.Type());
  }
  target.pk_columns = duckdb_primary_key::BuildPKColumns(table);
  if (target.pk_columns.empty()) {
    target.generated_pk_seq = entry.GetGeneratedPkSequence();
    SDB_ASSERT(target.generated_pk_seq);
  }
  return target;
}

catalog::TableInfoRef CreateCtasTable(duckdb::ClientContext& context,
                                      SearchInsertGlobalState& state,
                                      duckdb::BoundCreateTableInfo& info,
                                      duckdb::SchemaCatalogEntry& schema) {
  auto& schema_entry = schema.Cast<SereneDBSchemaEntry>();
  auto database_id = schema_entry.GetDatabaseId();
  auto& create_info = info.Base();
  auto& table_info = create_info.Cast<duckdb::CreateTableInfo>();

  auto options = std::make_shared<catalog::CreateTableInfo>();
  options->SetTableName(table_info.GetTableName());
  options->SetSchema(schema.name);
  for (auto& col : table_info.columns.Logical()) {
    duckdb::ColumnDefinition column{col.Name(), col.Type()};
    column.SetCatalogOid(catalog::NextId().id());
    if (col.Generated()) {
      column.SetGeneratedExpression(col.GeneratedExpression().Copy(),
                                    duckdb::TableColumnType::GENERATED_STORED);
    } else if (col.HasDefaultValue()) {
      column.SetDefaultValue(col.DefaultValue().Copy());
    }
    options->columns.AddColumn(std::move(column));
  }
  // CTAS declares no PRIMARY KEY, so the create wires up a generated one.
  ApplyStorageKind(context, *options, table_info.options);
  SDB_ASSERT(options->GetEngine() == catalog::TableEngine::Search,
             "SereneDBSearchInsert CTAS mode used for non-Search engine");

  auto& catalog_impl = catalog::GetCatalog();
  const bool if_not_exists =
    create_info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  catalog::CreateTableOperationOptions op_options;
  op_options.if_not_exists = if_not_exists;
  // A valid pre-allocated id puts CreateTable in CTAS mode: no backing store
  // table (a Search table never has one).
  op_options.table_id = catalog::NextId();

  auto catalog_table = catalog_impl.CreateTable(
    catalog::NoAccessCheck(context), database_id,
    schema.name.GetIdentifierName(), std::move(options), {}, op_options);
  if (!catalog_table) {
    return nullptr;
  }

  auto database = FindDatabase(&context, database_id);
  SDB_ASSERT(database);

  state.ctas_mode = true;
  state.ctas_database_name = database.Name();
  state.ctas_schema_name = schema.name.GetIdentifierName();
  state.ctas_table_name = table_info.GetTableName().GetIdentifierName();
  return catalog_table;
}

void FinalizeCtasIfNeeded(SearchInsertGlobalState& state) {
  if (!state.ctas_mode) {
    return;
  }
  SDB_IF_FAILURE("crash_before_catalog_commit") { SDB_IMMEDIATE_ABORT(); }
  state.ctas_finalized = true;

  // The load is done; start the table's background maintenance. CTAS skips
  // SchemaEntry::CreateTable (which does this for a plain CREATE), so otherwise
  // a CTAS search table would get no background maintenance until the next
  // boot. The shard is the one bound at create -- every version shares it.
  state.search_table->StartTasks();
}

}  // namespace

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan, SearchWriteTarget target,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _target(std::move(target)),
    _return_chunk(return_chunk) {}

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

  SearchWriteTarget ctas_target;
  if (_ctas_info) {
    auto table = CreateCtasTable(context, *state, *_ctas_info, *_ctas_schema);
    if (!table) {
      return nullptr;
    }
    const auto* entry = FindTableEntryIn(
      &context, SchemaDatabaseId(&context, table->GetParentId()),
      table->GetId());
    if (entry == nullptr) {
      return nullptr;
    }
    ctas_target = CtasWriteTarget(*table, *entry);
  }
  const auto& target = _ctas_info ? ctas_target : _target;

  state->table_id = target.table_id;

  state->search_table = target.data;
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  state->generated_pk_seq = target.generated_pk_seq;
  state->column_ids = target.column_ids;
  state->chunk_types = target.chunk_types;
  state->pk_columns = target.pk_columns;

  state->sdb_txn = &conn_ctx;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }

  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchInsert::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto* gstate =
    sink_state ? &sink_state->Cast<SearchInsertGlobalState>() : nullptr;
  auto lstate = duckdb::make_uniq<SearchInsertLocalState>();

  if (gstate == nullptr || gstate->search_table == nullptr) {
    lstate->no_op = true;
    return lstate;
  }

  lstate->bulk = context.pipeline && context.pipeline->GetMaxThreads() > 1;
  if (_return_chunk) {
    lstate->returned.emplace(context.client, GetTypes());
  }

  if (lstate->bulk) {
    lstate->search_trx = std::make_unique<irs::IndexWriter::Transaction>(
      gstate->search_table->GetTransaction());
    lstate->sink = MakeSearchTableInsertSink(*lstate->search_trx);
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto* lstate = basics::downCast<SearchInsertLocalState>(&input.local_state);

  const auto num_rows = chunk.size();
  if (num_rows == 0 || lstate->no_op) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  if (!lstate->sink) {
    SDB_ASSERT(!lstate->bulk);
    auto& trx = gstate.sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
      gstate.search_table,
      [&] { return gstate.search_table->GetTransaction(); });
    lstate->sink = MakeSearchTableInsertSink(trx);
  }

  const bool uses_generated_pk = gstate.generated_pk_seq != nullptr;
  const uint64_t pk_base =
    uses_generated_pk ? gstate.generated_pk_seq->Reserve(num_rows) : 0;
  WriteChunkToSearchSink(*lstate->sink, chunk, gstate.column_ids,
                         gstate.pk_columns, uses_generated_pk, pk_base);
  if (lstate->returned) {
    // The chunk is the whole row in table-column order -- the defaults and the
    // STORED generated columns were resolved into the plan below this sink --
    // which is exactly what RETURNING projects over.
    lstate->returned->Append(chunk);
  }

  if (lstate->bulk) {
    if (!lstate->chunk_writer) {
      lstate->chunk_writer.emplace(gstate.search_table->NewChunkWriter());
    }
    lstate->chunk_writer->Append(chunk, pk_base);
  } else {
    gstate.sdb_txn->SearchTxn().AddInlineInsertChunk(
      gstate.search_table,
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
  auto* lstate = basics::downCast<SearchInsertLocalState>(&input.local_state);
  if (lstate->no_op) {
    return duckdb::SinkCombineResultType::FINISHED;
  }
  lstate->sink.reset();

  if (lstate->returned && lstate->returned->Count() != 0) {
    std::lock_guard<std::mutex> lock(gstate.combine_mu);
    gstate.returned->Combine(*lstate->returned);
  }

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
    gstate.bulk_chunks.push_back(std::move(pending));
    gstate.sdb_txn->SearchTxn().AddParallelSearchTransaction(
      gstate.search_table, std::move(lstate->search_trx));
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  if (gstate.insert_count == 0) {
    FinalizeCtasIfNeeded(gstate);
    return duckdb::SinkFinalizeType::READY;
  }

  if (!gstate.bulk_chunks.empty()) {
    gstate.sdb_txn->SearchTxn().AddReferences(gstate.search_table,
                                              std::move(gstate.bulk_chunks));
  }

  FinalizeCtasIfNeeded(gstate);
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertSourceState>();
  if (_return_chunk && sink_state != nullptr) {
    auto& gstate = sink_state->Cast<SearchInsertGlobalState>();
    if (gstate.returned) {
      gstate.returned->InitializeScan(state->scan);
    }
  }
  return state;
}

duckdb::SourceResultType SereneDBSearchInsert::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchInsertSourceState>();
  auto& gstate = sink_state->Cast<SearchInsertGlobalState>();
  if (gstate.returned) {
    gstate.returned->Scan(source.scan, chunk);
    return chunk.size() == 0 ? duckdb::SourceResultType::FINISHED
                             : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
