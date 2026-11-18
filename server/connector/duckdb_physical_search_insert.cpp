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

#include <absl/synchronization/mutex.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/allocator.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/storage/data_table.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "catalog/transaction.h"
#include "connector/column_id.h"
#include "connector/search_sink_rows.h"
#include "connector/search_sink_writer.hpp"
#include "search/search_table_changes.h"
#include "search/store.h"
#include "server/utils/app_server.h"

namespace sdb::connector {
namespace {

struct SearchInsertGlobalState final : duckdb::GlobalSinkState {
  SearchSinkTable table;
  catalog::SessionTransaction* sdb_txn = nullptr;

  absl::Mutex mutex;
  duckdb::idx_t insert_count = 0;
  // RETURNING only: the inserted rows, merged out of the sink threads.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchInsertSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

struct SearchInsertLocalState final : duckdb::LocalSinkState {
  SearchSinkRows rows;
  duckdb::idx_t insert_count = 0;
  // RETURNING only: collected per sink thread so a parallel insert does not
  // serialise on one collection, and merged on Combine.
  std::optional<duckdb::ColumnDataCollection> returned;
};

}  // namespace

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> constraints,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(&table),
    _constraints(std::move(constraints)),
    _return_chunk(return_chunk) {}

SereneDBSearchInsert::SereneDBSearchInsert(
  duckdb::PhysicalPlan& plan,
  duckdb::unique_ptr<duckdb::BoundCreateTableInfo> info,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _ctas_info(std::move(info)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchInsert::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertGlobalState>();

  auto table = _table;
  if (_ctas_info) {
    auto& catalog = _ctas_info->schema.ParentCatalog();
    auto entry = catalog.CreateTable(catalog.GetCatalogTransaction(context),
                                     _ctas_info->schema, *_ctas_info);
    table = &entry->Cast<catalog::SearchTableEntry>();
  }

  auto& target = state->table;
  target.store = table->Storage();
  target.catalog = &table->catalog;
  target.table_id = table->oid;
  auto& transaction = catalog::SessionOf(context, table->catalog);
  // Before any sink reads the shard's index config, so a rebuild can tell
  // that this transaction predates a config it publishes.
  transaction.SearchTxn().RegisterWriter(target.store, *table);
  table->RequireCurrentSchema();

  const auto& columns = table->GetColumns();
  target.column_ids.reserve(columns.PhysicalColumnCount());
  target.types.reserve(columns.PhysicalColumnCount());
  for (const auto& column : columns.Physical()) {
    target.column_ids.emplace_back(TableColumnId(column));
    target.types.push_back(column.Type());
  }
  target.buffer_bytes = target.store->InlineRowsMax();

  state->sdb_txn = &transaction;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }

  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchInsert::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto lstate = duckdb::make_uniq<SearchInsertLocalState>();
  if (_return_chunk) {
    lstate->returned.emplace(context.client, GetTypes());
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto& lstate = input.local_state.Cast<SearchInsertLocalState>();

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  if (const auto& progress = duckdb::ClientConfig::GetConfig(context.client)
                               .sink_progress_callback) {
    progress(num_rows, chunk.GetAllocationSize());
  }
  if (_table) {
    duckdb::DataTable::VerifyRowConstraints(context.client, *_table,
                                            _constraints, chunk);
  }
  lstate.rows.Append(context.client, gstate.table, chunk);

  if (lstate.returned) {
    // The chunk is the whole row in table-column order -- the defaults and the
    // STORED generated columns were resolved into the plan below this sink --
    // which is exactly what RETURNING projects over.
    lstate.returned->Append(chunk);
  }
  lstate.insert_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchInsert::Combine(
  duckdb::ExecutionContext& context,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  auto& lstate = input.local_state.Cast<SearchInsertLocalState>();
  lstate.rows.Flush();
  absl::MutexLock lock{&gstate.mutex};
  lstate.rows.HandOver(context.client, gstate.table,
                       gstate.sdb_txn->SearchTxn());
  gstate.insert_count += lstate.insert_count;
  if (lstate.returned && lstate.returned->Count() != 0) {
    gstate.returned->Combine(*lstate.returned);
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  if (_ctas_info) {
    SDB_IF_FAILURE("crash_before_commit") { SDB_IMMEDIATE_ABORT(); }
  }
  auto& gstate = input.global_state.Cast<SearchInsertGlobalState>();
  gstate.sdb_txn->SearchTxn().CheckKeys(context, *gstate.table.store,
                                        _merge_insert);
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchInsertSourceState>();
  if (_return_chunk) {
    sink_state->Cast<SearchInsertGlobalState>().returned->InitializeScan(
      state->scan);
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

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
