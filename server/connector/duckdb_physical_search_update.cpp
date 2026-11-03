////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "connector/duckdb_physical_search_update.h"

#include <absl/synchronization/mutex.h>

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/row_id_deduplicator.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <iresearch/utils/assert.hpp>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/primary_key.h"
#include "connector/search_sink_rows.h"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table_changes.h"
#include "search/store.h"

namespace sdb::connector {
namespace {

struct SearchUpdateGlobalState final : duckdb::GlobalSinkState {
  SearchSinkTable table;
  query::Transaction* sdb_txn = nullptr;

  duckdb::vector<duckdb::column_t> new_row_src;

  primary_key::PKColumn rowid;
  std::optional<duckdb::RowIdDeduplicator> updated_rows;

  absl::Mutex mutex;
  duckdb::idx_t update_count = 0;
  // RETURNING only: the rows as this statement left them.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchUpdateLocalState final : duckdb::LocalSinkState {
  explicit SearchUpdateLocalState(uint64_t budget) : rows{budget} {}

  SearchSinkRows rows;
  irs::DocRemovalBuilder positions;
  duckdb::idx_t update_count = 0;
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchUpdateSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchUpdate::SereneDBSearchUpdate(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::PhysicalIndex> columns,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk,
  duckdb::RowIdHandling row_id_handling)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(table),
    _columns(std::move(columns)),
    _expressions(std::move(expressions)),
    _return_chunk(return_chunk),
    _row_id_handling(row_id_handling) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchUpdate::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchUpdateGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  auto& target = state->table;
  target.store = _table.Storage();
  target.catalog = &_table.catalog;
  target.table_id = _table.oid;
  conn_ctx.SearchTxn().RegisterWriter(target.store, _table);

  const auto& columns = _table.GetColumns();
  target.column_ids.reserve(columns.LogicalColumnCount());
  for (const auto& column : columns.Logical()) {
    target.column_ids.emplace_back(TableColumnId(column));
  }
  target.types = columns.GetColumnTypes();
  target.buffer_bytes =
    duckdb::DBConfig::GetConfig(context).options.checkpoint_wal_size;

  const auto p = target.column_ids.size();
  state->new_row_src.assign(p, duckdb::DConstants::INVALID_INDEX);
  SDB_ASSERT(_columns.size() == p,
             "search UPDATE must project every table column");
  for (size_t i = 0; i < _columns.size(); ++i) {
    const auto index = _columns[i].index;
    SDB_ASSERT(index < p,
               "projected update column is not a stored table column");
    SDB_ASSERT(
      _expressions[i]->GetExpressionType() == duckdb::ExpressionType::BOUND_REF,
      "search UPDATE expects every SET value to be projected");
    state->new_row_src[index] =
      _expressions[i]->Cast<duckdb::BoundReferenceExpression>().Index();
  }

  const auto& input_types = children[0].get().GetTypes();
  SDB_ASSERT(_table.GetRowIdColumns().size() == 1 && !input_types.empty());
  state->rowid = {input_types.size() - 1, input_types.back()};
  if (_row_id_handling != duckdb::RowIdHandling::ASSUME_UNIQUE) {
    state->updated_rows.emplace(
      context, duckdb::vector<duckdb::LogicalType>{state->rowid.type});
  }

  state->sdb_txn = &conn_ctx;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }
  return state;
}

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchUpdate::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  const auto threads =
    context.pipeline ? context.pipeline->GetMaxThreads() : duckdb::idx_t{1};
  auto lstate = duckdb::make_uniq<SearchUpdateLocalState>(
    gstate.table.buffer_bytes / std::max<duckdb::idx_t>(1, threads));
  if (_return_chunk) {
    lstate->returned.emplace(context.client, GetTypes());
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchUpdate::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchUpdateGlobalState>();
  auto& lstate = input.local_state.Cast<SearchUpdateLocalState>();
  if (gstate.updated_rows &&
      primary_key::KeepFirstRowIds(*gstate.updated_rows, chunk, gstate.rowid) ==
        0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  const auto num_rows = chunk.size();

  search::AddRowPositions(lstate.positions,
                          chunk.data[gstate.rowid.input_col_idx]);

  duckdb::DataChunk new_row;
  new_row.InitializeEmpty(gstate.table.types);
  new_row.ReferenceColumns(chunk, gstate.new_row_src);
  lstate.rows.Append(context.client, gstate.table, new_row);

  if (lstate.returned) {
    lstate.returned->Append(new_row);
  }
  lstate.update_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchUpdate::Combine(
  duckdb::ExecutionContext& context,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchUpdateGlobalState>();
  auto& lstate = input.local_state.Cast<SearchUpdateLocalState>();
  lstate.rows.Flush();
  absl::MutexLock lock{&gstate.mutex};
  auto& search_txn = gstate.sdb_txn->SearchTxn();
  lstate.rows.HandOver(context.client, gstate.table, search_txn);
  if (!lstate.positions.Empty()) {
    search_txn.AddRemoval(gstate.table.store, std::move(lstate.positions));
  }
  gstate.update_count += lstate.update_count;
  if (lstate.returned && lstate.returned->Count() != 0) {
    gstate.returned->Combine(*lstate.returned);
  }
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchUpdate::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchUpdateSourceState>();
  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  if (gstate.returned) {
    gstate.returned->InitializeScan(state->scan);
  }
  return state;
}

duckdb::SourceResultType SereneDBSearchUpdate::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchUpdateSourceState>();
  auto& gstate = sink_state->Cast<SearchUpdateGlobalState>();
  if (gstate.returned) {
    gstate.returned->Scan(source.scan, chunk);
    return chunk.size() == 0 ? duckdb::SourceResultType::FINISHED
                             : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count));
  return duckdb::SourceResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchUpdate::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& state = input.global_state.Cast<SearchUpdateGlobalState>();
  state.sdb_txn->SearchTxn().FlushBufferTransaction(_table.oid);
  return duckdb::SinkFinalizeType::READY;
}

}  // namespace sdb::connector
