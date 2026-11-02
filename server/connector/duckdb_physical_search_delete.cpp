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

#include "connector/duckdb_physical_search_delete.h"

#include <absl/algorithm/container.h>

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/row_id_deduplicator.hpp>
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
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchTableDeleteState final : duckdb::GlobalSinkState {
  query::Transaction* sdb_txn = nullptr;
  std::optional<duckdb::RowIdDeduplicator> deleted_rows;
  duckdb::idx_t delete_count = 0;
  std::shared_ptr<search::SearchTable> search_table;
  // RETURNING only: the rows this statement removed.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchDeleteSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk,
  duckdb::vector<duckdb::idx_t> return_columns,
  const duckdb::vector<duckdb::column_t>& returning_virtual_columns,
  duckdb::RowIdHandling row_id_handling)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(&table),
    _return_chunk(return_chunk),
    _return_columns(std::move(return_columns)),
    _row_id_handling(row_id_handling) {
  SDB_ASSERT(table.GetRowIdColumns().size() == 1);
  const auto& rowid =
    expressions.front()->Cast<duckdb::BoundReferenceExpression>();
  _rowid = {rowid.Index(), rowid.GetReturnType()};
  for (auto& column : _return_columns) {
    if (column != duckdb::DConstants::INVALID_INDEX) {
      column =
        expressions[column]->Cast<duckdb::BoundReferenceExpression>().Index();
    }
  }
  SDB_ASSERT(!_return_chunk ||
             absl::c_find(_return_columns, duckdb::DConstants::INVALID_INDEX) ==
               _return_columns.end());
  for (const auto column_id : returning_virtual_columns) {
    if (column_id == kColumnIdentifierDocAddress) {
      _return_columns.emplace_back(_rowid.input_col_idx);
      continue;
    }
    SDB_ASSERT(column_id == kColumnIdentifierTableOid);
    _return_columns.emplace_back(duckdb::DConstants::INVALID_INDEX);
  }
  SDB_ASSERT(!_return_chunk || _return_columns.size() == GetTypes().size());
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto& conn_ctx = GetSereneDBContext(context);
  auto state = duckdb::make_uniq<SearchTableDeleteState>();
  state->search_table = _table->Storage();
  conn_ctx.SearchTxn().RegisterWriter(state->search_table, *_table);

  if (_row_id_handling != duckdb::RowIdHandling::ASSUME_UNIQUE) {
    state->deleted_rows.emplace(
      context, duckdb::vector<duckdb::LogicalType>{_rowid.type});
  }

  state->sdb_txn = &conn_ctx;
  if (_return_chunk) {
    state->returned.emplace(context, GetTypes());
  }
  return state;
}

duckdb::SinkResultType SereneDBSearchDelete::Sink(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchTableDeleteState>();
  if (gstate.deleted_rows &&
      primary_key::KeepFirstRowIds(*gstate.deleted_rows, chunk, _rowid) == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  const auto num_rows = chunk.size();

  gstate.sdb_txn->SearchTxn().AddSearchDeletes(
    gstate.search_table, chunk.data[_rowid.input_col_idx]);

  if (gstate.returned) {
    duckdb::DataChunk row;
    row.InitializeEmpty(GetTypes());
    for (duckdb::idx_t i = 0; i < row.ColumnCount(); ++i) {
      if (const auto from = _return_columns[i];
          from != duckdb::DConstants::INVALID_INDEX) {
        row.data[i].Reference(chunk.data[from]);
      } else {
        row.data[i].Reference(
          duckdb::Value::BIGINT(static_cast<int64_t>(_table->oid)),
          duckdb::count_t(num_rows));
      }
    }
    row.SetCardinality(num_rows);
    gstate.returned->Append(row);
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchDelete::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchDeleteSourceState>();
  auto& gstate = sink_state->Cast<SearchTableDeleteState>();
  if (gstate.returned) {
    gstate.returned->InitializeScan(state->scan);
  }
  return state;
}

duckdb::SourceResultType SereneDBSearchDelete::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchDeleteSourceState>();
  auto& gstate = sink_state->Cast<SearchTableDeleteState>();
  if (gstate.returned) {
    gstate.returned->Scan(source.scan, chunk);
    return chunk.size() == 0 ? duckdb::SourceResultType::FINISHED
                             : duckdb::SourceResultType::HAVE_MORE_OUTPUT;
  }

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count));
  return duckdb::SourceResultType::FINISHED;
}

}  // namespace sdb::connector
