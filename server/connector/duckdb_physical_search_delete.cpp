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
#include <absl/synchronization/mutex.h>

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/row_id_deduplicator.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <iresearch/index/doc_removal.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
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
#include "search/search_table_changes.h"

namespace sdb::connector {
namespace {

struct SearchTableDeleteState final : duckdb::GlobalSinkState {
  query::Transaction* sdb_txn = nullptr;
  std::shared_ptr<search::SearchTable> search_table;

  // Guards the deduplicator and everything below, which the sink threads
  // share. The transaction's search state is not under it: only Finalize
  // touches that.
  absl::Mutex mu;
  std::optional<duckdb::RowIdDeduplicator> deleted_rows;
  duckdb::idx_t delete_count = 0;
  // RETURNING only: the rows this statement removed, merged out of the sink
  // threads.
  std::optional<duckdb::ColumnDataCollection> returned;
  // The rows to remove, merged out of the sink threads for Finalize to buffer
  // on the transaction.
  irs::DocRemovalBuilder removed_rows;
};

struct SearchDeleteSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

struct SearchDeleteLocalState final : duckdb::LocalSinkState {
  irs::DocRemovalBuilder removed_rows;
  duckdb::idx_t delete_count = 0;
  std::optional<duckdb::ColumnDataCollection> returned;
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

duckdb::unique_ptr<duckdb::LocalSinkState>
SereneDBSearchDelete::GetLocalSinkState(
  duckdb::ExecutionContext& context) const {
  auto lstate = duckdb::make_uniq<SearchDeleteLocalState>();
  if (_return_chunk) {
    lstate->returned.emplace(context.client, GetTypes());
  }
  return lstate;
}

duckdb::SinkResultType SereneDBSearchDelete::Sink(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchTableDeleteState>();
  auto* lstate =
    irs::utils::downCast<SearchDeleteLocalState>(&input.local_state);
  if (gstate.deleted_rows) {
    absl::MutexLock lock{&gstate.mu};
    if (primary_key::KeepFirstRowIds(*gstate.deleted_rows, chunk, _rowid) ==
        0) {
      return duckdb::SinkResultType::NEED_MORE_INPUT;
    }
  }
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  search::AppendRowPositions(lstate->removed_rows,
                             chunk.data[_rowid.input_col_idx]);

  if (lstate->returned) {
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
    lstate->returned->Append(row);
  }

  lstate->delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkCombineResultType SereneDBSearchDelete::Combine(
  duckdb::ExecutionContext& /*context*/,
  duckdb::OperatorSinkCombineInput& input) const {
  auto& gstate = input.global_state.Cast<SearchTableDeleteState>();
  auto* lstate =
    irs::utils::downCast<SearchDeleteLocalState>(&input.local_state);
  absl::MutexLock lock{&gstate.mu};
  if (lstate->returned && lstate->returned->Count() != 0) {
    gstate.returned->Combine(*lstate->returned);
  }
  gstate.delete_count += lstate->delete_count;
  gstate.removed_rows.Merge(std::move(lstate->removed_rows));
  return duckdb::SinkCombineResultType::FINISHED;
}

duckdb::SinkFinalizeType SereneDBSearchDelete::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& state = input.global_state.Cast<SearchTableDeleteState>();
  state.sdb_txn->SearchTxn().AddSearchDeletes(state.search_table,
                                              std::move(state.removed_rows));
  return duckdb::SinkFinalizeType::READY;
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
