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

#include <absl/synchronization/mutex.h>

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/row_id_deduplicator.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "catalog/entry/search_table.h"
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
  std::vector<primary_key::PKColumn> pk_columns;
  std::shared_ptr<search::SearchTable> search_table;
  std::shared_lock<std::shared_mutex> table_lock;

  // Guards the deduplicator, the transaction's write buffer and everything
  // below, which the sink threads share.
  absl::Mutex mu;
  std::optional<duckdb::RowIdDeduplicator> deleted_rows;
  duckdb::idx_t delete_count = 0;
  // RETURNING only: the rows this statement removed, merged out of the sink
  // threads.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchDeleteSourceState final : duckdb::GlobalSourceState {
  duckdb::ColumnDataScanState scan;
};

struct SearchDeleteLocalState final : duckdb::LocalSinkState {
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
  duckdb::RowIdHandling row_id_handling)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(&table),
    _return_chunk(return_chunk),
    _return_columns(std::move(return_columns)),
    _row_id_handling(row_id_handling) {
  const auto row_ids = table.GetRowIdColumns().size();
  _pk_columns.reserve(row_ids);
  for (duckdb::idx_t i = 0; i < row_ids; ++i) {
    const auto& ref = expressions[i]->Cast<duckdb::BoundReferenceExpression>();
    _pk_columns.emplace_back(ref.Index(), ref.GetReturnType());
  }
  for (auto& column : _return_columns) {
    if (column != duckdb::DConstants::INVALID_INDEX) {
      column =
        expressions[column]->Cast<duckdb::BoundReferenceExpression>().Index();
    }
  }
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto& conn_ctx = GetSereneDBContext(context);
  auto state = duckdb::make_uniq<SearchTableDeleteState>();
  state->search_table = _table->Storage();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};
  conn_ctx.SearchTxn().RegisterWriter(state->search_table, *_table);

  state->pk_columns = _pk_columns;
  if (_row_id_handling != duckdb::RowIdHandling::ASSUME_UNIQUE) {
    state->deleted_rows.emplace(context, duckdb::vector<duckdb::LogicalType>{
                                           state->pk_columns.front().type});
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
    if (primary_key::KeepFirstRowIds(*gstate.deleted_rows, chunk,
                                     gstate.pk_columns.front()) == 0) {
      return duckdb::SinkResultType::NEED_MORE_INPUT;
    }
  }
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  // Buffered, not removed here. The removal reaches iresearch when the write
  // buffer is replayed, after exactly the rows that precede it, which is what
  // reproduces the ordering these statements would have had.
  duckdb::UnifiedVectorFormat rowid;
  chunk.data[gstate.pk_columns[0].input_col_idx].ToUnifiedFormat(num_rows,
                                                                 rowid);
  const auto* rowid_data = duckdb::UnifiedVectorFormat::GetData<int64_t>(rowid);
  std::vector<int64_t> rows;
  rows.reserve(num_rows);
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    rows.push_back(rowid_data[rowid.sel->get_index(row)]);
  }
  {
    absl::MutexLock lock{&gstate.mu};
    gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, rows);
  }

  if (lstate->returned) {
    duckdb::DataChunk row;
    row.InitializeEmpty(GetTypes());
    for (duckdb::idx_t i = 0; i < row.ColumnCount(); ++i) {
      const auto from = i < _return_columns.size()
                          ? _return_columns[i]
                          : duckdb::DConstants::INVALID_INDEX;
      if (from == duckdb::DConstants::INVALID_INDEX) {
        row.data[i].Reference(duckdb::Value(row.data[i].GetType()),
                              duckdb::count_t(num_rows));
      } else {
        row.data[i].Reference(chunk.data[from]);
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
  return duckdb::SinkCombineResultType::FINISHED;
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

duckdb::SinkFinalizeType SereneDBSearchDelete::Finalize(
  duckdb::Pipeline&, duckdb::Event&, duckdb::ClientContext&,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& state = input.global_state.Cast<SearchTableDeleteState>();
  if (state.table_lock.owns_lock()) {
    state.table_lock.unlock();
  }
  return duckdb::SinkFinalizeType::READY;
}

}  // namespace sdb::connector
