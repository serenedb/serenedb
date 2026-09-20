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

#include <duckdb/common/types/column/column_data_collection.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
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
  duckdb::idx_t delete_count = 0;
  std::shared_ptr<search::SearchTable> search_table;
  std::shared_lock<std::shared_mutex> table_lock;
  // RETURNING only: the rows this statement removed.
  std::optional<duckdb::ColumnDataCollection> returned;

  irs::IndexWriter::Transaction& Trx() {
    return sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
      search_table, [&] { return search_table->GetTransaction(); });
  }
};

struct SearchDeleteSourceState : duckdb::GlobalSourceState {
  bool finished = false;
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan, const catalog::SearchTableEntry& table,
  duckdb::vector<duckdb::unique_ptr<duckdb::Expression>> expressions,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, bool return_chunk,
  duckdb::vector<duckdb::idx_t> return_columns)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(&table),
    _return_chunk(return_chunk),
    _return_columns(std::move(return_columns)) {
  _pk_columns.reserve(expressions.size());
  for (const auto& expr : expressions) {
    const auto& ref = expr->Cast<duckdb::BoundReferenceExpression>();
    _pk_columns.push_back(
      {.input_col_idx = ref.Index(), .type = ref.GetReturnType()});
  }
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto& conn_ctx = GetSereneDBContext(context);
  auto state = duckdb::make_uniq<SearchTableDeleteState>();
  state->search_table = _table->Storage();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};
  conn_ctx.SearchTxn().RegisterWriter(state->search_table);

  state->pk_columns = _pk_columns;

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
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  // A search table's removal key is the row's synthetic rowid, read from the
  // single slot the scan materialised and encoded exactly as the insert wrote
  // it. A view-backed reindex delete instead keys on its (file_index, row)
  // pair, which `pk_columns` describes.
  SearchSinkDeleteBaseImpl remover{gstate.Trx()};
  remover.InitImpl(num_rows);

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  std::vector<std::string> wal_pks;
  wal_pks.reserve(num_rows);
  std::string pk;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk.clear();
    primary_key::Create(pk_formats, gstate.pk_columns, row, pk);
    remover.DeleteRowImpl(pk);  // live iresearch removal
    wal_pks.emplace_back(pk);   // WAL delete payload
  }
  remover.FinishImpl();  // hands the removal filter to the trx

  gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, wal_pks);
  if (gstate.returned) {
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
    gstate.returned->Append(row);
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchDelete::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchDeleteSourceState>();
  if (sink_state) {
    auto& gstate = sink_state->Cast<SearchTableDeleteState>();
    if (gstate.returned) {
      gstate.returned->InitializeScan(state->scan);
    }
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
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  chunk.SetCardinality(1);
  chunk.SetValue(0, 0,
                 duckdb::Value::BIGINT(
                   sink_state->Cast<SearchTableDeleteState>().delete_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
