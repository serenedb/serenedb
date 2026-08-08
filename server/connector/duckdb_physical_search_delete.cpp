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
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <vector>

#include "catalog/duckdb_primary_key.h"
#include "catalog/identifiers/object_id.h"
#include "connector/duckdb_client_state.h"
#include "connector/search_sink_writer.hpp"
#include "connector/search_table_dispatch.h"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchDeleteGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;
  std::vector<catalog::duckdb_primary_key::PKColumn> pk_columns;
  std::shared_lock<std::shared_mutex> table_lock;
  duckdb::idx_t delete_count = 0;
  // RETURNING only: the rows this statement removed.
  std::optional<duckdb::ColumnDataCollection> returned;
};

struct SearchDeleteSourceState : duckdb::GlobalSourceState {
  bool finished = false;
  duckdb::ColumnDataScanState scan;
};

}  // namespace

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan, SearchWriteTarget target,
  std::vector<duckdb::idx_t> pk_col_indices,
  duckdb::vector<duckdb::LogicalType> types,
  std::vector<duckdb::idx_t> column_map, duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _target(std::move(target)),
    _pk_col_indices(std::move(pk_col_indices)),
    _column_map(std::move(column_map)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchDeleteGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  state->table_id = _target.table_id;

  state->search_table = _target.data;
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  state->pk_columns = RowIdentityPKColumns(_target, _pk_col_indices);

  state->sdb_txn = &conn_ctx;
  if (!_column_map.empty()) {
    state->returned.emplace(context, GetTypes());
  }
  return state;
}

duckdb::SinkResultType SereneDBSearchDelete::Sink(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SearchDeleteGlobalState>();
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }
  auto& trx = gstate.sdb_txn->SearchTxn().EnsureSerialSearchTransaction(
    gstate.search_table, [&] { return gstate.search_table->GetTransaction(); });

  // The removal term is the bare PK, encoded exactly as the insert wrote it
  // (Create -> AppendPKValue). For a no-PK table the rowid column holds the
  // generated PK (materialised by the scan), so the same encoding matches.
  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(num_rows);

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  catalog::duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns,
                                                pk_formats);

  std::vector<std::string> wal_pks;
  wal_pks.reserve(num_rows);
  std::string pk;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk.clear();
    catalog::duckdb_primary_key::Create(pk_formats, gstate.pk_columns, row, pk);
    remover.DeleteRowImpl(pk);  // live iresearch removal
    wal_pks.emplace_back(pk);   // WAL delete payload
  }
  remover.FinishImpl();  // hands the removal filter to the trx
  gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, wal_pks);

  if (gstate.returned) {
    // The scan already projected every column the RETURNING list can name, so
    // the rows come straight off the input chunk rather than being re-fetched.
    duckdb::DataChunk row;
    row.InitializeEmpty(GetTypes());
    BuildReturnedRow(row, chunk, _column_map);
    gstate.returned->Append(row);
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBSearchDelete::Finalize(
  duckdb::Pipeline& /*pipeline*/, duckdb::Event& /*event*/,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& /*input*/) const {
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBSearchDelete::GetGlobalSourceState(
  duckdb::ClientContext& /*context*/) const {
  auto state = duckdb::make_uniq<SearchDeleteSourceState>();
  if (sink_state != nullptr) {
    auto& gstate = sink_state->Cast<SearchDeleteGlobalState>();
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
  auto& gstate = sink_state->Cast<SearchDeleteGlobalState>();
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
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
