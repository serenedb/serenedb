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

#include <duckdb/common/types/data_chunk.hpp>
#include <memory>
#include <shared_mutex>
#include <string>
#include <vector>

#include "basics/primary_key.hpp"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

struct SearchDeleteGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<search::SearchTable> search_table;
  query::Transaction* sdb_txn = nullptr;
  std::shared_lock<std::shared_mutex> table_lock;
  duckdb::idx_t delete_count = 0;
};

struct SearchDeleteSourceState : duckdb::GlobalSourceState {
  bool finished = false;
};

}  // namespace

SereneDBSearchDelete::SereneDBSearchDelete(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::vector<duckdb::idx_t> pk_col_indices,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)),
    _pk_col_indices(std::move(pk_col_indices)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBSearchDelete::GetGlobalSinkState(duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SearchDeleteGlobalState>();
  auto& conn_ctx = GetSereneDBContext(context);

  state->table_id = _table->GetId();

  state->search_table = _table->GetData();
  state->table_lock = std::shared_lock{state->search_table->GetTableLock()};

  SDB_ASSERT(_pk_col_indices.size() == 1);
  state->sdb_txn = &conn_ctx;
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

  // The removal key is the row's synthetic generated PK, read from the single
  // rowid slot the scan materialised and encoded with AppendSigned exactly as
  // the insert wrote it.
  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(num_rows);

  duckdb::UnifiedVectorFormat gen_pk;
  chunk.data[_pk_col_indices[0]].ToUnifiedFormat(num_rows, gen_pk);
  const auto* gen_pk_data =
    duckdb::UnifiedVectorFormat::GetData<int64_t>(gen_pk);

  std::vector<std::string> wal_pks;
  wal_pks.reserve(num_rows);
  std::string pk;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk.clear();
    primary_key::AppendSigned(pk, gen_pk_data[gen_pk.sel->get_index(row)]);
    remover.DeleteRowImpl(pk);  // live iresearch removal
    wal_pks.emplace_back(pk);   // WAL delete payload
  }
  remover.FinishImpl();  // hands the removal filter to the trx
  gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.search_table, wal_pks);

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
  return duckdb::make_uniq<SearchDeleteSourceState>();
}

duckdb::SourceResultType SereneDBSearchDelete::GetDataInternal(
  duckdb::ExecutionContext& /*context*/, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SearchDeleteSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SearchDeleteGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
