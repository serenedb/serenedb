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
#include <string_view>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_primary_key.h"
#include "connector/key_utils.hpp"
#include "connector/search_sink_writer.hpp"
#include "pg/connection_context.h"
#include "query/transaction.h"
#include "search/search_table_shard.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {
namespace {

struct SearchDeleteGlobalState : duckdb::GlobalSinkState {
  ObjectId table_id;
  std::shared_ptr<TableShard> table_shard;
  search::SearchTableShard* search_shard = nullptr;
  query::Transaction* sdb_txn = nullptr;
  std::string table_key;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
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
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  state->table_shard = snapshot->GetTableShard(state->table_id);
  SDB_ASSERT(state->table_shard);
  SDB_ASSERT(state->table_shard->GetStorage() == catalog::StorageKind::kSearch,
             "SereneDBSearchDelete dispatched against a non-search shard");
  state->table_lock = std::shared_lock{state->table_shard->GetTableLock()};

  // Map each PK chunk position to its type (PlanDelete's layout): explicit-PK
  // columns by their declared type; the no-PK generated rowid as BIGINT.
  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();
  for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
    duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
    if (i < pk_col_ids.size()) {
      for (const auto& col : columns) {
        if (col.GetId() == pk_col_ids[i]) {
          pk_type = col.type;
          break;
        }
      }
    }
    state->pk_columns.push_back(duckdb_primary_key::PKColumn{
      .input_col_idx = _pk_col_indices[i],
      .type = pk_type,
    });
  }

  state->sdb_txn = &conn_ctx;
  state->search_shard =
    &basics::downCast<search::SearchTableShard>(*state->table_shard);
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
    gstate.table_shard,
    [&] { return gstate.search_shard->GetTransaction(); });

  // The PK term to remove is encoded exactly as the insert wrote it
  // (MakeColumnKey -> ExtractRowKey). For a no-PK table the rowid column holds
  // the generated PK (materialised by the scan), so the same encoding matches.
  SearchSinkDeleteBaseImpl remover{trx};
  remover.InitImpl(num_rows);

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  std::vector<std::string> wal_pks;
  wal_pks.reserve(num_rows);
  std::string key_buffer;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    // The callback receives the row-lock key [ObjectId][PK]; stripping the
    // ObjectId prefix yields the bare PK bytes -- the exact term the insert
    // stored (insert stores ExtractRowKey([ObjectId][ColumnId][PK]) == [PK]).
    duckdb_primary_key::MakeColumnKey(
      pk_formats, gstate.pk_columns, row, gstate.table_key,
      [&](std::string_view row_lock_key) {
        const auto pk = row_lock_key.substr(sizeof(ObjectId));
        remover.DeleteRowImpl(pk);             // live iresearch removal
        wal_pks.emplace_back(pk);              // WAL delete payload
      },
      key_buffer);
  }
  remover.FinishImpl();  // hands the removal filter to the trx
  gstate.sdb_txn->SearchTxn().AddSearchDeletes(gstate.table_shard, wal_pks);

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBSearchDelete::Finalize(
  duckdb::Pipeline& /*pipeline*/, duckdb::Event& /*event*/,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& /*input*/) const {
  auto& gstate = sink_state->Cast<SearchDeleteGlobalState>();
  if (gstate.delete_count > 0) {
    GetSereneDBContext(context).UpdateNumRows(
      gstate.table_id, -static_cast<int64_t>(gstate.delete_count));
  }
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
