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

#include "connector/duckdb_scan_base.hpp"

#include <duckdb.hpp>

#include "basics/assert.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/primary_key.hpp"
#include "pg/connection_context.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

CommonScanGlobalState::~CommonScanGlobalState() {
  // Only release if we took the snapshot via db->GetSnapshot().
  // When using a transaction, the snapshot is owned by the transaction.
  if (snapshot && !txn) {
    GetServerEngine().db()->ReleaseSnapshot(snapshot);
  }
  snapshot = nullptr;
}

void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input) {
  auto& engine = GetServerEngine();
  auto* db = engine.db();

  // When inside BEGIN/COMMIT, use the connection's transaction so the scan
  // sees the transaction's own uncommitted writes (read-your-writes).
  // Outside a transaction, use a DB snapshot for read-only scans.
  // If sdb_read_your_own_writes is false, always use a DB snapshot so reads
  // see only committed data even within an explicit transaction.
  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBRead();
  if (conn_ctx.GetReadYourOwnWrites() &&
      (!context.transaction.IsAutoCommit() || conn_ctx.HasRocksDBWrite())) {
    state.txn = &conn_ctx.EnsureRocksDBTransaction();
    state.snapshot = state.txn->GetSnapshot();
  } else {
    state.snapshot = db->GetSnapshot();
  }

  // Determine which columns DuckDB actually wants (projection pushdown).
  const auto num_bind_columns = bind_data.column_ids.size();
  for (auto col_id : input.column_ids) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
      state.scan_rowid = true;
      state.rowid_output_idx = state.projected_columns.size();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == kColumnIdentifierTableOid) {
      state.scan_tableoid = true;
      state.tableoid_output_idx = state.projected_columns.size();
      state.tableoid_value =
        static_cast<int64_t>(bind_data.table->GetId().id());
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      auto real_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(real_idx != duckdb::DConstants::INVALID_INDEX);
      SDB_ASSERT(real_idx < bind_data.column_ids.size());
      state.projected_columns.push_back(real_idx);
      state.projected_types.push_back(bind_data.column_types[real_idx]);
    } else if (col_id < num_bind_columns) {
      const auto catalog_col_id = bind_data.column_ids[col_id];
      if (catalog_col_id == catalog::Column::kInvertedIndexScoreId) {
        state.scan_score = true;
        state.score_output_idx = state.projected_columns.size();
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else {
        state.projected_columns.push_back(col_id);
        state.projected_types.push_back(bind_data.column_types[col_id]);
      }
    }
  }

  state.has_generated_pk = bind_data.table->PKColumns().empty();
}

constexpr size_t kScanKeyPrefixSize =
  sizeof(ObjectId) + sizeof(catalog::Column::Id);

duckdb::idx_t ReadGeneratedPKFromKeys(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows) {
  duckdb::idx_t count = 0;
  auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(output);
  while (it.Valid() && count < max_rows) {
    auto key = it.key().ToStringView();
    SDB_ASSERT(key.size() >= kScanKeyPrefixSize + sizeof(int64_t));
    data[count] = primary_key::ReadSigned<int64_t>(
      std::string_view{key.begin() + kScanKeyPrefixSize, key.end()});
    ++count;
    it.Next();
  }
  rocksutils::CheckIteratorStatus(it);
  return count;
}

duckdb::idx_t CommonScanRowsScanned(duckdb::GlobalTableFunctionState& gstate_p,
                                    duckdb::LocalTableFunctionState&) {
  auto& gstate = gstate_p.Cast<CommonScanGlobalState>();
  return gstate.produced_rows.load(std::memory_order_relaxed);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* /*global_state*/) {
  return duckdb::make_uniq<CommonScanLocalState>();
}

}  // namespace sdb::connector
