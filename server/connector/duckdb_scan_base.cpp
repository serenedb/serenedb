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
#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "catalog/table_options.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "pg/connection_context.h"
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
  const bool is_search_scan = bind_data.scan_source->IsSearchLike();
  if (is_search_scan && conn_ctx.GetReadYourOwnWrites() &&
      conn_ctx.HasRocksDBTransaction()) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "querying an index within a transaction is not supported when "
              "sdb_read_your_own_writes is enabled");
  }
  if (conn_ctx.GetReadYourOwnWrites() && conn_ctx.HasRocksDBTransaction()) {
    state.txn = &conn_ctx.GetRocksDBTransaction();
    state.snapshot = &conn_ctx.GetRocksDBSnapshot();
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
      // For view-backed indexes, no underlying SereneDB-table id; surface
      // the view's id so BM25/TFIDF expressions still bind.
      state.tableoid_value = static_cast<int64_t>(bind_data.RelationId().id());
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY ||
               col_id == duckdb::COLUMN_IDENTIFIER_ROW_NUMBER) {
      // DuckDB asks for these placeholder columns when no real data is
      // needed (e.g. COUNT(*)) -- emit a dummy slot the scan ignores.
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      // VirtualToPKColumnIndex returns the index in table->Columns(), which
      // includes the generated PK. column_ids skips kGeneratedPKId, so map
      // the catalog index -> column_ids index by catalog::Column::Id.
      // Only Table-backed binds set virtual PK columns; views surface
      // tableoid + rowid only.
      SDB_ASSERT(!bind_data.IsViewBacked(),
                 "virtual PK columns are not used for view-backed scans");
      auto cat_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(cat_idx != duckdb::DConstants::INVALID_INDEX);
      const auto& tbd = bind_data.As<TableScanBindData>();
      const auto& catalog_cols = tbd.table->Columns();
      SDB_ASSERT(cat_idx < catalog_cols.size());
      const auto catalog_col_id = catalog_cols[cat_idx].id;
      duckdb::idx_t bind_idx = duckdb::DConstants::INVALID_INDEX;
      for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
        if (bind_data.column_ids[i] == catalog_col_id) {
          bind_idx = i;
          break;
        }
      }
      SDB_ASSERT(bind_idx != duckdb::DConstants::INVALID_INDEX);
      state.projected_columns.push_back(bind_idx);
      state.projected_types.push_back(bind_data.column_types[bind_idx]);
    } else if (col_id < num_bind_columns) {
      const auto catalog_col_id = bind_data.column_ids[col_id];
      if (catalog_col_id == catalog::Column::kInvertedIndexScoreId) {
        state.scan_score = true;
        state.score_output_idx = state.projected_columns.size();
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexOffsetsId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(catalog::Column::MakeOffsetsType());
      } else {
        // Real column read. For view-backed indexes the materialisation
        // path is wired in a follow-up; the projected_columns entry is
        // recorded here, but the actual projection-time check (and error)
        // happens in the search-scan operators where we know whether the
        // optimizer actually swapped the function to one that bypasses
        // materialisation (e.g. IRESEARCH_COUNT for count(*)).
        state.projected_columns.push_back(col_id);
        state.projected_types.push_back(bind_data.column_types[col_id]);
      }
    }
  }

  // View-backed scans use synthetic PKs (file-position rowids) that
  // behave like generated PKs for the rocksdb-flavoured bookkeeping.
  state.has_generated_pk =
    bind_data.IsViewBacked()
      ? true
      : bind_data.As<TableScanBindData>().table->PKColumns().empty();
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

std::vector<std::string> InitPKScanColumns(
  PKScanGlobalState& state, const SereneDBScanBindData& bind_data) {
  // PK-keyed scans only fire on rocksdb-backed binds; view binds route
  // through the search-scan path well before reaching here.
  SDB_ASSERT(!bind_data.IsViewBacked(),
             "InitPKScanColumns: view-backed bind reached PK scan path");
  auto table_id = bind_data.As<TableScanBindData>().table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);

  std::vector<catalog::Column::Id> scan_column_ids;
  for (auto proj_idx : state.projected_columns) {
    if (proj_idx != duckdb::DConstants::INVALID_INDEX) {
      scan_column_ids.push_back(bind_data.column_ids[proj_idx]);
    }
  }

  if (scan_column_ids.empty() && !bind_data.column_ids.empty()) {
    scan_column_ids.push_back(bind_data.column_ids[0]);
  }

  if (state.has_generated_pk && state.scan_rowid &&
      !bind_data.column_ids.empty()) {
    scan_column_ids.insert(scan_column_ids.begin(), bind_data.column_ids[0]);
  }

  const auto num_scan = scan_column_ids.size();
  state.upper_bound_data.reserve(key_utils::kKeyPrefixSize * num_scan);
  state.upper_bound_slices.reserve(num_scan);

  std::vector<std::string> column_keys;
  column_keys.reserve(num_scan);

  for (auto column_id : scan_column_ids) {
    auto key = table_key;
    basics::StrResize(key, key_utils::kTablePrefixSize);

    state.upper_bound_data.append(key);
    key_utils::AppendColumnKey(state.upper_bound_data, column_id + 1);

    key_utils::AppendColumnKey(key, column_id);
    column_keys.push_back(std::move(key));
  }

  for (size_t i = 0; i < num_scan; ++i) {
    state.upper_bound_slices.emplace_back(
      state.upper_bound_data.data() + i * key_utils::kKeyPrefixSize,
      key_utils::kKeyPrefixSize);
  }

  return column_keys;
}

void PKScanFunctionImpl(
  CommonScanGlobalState& gstate,
  std::vector<std::unique_ptr<rocksdb::Iterator>>& iterators,
  duckdb::DataChunk& output) {
  if (gstate.finished || iterators.empty()) {
    output.SetCardinality(0);
    gstate.finished = true;
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  duckdb::idx_t count = 0;
  duckdb::idx_t iter_idx = 0;

  duckdb::idx_t first_real_output = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t out = 0; out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] != duckdb::DConstants::INVALID_INDEX) {
      first_real_output = out;
      break;
    }
  }

  const duckdb::idx_t real_iter_start =
    (gstate.scan_rowid && gstate.has_generated_pk) ? 1 : 0;

  if (first_real_output != duckdb::DConstants::INVALID_INDEX) {
    count = ReadColumnIntoDuckDB(
      *iterators[real_iter_start], output.data[first_real_output],
      gstate.projected_types[first_real_output], batch_size);
    iter_idx = real_iter_start + 1;
  } else {
    auto& it = *iterators[real_iter_start];
    while (it.Valid() && count < batch_size) {
      ++count;
      it.Next();
    }
    rocksutils::CheckIteratorStatus(it);
    iter_idx = real_iter_start + 1;
  }

  if (count == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  for (duckdb::idx_t out =
         (first_real_output == duckdb::DConstants::INVALID_INDEX
            ? 0
            : first_real_output + 1);
       out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(iter_idx < iterators.size());
    auto col_count =
      ReadColumnIntoDuckDB(*iterators[iter_idx], output.data[out],
                           gstate.projected_types[out], count);
    SDB_ASSERT(col_count == count);
    ++iter_idx;
  }

  if (gstate.scan_rowid) {
    if (gstate.has_generated_pk && real_iter_start == 1) {
      auto pk_count = ReadGeneratedPKFromKeys(
        *iterators[0], output.data[gstate.rowid_output_idx], count);
      SDB_ASSERT(pk_count == count);
    } else {
      auto* rowid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
        output.data[gstate.rowid_output_idx]);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        rowid_data[i] = static_cast<int64_t>(i);
      }
    }
  }

  if (gstate.scan_tableoid) {
    output.data[gstate.tableoid_output_idx].Reference(
      duckdb::Value::BIGINT(gstate.tableoid_value));
  }

  output.SetCardinality(count);
  if (count > 0) {
    gstate.produced_rows.fetch_add(count, std::memory_order_relaxed);
  }
}

}  // namespace sdb::connector
