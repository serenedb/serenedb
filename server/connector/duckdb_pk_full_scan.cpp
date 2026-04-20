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

#include "connector/duckdb_pk_full_scan.hpp"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "basics/string_utils.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> PKFullScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<PKFullScanGlobalState>();

  InitCommonState(*state, context, bind_data, input);

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  auto table_id = bind_data.table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);

  // Build the set of column IDs for which we need iterators.
  std::vector<catalog::Column::Id> scan_column_ids;
  for (auto proj_idx : state->projected_columns) {
    if (proj_idx != duckdb::DConstants::INVALID_INDEX) {
      scan_column_ids.push_back(bind_data.column_ids[proj_idx]);
    }
  }

  // If only rowid is requested, still need one column iterator to drive scan.
  if (scan_column_ids.empty() && !bind_data.column_ids.empty()) {
    scan_column_ids.push_back(bind_data.column_ids[0]);
  }

  // For generated-PK tables needing rowid, prepend a dedicated iterator on
  // the first real column for PK extraction from keys.
  if (state->has_generated_pk && state->scan_rowid &&
      !bind_data.column_ids.empty()) {
    scan_column_ids.insert(scan_column_ids.begin(), bind_data.column_ids[0]);
  }

  const auto num_scan = scan_column_ids.size();
  state->upper_bound_data.reserve(key_utils::kKeyPrefixSize * num_scan);
  state->upper_bound_slices.reserve(num_scan);

  std::vector<std::string> column_keys;
  column_keys.reserve(num_scan);

  for (auto column_id : scan_column_ids) {
    auto key = table_key;
    basics::StrResize(key, key_utils::kTablePrefixSize);

    state->upper_bound_data.append(key);
    key_utils::AppendColumnKey(state->upper_bound_data, column_id + 1);

    key_utils::AppendColumnKey(key, column_id);
    column_keys.push_back(std::move(key));
  }

  for (size_t i = 0; i < num_scan; ++i) {
    state->upper_bound_slices.emplace_back(
      state->upper_bound_data.data() + i * key_utils::kKeyPrefixSize,
      key_utils::kKeyPrefixSize);
  }

  rocksdb::ReadOptions ro;
  ro.snapshot = state->snapshot;
  ro.async_io = false;
  ro.adaptive_readahead = true;
  ro.auto_prefix_mode = true;

  for (size_t i = 0; i < num_scan; ++i) {
    ro.iterate_upper_bound = &state->upper_bound_slices[i];
    auto it = std::unique_ptr<rocksdb::Iterator>(
      state->txn ? state->txn->GetIterator(ro, cf) : db->NewIterator(ro, cf));
    it->Seek(column_keys[i]);
    state->iterators.push_back(std::move(it));
  }

  return state;
}

void PKFullScanFunction(duckdb::ClientContext& /*context*/,
                        duckdb::TableFunctionInput& data,
                        duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<PKFullScanGlobalState>();

  if (gstate.finished || gstate.iterators.empty()) {
    output.SetCardinality(0);
    gstate.finished = true;
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;

  duckdb::idx_t count = 0;
  duckdb::idx_t iter_idx = 0;

  // Find the first real (non-virtual) output column to drive the scan.
  duckdb::idx_t first_real_output = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t out = 0; out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] != duckdb::DConstants::INVALID_INDEX) {
      first_real_output = out;
      break;
    }
  }

  // For generated-PK tables, iterator[0] is a dedicated PK-extraction iterator.
  // Real column iterators start at index 1 in that case.
  const duckdb::idx_t real_iter_start =
    (gstate.scan_rowid && gstate.has_generated_pk) ? 1 : 0;

  if (first_real_output != duckdb::DConstants::INVALID_INDEX) {
    count = ReadColumnIntoDuckDB(
      *gstate.iterators[real_iter_start], output.data[first_real_output],
      gstate.projected_types[first_real_output], batch_size);
    iter_idx = real_iter_start + 1;
  } else if (!gstate.iterators.empty()) {
    auto& it = *gstate.iterators[real_iter_start];
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

  // Read remaining real columns.
  for (duckdb::idx_t out =
         (first_real_output == duckdb::DConstants::INVALID_INDEX
            ? 0
            : first_real_output + 1);
       out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(iter_idx < gstate.iterators.size());
    auto col_count =
      ReadColumnIntoDuckDB(*gstate.iterators[iter_idx], output.data[out],
                           gstate.projected_types[out], count);
    SDB_ASSERT(col_count == count);
    ++iter_idx;
  }

  // Fill rowid column.
  if (gstate.scan_rowid) {
    if (gstate.has_generated_pk && real_iter_start == 1) {
      auto pk_count = ReadGeneratedPKFromKeys(
        *gstate.iterators[0], output.data[gstate.rowid_output_idx], count);
      SDB_ASSERT(pk_count == count);
    } else {
      auto* rowid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
        output.data[gstate.rowid_output_idx]);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        rowid_data[i] = static_cast<int64_t>(i);
      }
    }
  }

  // Fill tableoid as constant vector.
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
