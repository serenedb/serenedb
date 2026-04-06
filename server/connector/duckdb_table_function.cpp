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

#include "connector/duckdb_table_function.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/function/table_function.hpp>

#include "basics/assert.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

constexpr size_t kTablePrefixSize = sizeof(ObjectId);
constexpr size_t kKeyPrefixSize =
  kTablePrefixSize + sizeof(catalog::Column::Id);

// --- SereneDBScanBindData ---

duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBindData::Copy() const {
  auto copy = duckdb::make_uniq<SereneDBScanBindData>();
  copy->table = table;
  copy->column_ids = column_ids;
  copy->column_types = column_types;
  copy->has_rowid = has_rowid;
  copy->table_entry = table_entry;
  return copy;
}

static duckdb::BindInfo SereneDBGetBindInfo(
  const duckdb::optional_ptr<duckdb::FunctionData> bind_data) {
  auto& data = const_cast<SereneDBScanBindData&>(bind_data->Cast<SereneDBScanBindData>());
  if (data.table_entry) {
    return duckdb::BindInfo(*data.table_entry);
  }
  return duckdb::BindInfo(duckdb::ScanType::TABLE);
}

bool SereneDBScanBindData::Equals(const duckdb::FunctionData& other) const {
  auto& o = other.Cast<SereneDBScanBindData>();
  return table == o.table && column_ids == o.column_ids;
}

// --- Global/Local state ---

struct SereneDBScanGlobalState : public duckdb::GlobalTableFunctionState {
  // Only iterators for the projected (requested) columns
  std::vector<std::unique_ptr<rocksdb::Iterator>> iterators;
  // Maps output column index → bind_data column index
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<std::string> column_keys;
  std::string upper_bound_data;
  std::vector<rocksdb::Slice> upper_bound_slices;
  const rocksdb::Snapshot* snapshot = nullptr;
  bool finished = false;
  bool scan_rowid = false;  // true if rowid column is in the projection
  duckdb::idx_t rowid_output_idx = 0;  // output column index for rowid

  ~SereneDBScanGlobalState() override {
    iterators.clear();  // Release iterators before snapshot
    if (snapshot) {
      GetServerEngine().db()->ReleaseSnapshot(snapshot);
      snapshot = nullptr;
    }
  }
};

struct SereneDBScanLocalState : public duckdb::LocalTableFunctionState {};

// --- Callbacks ---

static duckdb::unique_ptr<duckdb::FunctionData> SereneDBScanBind(
  duckdb::ClientContext& context, duckdb::TableFunctionBindInput& input,
  duckdb::vector<duckdb::LogicalType>& return_types,
  duckdb::vector<duckdb::string>& names) {
  throw duckdb::InternalException(
    "SereneDBScanBind: should be provided via GetScanFunction");
}

static duckdb::unique_ptr<duckdb::GlobalTableFunctionState>
SereneDBScanInitGlobal(duckdb::ClientContext& context,
                       duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SereneDBScanGlobalState>();

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  state->snapshot = db->GetSnapshot();

  auto table_id = bind_data.table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);

  // Determine which columns DuckDB actually wants (projection pushdown)
  const auto num_bind_columns = bind_data.column_ids.size();
  std::cerr << "SereneDB scan init: requested columns [";
  for (auto col_id : input.column_ids) {
    std::cerr << col_id << " ";
  }
  std::cerr << "]" << std::endl;
  for (auto col_id : input.column_ids) {
    if (col_id == duckdb::COLUMN_IDENTIFIER_ROW_ID) {
      // Dummy sequential rowid
      state->scan_rowid = true;
      state->rowid_output_idx = state->projected_columns.size();
      state->projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state->projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      // Virtual PK column: VIRTUAL_COLUMN_START + real_col_index
      auto real_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(real_idx != duckdb::DConstants::INVALID_INDEX);
      SDB_ASSERT(real_idx < bind_data.column_ids.size());
      state->projected_columns.push_back(real_idx);
      state->projected_types.push_back(bind_data.column_types[real_idx]);
    } else if (col_id < num_bind_columns) {
      state->projected_columns.push_back(col_id);
      state->projected_types.push_back(bind_data.column_types[col_id]);
    }
  }

  // Create iterators only for the real (non-rowid) projected columns
  std::vector<catalog::Column::Id> scan_column_ids;
  for (auto proj_idx : state->projected_columns) {
    if (proj_idx != duckdb::DConstants::INVALID_INDEX) {
      scan_column_ids.push_back(bind_data.column_ids[proj_idx]);
    }
  }

  // If only rowid is requested, we still need one column iterator to drive scan
  if (scan_column_ids.empty() && !bind_data.column_ids.empty()) {
    scan_column_ids.push_back(bind_data.column_ids[0]);
  }

  const auto num_scan = scan_column_ids.size();
  state->column_keys.reserve(num_scan);
  state->upper_bound_data.reserve(kKeyPrefixSize * num_scan);
  state->upper_bound_slices.reserve(num_scan);

  for (auto column_id : scan_column_ids) {
    auto key = table_key;
    basics::StrResize(key, kTablePrefixSize);

    state->upper_bound_data.append(key);
    key_utils::AppendColumnKey(state->upper_bound_data, column_id + 1);

    key_utils::AppendColumnKey(key, column_id);
    state->column_keys.push_back(std::move(key));
  }

  for (size_t i = 0; i < num_scan; ++i) {
    state->upper_bound_slices.emplace_back(
      state->upper_bound_data.data() + i * kKeyPrefixSize, kKeyPrefixSize);
  }

  rocksdb::ReadOptions ro;
  ro.snapshot = state->snapshot;
  ro.async_io = false;
  ro.adaptive_readahead = true;
  ro.auto_prefix_mode = true;

  for (size_t i = 0; i < num_scan; ++i) {
    ro.iterate_upper_bound = &state->upper_bound_slices[i];
    auto it = std::unique_ptr<rocksdb::Iterator>(db->NewIterator(ro, cf));
    it->Seek(state->column_keys[i]);
    state->iterators.push_back(std::move(it));
  }

  return state;
}

static duckdb::unique_ptr<duckdb::LocalTableFunctionState>
SereneDBScanInitLocal(duckdb::ExecutionContext& context,
                      duckdb::TableFunctionInitInput& input,
                      duckdb::GlobalTableFunctionState* global_state) {
  return duckdb::make_uniq<SereneDBScanLocalState>();
}

static void SereneDBScanFunction(duckdb::ClientContext& context,
                                 duckdb::TableFunctionInput& data,
                                 duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SereneDBScanGlobalState>();

  if (gstate.finished || gstate.iterators.empty()) {
    output.SetCardinality(0);
    gstate.finished = true;
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;

  // First iterator drives the row count.
  // If rowid is needed AND the first projected column is rowid, we need a
  // special path. Otherwise, read the first real column.
  duckdb::idx_t count = 0;
  duckdb::idx_t iter_idx = 0;  // which iterator we're consuming

  // Find the first real (non-rowid) output column to drive the scan
  duckdb::idx_t first_real_output = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t out = 0; out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] != duckdb::DConstants::INVALID_INDEX) {
      first_real_output = out;
      break;
    }
  }

  if (first_real_output != duckdb::DConstants::INVALID_INDEX) {
    // Read first real column to determine row count
    count = ReadColumnIntoDuckDB(
      *gstate.iterators[0], output.data[first_real_output],
      gstate.projected_types[first_real_output], batch_size);
    iter_idx = 1;
  } else if (!gstate.iterators.empty()) {
    // Only rowid requested — still need to iterate to count rows
    // Use a dummy read that just counts
    auto& it = *gstate.iterators[0];
    while (it.Valid() && count < batch_size) {
      ++count;
      it.Next();
    }
    rocksutils::CheckIteratorStatus(it);
    iter_idx = 1;
  }

  if (count == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  // Read remaining real columns
  for (duckdb::idx_t out = (first_real_output == duckdb::DConstants::INVALID_INDEX ? 0 : first_real_output + 1);
       out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] == duckdb::DConstants::INVALID_INDEX) {
      continue;  // rowid — already handled
    }
    SDB_ASSERT(iter_idx < gstate.iterators.size());
    auto col_count = ReadColumnIntoDuckDB(
      *gstate.iterators[iter_idx], output.data[out],
      gstate.projected_types[out], count);
    SDB_ASSERT(col_count == count);
    ++iter_idx;
  }

  // Fill dummy rowid column with sequential numbers if requested
  if (gstate.scan_rowid) {
    auto* rowid_data = duckdb::FlatVector::GetData<int64_t>(
      output.data[gstate.rowid_output_idx]);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      rowid_data[i] = static_cast<int64_t>(i);
    }
  }

  output.SetCardinality(count);
}

// --- Factory ---

duckdb::TableFunction CreateSereneDBScanFunction() {
  duckdb::TableFunction func("serenedb_scan", {}, SereneDBScanFunction,
                             SereneDBScanBind);
  func.init_global = SereneDBScanInitGlobal;
  func.init_local = SereneDBScanInitLocal;
  func.get_bind_info = SereneDBGetBindInfo;
  func.projection_pushdown = true;
  func.filter_pushdown = false;
  return func;
}

}  // namespace sdb::connector
