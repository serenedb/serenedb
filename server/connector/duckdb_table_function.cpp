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
  return copy;
}

bool SereneDBScanBindData::Equals(const duckdb::FunctionData& other) const {
  auto& o = other.Cast<SereneDBScanBindData>();
  return table == o.table && column_ids == o.column_ids;
}

// --- Global/Local state ---

struct SereneDBScanGlobalState : public duckdb::GlobalTableFunctionState {
  std::vector<std::unique_ptr<rocksdb::Iterator>> iterators;
  std::vector<std::string> column_keys;
  std::string upper_bound_data;
  std::vector<rocksdb::Slice> upper_bound_slices;
  const rocksdb::Snapshot* snapshot = nullptr;
  bool finished = false;

  ~SereneDBScanGlobalState() override {
    if (snapshot) {
      GetServerEngine().db()->ReleaseSnapshot(snapshot);
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

  const auto num_columns = bind_data.column_ids.size();
  state->column_keys.reserve(num_columns);
  state->upper_bound_data.reserve(kKeyPrefixSize * num_columns);
  state->upper_bound_slices.reserve(num_columns);

  for (auto column_id : bind_data.column_ids) {
    auto key = table_key;
    basics::StrResize(key, kTablePrefixSize);

    state->upper_bound_data.append(key);
    key_utils::AppendColumnKey(state->upper_bound_data, column_id + 1);

    key_utils::AppendColumnKey(key, column_id);
    state->column_keys.push_back(std::move(key));
  }

  for (size_t i = 0; i < num_columns; ++i) {
    state->upper_bound_slices.emplace_back(
      state->upper_bound_data.data() + i * kKeyPrefixSize, kKeyPrefixSize);
  }

  rocksdb::ReadOptions ro;
  ro.snapshot = state->snapshot;
  ro.async_io = false;
  ro.adaptive_readahead = true;
  ro.auto_prefix_mode = true;

  for (size_t i = 0; i < num_columns; ++i) {
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
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  auto& gstate = data.global_state->Cast<SereneDBScanGlobalState>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  const auto num_columns = bind_data.column_ids.size();
  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;

  if (num_columns == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  // Read first column — determines row count for this batch.
  // Type dispatch happens once here (inside ReadColumnIntoDuckDB).
  auto count = ReadColumnIntoDuckDB(*gstate.iterators[0], output.data[0],
                                    bind_data.column_types[0], batch_size);

  if (count == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  // Read remaining columns with same count.
  // Each call dispatches type once per column.
  for (size_t col = 1; col < num_columns; ++col) {
    auto col_count = ReadColumnIntoDuckDB(*gstate.iterators[col],
                                          output.data[col],
                                          bind_data.column_types[col], count);
    SDB_ASSERT(col_count == count);
  }

  output.SetCardinality(count);
}

// --- Factory ---

duckdb::TableFunction CreateSereneDBScanFunction() {
  duckdb::TableFunction func("serenedb_scan", {}, SereneDBScanFunction,
                             SereneDBScanBind);
  func.init_global = SereneDBScanInitGlobal;
  func.init_local = SereneDBScanInitLocal;
  func.projection_pushdown = false;
  func.filter_pushdown = false;
  return func;
}

}  // namespace sdb::connector
