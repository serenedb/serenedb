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

#include "connector/duckdb_physical_update.h"

#include <duckdb/common/types/data_chunk.hpp>

#include <iostream>

#include "basics/assert.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

struct SereneDBUpdateGlobalState : public duckdb::GlobalSinkState {
  std::atomic<duckdb::idx_t> update_count{0};
};

struct SereneDBUpdateSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

SereneDBPhysicalUpdate::SereneDBPhysicalUpdate(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::vector<duckdb::idx_t> pk_col_indices,
  std::vector<duckdb::PhysicalIndex> update_columns,
  std::vector<duckdb::idx_t> update_col_input_indices,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                              {duckdb::LogicalType::BIGINT},
                              estimated_cardinality),
    _table(std::move(table)),
    _pk_col_indices(std::move(pk_col_indices)),
    _update_columns(std::move(update_columns)),
    _update_col_input_indices(std::move(update_col_input_indices)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalUpdate::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SereneDBUpdateGlobalState>();
}

duckdb::SinkResultType SereneDBPhysicalUpdate::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBUpdateGlobalState>();

  chunk.Flatten();

  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  auto& conn_ctx = GetSereneDBContext(context.client);
  conn_ctx.AddRocksDBWrite();
  auto* txn = &conn_ctx.EnsureRocksDBTransaction();

  auto table_id = _table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);
  constexpr size_t kTablePrefixSize = sizeof(ObjectId);

  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();
  const auto num_rows = chunk.size();

  std::cerr << "SereneDB UPDATE: " << num_rows << " rows, "
            << chunk.ColumnCount() << " input cols, "
            << _update_columns.size() << " update cols, "
            << _pk_col_indices.size() << " pk cols" << std::endl;
  for (duckdb::idx_t i = 0; i < chunk.ColumnCount(); ++i) {
    std::cerr << "  input col " << i << ": "
              << chunk.data[i].GetType().ToString() << std::endl;
  }

  // Build PK bytes for each row from PK columns in the input
  std::string pk_buffer;
  std::string key_buffer;
  std::string value_buffer;

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    pk_buffer.clear();
    for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
      auto col_idx = _pk_col_indices[i];
      auto pk_id = pk_col_ids[i];
      duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
      for (const auto& col : columns) {
        if (col.id == pk_id) {
          pk_type = VeloxTypeToDuckDB(col.type);
          break;
        }
      }
      AppendPKValueFromDuckDB(pk_buffer, chunk.data[col_idx], row, pk_type);
    }

    // Write only the updated columns
    for (size_t i = 0; i < _update_columns.size(); ++i) {
      auto table_col_idx = _update_columns[i].index;
      auto input_col_idx = _update_col_input_indices[i];
      const auto& col = columns[table_col_idx];

      key_buffer = table_key;
      basics::StrResize(key_buffer, kTablePrefixSize);
      key_utils::AppendColumnKey(key_buffer, col.id);
      key_buffer.append(pk_buffer);

      auto duckdb_type = VeloxTypeToDuckDB(col.type);
      auto slice = SerializeScalarValue(chunk.data[input_col_idx], row,
                                        duckdb_type, value_buffer);

      rocksdb::Status status;
      if (slice.empty()) {
        rocksdb::Slice null_slice;
        status = txn->Put(cf, key_buffer, null_slice);
      } else {
        status = txn->Put(cf, key_buffer, slice);
      }
      if (!status.ok()) {
        SDB_THROW(ERROR_INTERNAL, "RocksDB update failed: ",
                  status.ToString());
      }
    }
  }

  gstate.update_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBPhysicalUpdate::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalUpdate::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SereneDBUpdateSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalUpdate::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SereneDBUpdateSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SereneDBUpdateGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count.load()));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
