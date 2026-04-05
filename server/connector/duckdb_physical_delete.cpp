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

#include "connector/duckdb_physical_delete.h"

#include <duckdb/common/types/data_chunk.hpp>

#include "basics/assert.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

struct SereneDBDeleteGlobalState : public duckdb::GlobalSinkState {
  std::atomic<duckdb::idx_t> delete_count{0};
};

struct SereneDBDeleteSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

SereneDBPhysicalDelete::SereneDBPhysicalDelete(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::vector<duckdb::idx_t> pk_col_indices,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                              {duckdb::LogicalType::BIGINT},
                              estimated_cardinality),
    _table(std::move(table)),
    _pk_col_indices(std::move(pk_col_indices)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalDelete::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SereneDBDeleteGlobalState>();
}

duckdb::SinkResultType SereneDBPhysicalDelete::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBDeleteGlobalState>();

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  rocksdb::WriteOptions wo;
  auto* txn = db->BeginTransaction(wo);
  SDB_ASSERT(txn);

  auto table_id = _table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);
  constexpr size_t kTablePrefixSize = sizeof(ObjectId);

  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();
  const auto num_rows = chunk.size();

  std::string pk_buffer;
  std::string key_buffer;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    // Build PK bytes from the PK column values in the input chunk
    pk_buffer.clear();
    if (_pk_col_indices.empty()) {
      // Generated PK — this shouldn't happen in DELETE since we can't
      // reference the generated PK in WHERE. Skip for now.
      continue;
    }
    for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
      auto col_idx = _pk_col_indices[i];
      // Find the DuckDB type for this PK column
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

    // Delete all column cells for this row
    for (const auto& col : columns) {
      if (col.id == catalog::Column::kGeneratedPKId) {
        continue;
      }
      key_buffer = table_key;
      basics::StrResize(key_buffer, kTablePrefixSize);
      key_utils::AppendColumnKey(key_buffer, col.id);
      key_buffer.append(pk_buffer);

      auto status = txn->Delete(cf, key_buffer);
      if (!status.ok()) {
        txn->Rollback();
        delete txn;
        SDB_THROW(ERROR_INTERNAL, "RocksDB delete failed: ",
                  status.ToString());
      }
    }
  }

  auto status = txn->Commit();
  delete txn;
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "RocksDB commit failed: ", status.ToString());
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBPhysicalDelete::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalDelete::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SereneDBDeleteSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalDelete::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SereneDBDeleteSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SereneDBDeleteGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count.load()));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
