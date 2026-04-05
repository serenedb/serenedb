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

#include "connector/duckdb_physical_insert.h"

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>

#include "basics/assert.h"
#include "catalog/identifiers/revision_id.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "connector/rocksdb_sink_writer.hpp"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

struct SereneDBInsertGlobalState : public duckdb::GlobalSinkState {
  std::atomic<duckdb::idx_t> insert_count{0};
};

struct SereneDBInsertSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

SereneDBPhysicalInsert::SereneDBPhysicalInsert(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality,
  duckdb::OnConflictAction on_conflict)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                              std::move(types), estimated_cardinality),
    _table(std::move(table)),
    _on_conflict(on_conflict) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalInsert::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SereneDBInsertGlobalState>();
}

duckdb::SinkResultType SereneDBPhysicalInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBInsertGlobalState>();

  auto& engine = GetServerEngine();
  auto* db = engine.db();
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(cf);

  // Create a RocksDB transaction for this batch
  rocksdb::WriteOptions write_options;
  auto* txn = db->BeginTransaction(write_options);
  SDB_ASSERT(txn);

  const auto& columns = _table->Columns();
  auto table_id = _table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);

  const auto num_rows = chunk.size();
  const auto num_input_cols = chunk.ColumnCount();
  constexpr size_t kTablePrefixSize = sizeof(ObjectId);

  // Determine PK columns
  const auto& pk_columns = _table->PKColumns();
  bool has_generated_pk = pk_columns.empty();

  // Build row keys (PK bytes) for all rows
  std::vector<std::string> row_keys(num_rows);
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    if (has_generated_pk) {
      auto generated_pk =
        std::bit_cast<int64_t>(RevisionId::create().id());
      primary_key::AppendSigned(row_keys[row], generated_pk);
    } else {
      // Build PK from explicit PK columns in the input
      for (size_t pk_idx = 0; pk_idx < pk_columns.size(); ++pk_idx) {
        // Find which input column corresponds to this PK column
        auto pk_col_id = pk_columns[pk_idx];
        duckdb::idx_t input_col = 0;
        for (size_t i = 0; i < columns.size(); ++i) {
          if (columns[i].id == pk_col_id) {
            input_col = i;
            break;
          }
        }
        SDB_ASSERT(input_col < num_input_cols);
        auto duckdb_type = VeloxTypeToDuckDB(columns[input_col].type);
        AppendPKValueFromDuckDB(row_keys[row], chunk.data[input_col], row,
                                duckdb_type);
      }
    }
  }

  // Conflict detection for explicit PKs
  // Check if any row key already exists in RocksDB
  std::vector<bool> skip_row(num_rows, false);
  if (!has_generated_pk) {
    // Use first column's key prefix to check existence
    rocksdb::ReadOptions ro;
    ro.snapshot = txn->GetSnapshot();
    std::string check_key;
    rocksdb::PinnableSlice check_value;
    auto first_col_id = columns[0].id;
    if (first_col_id == catalog::Column::kGeneratedPKId && columns.size() > 1) {
      first_col_id = columns[1].id;
    }
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      check_key = table_key;
      basics::StrResize(check_key, kTablePrefixSize);
      key_utils::AppendColumnKey(check_key, first_col_id);
      check_key.append(row_keys[row]);

      check_value.Reset();
      auto s = txn->Get(ro, cf, check_key, &check_value);
      if (s.ok()) {
        // Key exists — PK conflict
        if (_on_conflict == duckdb::OnConflictAction::NOTHING) {
          skip_row[row] = true;
          continue;
        }
        if (_on_conflict == duckdb::OnConflictAction::REPLACE) {
          // Overwrite — proceed without error
          continue;
        }
        // THROW
        txn->Rollback();
        delete txn;
        SDB_THROW(ERROR_BAD_PARAMETER,
                  "duplicate key value violates unique constraint");
      }
      if (!s.IsNotFound()) {
        txn->Rollback();
        delete txn;
        SDB_THROW(ERROR_INTERNAL, "RocksDB error: ", s.ToString());
      }
    }
  }

  // Write each column to RocksDB
  std::string key_buffer;
  std::string value_buffer;

  for (size_t col_idx = 0; col_idx < columns.size(); ++col_idx) {
    const auto& col = columns[col_idx];
    if (col.id == catalog::Column::kGeneratedPKId) {
      continue;  // Generated PK is not stored
    }
    if (col_idx >= num_input_cols) {
      continue;  // Column not in input (has default — skip for now)
    }

    auto duckdb_type = VeloxTypeToDuckDB(col.type);

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (skip_row[row]) {
        continue;
      }
      // Build full key: [ObjectId][ColumnId][PK]
      key_buffer = table_key;
      basics::StrResize(key_buffer, kTablePrefixSize);
      key_utils::AppendColumnKey(key_buffer, col.id);
      key_buffer.append(row_keys[row]);

      // Serialize value
      auto slice =
        SerializeScalarValue(chunk.data[col_idx], row, duckdb_type,
                             value_buffer);

      // Write to RocksDB
      rocksdb::Status status;
      if (slice.empty()) {
        // NULL value
        rocksdb::Slice null_slice;
        status = txn->Put(cf, key_buffer, null_slice);
      } else {
        status = txn->Put(cf, key_buffer, slice);
      }
      if (!status.ok()) {
        txn->Rollback();
        delete txn;
        SDB_THROW(ERROR_INTERNAL, "RocksDB write failed: ",
                  status.ToString());
      }
    }
  }

  // Commit transaction
  auto status = txn->Commit();
  delete txn;
  if (!status.ok()) {
    SDB_THROW(ERROR_INTERNAL, "RocksDB commit failed: ", status.ToString());
  }

  auto skipped = std::count(skip_row.begin(), skip_row.end(), true);
  gstate.insert_count += num_rows - skipped;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBPhysicalInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  return duckdb::SinkFinalizeType::READY;
}

duckdb::unique_ptr<duckdb::GlobalSourceState>
SereneDBPhysicalInsert::GetGlobalSourceState(
  duckdb::ClientContext& context) const {
  return duckdb::make_uniq<SereneDBInsertSourceState>();
}

duckdb::SourceResultType SereneDBPhysicalInsert::GetDataInternal(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSourceInput& input) const {
  auto& source = input.global_state.Cast<SereneDBInsertSourceState>();
  if (source.finished) {
    return duckdb::SourceResultType::FINISHED;
  }
  source.finished = true;

  auto& gstate = sink_state->Cast<SereneDBInsertGlobalState>();
  chunk.SetCardinality(1);
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count.load()));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
