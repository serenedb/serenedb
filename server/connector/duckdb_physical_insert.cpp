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
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

// Column metadata computed once in GetGlobalSinkState
struct InsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

struct SereneDBInsertGlobalState : public duckdb::GlobalSinkState {
  std::atomic<duckdb::idx_t> insert_count{0};

  // Table metadata (computed once)
  ObjectId table_id;
  std::string table_key;
  std::vector<InsertColumnMeta> columns;  // non-generated-PK columns
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  bool has_generated_pk = false;

  // RocksDB handles
  rocksdb::ColumnFamilyHandle* cf = nullptr;

  // Index writers — created once, reused per Sink() call
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> index_writers;

  // Reusable buffers
  std::vector<std::string> row_keys;
  std::vector<bool> skip_row;
  std::vector<DuckDBSinkIndexWriter*> active_writers;
  std::string key_buffer;
  std::string value_buffer;
};

struct SereneDBInsertSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

// --- Constructor ---

SereneDBPhysicalInsert::SereneDBPhysicalInsert(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality,
  duckdb::OnConflictAction on_conflict)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                              std::move(types), estimated_cardinality),
    _table(std::move(table)),
    _on_conflict(on_conflict) {}

// --- GetGlobalSinkState: set up once ---

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalInsert::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SereneDBInsertGlobalState>();

  state->cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(state->cf);

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  // Build column metadata
  const auto& columns = _table->Columns();
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->columns.push_back(InsertColumnMeta{
      .id = columns[i].id,
      .duckdb_type = VeloxTypeToDuckDB(columns[i].type),
      .input_col_idx = i,
    });
  }

  // PK column mappings
  state->pk_columns = duckdb_primary_key::BuildPKColumns(*_table);
  state->has_generated_pk = _table->PKColumns().empty();

  // Create index writers once (needs transaction context)
  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBWrite();
  state->index_writers = CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
    state->table_id, conn_ctx, *_table);

  return state;
}

// --- Sink ---

duckdb::SinkResultType SereneDBPhysicalInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBInsertGlobalState>();

  chunk.Flatten();
  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto& conn_ctx = GetSereneDBContext(context.client);
  conn_ctx.AddRocksDBWrite();
  auto* txn = &conn_ctx.EnsureRocksDBTransaction();

  constexpr size_t kTablePrefixSize = sizeof(ObjectId);

  // 1. Build PK keys for all rows
  duckdb_primary_key::CreateBatch(chunk, gstate.pk_columns, gstate.row_keys);

  // 2. Conflict detection for explicit PKs
  gstate.skip_row.assign(num_rows, false);
  if (!gstate.has_generated_pk) {
    rocksdb::ReadOptions ro;
    ro.snapshot = txn->GetSnapshot();
    std::string check_key;
    rocksdb::PinnableSlice check_value;

    // Use first data column for existence check
    auto first_col_id = gstate.columns.front().id;

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      check_key = gstate.table_key;
      basics::StrResize(check_key, kTablePrefixSize);
      key_utils::AppendColumnKey(check_key, first_col_id);
      check_key.append(gstate.row_keys[row]);

      check_value.Reset();
      auto s = txn->Get(ro, gstate.cf, check_key, &check_value);
      if (s.ok()) {
        if (_on_conflict == duckdb::OnConflictAction::NOTHING) {
          gstate.skip_row[row] = true;
          continue;
        }
        if (_on_conflict == duckdb::OnConflictAction::REPLACE) {
          continue;
        }
        SDB_THROW(ERROR_BAD_PARAMETER,
                  "duplicate key value violates unique constraint");
      }
      if (!s.IsNotFound()) {
        SDB_THROW(ERROR_INTERNAL, "RocksDB error: ", s.ToString());
      }
    }
  }

  // 3. Init index writers for this batch
  for (auto& writer : gstate.index_writers) {
    writer->Init(num_rows, chunk);
  }

  // 4. Write each column: data + index writers
  for (const auto& col : gstate.columns) {
    if (col.input_col_idx >= chunk.ColumnCount()) {
      continue;  // Column not in input (default value)
    }

    // Prepare index writers: filter to those interested in this column
    gstate.active_writers.clear();
    for (auto& writer : gstate.index_writers) {
      if (writer->SwitchColumn(col.duckdb_type, /*have_nulls=*/true, col.id)) {
        gstate.active_writers.push_back(writer.get());
      }
    }

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (gstate.skip_row[row]) {
        continue;
      }

      // Build full key: [ObjectId][ColumnId][PK]
      gstate.key_buffer = gstate.table_key;
      basics::StrResize(gstate.key_buffer, kTablePrefixSize);
      key_utils::AppendColumnKey(gstate.key_buffer, col.id);
      gstate.key_buffer.append(gstate.row_keys[row]);

      // Serialize value
      auto slice = SerializeScalarValue(chunk.data[col.input_col_idx], row,
                                        col.duckdb_type, gstate.value_buffer);

      // Write to RocksDB
      auto status = txn->Put(gstate.cf, gstate.key_buffer, slice);
      if (!status.ok()) {
        SDB_THROW(ERROR_INTERNAL, "RocksDB write failed: ",
                  status.ToString());
      }

      // Write to index writers
      for (auto* writer : gstate.active_writers) {
        writer->Write({&slice, 1}, gstate.key_buffer);
      }
    }
  }

  // 5. Finish index writers for this batch
  for (auto& writer : gstate.index_writers) {
    writer->Finish();
  }

  auto skipped =
    std::count(gstate.skip_row.begin(), gstate.skip_row.end(), true);
  gstate.insert_count += num_rows - skipped;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

// --- Finalize ---

duckdb::SinkFinalizeType SereneDBPhysicalInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  return duckdb::SinkFinalizeType::READY;
}

// --- Source (returns insert count) ---

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
