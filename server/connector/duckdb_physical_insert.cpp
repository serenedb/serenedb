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

#include <absl/synchronization/mutex.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <shared_mutex>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/sequence.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_constraint_verify.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

// Column metadata computed once in GetGlobalSinkState
struct InsertColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  size_t input_col_idx;
};

struct SereneDBInsertGlobalState : public duckdb::GlobalSinkState {
  duckdb::idx_t insert_count = 0;

  // Table metadata (computed once)
  ObjectId table_id;
  std::string table_key;
  std::string table_name;
  std::vector<InsertColumnMeta> columns;  // non-generated-PK columns
  std::vector<duckdb_primary_key::PKColumn> pk_columns;
  std::vector<std::string> pk_col_names;

  // RocksDB handles
  rocksdb::ColumnFamilyHandle* cf = nullptr;
  rocksdb::Transaction* txn = nullptr;

  std::shared_ptr<catalog::Sequence> generated_pk_seq;

  // Index writers -- created once, reused per Sink() call
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> index_writers;

  // Reusable buffers
  std::vector<std::string> row_keys;
  std::vector<DuckDBSinkIndexWriter*> active_writers;
  std::string value_buffer;
  duckdb::unique_ptr<DuckDBColumnSerializer> serializer;
  DuckDBWriteConflictResolver conflict_resolver;

  std::shared_ptr<TableShard> table_shard;
  std::shared_lock<std::shared_mutex> table_lock;
};

struct SereneDBInsertSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

// --- Constructor ---

SereneDBPhysicalInsert::SereneDBPhysicalInsert(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  duckdb::vector<duckdb::LogicalType> types,
  duckdb::idx_t estimated_cardinality, duckdb::OnConflictAction on_conflict,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> bound_constraints)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             std::move(types), estimated_cardinality),
    _table(std::move(table)),
    _on_conflict(on_conflict),
    _bound_constraints(std::move(bound_constraints)) {}

// --- GetGlobalSinkState: set up once ---

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalInsert::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SereneDBInsertGlobalState>();
  state->serializer = duckdb::make_uniq<DuckDBColumnSerializer>(
    duckdb::BufferAllocator::Get(context));

  state->cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(state->cf);

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  auto& conn_ctx = GetSereneDBContext(context);
  state->table_shard =
    conn_ctx.EnsureCatalogSnapshot()->GetTableShard(state->table_id);
  SDB_ASSERT(state->table_shard);
  state->table_lock = std::shared_lock{state->table_shard->GetTableLock()};

  // Build column metadata
  const auto& columns = _table->Columns();
  size_t input_idx = 0;
  for (size_t i = 0; i < columns.size(); ++i) {
    if (columns[i].id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->columns.push_back(InsertColumnMeta{
      .id = columns[i].id,
      .duckdb_type = columns[i].type,
      .input_col_idx = input_idx++,
    });
  }

  // PK column mappings
  state->pk_columns = duckdb_primary_key::BuildPKColumns(*_table);
  state->table_name = _table->GetName();
  for (auto pk_id : _table->PKColumns()) {
    for (const auto& col : columns) {
      if (col.id == pk_id) {
        state->pk_col_names.push_back(col.name);
        break;
      }
    }
  }

  // Set up transaction and index writers once
  conn_ctx.AddRocksDBWrite();
  state->txn = &conn_ctx.EnsureRocksDBTransaction();
  state->conflict_resolver.Init(*state->txn, *state->cf, _on_conflict,
                                state->table_name);
  state->index_writers = CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
    state->table_id, conn_ctx, *_table);

  state->generated_pk_seq =
    conn_ctx.EnsureCatalogSnapshot()->GetObject<catalog::Sequence>(
      _table->GetGeneratedPkSeqId());
  SDB_ASSERT(state->generated_pk_seq || !_table->PKColumns().empty());

  return state;
}

// --- Sink ---

duckdb::SinkResultType SereneDBPhysicalInsert::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBInsertGlobalState>();

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto* txn = gstate.txn;
  // Verify constraints (CHECK, NOT NULL) before writing
  VerifyAppendConstraints(context.client, *_table, _bound_constraints, chunk);

  // 1. Build row keys and acquire row-level locks per row:
  gstate.row_keys.clear();
  gstate.row_keys.reserve(num_rows);

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  uint64_t generated_pk_base =
    gstate.generated_pk_seq
      ? gstate.generated_pk_seq->ReserveWriteUnsafe(num_rows)
      : 0;

  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    auto& key_buffer = gstate.row_keys.emplace_back();
    duckdb_primary_key::MakeColumnKey(
      pk_formats, gstate.pk_columns, row, generated_pk_base + row,
      gstate.table_key,
      [&](std::string_view row_key) {
        auto status = txn->GetKeyLock(gstate.cf, row_key, false, true);
        if (!status.ok()) {
          auto result = rocksutils::ConvertStatus(status);
          SDB_THROW(result.errorNumber(),
                    "Failed to acquire row lock for table ",
                    gstate.table_id.id(), " error: ", result.errorMessage());
        }
      },
      key_buffer);
    key_utils::SetupColumnForKey(key_buffer, gstate.columns.front().id);
  }

  // 2. Conflict detection for explicit PKs
  size_t rows_skipped = 0;
  if (!gstate.generated_pk_seq) {
    rows_skipped = gstate.conflict_resolver.HandleWriteConflicts<false>(
      gstate.row_keys, chunk, gstate.pk_columns, gstate.pk_col_names);
  }

  auto affected_rows = num_rows - rows_skipped;
  if (affected_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  // 3. Init index writers with affected rows count (not total)
  for (auto& writer : gstate.index_writers) {
    writer->Init(affected_rows, chunk);
  }

  // 4. Write each column via DuckDBColumnSerializer
  DuckDBColumnSerializer::TxnWriter txn_writer{txn, gstate.cf};

  for (const auto& col : gstate.columns) {
    if (col.input_col_idx >= chunk.ColumnCount()) {
      continue;
    }

    gstate.active_writers.clear();
    for (auto& writer : gstate.index_writers) {
      auto& vec = chunk.data[col.input_col_idx];
      bool may_have_nulls =
        vec.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
        !duckdb::FlatVector::Validity(vec).CannotHaveNull();
      if (writer->SwitchColumn(col.duckdb_type, may_have_nulls, col.id)) {
        gstate.active_writers.push_back(writer.get());
      }
    }

    // Setup ColumnId in all row keys for this column
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      if (!gstate.row_keys[row].empty()) {
        key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
      }
    }

    gstate.serializer->WriteColumn(txn_writer, chunk.data[col.input_col_idx],
                                   col.duckdb_type, num_rows, gstate.row_keys,
                                   gstate.active_writers);
  }

  // 5. Finish index writers
  for (auto& writer : gstate.index_writers) {
    writer->Finish();
  }

  gstate.insert_count += affected_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

// --- Finalize ---

duckdb::SinkFinalizeType SereneDBPhysicalInsert::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBInsertGlobalState>();
  if (gstate.insert_count > 0) {
    auto& conn_ctx = GetSereneDBContext(context);
    conn_ctx.UpdateNumRows(gstate.table_id, gstate.insert_count);
  }
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
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.insert_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
