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
#include <duckdb/execution/execution_context.hpp>

#include "basics/assert.h"
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
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

struct UpdateColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  duckdb::idx_t table_col_idx;
};

struct SereneDBUpdateGlobalState : public duckdb::GlobalSinkState {
  duckdb::idx_t update_count = 0;

  ObjectId table_id;
  std::string table_key;

  // All non-generated columns (for index insert dispatch)
  struct ColumnMeta {
    catalog::Column::Id id;
    duckdb::LogicalType duckdb_type;
  };
  std::vector<ColumnMeta> all_columns;

  // Parallel to _update_columns. Resolved SET values arrive in the first
  // update_columns.size() slots of the Sink chunk.
  std::vector<UpdateColumnMeta> update_columns;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  rocksdb::ColumnFamilyHandle* cf = nullptr;
  rocksdb::Transaction* txn = nullptr;

  // Separate writers for delete (old entries) and insert (new entries)
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> delete_index_writers;
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> insert_index_writers;

  // Reusable buffers
  std::vector<std::string> row_keys;
  std::vector<DuckDBSinkIndexWriter*> active_writers;
  DuckDBColumnSerializer serializer;
  std::string value_buffer;
};

struct SereneDBUpdateSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

SereneDBPhysicalUpdate::SereneDBPhysicalUpdate(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::vector<duckdb::idx_t> pk_col_indices,
  std::vector<duckdb::PhysicalIndex> update_columns,
  std::vector<duckdb::idx_t> indexed_col_indices,
  duckdb::idx_t estimated_cardinality,
  duckdb::vector<duckdb::unique_ptr<duckdb::BoundConstraint>> bound_constraints)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)),
    _pk_col_indices(std::move(pk_col_indices)),
    _update_columns(std::move(update_columns)),
    _indexed_col_indices(std::move(indexed_col_indices)),
    _bound_constraints(std::move(bound_constraints)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalUpdate::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SereneDBUpdateGlobalState>();

  state->cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(state->cf);

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);

  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();

  for (const auto& col : columns) {
    if (col.id == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->all_columns.push_back(SereneDBUpdateGlobalState::ColumnMeta{
      .id = col.id,
      .duckdb_type = col.type,
    });
  }

  for (size_t i = 0; i < _update_columns.size(); ++i) {
    auto table_col_idx = _update_columns[i].index;
    const auto& col = columns[table_col_idx];
    state->update_columns.push_back(UpdateColumnMeta{
      .id = col.id,
      .duckdb_type = col.type,
      .table_col_idx = table_col_idx,
    });
  }

  for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
    duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
    if (i < pk_col_ids.size()) {
      for (const auto& col : columns) {
        if (col.id == pk_col_ids[i]) {
          pk_type = col.type;
          break;
        }
      }
    }
    state->pk_columns.push_back(duckdb_primary_key::PKColumn{
      .input_col_idx = _pk_col_indices[i],
      .type = pk_type,
    });
  }

  auto& conn_ctx = GetSereneDBContext(context);
  conn_ctx.AddRocksDBWrite();
  state->txn = &conn_ctx.EnsureRocksDBTransaction();

  // Build column-ID-to-chunk-position mapping.
  // UPDATE chunk layout: [SET_vals..., pk_virtuals..., idx_virtuals..., rowid]
  // Delete writers need old indexed column positions (from scan virtual cols).
  // Insert writers need new values (from SET positions for updated cols).
  ColumnChunkMapping del_col_mapping;

  // PK columns
  for (size_t i = 0; i < _pk_col_indices.size() && i < pk_col_ids.size(); ++i) {
    del_col_mapping[pk_col_ids[i]] = _pk_col_indices[i];
  }

  // Indexed (non-PK) columns
  {
    containers::FlatHashSet<size_t> pk_table_indices;
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].id == pk_id) {
          pk_table_indices.insert(i);
          break;
        }
      }
    }
    auto snapshot = conn_ctx.EnsureCatalogSnapshot();
    auto indexes = snapshot->GetIndexesByTable(state->table_id);
    std::vector<catalog::Column::Id> non_pk_idx_col_ids;
    containers::FlatHashSet<size_t> seen;
    for (auto& index : indexes) {
      for (auto col_id : index->GetColumnIds()) {
        for (size_t i = 0; i < columns.size(); ++i) {
          if (columns[i].id == col_id && !pk_table_indices.contains(i) &&
              !seen.contains(i)) {
            seen.insert(i);
            non_pk_idx_col_ids.push_back(col_id);
            break;
          }
        }
      }
    }
    std::sort(non_pk_idx_col_ids.begin(), non_pk_idx_col_ids.end(),
              [&](auto a, auto b) {
                size_t pos_a = 0, pos_b = 0;
                for (size_t i = 0; i < columns.size(); ++i) {
                  if (columns[i].id == a) {
                    pos_a = i;
                  }
                  if (columns[i].id == b) {
                    pos_b = i;
                  }
                }
                return pos_a < pos_b;
              });
    for (size_t i = 0;
         i < _indexed_col_indices.size() && i < non_pk_idx_col_ids.size();
         ++i) {
      del_col_mapping[non_pk_idx_col_ids[i]] = _indexed_col_indices[i];
    }
  }

  // Only create index writers for indexes whose columns overlap with
  // the updated columns -- indexes on non-updated columns are untouched
  std::vector<catalog::Column::Id> updated_col_ids;
  for (const auto& upd : state->update_columns) {
    updated_col_ids.push_back(upd.id);
  }

  state->delete_index_writers =
    CreateDuckDBIndexWriters<DuckDBWriteKind::Delete>(
      state->table_id, conn_ctx, *_table, del_col_mapping, updated_col_ids);

  // Insert writers need new values for updated columns (from SET positions)
  // and old values for non-updated index columns (from virtual positions).
  ColumnChunkMapping ins_col_mapping =
    del_col_mapping;  // PK + old indexed cols
  for (const auto& upd : state->update_columns) {
    ins_col_mapping[upd.id] = upd.table_col_idx;  // override with SET position
  }

  state->insert_index_writers =
    CreateDuckDBIndexWriters<DuckDBWriteKind::Insert>(
      state->table_id, conn_ctx, *_table, ins_col_mapping, updated_col_ids);

  return state;
}

duckdb::SinkResultType SereneDBPhysicalUpdate::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBUpdateGlobalState>();

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto* txn = gstate.txn;

  chunk.Flatten();

  // chunk layout: [resolved SET vals, pk_virtuals, idx_virtuals, rowid].
  // _update_columns names the leading SET slots; _pk_col_indices tells the
  // verifier where to find each PK for the "Failing row contains" detail.
  VerifyUpdateConstraints(context.client, *_table, _bound_constraints, chunk,
                          _update_columns, _pk_col_indices);

  // 1. Build row keys, lock rows, delete old index entries
  for (auto& writer : gstate.delete_index_writers) {
    writer->Init(num_rows, chunk);
  }

  gstate.row_keys.clear();
  gstate.row_keys.reserve(num_rows);
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
    auto& key_buffer = gstate.row_keys.emplace_back();
    duckdb_primary_key::MakeColumnKey(
      chunk, gstate.pk_columns, row, gstate.table_key,
      [&](std::string_view row_key) {
        auto status = txn->GetKeyLock(gstate.cf, row_key, false, true);
        if (!status.ok()) {
          auto result = rocksutils::ConvertStatus(status);
          SDB_THROW(result.errorNumber(),
                    "Failed to acquire row lock for table ",
                    gstate.table_id.id(), " error: ", result.errorMessage());
        }
        auto pk_bytes = row_key.substr(sizeof(ObjectId));
        for (auto& writer : gstate.delete_index_writers) {
          writer->DeleteRow(pk_bytes);
        }
      },
      key_buffer);
  }

  for (auto& writer : gstate.delete_index_writers) {
    writer->Finish();
  }

  // 2. Write updated columns + insert new index entries
  DuckDBColumnSerializer::TxnWriter txn_writer{txn, gstate.cf};

  for (auto& writer : gstate.insert_index_writers) {
    writer->Init(num_rows, chunk);
  }

  for (duckdb::idx_t i = 0; i < gstate.update_columns.size(); ++i) {
    const auto& col = gstate.update_columns[i];
    gstate.active_writers.clear();
    for (auto& writer : gstate.insert_index_writers) {
      if (writer->SwitchColumn(col.duckdb_type, /*have_nulls=*/true, col.id)) {
        gstate.active_writers.push_back(writer.get());
      }
    }

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
    }
    gstate.serializer.WriteColumn(txn_writer, chunk.data[i], col.duckdb_type,
                                  num_rows, gstate.row_keys,
                                  gstate.active_writers);
  }

  for (auto& writer : gstate.insert_index_writers) {
    writer->Finish();
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
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.update_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
