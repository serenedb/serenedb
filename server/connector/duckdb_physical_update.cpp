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

#include <absl/synchronization/mutex.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <shared_mutex>

#include "basics/assert.h"
#include "basics/containers/flat_hash_set.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_constraint_verify.h"
// THROW_SQL_ERROR + ERRCODE_UNIQUE_VIOLATION for intra-batch duplicate check
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/indexonly_marker.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

struct UpdateColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
  duckdb::idx_t table_col_idx;
  catalog::ColumnStoreMode store_mode;
};

struct SereneDBUpdateGlobalState : public duckdb::GlobalSinkState {
  explicit SereneDBUpdateGlobalState(duckdb::Allocator& allocator)
    : serializer(allocator) {}

  duckdb::idx_t update_count = 0;

  ObjectId table_id;
  std::string table_key;
  std::string table_name;

  // All non-generated columns (for index insert dispatch)
  struct ColumnMeta {
    catalog::Column::Id id;
    duckdb::LogicalType duckdb_type;
    catalog::ColumnStoreMode store_mode;
  };
  std::vector<ColumnMeta> all_columns;

  // Parallel to _update_columns. Resolved SET values arrive in the first
  // update_columns.size() slots of the Sink chunk.
  std::vector<UpdateColumnMeta> update_columns;
  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  rocksdb::ColumnFamilyHandle* cf = nullptr;
  rocksdb::Transaction* txn = nullptr;
  // sdb-side transaction; the IndexOnly writer registers markers on it.
  query::Transaction* sdb_txn = nullptr;

  // True iff some inverted index on the table is built exclusively over
  // IndexOnly columns and would not see the row delete through normal
  // WAL replay. Drives the per-row [RD] marker emission in the PK-update
  // path.
  bool needs_rd_markers = false;

  // Single set of Update writers that handle both DeleteRow and Write.
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> index_writers;

  // Reusable buffers
  std::vector<std::string> row_keys;
  std::vector<DuckDBSinkColumnWriter*> active_writers;
  DuckDBColumnSerializer serializer;
  // saved_columns[col_id][row] = serialized value for non-updated columns
  containers::FlatHashMap<catalog::Column::Id, std::vector<std::string>>
    saved_columns;

  // update_pk support: when a PK column is in the SET clause
  bool update_pk = false;
  std::vector<duckdb_primary_key::PKColumn> new_pk_columns;
  std::vector<std::string> new_row_keys;
  containers::FlatHashSet<std::string> batch_keys;

  // PK column names for error messages
  std::vector<std::string> pk_col_names;
  // Set of column IDs that are being updated, for fast lookup
  containers::FlatHashSet<catalog::Column::Id> update_col_id_set;

  // Non-updated columns that are part of at least one index.
  // Their chunk position is the idx_virtual slot assigned in PlanUpdate.
  struct NonUpdateIdxColMeta {
    catalog::Column::Id id;
    duckdb::LogicalType duckdb_type;
    duckdb::idx_t chunk_idx;
    catalog::ColumnStoreMode store_mode;
  };
  std::vector<NonUpdateIdxColMeta> non_update_idx_cols;

  DuckDBWriteConflictResolver conflict_resolver;

  std::shared_ptr<TableShard> table_shard;
  std::shared_lock<std::shared_mutex> table_lock;
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
    _bound_constraints(std::move(bound_constraints)) {
  // Detect if any PK column is being updated.
  const auto& pk_col_ids = _table->PKColumns();
  const auto& columns = _table->Columns();

  // Map PK column IDs to their table column indices.
  std::vector<std::pair<catalog::Column::Id, size_t>> pk_id_to_table_idx;
  for (auto pk_id : pk_col_ids) {
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == pk_id) {
        pk_id_to_table_idx.emplace_back(pk_id, i);
        break;
      }
    }
  }

  for (size_t pi = 0; pi < pk_id_to_table_idx.size(); ++pi) {
    auto [pk_id, pk_table_idx] = pk_id_to_table_idx[pi];
    for (size_t ui = 0; ui < _update_columns.size(); ++ui) {
      if (_update_columns[ui].index == pk_table_idx) {
        _update_pk = true;
        break;
      }
    }
    if (_update_pk) {
      break;
    }
  }

  if (_update_pk) {
    // Build _new_pk_col_indices: for each PK column, point to the SET position
    // if it's being updated, or the pk_virtual position if not.
    _new_pk_col_indices.reserve(pk_id_to_table_idx.size());
    for (size_t pi = 0; pi < pk_id_to_table_idx.size(); ++pi) {
      auto [pk_id, pk_table_idx] = pk_id_to_table_idx[pi];
      bool found = false;
      for (size_t ui = 0; ui < _update_columns.size(); ++ui) {
        if (_update_columns[ui].index == pk_table_idx) {
          _new_pk_col_indices.push_back(ui);  // SET position in chunk
          found = true;
          break;
        }
      }
      if (!found) {
        _new_pk_col_indices.push_back(_pk_col_indices[pi]);  // pk_virtual
      }
    }
  }
}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalUpdate::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SereneDBUpdateGlobalState>(
    duckdb::BufferAllocator::Get(context));

  state->cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  SDB_ASSERT(state->cf);

  state->table_id = _table->GetId();
  state->table_key = key_utils::PrepareTableKey(state->table_id);
  state->table_name = _table->GetName();
  state->update_pk = _update_pk;

  auto& conn_ctx = GetSereneDBContext(context);
  state->table_shard =
    conn_ctx.EnsureCatalogSnapshot()->GetTableShard(state->table_id);
  SDB_ASSERT(state->table_shard);
  // Re-added in M4 PR 4.1: the SCAN guard in GetScanFunction no longer
  // catches DML on kSearch shards (it now dispatches SELECT to the
  // search scan). UPDATE / DELETE / TRUNCATE land in M6.
  RejectIfSearchTable(*state->table_shard, "UPDATE");
  state->table_lock = std::shared_lock{state->table_shard->GetTableLock()};

  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();

  for (const auto& col : columns) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->all_columns.push_back(SereneDBUpdateGlobalState::ColumnMeta{
      .id = col.GetId(),
      .duckdb_type = col.type,
      .store_mode = col.store_mode,
    });
  }

  for (size_t i = 0; i < _update_columns.size(); ++i) {
    auto table_col_idx = _update_columns[i].index;
    const auto& col = columns[table_col_idx];
    state->update_columns.push_back(UpdateColumnMeta{
      .id = col.GetId(),
      .duckdb_type = col.type,
      .table_col_idx = table_col_idx,
      .store_mode = col.store_mode,
    });
    state->update_col_id_set.insert(col.GetId());
  }

  for (size_t i = 0; i < _pk_col_indices.size(); ++i) {
    duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
    if (i < pk_col_ids.size()) {
      for (const auto& col : columns) {
        if (col.GetId() == pk_col_ids[i]) {
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

  // Build new PK columns and PK column names for update_pk path.
  if (_update_pk) {
    for (size_t i = 0; i < _new_pk_col_indices.size(); ++i) {
      duckdb::LogicalType pk_type = duckdb::LogicalType::BIGINT;
      if (i < pk_col_ids.size()) {
        for (const auto& col : columns) {
          if (col.GetId() == pk_col_ids[i]) {
            pk_type = col.type;
            break;
          }
        }
      }
      state->new_pk_columns.push_back(duckdb_primary_key::PKColumn{
        .input_col_idx = _new_pk_col_indices[i],
        .type = pk_type,
      });
    }
    for (auto pk_id : pk_col_ids) {
      for (const auto& col : columns) {
        if (col.GetId() == pk_id) {
          state->pk_col_names.emplace_back(col.GetName());
          break;
        }
      }
    }
  }

  state->txn = &conn_ctx.GetRocksDBTransaction();
  state->sdb_txn = &conn_ctx;
  state->conflict_resolver.Init(*state->txn, *state->cf,
                                duckdb::OnConflictAction::THROW,
                                state->table_name);

  // Reused by both the marker-need check and the indexed-column mapping.
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto indexes = snapshot->GetIndexesByRelation(state->table_id);

  state->needs_rd_markers = NeedsRowDeleteMarkers(indexes, columns);

  // Build column-ID-to-chunk-position mapping.
  // Chunk layout: [SET_vals..., pk_virtuals..., non_pk_virtuals..., rowid]
  // _indexed_col_indices maps 1:1 to non-PK non-gen columns in table order.
  ColumnChunkMapping del_col_mapping;

  // PK columns
  for (size_t i = 0; i < _pk_col_indices.size() && i < pk_col_ids.size(); ++i) {
    del_col_mapping[pk_col_ids[i]] = _pk_col_indices[i];
  }

  // Indexed non-PK columns -- _indexed_col_indices maps 1:1 to them in table
  // column order (matching PlanUpdate's non_pk_idx construction from sorted
  // idx_col_indices minus PK positions). Other non-PK columns are NOT in the
  // chunk and must not be added here.
  {
    containers::FlatHashSet<catalog::Column::Id> pk_id_set(pk_col_ids.begin(),
                                                           pk_col_ids.end());
    containers::FlatHashSet<catalog::Column::Id> indexed_col_ids;
    for (auto& index : indexes) {
      for (auto col_id : index->GetReferencedColumnIds()) {
        if (!pk_id_set.contains(col_id)) {
          indexed_col_ids.insert(col_id);
        }
      }
    }
    std::vector<catalog::Column::Id> non_pk_idx_col_ids;
    non_pk_idx_col_ids.reserve(indexed_col_ids.size());
    for (size_t i = 0; i < columns.size(); ++i) {
      if (columns[i].GetId() == catalog::Column::kGeneratedPKId) {
        continue;
      }
      if (indexed_col_ids.contains(columns[i].GetId())) {
        non_pk_idx_col_ids.push_back(columns[i].GetId());
      }
    }
    SDB_ASSERT(non_pk_idx_col_ids.size() == _indexed_col_indices.size());
    for (size_t i = 0; i < _indexed_col_indices.size(); ++i) {
      del_col_mapping[non_pk_idx_col_ids[i]] = _indexed_col_indices[i];
    }

    for (size_t i = 0; i < _indexed_col_indices.size(); ++i) {
      auto col_id = non_pk_idx_col_ids[i];
      if (state->update_col_id_set.contains(col_id)) {
        continue;
      }
      for (const auto& col : columns) {
        if (col.GetId() == col_id) {
          state->non_update_idx_cols.push_back(
            SereneDBUpdateGlobalState::NonUpdateIdxColMeta{
              .id = col_id,
              .duckdb_type = col.type,
              .chunk_idx = _indexed_col_indices[i],
              .store_mode = col.store_mode,
            });
          break;
        }
      }
    }

    // Also handle PK columns that are explicitly listed in an index (e.g.
    // inverted indexes index PK as a searchable field). Secondary index writers
    // ignore them via SwitchColumn returning false.
    for (size_t i = 0; i < _pk_col_indices.size() && i < pk_col_ids.size();
         ++i) {
      auto pk_col_id = pk_col_ids[i];
      if (state->update_col_id_set.contains(pk_col_id)) {
        continue;
      }
      bool in_any_index = false;
      for (auto& index : indexes) {
        for (auto idx_col_id : index->GetColumnIds()) {
          if (idx_col_id == pk_col_id) {
            in_any_index = true;
            break;
          }
        }
        if (in_any_index) {
          break;
        }
      }
      if (!in_any_index) {
        continue;
      }
      for (const auto& col : columns) {
        if (col.GetId() == pk_col_id) {
          state->non_update_idx_cols.push_back(
            SereneDBUpdateGlobalState::NonUpdateIdxColMeta{
              .id = pk_col_id,
              .duckdb_type = col.type,
              .chunk_idx = _pk_col_indices[i],
              .store_mode = col.store_mode,
            });
          break;
        }
      }
    }
  }

  // When update_pk, ALL secondary indexes must be refreshed because PK bytes
  // appear in every index entry's suffix. Pass empty updated_col_ids so
  // CreateDuckDBIndexWriters creates writers for ALL indexes.
  // Otherwise, only indexes whose columns overlap with the SET clause.
  std::vector<catalog::Column::Id> updated_col_ids;
  if (!_update_pk) {
    for (const auto& upd : state->update_columns) {
      updated_col_ids.push_back(upd.id);
    }
  }

  // Update writers: Write() uses ins_col_mapping (new values),
  //                 DeleteRow() uses del_col_mapping (old values).
  // Same as old path: single set of writers handles both delete + insert.
  ColumnChunkMapping ins_col_mapping =
    del_col_mapping;  // PK + old indexed cols
  for (size_t i = 0; i < state->update_columns.size(); ++i) {
    ins_col_mapping[state->update_columns[i].id] = i;
  }
  if (_update_pk) {
    for (size_t i = 0; i < pk_col_ids.size() && i < _new_pk_col_indices.size();
         ++i) {
      ins_col_mapping[pk_col_ids[i]] = _new_pk_col_indices[i];
    }
  }

  state->index_writers = CreateDuckDBIndexWriters<DuckDBWriteKind::Update>(
    state->table_id, conn_ctx, *_table, ins_col_mapping, updated_col_ids,
    del_col_mapping);

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

  // chunk layout: [resolved SET vals, pk_virtuals, idx_virtuals, rowid].
  // _update_columns names the leading SET slots; _pk_col_indices tells the
  // verifier where to find each PK for the "Failing row contains" detail.
  VerifyUpdateConstraints(context.client, *_table, _bound_constraints, chunk,
                          _update_columns, _pk_col_indices);

  // Init index writers (single set handles both DeleteRow and Write).
  for (auto& writer : gstate.index_writers) {
    writer->Init(num_rows, chunk);
  }

  if (gstate.update_pk) {
    // --- UPDATE PK path: delete old rows, then write all columns at new keys.

    std::vector<duckdb::UnifiedVectorFormat> new_pk_formats;
    duckdb_primary_key::PreparePKFormats(chunk, gstate.new_pk_columns,
                                         new_pk_formats);
    std::vector<duckdb::UnifiedVectorFormat> old_pk_formats;
    duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns,
                                         old_pk_formats);

    // 1. Build NEW keys, check intra-batch duplicates, lock new rows.
    gstate.new_row_keys.clear();
    gstate.new_row_keys.reserve(num_rows);
    gstate.batch_keys.clear();
    gstate.batch_keys.reserve(num_rows);

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      auto& key_buffer = gstate.new_row_keys.emplace_back();
      duckdb_primary_key::MakeColumnKey(
        new_pk_formats, gstate.new_pk_columns, row, gstate.table_key,
        [&](std::string_view row_key) {
          auto pk_bytes = row_key.substr(sizeof(ObjectId));
          if (!gstate.batch_keys.emplace(std::string(pk_bytes)).second) {
            THROW_SQL_ERROR(
              ERR_CODE(ERRCODE_UNIQUE_VIOLATION),
              ERR_MSG("duplicate key value violates unique constraint \"",
                      gstate.table_name, "_pkey\""),
              ERR_DETAIL(BuildPKViolationDetail(chunk, gstate.new_pk_columns,
                                                gstate.pk_col_names, row)));
          }
          auto status = txn->GetKeyLock(gstate.cf, row_key, false, true);
          if (!status.ok()) {
            auto result = rocksutils::ConvertStatus(status);
            SDB_THROW(result.errorNumber(),
                      "Failed to acquire row lock for table ",
                      gstate.table_id.id(), " error: ", result.errorMessage());
          }
        },
        key_buffer);
      key_utils::SetupColumnForKey(key_buffer, gstate.all_columns.front().id);
    }

    // 2. Build OLD keys, lock old rows, delete old index entries.
    gstate.row_keys.clear();
    gstate.row_keys.reserve(num_rows);
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      auto& key_buffer = gstate.row_keys.emplace_back();
      duckdb_primary_key::MakeColumnKey(
        old_pk_formats, gstate.pk_columns, row, gstate.table_key,
        [&](std::string_view row_key) {
          auto status = txn->GetKeyLock(gstate.cf, row_key, false, true);
          if (!status.ok()) {
            auto result = rocksutils::ConvertStatus(status);
            SDB_THROW(result.errorNumber(),
                      "Failed to acquire row lock for table ",
                      gstate.table_id.id(), " error: ", result.errorMessage());
          }
          auto pk_bytes = row_key.substr(sizeof(ObjectId));
          for (auto& writer : gstate.index_writers) {
            writer->DeleteRow(pk_bytes);
          }
        },
        key_buffer);
      // Align layout with new_row_keys so the "old == new" fast-path in
      // HandleWriteConflicts<true> can detect a PK-preserving update by
      // byte-equality. Without this, the column_id slot differs (new keys
      // have the first column id; old keys have leftover object_id bytes),
      // and a same-PK update is incorrectly flagged as a duplicate.
      key_utils::SetupColumnForKey(key_buffer, gstate.all_columns.front().id);
    }

    // 3. Conflict detection: check new keys against existing DB data.
    gstate.conflict_resolver.HandleWriteConflicts<true>(
      gstate.new_row_keys, chunk, gstate.new_pk_columns, gstate.pk_col_names,
      gstate.row_keys);

    // 4. Read non-updated column values (must happen before delete).
    rocksdb::ReadOptions read_opts;
    read_opts.snapshot = txn->GetSnapshot();
    rocksdb::PinnableSlice rewrite_value;
    gstate.saved_columns.clear();

    for (const auto& col : gstate.all_columns) {
      if (gstate.update_col_id_set.contains(col.id)) {
        continue;
      }
      auto& col_vals = gstate.saved_columns[col.id];
      col_vals.resize(num_rows);
      for (duckdb::idx_t row = 0; row < num_rows; ++row) {
        key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
        rewrite_value.Reset();
        col_vals[row].clear();
        auto s =
          txn->Get(read_opts, gstate.cf, gstate.row_keys[row], &rewrite_value);
        if (s.ok()) {
          col_vals[row].assign(rewrite_value.data(), rewrite_value.size());
        } else if (!s.IsNotFound()) {
          SDB_THROW(ERROR_INTERNAL, "RocksDB Get error: ", s.ToString());
        }
      }
    }

    // 5. Delete old rows (same as SereneDBPhysicalDelete::Sink).
    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      for (const auto& col : gstate.all_columns) {
        key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
        auto s = txn->Delete(gstate.cf, gstate.row_keys[row]);
        if (!s.ok()) {
          SDB_THROW(ERROR_INTERNAL, "RocksDB Delete error: ", s.ToString());
        }
      }
      // Row-level marker for indexes that normal WAL replay would miss;
      // see `needs_rd_markers` in the gstate definition.
      if (gstate.needs_rd_markers) {
        indexonly_marker::EmitRD(*gstate.sdb_txn, gstate.row_keys[row]);
      }
    }

    // 6. Write ALL columns at new keys.
    DuckDBColumnSerializer::TxnWriter txn_writer{*gstate.sdb_txn, gstate.cf};

    std::vector<std::string_view> view_new_row_keys{gstate.new_row_keys.begin(),
                                                    gstate.new_row_keys.end()};

    // Updated columns: from SET positions in the chunk.
    for (duckdb::idx_t i = 0; i < gstate.update_columns.size(); ++i) {
      const auto& col = gstate.update_columns[i];
      const ColumnDescriptor desc{col.id, col.store_mode, col.duckdb_type};
      gstate.active_writers.clear();
      for (auto& writer : gstate.index_writers) {
        if (writer->SwitchColumn(desc, chunk.data[i], view_new_row_keys,
                                 num_rows)) {
          gstate.active_writers.push_back(
            basics::downCast<DuckDBSinkColumnWriter>(writer.get()));
        }
      }
      for (duckdb::idx_t row = 0; row < num_rows; ++row) {
        key_utils::SetupColumnForKey(gstate.new_row_keys[row], col.id);
      }
      gstate.serializer.WriteColumn(txn_writer, chunk.data[i], num_rows,
                                    gstate.new_row_keys, gstate.active_writers,
                                    desc);
    }

    // Trigger index writers whose first column is not in the SET clause.
    // Use SstWriter{nullptr} to skip RocksDB puts (values already written
    // above); index Write() calls still fire via WriteRowSlices.
    {
      DuckDBColumnSerializer::SstWriter noop_writer{nullptr};
      for (const auto& col : gstate.non_update_idx_cols) {
        const ColumnDescriptor desc{col.id, col.store_mode, col.duckdb_type};
        gstate.active_writers.clear();
        for (auto& writer : gstate.index_writers) {
          if (writer->SwitchColumn(desc, chunk.data[col.chunk_idx],
                                   view_new_row_keys, num_rows)) {
            gstate.active_writers.push_back(
              basics::downCast<DuckDBSinkColumnWriter>(writer.get()));
          }
        }
        if (gstate.active_writers.empty()) {
          continue;
        }
        for (duckdb::idx_t row = 0; row < num_rows; ++row) {
          key_utils::SetupColumnForKey(gstate.new_row_keys[row], col.id);
        }
        gstate.serializer.WriteColumn(noop_writer, chunk.data[col.chunk_idx],
                                      num_rows, gstate.new_row_keys,
                                      gstate.active_writers, desc);
      }
    }

    // Non-updated columns: from saved values via Put.
    for (auto& [col_id, col_vals] : gstate.saved_columns) {
      for (duckdb::idx_t row = 0; row < num_rows; ++row) {
        if (col_vals[row].empty()) {
          continue;  // NULL -- no entry
        }
        key_utils::SetupColumnForKey(gstate.new_row_keys[row], col_id);
        auto s = txn->Put(gstate.cf, gstate.new_row_keys[row], col_vals[row]);
        if (!s.ok()) {
          SDB_THROW(ERROR_INTERNAL, "RocksDB Put error: ", s.ToString());
        }
      }
    }
  } else {
    // --- Normal (non-PK-update) path ---

    // 1. Build row keys, lock rows, delete old index entries
    gstate.row_keys.clear();
    gstate.row_keys.reserve(num_rows);

    std::vector<duckdb::UnifiedVectorFormat> pk_formats;
    duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

    for (duckdb::idx_t row = 0; row < num_rows; ++row) {
      auto& key_buffer = gstate.row_keys.emplace_back();
      duckdb_primary_key::MakeColumnKey(
        pk_formats, gstate.pk_columns, row, gstate.table_key,
        [&](std::string_view row_key) {
          auto status = txn->GetKeyLock(gstate.cf, row_key, false, true);
          if (!status.ok()) {
            auto result = rocksutils::ConvertStatus(status);
            SDB_THROW(result.errorNumber(),
                      "Failed to acquire row lock for table ",
                      gstate.table_id.id(), " error: ", result.errorMessage());
          }
          auto pk_bytes = row_key.substr(sizeof(ObjectId));
          for (auto& writer : gstate.index_writers) {
            writer->DeleteRow(pk_bytes);
          }
        },
        key_buffer);
    }

    // 2. Write updated columns (index writers get new values via Write)
    DuckDBColumnSerializer::TxnWriter txn_writer{*gstate.sdb_txn, gstate.cf};

    std::vector<std::string_view> view_row_keys{gstate.row_keys.begin(),
                                                gstate.row_keys.end()};

    for (duckdb::idx_t i = 0; i < gstate.update_columns.size(); ++i) {
      const auto& col = gstate.update_columns[i];
      const ColumnDescriptor desc{col.id, col.store_mode, col.duckdb_type};
      gstate.active_writers.clear();
      for (auto& writer : gstate.index_writers) {
        if (writer->SwitchColumn(desc, chunk.data[i], view_row_keys,
                                 num_rows)) {
          gstate.active_writers.push_back(
            basics::downCast<DuckDBSinkColumnWriter>(writer.get()));
        }
      }

      for (duckdb::idx_t row = 0; row < num_rows; ++row) {
        key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
      }
      gstate.serializer.WriteColumn(txn_writer, chunk.data[i], num_rows,
                                    gstate.row_keys, gstate.active_writers,
                                    desc);
    }

    // Trigger index writers whose first column is not in the SET clause.
    {
      DuckDBColumnSerializer::SstWriter noop_writer{nullptr};
      for (const auto& col : gstate.non_update_idx_cols) {
        const ColumnDescriptor desc{col.id, col.store_mode, col.duckdb_type};
        gstate.active_writers.clear();
        for (auto& writer : gstate.index_writers) {
          if (writer->SwitchColumn(desc, chunk.data[col.chunk_idx],
                                   view_row_keys, num_rows)) {
            gstate.active_writers.push_back(
              basics::downCast<DuckDBSinkColumnWriter>(writer.get()));
          }
        }
        if (gstate.active_writers.empty()) {
          continue;
        }
        for (duckdb::idx_t row = 0; row < num_rows; ++row) {
          key_utils::SetupColumnForKey(gstate.row_keys[row], col.id);
        }
        gstate.serializer.WriteColumn(noop_writer, chunk.data[col.chunk_idx],
                                      num_rows, gstate.row_keys,
                                      gstate.active_writers, desc);
      }
    }

    // Re-index every catalog-registered expression for each row. Build a
    // slot_to_col_id mapping that matches the chunk layout we've fed
    // through above so EvaluateExprOverChunk can resolve dep-column refs.
    std::vector<catalog::Column::Id> slot_to_col_id;
    slot_to_col_id.reserve(gstate.update_columns.size() +
                           gstate.non_update_idx_cols.size());
    for (const auto& col : gstate.update_columns) {
      slot_to_col_id.push_back(col.id);
    }
    // Non-update cols sit at their `chunk_idx`; pad slot_to_col_id with
    // kInvalidId so direct indexing into chunk.data[chunk_idx] resolves
    // to the right Column::Id at that slot.
    for (const auto& col : gstate.non_update_idx_cols) {
      while (slot_to_col_id.size() <= col.chunk_idx) {
        slot_to_col_id.push_back(catalog::Column::kInvalidId);
      }
      slot_to_col_id[col.chunk_idx] = col.id;
    }
    for (auto& writer : gstate.index_writers) {
      auto exprs = writer->IndexedExpressions();
      if (exprs.empty()) {
        continue;
      }
      EvaluateAndWriteIndexedExpressions(*writer, exprs, chunk, gstate.table_id,
                                         slot_to_col_id, context.client,
                                         num_rows, gstate.row_keys);
    }
  }

  for (auto& writer : gstate.index_writers) {
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
