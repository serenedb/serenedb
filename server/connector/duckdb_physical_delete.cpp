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

#include <absl/synchronization/mutex.h>

#include <duckdb/common/types/data_chunk.hpp>
#include <shared_mutex>

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_utils.h"
#include "connector/duckdb_primary_key.h"
#include "connector/duckdb_rocksdb_writer.h"
#include "connector/duckdb_table_entry.h"
#include "connector/indexonly_marker.h"
#include "connector/key_utils.hpp"
#include "pg/connection_context.h"
#include "rocksdb/utilities/transaction_db.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/table_shard.h"

namespace sdb::connector {

struct DeleteColumnMeta {
  catalog::Column::Id id;
  duckdb::LogicalType duckdb_type;
};

struct SereneDBDeleteGlobalState : public duckdb::GlobalSinkState {
  duckdb::idx_t delete_count = 0;

  ObjectId table_id;
  std::string table_key;
  std::vector<DeleteColumnMeta> columns;

  std::vector<duckdb_primary_key::PKColumn> pk_columns;

  rocksdb::ColumnFamilyHandle* cf = nullptr;
  rocksdb::Transaction* txn = nullptr;
  // sdb-side transaction; the IndexOnly writer registers markers on it.
  query::Transaction* sdb_txn = nullptr;

  // True iff some inverted index on the table is built exclusively over
  // IndexOnly columns and would not see the row delete through normal
  // WAL replay. Drives the per-row [RD] marker emission.
  bool needs_rd_markers = false;

  // Index writers
  std::vector<std::unique_ptr<DuckDBSinkIndexWriter>> index_writers;

  // Reusable buffers
  std::vector<std::string> row_keys;

  std::shared_ptr<TableShard> table_shard;
  std::shared_lock<std::shared_mutex> table_lock;
};

struct SereneDBDeleteSourceState : public duckdb::GlobalSourceState {
  bool finished = false;
};

SereneDBPhysicalDelete::SereneDBPhysicalDelete(
  duckdb::PhysicalPlan& plan, std::shared_ptr<catalog::Table> table,
  std::vector<duckdb::idx_t> pk_col_indices,
  std::vector<duckdb::idx_t> indexed_col_indices,
  duckdb::idx_t estimated_cardinality)
  : duckdb::PhysicalOperator(plan, duckdb::PhysicalOperatorType::EXTENSION,
                             {duckdb::LogicalType::BIGINT},
                             estimated_cardinality),
    _table(std::move(table)),
    _pk_col_indices(std::move(pk_col_indices)),
    _indexed_col_indices(std::move(indexed_col_indices)) {}

duckdb::unique_ptr<duckdb::GlobalSinkState>
SereneDBPhysicalDelete::GetGlobalSinkState(
  duckdb::ClientContext& context) const {
  auto state = duckdb::make_uniq<SereneDBDeleteGlobalState>();

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

  const auto& columns = _table->Columns();
  const auto& pk_col_ids = _table->PKColumns();

  for (const auto& col : columns) {
    if (col.GetId() == catalog::Column::kGeneratedPKId) {
      continue;
    }
    state->columns.push_back(DeleteColumnMeta{
      .id = col.GetId(),
      .duckdb_type = col.type,
    });
  }

  // PK columns -- map input chunk positions to types
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

  state->txn = &conn_ctx.GetRocksDBTransaction();
  state->sdb_txn = &conn_ctx;

  // Reused by both the marker-need check and the indexed-column setup.
  auto snapshot = conn_ctx.EnsureCatalogSnapshot();
  auto indexes = snapshot->GetIndexesByRelation(state->table_id);

  // Decide whether row deletes need [RD] WAL markers; see
  // NeedsRowDeleteMarkers for the precise rule.
  state->needs_rd_markers = NeedsRowDeleteMarkers(indexes, columns);

  // Build column-ID-to-chunk-position mapping for index writers.
  // The scan output has: [..., pk_cols, indexed_cols, rowid].
  ColumnChunkMapping col_mapping;

  // PK columns
  for (size_t i = 0; i < _pk_col_indices.size() && i < pk_col_ids.size(); ++i) {
    col_mapping[pk_col_ids[i]] = _pk_col_indices[i];
  }

  // Indexed (non-PK) columns -- _indexed_col_indices maps 1:1 to the
  // non-PK indexed columns from GetRowIdColumns(), which are in the same
  // order as the table entry's indexed_col_indices.
  {
    // Reconstruct which column IDs are in the indexed positions
    containers::FlatHashSet<catalog::Column::Id> pk_id_set(pk_col_ids.begin(),
                                                           pk_col_ids.end());
    // Get all indexed table-column-indices (from the table entry)
    // We need to figure out which column IDs correspond to _indexed_col_indices
    // They're the non-PK indexed columns in sorted order
    std::vector<catalog::Column::Id> non_pk_idx_col_ids;
    containers::FlatHashSet<size_t> pk_table_indices;
    for (auto pk_id : pk_col_ids) {
      for (size_t i = 0; i < columns.size(); ++i) {
        if (columns[i].GetId() == pk_id) {
          pk_table_indices.insert(i);
          break;
        }
      }
    }
    containers::FlatHashSet<size_t> seen;
    for (auto& index : indexes) {
      for (auto col_id : index->GetColumnIds()) {
        for (size_t i = 0; i < columns.size(); ++i) {
          if (columns[i].GetId() == col_id && !pk_table_indices.contains(i) &&
              !seen.contains(i)) {
            seen.insert(i);
            non_pk_idx_col_ids.push_back(col_id);
            break;
          }
        }
      }
    }
    // Sort by table position (matching GetRowIdColumns order)
    std::sort(non_pk_idx_col_ids.begin(), non_pk_idx_col_ids.end(),
              [&](auto a, auto b) {
                size_t pos_a = 0, pos_b = 0;
                for (size_t i = 0; i < columns.size(); ++i) {
                  if (columns[i].GetId() == a) {
                    pos_a = i;
                  }
                  if (columns[i].GetId() == b) {
                    pos_b = i;
                  }
                }
                return pos_a < pos_b;
              });
    for (size_t i = 0;
         i < _indexed_col_indices.size() && i < non_pk_idx_col_ids.size();
         ++i) {
      col_mapping[non_pk_idx_col_ids[i]] = _indexed_col_indices[i];
    }
  }

  // Also map all table columns by their table position (for INSERT-style
  // writers)
  for (size_t i = 0; i < columns.size(); ++i) {
    if (!col_mapping.contains(columns[i].GetId())) {
      col_mapping[columns[i].GetId()] = i;  // fallback: table position
    }
  }

  state->index_writers = CreateDuckDBIndexWriters<DuckDBWriteKind::Delete>(
    state->table_id, conn_ctx, *_table, col_mapping);

  return state;
}

duckdb::SinkResultType SereneDBPhysicalDelete::Sink(
  duckdb::ExecutionContext& context, duckdb::DataChunk& chunk,
  duckdb::OperatorSinkInput& input) const {
  auto& gstate = input.global_state.Cast<SereneDBDeleteGlobalState>();

  const auto num_rows = chunk.size();
  if (num_rows == 0) {
    return duckdb::SinkResultType::NEED_MORE_INPUT;
  }

  auto* txn = gstate.txn;

  for (auto& writer : gstate.index_writers) {
    writer->Init(num_rows, chunk);
  }

  std::vector<duckdb::UnifiedVectorFormat> pk_formats;
  duckdb_primary_key::PreparePKFormats(chunk, gstate.pk_columns, pk_formats);

  std::string key_buffer;
  for (duckdb::idx_t row = 0; row < num_rows; ++row) {
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

    for (const auto& col : gstate.columns) {
      key_utils::SetupColumnForKey(key_buffer, col.id);
      auto status = txn->Delete(gstate.cf, key_buffer);
      if (!status.ok()) {
        SDB_THROW(sdb::ERROR_INTERNAL, "RocksDB delete failed: ", status.ToString());
      }
    }

    // Row-level marker for indexes that normal WAL replay would miss; see
    // `needs_rd_markers` in the gstate definition.
    if (gstate.needs_rd_markers) {
      indexonly_marker::EmitRD(*gstate.sdb_txn, key_buffer);
    }
  }

  for (auto& writer : gstate.index_writers) {
    writer->Finish();
  }

  gstate.delete_count += num_rows;
  return duckdb::SinkResultType::NEED_MORE_INPUT;
}

duckdb::SinkFinalizeType SereneDBPhysicalDelete::Finalize(
  duckdb::Pipeline& pipeline, duckdb::Event& event,
  duckdb::ClientContext& context,
  duckdb::OperatorSinkFinalizeInput& input) const {
  auto& gstate = sink_state->Cast<SereneDBDeleteGlobalState>();
  if (gstate.delete_count > 0) {
    auto& conn_ctx = GetSereneDBContext(context);
    conn_ctx.UpdateNumRows(gstate.table_id, -gstate.delete_count);
  }
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
  chunk.SetValue(0, 0, duckdb::Value::BIGINT(gstate.delete_count));
  return duckdb::SourceResultType::HAVE_MORE_OUTPUT;
}

}  // namespace sdb::connector
