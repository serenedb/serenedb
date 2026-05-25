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

#include "connector/duckdb_scan_base.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <iresearch/columnstore/format.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/directory_reader_impl.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/store/directory.hpp>
#include <numeric>
#include <ranges>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/string_utils.h"
#include "catalog/inverted_index.h"
#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_rocksdb_reader.h"
#include "connector/duckdb_table_entry.h"
#include "connector/duckdb_table_function.h"
#include "connector/key_utils.hpp"
#include "connector/primary_key.hpp"
#include "pg/connection_context.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"
#include "query/duckdb_engine.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"

namespace sdb::connector {

CommonScanGlobalState::~CommonScanGlobalState() {
  // Only release if we took the snapshot via db->GetSnapshot().
  // When using a transaction, the snapshot is owned by the transaction.
  if (snapshot && !txn) {
    GetServerEngine().db()->ReleaseSnapshot(snapshot);
  }
  snapshot = nullptr;
}

void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input) {
  auto& engine = GetServerEngine();
  auto* db = engine.db();

  // When inside BEGIN/COMMIT, use the connection's transaction so the scan
  // sees the transaction's own uncommitted writes (read-your-writes).
  // Outside a transaction, use a DB snapshot for read-only scans.
  // If sdb_read_your_own_writes is false, always use a DB snapshot so reads
  // see only committed data even within an explicit transaction.
  auto& conn_ctx = GetSereneDBContext(context);
  const bool is_search_scan = bind_data.scan_source->IsSearchLike();
  if (is_search_scan && conn_ctx.GetReadYourOwnWrites() &&
      conn_ctx.HasRocksDBTransaction()) {
    SDB_THROW(ERROR_NOT_IMPLEMENTED,
              "querying an index within a transaction is not supported when "
              "sdb_read_your_own_writes is enabled");
  }
  if (conn_ctx.GetReadYourOwnWrites() && conn_ctx.HasRocksDBTransaction()) {
    state.txn = &conn_ctx.GetRocksDBTransaction();
    state.snapshot = &conn_ctx.GetRocksDBSnapshot();
  } else {
    state.snapshot = db->GetSnapshot();
  }

  // Determine which columns DuckDB actually wants (projection pushdown).
  const auto num_bind_columns = bind_data.column_ids.size();
  for (auto col_id : input.column_ids) {
    if (col_id == kColumnIdentifierGeneratedPk) {
      state.scan_rowid = true;
      state.rowid_output_idx = state.projected_columns.size();
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == kColumnIdentifierTableOid) {
      state.scan_tableoid = true;
      state.tableoid_output_idx = state.projected_columns.size();
      state.tableoid_value = static_cast<int64_t>(bind_data.RelationId().id());
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BIGINT);
    } else if (col_id == duckdb::COLUMN_IDENTIFIER_EMPTY) {
      state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
      state.projected_types.push_back(duckdb::LogicalType::BOOLEAN);
    } else if (col_id >= duckdb::VIRTUAL_COLUMN_START) {
      SDB_ASSERT(!bind_data.IsViewBacked(),
                 "virtual PK columns are not used for view-backed scans");
      auto cat_idx = SereneDBTableEntry::VirtualToPKColumnIndex(col_id);
      SDB_ASSERT(cat_idx != duckdb::DConstants::INVALID_INDEX);
      const auto& tbd = bind_data.As<TableScanBindData>();
      const auto& catalog_cols = tbd.table->Columns();
      SDB_ASSERT(cat_idx < catalog_cols.size());
      const auto catalog_col_id = catalog_cols[cat_idx].GetId();
      duckdb::idx_t bind_idx = duckdb::DConstants::INVALID_INDEX;
      for (duckdb::idx_t i = 0; i < bind_data.column_ids.size(); ++i) {
        if (bind_data.column_ids[i] == catalog_col_id) {
          bind_idx = i;
          break;
        }
      }
      SDB_ASSERT(bind_idx != duckdb::DConstants::INVALID_INDEX);
      state.projected_columns.push_back(bind_idx);
      state.projected_types.push_back(bind_data.column_types[bind_idx]);
    } else if (col_id < num_bind_columns) {
      const auto catalog_col_id = bind_data.column_ids[col_id];
      if (catalog_col_id == catalog::Column::kInvertedIndexScoreId) {
        state.scan_score = true;
        state.score_output_idx = state.projected_columns.size();
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(duckdb::LogicalType::FLOAT);
      } else if (catalog_col_id == catalog::Column::kInvertedIndexOffsetsId) {
        state.projected_columns.push_back(duckdb::DConstants::INVALID_INDEX);
        state.projected_types.push_back(catalog::Column::MakeOffsetsType());
      } else {
        // Real column read. For view-backed indexes the materialisation
        // path is wired in a follow-up; the projected_columns entry is
        // recorded here, but the actual projection-time check (and error)
        // happens in the search-scan operators where we know whether the
        // optimizer actually swapped the function to one that bypasses
        // materialisation (e.g. IRESEARCH_COUNT for count(*)).
        //
        // An IndexOnly column reaching the materialisation path means one
        // of: (1) a query asks for its value -- reject, no main-storage
        // source exists; (2) CREATE INDEX backfill -- mark finished so
        // pre-existing rows are skipped honestly (future INSERTs will
        // index normally).
        if (!bind_data.IsViewBacked()) {
          const auto& tbd = bind_data.As<TableScanBindData>();
          for (const auto& col : tbd.table->Columns()) {
            if (col.GetId() != catalog_col_id ||
                col.store_mode != catalog::ColumnStoreMode::kIndexOnly) {
              continue;
            }
            if (bind_data.is_create_index) {
              state.finished = true;
            } else {
              THROW_SQL_ERROR(
                ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                ERR_MSG(
                  "column \"", col.GetName(),
                  "\" has sdb_indexonly storage and cannot be read directly;"
                  " it is only accessible through an inverted-index search"
                  " predicate"));
            }
            break;
          }
        }
        state.projected_columns.push_back(col_id);
        state.projected_types.push_back(bind_data.column_types[col_id]);
      }
    }
  }

  // View-backed scans use synthetic PKs (file-position rowids) that
  // behave like generated PKs for the rocksdb-flavoured bookkeeping.
  state.has_generated_pk =
    bind_data.IsViewBacked()
      ? true
      : bind_data.As<TableScanBindData>().table->PKColumns().empty();

  state.external_projected_columns = state.projected_columns;
}

void ClassifyColumnstoreProjections(CommonScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data) {
  if (!bind_data.IsInvertedIndexEntry() || !bind_data.inverted_index) {
    state.has_external_projections = absl::c_any_of(
      state.projected_columns,
      [](auto p) { return p != duckdb::DConstants::INVALID_INDEX; });
    return;
  }
  for (duckdb::idx_t proj = 0; proj < state.projected_columns.size(); ++proj) {
    const auto bind_col = state.projected_columns[proj];
    if (bind_col == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    const auto col_id = bind_data.column_ids[bind_col];
    const auto* info = bind_data.inverted_index->FindColumnInfo(col_id);
    const bool is_blob_synthetic_pk =
      col_id == catalog::Column::kGeneratedPKId &&
      state.projected_types[proj].id() == duckdb::LogicalTypeId::BLOB;
    if ((info && info->store_values) || is_blob_synthetic_pk) {
      state.cs_projections.push_back({proj, col_id});
      state.external_projected_columns[proj] =
        duckdb::DConstants::INVALID_INDEX;
      continue;
    }
    state.has_external_projections = true;
  }
  state.cs_field_ids.reserve(state.cs_projections.size());
  state.cs_output_slots.reserve(state.cs_projections.size());
  for (const auto& cp : state.cs_projections) {
    state.cs_field_ids.push_back(static_cast<irs::field_id>(cp.column_id));
    state.cs_output_slots.push_back(cp.output_slot);
  }
}

ColumnstoreMaterializer* GetOrOpenSegmentMaterializer(
  CommonScanGlobalState& gstate, const irs::IndexReader& reader,
  size_t seg_idx) {
  if (gstate.cs_field_ids.empty() || seg_idx >= reader.size()) {
    return nullptr;
  }
  // Parallel scans (ANN/BM25 top-K) call this from multiple threads.
  // The resize + slot lazy-build must be guarded; once a slot is filled,
  // it's immutable for the rest of the query, so subsequent reads are
  // safe outside the mutex.
  std::lock_guard lock{gstate.cs_materializers_mu};
  if (gstate.cs_materializers.size() < reader.size()) {
    gstate.cs_materializers.resize(reader.size());
  }
  auto& slot = gstate.cs_materializers[seg_idx];
  if (!slot) {
    const auto* cs_reader = reader[seg_idx].CsReader();
    if (!cs_reader) {
      return nullptr;
    }
    slot = std::make_unique<ColumnstoreMaterializer>(
      *cs_reader, gstate.cs_field_ids, gstate.cs_output_slots);
  }
  return slot.get();
}

void MaterializeIncludeColumnsBatched(CommonScanGlobalState& gstate,
                                      const irs::IndexReader& reader,
                                      std::span<const SegDoc> seg_doc_batched,
                                      duckdb::DataChunk& output) {
  const auto num_rows = seg_doc_batched.size();
  if (num_rows == 0 || gstate.cs_projections.empty()) {
    return;
  }

  // SelectByDocIds only resets LIST/MAP list size when output_start == 0,
  // which is fine for the first segment in the batch but misses any
  // LIST/MAP column that first appears in a later segment (e.g. cs
  // column present in seg 1 but not seg 0). Reset every LIST/MAP cs
  // projection up front so per-segment Materialize calls always start
  // from a clean offset table.
  for (const auto& cp : gstate.cs_projections) {
    const auto type_id = gstate.projected_types[cp.output_slot].id();
    if (type_id == duckdb::LogicalTypeId::LIST ||
        type_id == duckdb::LogicalTypeId::MAP) {
      duckdb::ListVector::SetListSize(output.data[cp.output_slot], 0);
    }
  }

  // Caller passes hits already sorted by (segment, doc) -- both because
  // consecutive same-segment runs let us batch one MaterializeNode call
  // per segment and because the columnstore reader is forward-only. Each
  // row's column value lands at slot `seg_start + k`, so the output
  // stays aligned with the score column / RocksDB columns (which also
  // write to their input-position slots). No internal sort, no dict_sel
  // un-shuffle.
  std::vector<irs::doc_id_t> seg_docs;
  size_t i = 0;
  while (i < num_rows) {
    const uint32_t seg_id = seg_doc_batched[i].segment_idx;
    const size_t seg_start = i;
    do {
      ++i;
    } while (i < num_rows && seg_doc_batched[i].segment_idx == seg_id);
    auto* mat = GetOrOpenSegmentMaterializer(gstate, reader, seg_id);
    if (!mat || !mat->HasAny()) {
      continue;
    }
    seg_docs.clear();
    seg_docs.reserve(i - seg_start);
    for (size_t k = seg_start; k < i; ++k) {
      seg_docs.push_back(seg_doc_batched[k].doc_pos);
    }
    mat->SelectByDocIds(seg_docs, output,
                        static_cast<duckdb::idx_t>(seg_start));
  }
}

constexpr size_t kScanKeyPrefixSize =
  sizeof(ObjectId) + sizeof(catalog::Column::Id);

duckdb::idx_t ReadGeneratedPKFromKeys(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows) {
  duckdb::idx_t count = 0;
  auto* data = duckdb::FlatVector::GetDataMutable<int64_t>(output);
  while (it.Valid() && count < max_rows) {
    auto key = it.key().ToStringView();
    SDB_ASSERT(key.size() >= kScanKeyPrefixSize + sizeof(int64_t));
    data[count] = primary_key::ReadSigned<int64_t>(
      std::string_view{key.begin() + kScanKeyPrefixSize, key.end()});
    ++count;
    it.Next();
  }
  rocksutils::CheckIteratorStatus(it);
  return count;
}

duckdb::idx_t CommonScanRowsScanned(duckdb::GlobalTableFunctionState& gstate_p,
                                    duckdb::LocalTableFunctionState&) {
  auto& gstate = gstate_p.Cast<CommonScanGlobalState>();
  return gstate.produced_rows.load(std::memory_order_relaxed);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* /*global_state*/) {
  return duckdb::make_uniq<CommonScanLocalState>();
}

void WriteVirtualColumns(CommonScanGlobalState& gstate, duckdb::idx_t num_rows,
                         duckdb::DataChunk& output,
                         std::span<const float> scores_or_empty) {
  for (duckdb::idx_t proj = 0; proj < gstate.projected_columns.size(); ++proj) {
    if (gstate.projected_columns[proj] != duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    if (gstate.scan_tableoid && proj == gstate.tableoid_output_idx) {
      output.data[proj].Reference(duckdb::Value::BIGINT(gstate.tableoid_value));
    } else if (gstate.scan_score && proj == gstate.score_output_idx) {
      // Empty scores_or_empty means the path wrote scores inline
      // (BM25 streaming) or there's no score column to populate.
      if (!scores_or_empty.empty()) {
        SDB_ASSERT(scores_or_empty.size() >= num_rows);
        auto* score_data =
          duckdb::FlatVector::GetDataMutable<float>(output.data[proj]);
        std::memcpy(score_data, scores_or_empty.data(),
                    num_rows * sizeof(float));
      }
    } else if (gstate.scan_rowid && proj == gstate.rowid_output_idx) {
      auto* data =
        duckdb::FlatVector::GetDataMutable<int64_t>(output.data[proj]);
      for (duckdb::idx_t i = 0; i < num_rows; ++i) {
        data[i] = static_cast<int64_t>(i);
      }
    }
  }
}

std::vector<std::string> InitPKScanColumns(
  PKScanGlobalState& state, const SereneDBScanBindData& bind_data) {
  // PK-keyed scans only fire on rocksdb-backed binds; view binds route
  // through the search-scan path well before reaching here.
  SDB_ASSERT(!bind_data.IsViewBacked(),
             "InitPKScanColumns: view-backed bind reached PK scan path");
  auto table_id = bind_data.As<TableScanBindData>().table->GetId();
  std::string table_key = key_utils::PrepareTableKey(table_id);

  std::vector<catalog::Column::Id> scan_column_ids;
  for (auto proj_idx : state.projected_columns) {
    if (proj_idx != duckdb::DConstants::INVALID_INDEX) {
      scan_column_ids.push_back(bind_data.column_ids[proj_idx]);
    }
  }

  if (scan_column_ids.empty() && !bind_data.column_ids.empty()) {
    scan_column_ids.push_back(bind_data.column_ids[0]);
  }

  if (state.has_generated_pk && state.scan_rowid &&
      !bind_data.column_ids.empty()) {
    scan_column_ids.insert(scan_column_ids.begin(), bind_data.column_ids[0]);
  }

  const auto num_scan = scan_column_ids.size();
  state.upper_bound_data.reserve(key_utils::kKeyPrefixSize * num_scan);
  state.upper_bound_slices.reserve(num_scan);

  std::vector<std::string> column_keys;
  column_keys.reserve(num_scan);

  for (auto column_id : scan_column_ids) {
    auto key = table_key;
    basics::StrResize(key, key_utils::kTablePrefixSize);

    state.upper_bound_data.append(key);
    key_utils::AppendColumnKey(state.upper_bound_data,
                               catalog::Column::Id{column_id.id() + 1});

    key_utils::AppendColumnKey(key, column_id);
    column_keys.push_back(std::move(key));
  }

  for (size_t i = 0; i < num_scan; ++i) {
    state.upper_bound_slices.emplace_back(
      state.upper_bound_data.data() + i * key_utils::kKeyPrefixSize,
      key_utils::kKeyPrefixSize);
  }

  return column_keys;
}

void PKScanFunctionImpl(
  CommonScanGlobalState& gstate,
  std::vector<std::unique_ptr<rocksdb::Iterator>>& iterators,
  duckdb::DataChunk& output) {
  if (gstate.finished || iterators.empty()) {
    output.SetCardinality(0);
    gstate.finished = true;
    return;
  }

  const duckdb::idx_t batch_size = STANDARD_VECTOR_SIZE;
  duckdb::idx_t count = 0;
  duckdb::idx_t iter_idx = 0;

  duckdb::idx_t first_real_output = duckdb::DConstants::INVALID_INDEX;
  for (duckdb::idx_t out = 0; out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] != duckdb::DConstants::INVALID_INDEX) {
      first_real_output = out;
      break;
    }
  }

  const duckdb::idx_t real_iter_start =
    (gstate.scan_rowid && gstate.has_generated_pk) ? 1 : 0;

  if (first_real_output != duckdb::DConstants::INVALID_INDEX) {
    count = ReadColumnIntoDuckDB(
      *iterators[real_iter_start], output.data[first_real_output],
      gstate.projected_types[first_real_output], batch_size);
    iter_idx = real_iter_start + 1;
  } else {
    auto& it = *iterators[real_iter_start];
    while (it.Valid() && count < batch_size) {
      ++count;
      it.Next();
    }
    rocksutils::CheckIteratorStatus(it);
    iter_idx = real_iter_start + 1;
  }

  if (count == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }

  for (duckdb::idx_t out =
         (first_real_output == duckdb::DConstants::INVALID_INDEX
            ? 0
            : first_real_output + 1);
       out < gstate.projected_columns.size(); ++out) {
    if (gstate.projected_columns[out] == duckdb::DConstants::INVALID_INDEX) {
      continue;
    }
    SDB_ASSERT(iter_idx < iterators.size());
    auto col_count =
      ReadColumnIntoDuckDB(*iterators[iter_idx], output.data[out],
                           gstate.projected_types[out], count);
    SDB_ASSERT(col_count == count);
    ++iter_idx;
  }

  if (gstate.scan_rowid) {
    if (gstate.has_generated_pk && real_iter_start == 1) {
      auto pk_count = ReadGeneratedPKFromKeys(
        *iterators[0], output.data[gstate.rowid_output_idx], count);
      SDB_ASSERT(pk_count == count);
    } else {
      auto* rowid_data = duckdb::FlatVector::GetDataMutable<int64_t>(
        output.data[gstate.rowid_output_idx]);
      for (duckdb::idx_t i = 0; i < count; ++i) {
        rowid_data[i] = static_cast<int64_t>(i);
      }
    }
  }

  if (gstate.scan_tableoid) {
    output.data[gstate.tableoid_output_idx].Reference(
      duckdb::Value::BIGINT(gstate.tableoid_value));
  }

  output.SetCardinality(count);
  if (count > 0) {
    gstate.produced_rows.fetch_add(count, std::memory_order_relaxed);
  }
}

}  // namespace sdb::connector
