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

#pragma once

#include <atomic>
#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <iresearch/types.hpp>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "catalog/table_options.h"
#include "connector/columnstore_materializer.h"
#include "rocksdb/iterator.h"
#include "rocksdb/slice.h"
#include "rocksdb/snapshot.h"
#include "rocksdb/utilities/transaction.h"

namespace sdb::connector {

struct SereneDBScanBindData;
class IndexSource;

struct ColumnstoreProjection {
  duckdb::idx_t output_slot;
  catalog::Column::Id column_id;
};

// Common state inherited by all per-scan global states.
// Holds the fields shared across every scan strategy: isolation context,
// projection mapping, virtual column metadata, and the termination flag.
struct CommonScanGlobalState : public duckdb::GlobalTableFunctionState {
  // Isolation: exactly one of txn / snapshot is set.
  rocksdb::Transaction* txn = nullptr;
  const rocksdb::Snapshot* snapshot = nullptr;

  // Maps output column index -> bind_data column index.
  // INVALID_INDEX marks virtual columns (rowid, tableoid, score).
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;

  // Same shape as `projected_columns` but with INCLUDE'd columnstore
  // slots reset to INVALID_INDEX. Pass this (not `projected_columns`)
  // to `MakeIndexSource` so RocksDB does not double-materialize columns
  // the cs overlay will overwrite. Default: copy of projected_columns
  // (no behavior change until `ClassifyColumnstoreProjections` runs).
  std::vector<duckdb::idx_t> external_projected_columns;

  // INCLUDE'd projections served by the columnstore overlay.
  // Populated by `ClassifyColumnstoreProjections`.
  std::vector<ColumnstoreProjection> cs_projections;
  // Parallel arrays derived from `cs_projections` once at init time --
  // bulk + streaming + score-ordered materializer paths all want them
  // in this shape and would otherwise rebuild per batch.
  std::vector<irs::field_id> cs_field_ids;
  std::vector<duckdb::idx_t> cs_output_slots;
  // True iff at least one projected real column is NOT an INCLUDE
  // column (i.e. IndexSource still has work to do).
  bool has_external_projections = false;

  // Per-segment materializers shared by streaming, top-K, ANN, range,
  // and bulk-scan paths. Populated lazily on first batch that touches
  // the segment; lives for the whole query. Sized to the index reader's
  // segment count on first use (see GetOrOpenSegmentMaterializer).
  std::vector<std::unique_ptr<ColumnstoreMaterializer>> cs_materializers;

  // Rowid virtual column
  bool has_generated_pk = false;
  bool scan_rowid = false;
  duckdb::idx_t rowid_output_idx = 0;

  // Tableoid virtual column
  bool scan_tableoid = false;
  duckdb::idx_t tableoid_output_idx = 0;
  int64_t tableoid_value = 0;

  // Score virtual column (set only for search scans with BM25/TFIDF scorer)
  bool scan_score = false;
  duckdb::idx_t score_output_idx = 0;

  bool finished = false;

  // Rows emitted -- read by the rows_scanned DuckDB callback.
  std::atomic<duckdb::idx_t> produced_rows{0};

  // Cached IndexSource adapter for the table being scanned: holds whatever
  // bind/session state the per-source materialiser needs (file-backed: a
  // bound MultiFileBindData + lookup gstate; rocksdb: small projection +
  // snapshot/txn). Lazy-built via MakeIndexSource on first use; reused
  // across batches.
  // shared_ptr (not unique_ptr) so derived-state destructors don't need
  // IndexSource's complete type -- the deleter is type-erased.
  std::shared_ptr<IndexSource> index_source;

  duckdb::idx_t MaxThreads() const override { return 1; }

  ~CommonScanGlobalState() override;
};

struct CommonScanLocalState : public duckdb::LocalTableFunctionState {};

// Shared base for PK-based scan states (full and range).
// Holds the upper-bound key storage that column iterators reference.
// Iterators must be declared in derived classes after split_* key vectors so
// they are destroyed first (C++ destroys members in reverse declaration order,
// derived before base).
struct PKScanGlobalState : public CommonScanGlobalState {
  // upper_bound_slices point into upper_bound_data; both must outlive
  // iterators.
  std::string upper_bound_data;
  std::vector<rocksdb::Slice> upper_bound_slices;
};

// Fills the common fields of `state` from `bind_data` and `input`.
// Handles: snapshot/txn isolation setup, projection pushdown, has_generated_pk,
// and virtual column (rowid, tableoid, score) detection.
// Does NOT create iterators -- each scan's InitGlobal does that afterward.
void InitCommonState(CommonScanGlobalState& state,
                     duckdb::ClientContext& context,
                     const SereneDBScanBindData& bind_data,
                     duckdb::TableFunctionInitInput& input);

void ClassifyColumnstoreProjections(CommonScanGlobalState& state,
                                    const SereneDBScanBindData& bind_data);

struct SegDoc {
  uint32_t segment_idx;
  irs::doc_id_t doc_pos;
};

}  // namespace sdb::connector
namespace irs {

class IndexReader;

}  // namespace irs
namespace sdb::connector {

// Returns the materializer for `seg_idx`, lazy-building it on first call.
// Returns nullptr when the segment has no cs reader or no bound INCLUDE'd
// columns. The returned pointer is owned by `gstate.cs_materializers` and
// lives for the rest of the query.
ColumnstoreMaterializer* GetOrOpenSegmentMaterializer(
  CommonScanGlobalState& gstate, const irs::IndexReader& reader,
  size_t seg_idx);

void MaterializeIncludeColumnsScoreOrder(
  CommonScanGlobalState& gstate, const irs::IndexReader& reader,
  std::span<const SegDoc> seg_doc_score_order, duckdb::DataChunk& output);

// Read generated PK int64 values from RocksDB iterator keys into output.
// Key format: [ObjectId(8)][ColumnId(8)][PK int64 big-endian XOR 0x80].
duckdb::idx_t ReadGeneratedPKFromKeys(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows);

// DuckDB get_metrics callback: writes produced_rows from CommonScanGlobalState
// into input.operator_metrics.rows_scanned.
void CommonScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

// DuckDB init_local callback: creates an empty CommonScanLocalState.
duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

// Builds the scan column list and populates state.upper_bound_data and
// state.upper_bound_slices. Must be called after InitCommonState.
// Returns per-column prefix keys (one per scan column, same order).
std::vector<std::string> InitPKScanColumns(
  PKScanGlobalState& state, const SereneDBScanBindData& bind_data);

// Shared scan body for PK full-scan and range-scan functions.
// `iterators` is passed explicitly (not stored in PKScanGlobalState) so each
// derived state can declare it last and guarantee correct destruction order.
void PKScanFunctionImpl(
  CommonScanGlobalState& gstate,
  std::vector<std::unique_ptr<rocksdb::Iterator>>& iterators,
  duckdb::DataChunk& output);

}  // namespace sdb::connector
