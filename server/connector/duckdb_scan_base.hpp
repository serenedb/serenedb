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

#include <algorithm>
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

namespace irs {

class IndexReader;

}  // namespace irs
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

  // Per-segment INCLUDE materializers. Lazy-built under
  // cs_materializers_mu (parallel paths claim concurrently).
  std::vector<std::unique_ptr<ColumnstoreMaterializer>> cs_materializers;
  absl::Mutex cs_materializers_mu;

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

  // Parallel scan coordination consumed by RunParallelInvertedIndexScan.
  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  std::atomic_uint32_t next_segment{0};

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

struct CommonScanLocalState : public duckdb::LocalTableFunctionState {
  // Set by the skeleton after OnSegmentsExhausted; lets EmitNextChunk
  // distinguish "still collecting" from "done".
  bool segments_exhausted = false;

  // Default no-op; top-K backends shadow it with prep work (sort, pk_batch).
  void OnSegmentsExhausted(duckdb::ClientContext& /*ctx*/,
                           CommonScanGlobalState& /*g*/) {}
};

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

// Returns the materializer for `seg_idx`, lazy-building it on first call.
// Returns nullptr when the segment has no cs reader or no bound INCLUDE'd
// columns. The returned pointer is owned by `gstate.cs_materializers` and
// lives for the rest of the query.
ColumnstoreMaterializer* GetOrOpenSegmentMaterializer(
  CommonScanGlobalState& gstate, const irs::IndexReader& reader,
  size_t seg_idx);

// Materialise INCLUDE'd columnstore columns. `seg_doc_batched` must be
// sorted by (segment_idx, doc_pos) -- iresearch's ScanCursor is forward-only.
void MaterializeIncludeColumnsBatched(CommonScanGlobalState& gstate,
                                      const irs::IndexReader& reader,
                                      std::span<const SegDoc> seg_doc_batched,
                                      duckdb::DataChunk& output);

// Read generated PK int64 values from RocksDB iterator keys into output.
// Key format: [ObjectId(8)][ColumnId(8)][PK int64 big-endian XOR 0x80].
duckdb::idx_t ReadGeneratedPKFromKeys(rocksdb::Iterator& it,
                                      duckdb::Vector& output,
                                      duckdb::idx_t max_rows);

// DuckDB rows_scanned callback: returns produced_rows from
// CommonScanGlobalState.
duckdb::idx_t CommonScanRowsScanned(duckdb::GlobalTableFunctionState& gstate,
                                    duckdb::LocalTableFunctionState&);

// DuckDB init_local callback: creates an empty CommonScanLocalState.
duckdb::unique_ptr<duckdb::LocalTableFunctionState> CommonScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

// Fill virtual-column slots (tableoid / rowid / score). Pass `{}` for
// scores when the path wrote them inline or there's no score column.
void WriteVirtualColumns(CommonScanGlobalState& gstate, duckdb::idx_t num_rows,
                         duckdb::DataChunk& output,
                         std::span<const float> scores_or_empty);

// Drain one chunk from a per-thread (seg, doc) buffer: virtual cols +
// external (RocksDB) + INCLUDE cs cols, advances current_idx. Returns
// rows emitted (0 = drained). Lstate must expose current_idx, pk_batch,
// index_source.
template<class Gstate, class Lstate>
duckdb::idx_t EmitChunkFromBuffer(duckdb::ClientContext& ctx, Gstate& g,
                                  Lstate& l,
                                  std::span<const SegDoc> all_seg_docs,
                                  std::span<const float> all_scores_or_empty,
                                  duckdb::DataChunk& output) {
  const size_t remaining = all_seg_docs.size() - l.current_idx;
  if (remaining == 0) {
    return 0;
  }
  const auto num_rows = static_cast<duckdb::idx_t>(
    std::min<size_t>(remaining, STANDARD_VECTOR_SIZE));

  std::span<const SegDoc> chunk = all_seg_docs.subspan(l.current_idx, num_rows);
  std::span<const float> score_chunk =
    all_scores_or_empty.empty()
      ? std::span<const float>{}
      : all_scores_or_empty.subspan(l.current_idx, num_rows);

  WriteVirtualColumns(g, num_rows, output, score_chunk);

  if (g.has_external_projections) {
    l.index_source->Materialize(ctx, l.pk_batch, l.current_idx, num_rows,
                                output);
  }
  if (!g.cs_projections.empty()) {
    MaterializeIncludeColumnsBatched(g, *g.reader, chunk, output);
  }

  l.current_idx += num_rows;
  output.SetCardinality(num_rows);
  g.produced_rows.fetch_add(num_rows, std::memory_order_relaxed);
  return num_rows;
}

// EmitNextChunk return:
//   Chunk         - rows written, return to DuckDB
//   NeedsSegment  - ask skeleton for more work
//   Finished      - this thread is done
enum class EmitOutput { Chunk, NeedsSegment, Finished };

template<class Gstate, class Lstate>
void RunParallelInvertedIndexScan(duckdb::ClientContext& ctx, Gstate& g,
                                  Lstate& l, duckdb::DataChunk& output) {
  while (true) {
    switch (l.EmitNextChunk(ctx, g, output)) {
      case EmitOutput::Chunk:
        return;
      case EmitOutput::Finished:
        output.SetCardinality(0);
        return;
      case EmitOutput::NeedsSegment: {
        const auto seg = g.next_segment.fetch_add(1, std::memory_order_relaxed);
        if (seg < g.total_segments) {
          // Invariant: iresearch never commits empty segments
          // (index_writer.cpp drops live_docs_count==0 at flush/commit).
          SDB_ASSERT((*g.reader)[seg].live_docs_count() != 0);
          l.OnSegment(ctx, (*g.reader)[seg], seg, g);
        } else {
          l.OnSegmentsExhausted(ctx, g);
          l.segments_exhausted = true;
        }
        break;
      }
    }
  }
}

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
