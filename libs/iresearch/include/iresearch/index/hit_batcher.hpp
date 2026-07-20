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

#include <array>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_extract.hpp"
#include "iresearch/index/table_filter_iterator.hpp"

namespace sdb::connector {

class HitBatcher {
 public:
  HitBatcher(std::span<const ColumnstoreProjection> projections,
             irs::field_id pk_field_id, bool track_scores);

  HitBatcher(const HitBatcher&) = delete;
  HitBatcher& operator=(const HitBatcher&) = delete;
  ~HitBatcher();

  // `filters` are this segment's ACTIVE pushed `.col`/score table filters,
  // applied in-scan (RowGroup::Scan-style: codec Filter narrows survivors,
  // decoded once into the projected output slot then Sliced). The caller has
  // already classified them against the segment's whole-file statistics
  // (ClassifySegmentColFilters) -- no re-check here, and `states` is the
  // worker's filter-state cache backing them. Both empty/null on the
  // count/top-k paths, where filtering happens in the DocIterator instead.
  void BeginSegment(
    uint32_t seg_idx, const irs::ColReader* col_reader,
    duckdb::ClientContext* context, ColFilterStateCache* states = nullptr,
    std::span<const TableFilterDocIterator::FilterSpec> filters = {});

  duckdb::idx_t OpenWindow(uint64_t row);
  // Batched fill: the current window's ids/scores go to [WindowHead(), ...) /
  // [ScoreHead(), ...), then CommitWindow(n) records how many were emitted.
  irs::doc_id_t* WindowHead() noexcept { return &_docs[_len]; }
  irs::score_t* ScoreHead() { return ScoreData() + _len; }
  void CommitWindow(duckdb::idx_t n) noexcept {
    _len += n;
    SDB_ASSERT(_len <= STANDARD_VECTOR_SIZE);
  }

  void Finalize();

  // The segment's bound `.col` filter chain (empty when none): the streaming
  // loop consults its zonemap skip before staging windows.
  ColFilterChain& Filters() noexcept { return _filters; }
  // Rows in the segment's columnstore (0 when no column is bound).
  uint64_t SegmentRowCount() const noexcept {
    return _rg_col != nullptr ? _rg_col->RowCount() : 0;
  }

  uint32_t Segment() const noexcept { return _seg_idx; }

  bool Ready() const noexcept { return _ready != Pending::None; }
  bool Empty() const noexcept {
    return _ready == Pending::None && _len == 0 && !_compact;
  }

  struct Batch {
    std::span<const irs::doc_id_t> docs;
    std::span<const float> scores;
    uint32_t seg = 0;
    duckdb::Vector* pk = nullptr;
    // The staged score vector backing `scores` (null when scores are not
    // tracked): the output chunk References it -- no copy at emit.
    duckdb::Vector* score_vec = nullptr;
    duckdb::idx_t count = 0;
  };

  Batch Emit(duckdb::DataChunk& output);

 private:
  struct Column {
    const irs::ColumnReader* reader = nullptr;
    duckdb::idx_t slot = 0;
    bool is_pk = false;
    bool extract = false;
    bool list_like = false;
    // The emitted batch References scratch into the output chunk, which stays
    // referenced until the consumer pulls the next batch -- so batches
    // alternate between two cache-backed buffers instead of allocating a
    // fresh Vector per batch.
    uint8_t scratch_idx = 0;
    duckdb::LogicalType out_type;
    std::unique_ptr<irs::ColumnReader::ScanState> state;
    std::unique_ptr<ExtractBinding> extract_binding;
    std::array<std::unique_ptr<irs::ColumnReader::VectorScratch>, 2> scratch;
  };

  enum class Pending : uint8_t { None, Dense, Scratch };

  bool HasFilters() const noexcept {
    return !_filters.Empty() || _score_filter != nullptr;
  }

  void CloseGroup();
  void ScatterGroup();
  Batch EmitFiltered(duckdb::DataChunk& output);
  void Compact();
  float* ScoreData();
  duckdb::Vector* ScoreVector() noexcept {
    auto& s = _score_bufs[_score_idx];
    return s ? &s->vector : nullptr;
  }
  void MaterializeColumn(Column& c, uint64_t anchor, duckdb::idx_t span,
                         duckdb::idx_t hits, duckdb::idx_t first,
                         duckdb::Vector& out, duckdb::idx_t at, bool dense);
  duckdb::Vector& Scratch(Column& c);
  duckdb::Vector& PkOut();
  uint64_t Row(duckdb::idx_t i) const noexcept {
    return _docs[i] - irs::doc_limits::min();
  }
  // Row-group window end for a group starting at `row`: the segment's row-group
  // boundary (if any) capped to a single output vector.
  uint64_t RgEndFor(uint64_t row) const noexcept;

  std::span<const ColumnstoreProjection> _projections;
  const irs::field_id _pk_field_id;
  const bool _track_scores;

  std::unique_ptr<irs::ReadContext> _ctx;
  std::vector<Column> _columns;
  uint32_t _seg_idx = 0;
  const irs::ColumnReader* _rg_col = nullptr;

  std::array<irs::doc_id_t, STANDARD_VECTOR_SIZE> _docs;
  // Score staging IS the output: the emitted batch's chunk References the
  // staged vector (no copy), so batches ping-pong between two cache-backed
  // buffers -- Compact() flips and carries the leftover tail across.
  std::array<std::unique_ptr<irs::ColumnReader::VectorScratch>, 2> _score_bufs;
  uint8_t _score_idx = 0;
  duckdb::idx_t _len = 0;
  duckdb::idx_t _group = 0;
  duckdb::idx_t _batch = 0;
  uint64_t _group_rg_end = 0;

  // Codec scans may zero-copy the output (e.g. FixedSizeScan SetData's the
  // vector straight at the pinned block), so every batch goes through
  // VectorScratch::Reset() -- a stale pointer outlives the segment's pins.
  std::unique_ptr<irs::ColumnReader::VectorScratch> _pk_out;

  // Pushed table filters: the per-segment chain rebuilt by BeginSegment from
  // the segment's active specs. `_score_filter` is the (computed) score-column
  // filter, applied on `_scores` before the `.col` pass; its state is owned by
  // the worker's ColFilterStateCache.
  ColFilterChain _filters;
  const duckdb::TableFilter* _score_filter = nullptr;
  duckdb::TableFilterState* _score_state = nullptr;

  duckdb::buffer_ptr<duckdb::SelectionData> _sel_data;
  duckdb::SelectionVector _sel;

  Pending _ready = Pending::None;
  bool _compact = false;
  bool _compact_dense = false;
};

}  // namespace sdb::connector
