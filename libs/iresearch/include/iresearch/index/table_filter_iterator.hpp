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
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/types/vector_cache.hpp>
#include <duckdb/execution/adaptive_filter.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/iterators.hpp"

namespace duckdb {

class ClientContext;

}  // namespace duckdb
namespace sdb::connector {

struct ColFilterSpec {
  irs::field_id field;
  const duckdb::TableFilter* filter;
  // Filter on the computed score column (applied on the score vector after
  // scoring) rather than a `.col` field.
  bool is_score = false;
  // Per-filter invariants, computed once at pushdown (walking the filter
  // expression per segment would repeat identical work):
  // a dynamic filter's bound tightens while the scan runs -- its zonemap
  // verdict is re-evaluated per window instead of cached.
  bool is_dynamic = false;
  // Root non-selectivity optional filter (e.g. the TOP_N dynamic filter):
  // advisory, so classification keeps it active on ALWAYS_TRUE (a dynamic
  // bound may tighten) and never swaps it for IS NOT NULL (it must not drop
  // null order keys). Evaluation is a no-op pass-through; the win is the
  // codec filters' group-granular consult of the live bound.
  bool zonemap_only = false;
  // Bare IS [NOT] NULL: evaluated on the validity child alone where the block
  // codec keeps validity separate (see ColumnReader::GatherFilter).
  irs::NullCheckKind null_check = irs::NullCheckKind::None;
  // IS NOT NULL replacement used when the segment's statistics classify the
  // filter TRUE_OR_NULL; owned by the scan state.
  const duckdb::TableFilter* not_null = nullptr;
};

// Per-worker cache of duckdb filter-evaluation state, keyed by the pushed
// TableFilter (pointer identity -- the pushed filters and their IS NOT NULL
// replacements live for the query). duckdb builds these once per scan
// (ScanFilterInfo::Initialize) and reuses them across row groups; rebuilding
// an ExpressionExecutor per segment is pure waste. The VectorScratch is the
// filter-only decode target: the engines on one worker run sequentially and
// every use goes through Reset(), so one per filter is enough.
class ColFilterStateCache {
 public:
  duckdb::TableFilterState& State(duckdb::ClientContext& context,
                                  const duckdb::TableFilter& filter);
  irs::ColumnReader::VectorScratch& Scratch(const duckdb::TableFilter& filter,
                                            const duckdb::LogicalType& type);

 private:
  struct Entry {
    const duckdb::TableFilter* filter = nullptr;
    duckdb::unique_ptr<duckdb::TableFilterState> state;
    std::unique_ptr<irs::ColumnReader::VectorScratch> scratch;
  };

  Entry& Find(const duckdb::TableFilter& filter);

  std::vector<Entry> _entries;
};

// One segment's bound `.col` filters plus the per-block filter chain shared
// by the three filter engines (TableFilterDocIterator::FilterBlock,
// HitBatcher::EmitFiltered, FullScanner::Scan): cached zonemap verdict ->
// codec GatherFilter narrowing -> Slice/Reference (or deferred dense gather)
// of the projected filter columns.
class ColFilterChain {
 public:
  struct Col {
    const irs::ColumnReader* reader = nullptr;
    irs::field_id field = 0;
    const duckdb::TableFilter* filter = nullptr;
    const duckdb::ExpressionFilter* expr = nullptr;
    // Projected slots this filter column also outputs to (a column can be
    // projected more than once): the codec decode doubles as the
    // materialization of the first slot (Sliced), the rest Reference it
    // zero-copy. Empty => filter-only: decodes into `scratch` purely to
    // evaluate the predicate.
    std::vector<duckdb::idx_t> output_slots;
    // Window-local: the zonemap said ALWAYS_TRUE, so the filter pass skipped
    // this column and the final survivors gather it densely instead.
    bool deferred = false;
    // A dynamic filter's bound tightens while the scan runs; its verdict is
    // re-evaluated per window instead of cached (duckdb's segment_checked).
    bool is_dynamic = false;
    // Bare IS [NOT] NULL on a filter-only column: GatherFilter answers it
    // from the validity child alone where the block codec keeps validity
    // separate (a projected column decodes anyway, so it filters as usual).
    irs::NullCheckKind null_check = irs::NullCheckKind::None;
    // Structured columns (struct/list parents) hold their blocks on the
    // children (a list parent's own blocks are its offsets): no per-block
    // zonemap, every window evaluates on a compact decode.
    bool has_blocks = true;
    bool list_like = false;
    // Per-block zonemap verdict, computed once per block: windows ascend, so
    // the cache is re-filled exactly when the anchor leaves `checked`.
    irs::BlockWindow checked{};
    duckdb::FilterPropagateResult verdict =
      duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    // Owned by the worker's ColFilterStateCache, reused across segments.
    duckdb::TableFilterState* state = nullptr;
    irs::ColumnReader::ScanState scan;
    // Codec Scan/Filter may morph the vector (e.g. dict_fsst emits a
    // DICTIONARY view over codec-owned buffers), so every use goes through
    // VectorScratch::Reset() -- never reuse it dirty. Cache-owned.
    irs::ColumnReader::VectorScratch* scratch = nullptr;
  };

  bool Empty() const noexcept { return _cols.empty(); }
  std::span<Col> Cols() noexcept { return {_cols.data(), _cols.size()}; }
  void Clear() noexcept { _cols.clear(); }

  // Binds the non-score specs against this segment's columnstore (score specs
  // are the caller's -- they filter the computed score vector, not `.col`).
  // Every spec's column must exist: the per-segment classification resolves
  // absent columns (all NULL there) before anything reaches a chain. Filter
  // states and scratches come from the worker's `states` cache.
  void Bind(const irs::ColReader& col_reader, irs::ReadContext& ctx,
            std::span<const ColFilterSpec> specs,
            duckdb::ClientContext& context, ColFilterStateCache& states);
  // Attaches a projected output slot to the filter bound on `field`; false
  // when no filter binds that column (the caller scans it as a plain column).
  bool AttachOutputSlot(irs::field_id field, duckdb::idx_t slot);
  // Allocates the private scratches of filter-only columns; call once after
  // every AttachOutputSlot.
  void FinishBind();

  // The filter chain over one block window [anchor, anchor+span): narrows
  // sel[0..survivors) in place and returns the new survivor count. Projected
  // filter columns decode over the full span into their `output` slot (the
  // caller Slices via FinishOutputs); null when nothing is projected. A
  // window that crosses a block boundary falls back to per-row evaluation
  // (the cached block verdict cannot cover it).
  duckdb::idx_t FilterWindow(uint64_t anchor, duckdb::idx_t span,
                             duckdb::SelectionVector& sel,
                             duckdb::idx_t survivors,
                             duckdb::DataChunk* output);
  // Slice each projected filter column (decoded over the full span) down to
  // the final survivors -- zero-copy dictionary view, like RowGroup::Scan's
  // Slice -- and Reference it into any further slots; a window-deferred
  // column (zonemap ALWAYS_TRUE) was not decoded by the filter pass and
  // gathers the survivors densely instead.
  void FinishOutputs(uint64_t anchor, duckdb::idx_t span,
                     const duckdb::SelectionVector& sel,
                     duckdb::idx_t survivors, duckdb::DataChunk& output);

  // Zonemap skip: when some filter's block verdict at `row` is a definite
  // FALSE, no row before that block's end can survive -- returns the furthest
  // such end so the caller can jump the scan there (duckdb
  // CheckZonemapSegments) instead of emitting and dropping window by window.
  // 0 = no skip. Shares the per-filter verdict cache with FilterWindow.
  uint64_t DeadUntil(uint64_t row);

  // Compacts docs[0..n) (and scores[0..n) when non-null, ascending) in place
  // to the rows passing every bound filter, block by block: groups the docs
  // that fall in one columnstore block (zonemap and the codec filter both work
  // per block), narrows with FilterWindow and maps the survivors back.
  // Returns the survivor count.
  duckdb::idx_t FilterDocs(irs::doc_id_t* docs, irs::score_t* scores,
                           duckdb::idx_t n);

  // Applies the computed-score filter on scores[0..n), compacting docs/scores
  // in place (survivor indices ascend, so writes never clobber unread input);
  // returns the survivor count.
  static duckdb::idx_t FilterScores(const duckdb::TableFilter& filter,
                                    duckdb::TableFilterState& state,
                                    irs::doc_id_t* docs, irs::score_t* scores,
                                    duckdb::idx_t n);
  // sel[0..survivors) hold the window's surviving span offsets (ascending):
  // compacts docs (and scores when non-null) so entry w corresponds to
  // anchor + sel[w]. In-place friendly: the write cursor never passes the
  // read cursor.
  static void CompactByOffsets(const duckdb::SelectionVector& sel,
                               duckdb::idx_t survivors, uint64_t anchor,
                               const irs::doc_id_t* docs_in,
                               const irs::score_t* scores_in,
                               irs::doc_id_t* docs_out,
                               irs::score_t* scores_out);

 private:
  duckdb::ClientContext* _context = nullptr;
  ColFilterStateCache* _states = nullptr;
  std::vector<Col> _cols;
  // FilterDocs' block-run selection. FilterSelection repoints `_sel` to a
  // buffer sized to the entering survivor count, so every block rebinds it to
  // the full-capacity `_sel_data` store before refilling.
  duckdb::buffer_ptr<duckdb::SelectionData> _sel_data;
  duckdb::SelectionVector _sel;
  // Adaptive execution order over `_cols` (duckdb ScanFilterInfo): cheaper /
  // more selective filters drift to the front by measured runtime. Kept
  // across segments while the filter count is stable.
  duckdb::unique_ptr<duckdb::AdaptiveFilter> _adaptive;
};

// A DocIterator = the inner search DocIterator + `.col` table filters. It
// yields only doc-ids whose stored (INCLUDE'd) column values pass the filters,
// narrowing the selection during the read via per-block zonemap skip + codec
// ColumnSegment::Filter (ColumnReader::GatherFilter) -- exactly like
// RowGroup::Scan. Because it IS a DocIterator, every consumer works unchanged:
// count() (count-only), Collect() (top-k, feeding NthPartitionScoreCollector),
// and EmitScoredDocs()/EmitDocs() (streaming). So `.col` table filters apply to
// count-only, top-k and streaming through one wrapper.
class TableFilterDocIterator : public irs::DocIterator {
 public:
  using FilterSpec = ColFilterSpec;

  // Whole-segment classification result (see ClassifySegmentColFilters):
  // `segment_dead` when a filter is ALWAYS_FALSE against the segment's
  // whole-file statistics; `active` holds the specs that still need work.
  struct SegmentClassification {
    bool segment_dead = false;
    std::vector<FilterSpec> active;
  };

  TableFilterDocIterator(irs::DocIterator::ptr inner,
                         const irs::ColReader& col_reader,
                         std::span<const FilterSpec> filters,
                         duckdb::ClientContext& context,
                         ColFilterStateCache& states);

  irs::doc_id_t advance() final {
    _doc = _inner->advance();
    return _doc;
  }
  irs::doc_id_t seek(irs::doc_id_t target) final {
    _doc = _inner->seek(target);
    return _doc;
  }
  irs::doc_id_t LazySeek(irs::doc_id_t target) final {
    _doc = _inner->LazySeek(target);
    return _doc;
  }
  irs::Attribute* GetMutable(irs::TypeInfo::type_id id) noexcept final {
    return _inner->GetMutable(id);
  }
  irs::ScoreFunction PrepareScore(const irs::PrepareScoreContext& ctx) final {
    return _inner->PrepareScore(ctx);
  }
  void FetchScoreArgs(uint16_t index) final { _inner->FetchScoreArgs(index); }
  uint32_t GetFreq() const final { return _inner->GetFreq(); }

  uint32_t count() final;
  uint32_t EmitDocs(irs::doc_id_t* out, irs::doc_id_t min,
                    irs::doc_id_t max) final;
  uint32_t EmitScoredDocs(irs::doc_id_t* out, irs::score_t* scores,
                          irs::doc_id_t max, const irs::ScoreFunction& scorer,
                          irs::ColumnArgsFetcher* fetcher,
                          irs::doc_id_t min) final;
  void Collect(const irs::ScoreFunction& scorer,
               irs::ColumnArgsFetcher& fetcher,
               irs::ScoreCollector& collector) final;
  std::pair<irs::doc_id_t, bool> FillBlock(
    irs::doc_id_t min, irs::doc_id_t max, uint64_t* mask,
    irs::FillBlockScoreContext score, irs::FillBlockMatchContext match) final {
    // Forwarding would silently skip the `.col` filters: no filtered path
    // (count/Collect/EmitScoredDocs cover them all) may reach this.
    SDB_ASSERT(false, "FillBlock on a table-filtered iterator");
    return _inner->FillBlock(min, max, mask, score, match);
  }

 private:
  // Compacts docs[0..n) (and scores[0..n) when non-null, ascending) in place to
  // the subset passing every `.col` filter, block by block: whole-block zonemap
  // skip, then codec GatherFilter narrowing. Returns the survivor count.
  duckdb::idx_t FilterBlock(irs::doc_id_t* docs, irs::score_t* scores,
                            duckdb::idx_t n);

  irs::DocIterator::ptr _inner;
  irs::ReadContext _ctx;
  ColFilterChain _filters;
  // Filter on the computed score (not a `.col` field): applied on the score
  // vector after scoring. Null when there is no score filter; the state is
  // cache-owned.
  const duckdb::TableFilter* _score_filter = nullptr;
  duckdb::TableFilterState* _score_state = nullptr;
  std::array<irs::doc_id_t, STANDARD_VECTOR_SIZE> _docbuf;
  std::array<irs::score_t, STANDARD_VECTOR_SIZE> _scorebuf;
};

}  // namespace sdb::connector
