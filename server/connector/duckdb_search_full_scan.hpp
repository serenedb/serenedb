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

#include <absl/synchronization/notification.h>

#include <algorithm>
#include <atomic>
#include <duckdb.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <exception>
#include <iresearch/index/column_extract.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/search/filter.hpp>
#include <iresearch/search/scorer.hpp>
#include <iresearch/types.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <vector>

#include "connector/duckdb_table_function.h"

namespace irs {

class IndexReader;

}  // namespace irs
namespace sdb::connector {

struct SereneDBScanBindData;

// How the scan executes, decided once in IResearchScanInitGlobal
// (DecideScanMode): the fastest mode that can apply every pushed filter.
enum class ScanMode : uint8_t {
  // ts_dict_agg() term enumeration.
  TsDict,
  // count(*) with no predicate and no pushed filters: the whole-reader
  // live_docs_count answers without touching a segment.
  CountFast,
  // count(*): per-segment count() -- whole-file statistics settle or kill the
  // pushed `.col` filters, the rest apply through the TableFilterDocIterator
  // wrapper. Score/lookup filters cannot run here (no scoring, no source
  // materialization), so their plans take Stream instead.
  Count,
  // ORDER BY score LIMIT k: parallel top-k collectors.
  TopK,
  // Match-all with every needed column covered (the FullScanner case): bulk
  // work units read `.col` directly; a segment with deletes falls back to the
  // masked streaming walk. Never scores, never touches the lookup source.
  ColScan,
  // Streaming DocIterator -> HitBatcher (WAND-seeded when eligible). The only
  // mode that materializes through the lookup source, engaged if and only if
  // a lookup column is needed -- for a filter or for the output.
  Stream,
};

// A segment with no deletions must pay for them as if they did not exist: its
// `.col` scan reads its rows in bulk instead of walking a masked doc iterator,
// its ts_dict counts come from term metadata instead of intersecting postings,
// and `SubReader::mask` hands the iterator back unwrapped. All three ask this,
// and it holds exactly when the segment carries no document mask.
inline bool SegmentAllLive(const irs::SubReader& seg) noexcept {
  return seg.live_docs_count() == seg.docs_count();
}

// Global scan state, filled by IResearchScanInitGlobal in pipeline order:
// the projection walk (InitScanState), the pushed-filter classification
// (BuildTableFilter), the column dispositions
// (ClassifyColumnstoreProjections), then DecideScanMode and the decided
// mode's own state. Per-mode state lives in named sub-structs -- always
// present (no tag checks on access), used only by their mode.
struct IResearchScanGlobalState : public duckdb::GlobalTableFunctionState {
  // --- Query shape: the bind data and the snapshot it scans. ---------------
  const SereneDBScanBindData* scan = nullptr;
  duckdb::ClientContext* client_context = nullptr;
  const irs::IndexReader* reader = nullptr;
  size_t total_segments = 0;
  const VectorScorerOptions* vector_scorer = nullptr;

  // --- The projection walk: what duckdb asked the scan for. ----------------
  std::vector<duckdb::idx_t> projected_columns;
  std::vector<duckdb::LogicalType> projected_types;
  std::vector<duckdb::ColumnIndex> projected_column_indexes;
  // filter_prune: when set, the scanned column_ids include filter-only columns
  // the output must not emit. These are indexes into the scanned columns that
  // form the output (a reorder/narrow); empty = emit the scanned columns as-is.
  duckdb::vector<duckdb::idx_t> output_projection_ids;
  // Output slots of the scan-computed virtual columns; INVALID_INDEX when not
  // projected. tableoid caches its constant value; the generated PK (search
  // table rowid) materializes from `.col` as a covered column.
  duckdb::idx_t score_output_idx = duckdb::DConstants::INVALID_INDEX;
  duckdb::idx_t tableoid_output_idx = duckdb::DConstants::INVALID_INDEX;
  int64_t tableoid_value = 0;
  duckdb::idx_t generated_pk_output_idx = duckdb::DConstants::INVALID_INDEX;
  // Any real (non-virtual) column is scanned / any output column emits values
  // (the empty virtual column does not): !has_output_column means the scan
  // only reports row counts (count(*) shapes).
  bool has_real_column = false;
  bool has_output_column = false;

  // The score column is scanned, so scores must be computed.
  bool ScanScore() const noexcept {
    return score_output_idx != duckdb::DConstants::INVALID_INDEX;
  }

  // --- Column dispositions (ClassifyColumnstoreProjections): covered columns
  // materialize from `.col` (`cs_projections`), the rest stay in
  // `lookup_projected_columns` for the lookup source. `needs_lookup` is set if
  // and only if a lookup column is needed -- in the output or by a pushed
  // filter (the source applies it natively during materialization); a column
  // needed by neither (left dangling by a statistics-eliminated filter) is
  // read nowhere. ------------------------------------------------------------
  std::vector<ColumnstoreProjection> cs_projections;
  std::vector<duckdb::idx_t> lookup_projected_columns;
  bool needs_lookup = false;

  // --- Pushed filters (BuildTableFilter). -----------------------------------
  // The scan's pushed filters (as duckdb hands them to us), forwarded verbatim
  // to the lookup source so its native scan evaluates lookup-column filters
  // (FilterSelection + late materialization). Lives for the query.
  const duckdb::TableFilterSet* pushed_filters = nullptr;
  // A pushed filter targets a lookup (source-only) column: it can only be
  // applied during the source lookup, so it forbids the fast collector/bulk
  // paths (which never run the lookup per candidate) -- forces streaming.
  bool has_lookup_filter = false;
  // Covered (INCLUDE'd) `.col` filters, applied in-scan by wrapping the search
  // DocIterator in a TableFilterDocIterator (codec Filter + zonemap). `field`
  // keys the segment columnstore; `filter` is the pushed ExpressionFilter.
  // Empty => no `.col` filtering, no wrapper, zero cost.
  struct ColFilter {
    irs::field_id field;
    const duckdb::TableFilter* filter;
    // A filter on the computed score column (not a `.col` field): applied on
    // the score vector after scoring instead of via the columnstore codec.
    bool is_score = false;
    // Per-filter invariants, computed once at pushdown (see ColFilterSpec).
    bool is_dynamic = false;
    bool zonemap_only = false;
    irs::NullCheckKind null_check = irs::NullCheckKind::None;
    // The filtered column's type (a segment lacking the column classifies by
    // evaluating the filter on a NULL of this type).
    duckdb::LogicalType type;
    // IS NOT NULL replacement for segments whose statistics say TRUE_OR_NULL
    // (every non-null row passes) -- built once at pushdown, duckdb
    // propagate_get-style. Null when the filter shape can't be replaced.
    duckdb::unique_ptr<duckdb::TableFilter> not_null;
  };
  std::vector<ColFilter> col_filters;
  // Owns the emit-adjusted copies of pushed score filters (the predicate with
  // the score-emit baked into the score reference, so it evaluates in the
  // user-facing space it was written in). A `col_filters` entry points into it.
  std::vector<duckdb::unique_ptr<duckdb::TableFilter>> emit_score_filters;
  // Score-filter machinery. `score_dynamic_filter` is the shared runtime bound
  // TOP_N updates (captured from the pushed dynamic score filter; null when
  // none). `score_static_floor` is the lower bound implied by pushed static
  // score filters (`score > c`, Lucene min_score-style): it seeds the WAND
  // threshold and the top-k collectors so below-bound blocks are skipped from
  // the first window; the pushed filter still enforces the exact bound
  // (lowest() = no bound). When `wand_streaming` (Stream mode with a
  // WAND-enabled text scorer and one of those bounds), the streaming
  // DocIterator runs with WAND and its ScoreThresholdAttr is seeded from the
  // bound before each emit -- the HitBatcher score filter still enforces the
  // exact boundary on the docs that are produced.
  duckdb::shared_ptr<duckdb::DynamicFilterData> score_dynamic_filter;
  float score_static_floor = std::numeric_limits<float>::lowest();
  bool wand_streaming = false;

  // --- The search predicate (`@@` / vector query) and scoring machinery.
  // `owned_filter` backs `filter` for vector/match-all queries; the prepare
  // phase fills per-segment queries + merged term statistics. ----------------
  const irs::Filter* filter = nullptr;
  irs::Filter::ptr owned_filter;
  std::unique_ptr<irs::Scorer> scorer_obj;
  // One prepared query per segment, shared by every worker that claims a row
  // group of it: the dictionary lookups behind it are the per-segment prepare
  // cost and are paid once. `published` is the whole read path -- one acquire
  // load, and null means "not built yet", never "wait". `owned` exists only to
  // keep the winner's build alive for the scan; it is written by the worker
  // whose publication won and read again at teardown, so it never needs to be
  // atomic (EnsureSegmentQuery states why racing builders are legal).
  struct QuerySlot {
    irs::QueryBuilder::ptr owned;
    std::atomic<const irs::QueryBuilder*> published{nullptr};
  };
  std::vector<QuerySlot> queries;
  std::vector<irs::PrepareCollector::ptr> collectors;
  std::optional<irs::StatsBuffer> stats;
  // What the reduce threw, if it threw. The barrier has to be released even
  // then -- workers parked on it would wait forever -- so the failure travels
  // with it: the reducing worker stores it before Notify(), every worker that
  // passes the barrier rethrows it. Written by exactly one thread, read only
  // after the notification, so the notification is its publication edge.
  std::exception_ptr prepare_error;
  // The scored-query barrier: `prepare_finished` is both the published-stats
  // flag (HasBeenNotified) and what a worker with no claimable prepare unit
  // waits on when it cannot park (see PreparePhase).
  absl::Notification prepare_finished;
  std::atomic_uint32_t prepare_segment = 0;
  std::atomic_uint32_t prepare_count = 0;
  std::atomic_uint32_t collector_slots = 0;

  // --- Segment claiming: claimed slots in [0, claimable_segments) map through
  // `segment_order` -- empty = identity over all segments. Init-time
  // whole-file classification against the static pushed filters shrinks the
  // list to survivors (a dynamic bound is still uninitialized at init, so it
  // classifies NO_PRUNING and never excludes; survivors still classify at
  // claim, where dynamic bounds have tightened), and ORDER BY <covered column>
  // LIMIT (bind_data.scan_order) permutes it best-first (scheduling only --
  // the TopN above still sorts). The scorer prepare phase walks every segment
  // regardless (corpus-level term stats). ------------------------------------
  std::vector<uint32_t> segment_order;
  uint32_t claimable_segments = 0;

  uint32_t SegmentAt(uint32_t claimed) const {
    return segment_order.empty() ? claimed : segment_order[claimed];
  }

  // --- The work grid: one row group of one segment is the work item. -------
  // A slot is a claimable segment, in claim order: `BuildGrid` compacts
  // `segment_order` onto exactly those, so slot `i` IS `SegmentAt(i)` and the
  // grid stores no segment id of its own. Each slot has its own row-group
  // cursor, so several workers run different row groups of the SAME segment --
  // a one-segment index parallelizes exactly like a many-segment one. A worker
  // claims a RUN of row groups per cursor bump, so affinity is what the claim
  // hands it rather than something it has to ask for again; it moves to a
  // fresh slot when its own is drained, and steals from the others only when
  // there is no fresh slot left.
  //
  // Longest run a single claim may take. Bounded so a worker cannot swallow a
  // slot that other workers still need, and so the best-first claim policy
  // below still reaches the second-best row group early.
  static constexpr uint32_t kMaxRgRun = 8;
  // Cursors sit one per cache line: adjacent slots are claimed by different
  // workers at the same time, and a 4-byte stride would put sixteen slots'
  // claims on one line.
  static constexpr size_t kCacheLine = 64;
  struct alignas(kCacheLine) RgCursor {
    std::atomic_uint32_t next{0};
  };
  struct RgGrid {
    // Slot `i` owns row groups `[rg_base[i], rg_base[i + 1])` of `rg_order`,
    // which is `TermRangeSlot`'s `[begin, count)` with the count derived
    // rather than stored. Sized slots + 1, so `back()` is the grid total.
    std::vector<uint32_t> rg_base;
    // Claim policy, flat: the i-th claim of slot `s` runs
    // `rg_order[rg_base[s] + i]`. Empty = ascending row-group order in every
    // slot; ORDER BY <covered column> LIMIT fills it best-first by the order
    // key's per-column-segment statistics.
    std::vector<uint32_t> rg_order;
    std::vector<RgCursor> cursors;
    std::atomic_uint32_t next_slot{0};
    uint32_t run = 1;

    uint32_t Slots() const noexcept {
      return static_cast<uint32_t>(cursors.size());
    }
    uint32_t RgCount(uint32_t slot) const noexcept {
      return rg_base[slot + 1] - rg_base[slot];
    }
    // Claimable row groups over all slots: what bounds the useful worker
    // count, and the denominator of scan progress.
    uint32_t TotalRgs() const noexcept {
      return rg_base.empty() ? 0 : rg_base.back();
    }
    uint32_t RgAt(uint32_t slot, uint32_t claimed) const noexcept {
      return rg_order.empty() ? claimed : rg_order[rg_base[slot] + claimed];
    }
  };
  RgGrid grid;

  // A claimed work item plus the run it came from: `slot` survives the claim
  // so the next one comes from the same segment while it has row groups left,
  // and `[taken, end)` is the rest of that run, which costs no atomic.
  static constexpr uint32_t kNoSlot = std::numeric_limits<uint32_t>::max();
  struct RgClaim {
    uint32_t slot = kNoSlot;
    uint32_t seg = 0;
    uint32_t rg = 0;
    uint32_t taken = 0;
    uint32_t end = 0;

    // The claim that took the slot's very first row group. A whole-segment
    // answer -- count mode's live count -- belongs to exactly one claimer, and
    // this is which one.
    bool FirstOfSlot() const noexcept { return taken == 1; }
  };

  // --- The ts_dict work grid: one term range of one field of one segment. --
  // A dictionary is a segment's and a term's count is summed over its row
  // groups, so row groups cannot divide term enumeration -- claiming them
  // would re-enumerate the dictionary once per row group and emit one row per
  // (segment, rg, term) instead of one per (segment, term). A term range
  // divides it and changes neither: the ranges of a field partition its terms,
  // so between them the workers emit exactly what one walk emits.
  struct TermRangeUnit {
    // The ts_dict request this range enumerates, by index.
    uint32_t field = 0;
    irs::TermRange range;
    // The field's NULL-term row is one row per (segment, field): the unit
    // holding the field's first range is the one that emits it.
    bool nulls = false;
  };
  struct TermRangeSlot {
    uint32_t seg = 0;
    // This segment's units, as a span of `term_grid.units`.
    uint32_t begin = 0;
    uint32_t count = 0;
    std::atomic_uint32_t next{0};
  };
  struct TermRangeGrid {
    std::vector<TermRangeUnit> units;
    std::vector<TermRangeSlot> slots;
    std::atomic_uint32_t next_slot{0};
  };
  TermRangeGrid term_grid;

  struct TermRangeClaim {
    uint32_t slot = kNoSlot;
    uint32_t seg = 0;
    uint32_t unit = 0;
  };

  // Claim one term range. False = every slot is exhausted. Same policy as
  // ClaimRowGroup: affinity to the segment the worker already classified,
  // then a fresh segment, then stealing.
  bool ClaimTermRange(TermRangeClaim& claim);

  uint64_t ClaimedTermRanges() const;

  void BuildTermRangeGrid(const SereneDBScanBindData& bind);

  // One publication slot per segment for the shared prepared queries.
  void InitQuerySlots() { queries = std::vector<QuerySlot>(total_segments); }

  // The segment's prepared query, built once for every worker that claims a
  // row group of it. `worker_collector` is the caller's statistics collector
  // slot (scored scans only), allocated on first use and reused across
  // segments.
  const irs::QueryBuilder& EnsureSegmentQuery(
    irs::PrepareCollector*& worker_collector, const irs::SubReader& seg,
    uint32_t seg_idx);

  // Claim one row group. False = the grid is drained (every slot exhausted).
  bool ClaimRowGroup(RgClaim& claim);

  // Row groups claimed so far, for progress reporting.
  uint64_t ClaimedRowGroups() const;

  void BuildGrid(const SereneDBScanBindData& bind);

  // --- The decided plan and its mode-specific state. ------------------------
  ScanMode mode = ScanMode::Stream;

  // Top-k (ORDER BY score LIMIT k): cross-thread k-th score for WAND pruning,
  // and the over-fetch pool size when quantization / a lookup filter requires
  // reranking or survivor slack.
  struct TopKState {
    std::atomic<irs::score_t> global_kth_score =
      std::numeric_limits<irs::score_t>::lowest();
    uint32_t rerank_pool = 0;
  };
  TopKState topk;

  std::atomic<duckdb::idx_t> produced_rows{0};

  // What the task scheduler could actually run in parallel, read once at init:
  // the work counts below are capped by it so the per-worker state the scan
  // sizes off MaxThreads() (collector slots) is never over-allocated.
  duckdb::idx_t pool_threads = 1;

  duckdb::idx_t MaxThreads() const final {
    const auto cap = [&](duckdb::idx_t units) {
      return std::max<duckdb::idx_t>(1, std::min(pool_threads, units));
    };
    switch (mode) {
      case ScanMode::CountFast:
        return 1;
      case ScanMode::TsDict:
        // One worker per claimable term range.
        return cap(term_grid.units.size());
      default:
        // One worker per claimable row group. The scorer prepare phase walks
        // every segment (corpus-level term statistics), even ones the
        // whole-file classification excluded, so a scored scan is additionally
        // allowed a worker per segment.
        return cap(scorer_obj
                     ? std::max<duckdb::idx_t>(grid.TotalRgs(), total_segments)
                     : grid.TotalRgs());
    }
  }
};

// Lower bound implied by a static score filter (`score > c` / `score >= c`,
// AND-conjunctions take the max): the largest float T such that every score
// passing the filter exceeds T. `exact` is set when the whole expression IS
// that bound, so enforcing `score > T` replaces evaluating the filter.
// lowest() when the expression implies no usable lower bound.
float StaticScoreFloor(const duckdb::Expression& expr, bool& exact);

// Decode a pushdown-extract ColumnIndex into its dotted field-path components.
// Struct steps carry a numeric index resolved against `root_type`; variant
// steps carry the field name directly. `column_index` must be a pushdown
// extract with children. Components are appended to `out` and borrow from the
// type/index (valid for the scan's lifetime).
void DecodeExtractPath(const duckdb::ColumnIndex& column_index,
                       const duckdb::LogicalType& root_type,
                       std::vector<std::string_view>& out);

void IResearchScanGetMetrics(duckdb::TableFunctionGetMetricsInput& input);

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> IResearchScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> IResearchScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void IResearchScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

void IResearchSetScanOrder(
  duckdb::ClientContext& context,
  duckdb::unique_ptr<duckdb::RowGroupOrderOptions> options,
  duckdb::optional_ptr<duckdb::FunctionData> bind_data);

}  // namespace sdb::connector
