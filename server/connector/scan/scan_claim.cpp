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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <functional>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/filters/all_filter.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/filters/granular_range_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <optional>
#include <vector>

#include "connector/scan/scan_function.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {

SegmentRowGroups::SegmentRowGroups(const irs::SubReader& segment,
                                   uint64_t row_group_size)
  : row_group_size{row_group_size}, docs{segment.Meta().docs_count} {
  if (const auto* columns = segment.GetColReader()) {
    starts = columns->RowGroupStarts();
  }
}

UnitRows ScanGlobalState::RowsOf(const ScanUnit& unit) const noexcept {
  const auto& groups = segments[unit.seg].groups;
  return {.begin = groups.Begin(unit.rg_begin),
          .end = unit.rg_end < groups.Count() ? groups.Begin(unit.rg_end)
                                              : groups.docs};
}

irs::DocRange ScanGlobalState::RangeOf(const ScanUnit& unit) const noexcept {
  const auto rows = RowsOf(unit);
  return {
    .begin = irs::doc_limits::min() + static_cast<irs::doc_id_t>(rows.begin),
    .end = rows.end == (*reader)[unit.seg].Meta().docs_count
             ? irs::doc_limits::eof()
             : irs::doc_limits::min() + static_cast<irs::doc_id_t>(rows.end)};
}

irs::doc_id_t ScanGlobalState::UnitSpan(const ScanUnit& unit) const noexcept {
  if (unit.whole || (unit.rg_begin == 0 && !ordered &&
                     segments[unit.seg].rg_count <= fold_rgs)) {
    return 0;
  }
  return static_cast<irs::doc_id_t>(rg_size);
}

namespace {

duckdb::RowGroupOrderKey OrderKey(const ScanGlobalState& g,
                                  const duckdb::BaseStatistics& stats) {
  const auto& order = *g.Bind().scan_order;
  return duckdb::RowGroupOrderKeys::Get(stats, order.order_by, order.order_type,
                                        order.null_order);
}

const irs::ColumnReader* OrderColumn(const ScanGlobalState& g, uint32_t seg) {
  const auto* col_reader = (*g.reader)[seg].GetColReader();
  const auto* column =
    col_reader ? col_reader->Column(g.Bind().scan_order->column) : nullptr;
  return column && column->MergedStatistics().GetType() == g.order_type
           ? column
           : nullptr;
}

duckdb::RowGroupOrderKey SegmentKey(const ScanGlobalState& g, uint32_t seg) {
  if (const auto* column = OrderColumn(g, seg)) {
    return OrderKey(g, column->MergedStatistics());
  }
  return duckdb::RowGroupOrderKeys::Null(g.Bind().scan_order->null_order);
}

bool IsNullKey(const ScanGlobalState& g, const duckdb::RowGroupOrderKey& key) {
  return duckdb::RowGroupOrderKeys::IsNull(key,
                                           g.Bind().scan_order->null_order);
}

duckdb::RowGroupOrderKey RowGroupKey(const ScanGlobalState& g,
                                     const irs::ColumnReader& column,
                                     uint64_t begin, uint64_t end,
                                     irs::BlockWindow& w) {
  auto best = duckdb::RowGroupOrderKey();
  for (auto row = begin; row < end; row = w.end) {
    w = column.Locate(row, w);
    const auto block = OrderKey(g, column.RowGroupStatistics(w.block));
    if (IsNullKey(g, block)) {
      return block;
    }
    if (row == begin || block < best) {
      best = block;
    }
  }
  return best;
}

duckdb::shared_ptr<duckdb::DynamicFilterData> FindOrderDynamicFilter(
  const ScanGlobalState& g) {
  const auto& order = *g.Bind().scan_order;
  if (order.column_type != duckdb::OrderByColumnType::NUMERIC) {
    return nullptr;
  }
  const bool descending = order.order_type == duckdb::OrderType::DESCENDING;
  for (const auto& cf : g.col_filters) {
    if (!cf.is_dynamic || cf.field != order.column) {
      continue;
    }
    auto dyn =
      duckdb::ExpressionFilter::GetOptionalDynamicFilterData(*cf.filter);
    if (!dyn) {
      continue;
    }
    const auto cmp = dyn->comparison_type;
    const bool monotone =
      descending ? cmp == duckdb::ExpressionType::COMPARE_GREATERTHAN ||
                     cmp == duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO
                 : cmp == duckdb::ExpressionType::COMPARE_LESSTHAN ||
                     cmp == duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
    if (monotone) {
      return dyn;
    }
  }
  return nullptr;
}

const duckdb::RowGroupOrderKey* OrderBound(ScanGlobalState& g) {
  const auto& dyn = g.order_dynamic_filter;
  if (!dyn) {
    return nullptr;
  }
  auto& bound = g.order_bound;
  const auto version = dyn->version.load(std::memory_order_acquire);
  if (version != bound.version) {
    duckdb::Value constant;
    {
      duckdb::lock_guard<duckdb::mutex> lock{dyn->lock};
      constant = dyn->constant;
    }
    if (constant.type() != g.order_type) {
      auto cast = constant.DefaultTryCastAs(g.order_type, nullptr, true);
      constant = cast ? std::move(*cast) : duckdb::Value{};
    }
    bound.valid = !constant.IsNull();
    if (bound.valid) {
      const auto& order = *g.Bind().scan_order;
      bound.key = duckdb::RowGroupOrderKeys::Get(constant, order.order_type,
                                                 order.null_order);
    }
    bound.version = version;
  }
  return bound.valid ? &bound.key : nullptr;
}

bool OrderPruned(const ScanGlobalState& g,
                 const duckdb::RowGroupOrderKey* bound,
                 const duckdb::RowGroupOrderKey& key) {
  if (!bound || IsNullKey(g, key)) {
    return false;
  }
  const auto order = key <=> *bound;
  return order > 0 || (order == 0 && g.order_strict);
}

bool DropPruned(ScanGlobalState& g, const duckdb::RowGroupOrderKey* bound) {
  auto& heap = g.ordered_heap;
  if (!heap.empty() && OrderPruned(g, bound, heap.front().key)) {
    std::erase_if(heap, [&](const ScanOrderKey& entry) {
      return !IsNullKey(g, entry.key);
    });
    absl::c_make_heap(heap, std::greater{});
  }
  return heap.empty();
}

ScanUnit WholeUnit(uint32_t seg, const SegmentWork& work) {
  return {.seg = seg,
          .rg_begin = 0,
          .rg_end = work.rg_count,
          .live = work.rg_count,
          .whole = true};
}

std::unique_ptr<OrderedUnits> BuildSegmentOrderedUnits(
  const ScanGlobalState& g, const SegmentWork& work, uint32_t seg,
  const duckdb::RowGroupOrderKey& segment_key) {
  auto built = std::make_unique<OrderedUnits>();
  if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
    built->units.push_back(WholeUnit(seg, work));
    built->keys.push_back(segment_key);
    return built;
  }
  const auto* column = OrderColumn(g, seg);
  std::vector<ScanOrderKey> rgs;
  rgs.reserve(work.LiveCount());
  irs::BlockWindow w;
  auto skipped = work.skipped_rgs.begin();
  for (uint32_t rg = 0; rg < work.rg_count; ++rg) {
    if (skipped != work.skipped_rgs.end() && *skipped == rg) {
      ++skipped;
      continue;
    }
    rgs.push_back({
      .key = column ? RowGroupKey(g, *column, work.groups.Begin(rg),
                                  work.groups.End(rg), w)
                    : segment_key,
      .id = rg,
    });
  }
  absl::c_sort(rgs);
  built->units.reserve(rgs.size());
  built->keys.reserve(rgs.size());
  for (const auto& rg : rgs) {
    built->units.push_back({.seg = seg,
                            .rg_begin = rg.id,
                            .rg_end = rg.id + 1,
                            .live = 1,
                            .whole = false});
    built->keys.push_back(rg.key);
  }
  return built;
}

void BuildOrderedHeap(ScanGlobalState& g) {
  g.order_dynamic_filter = FindOrderDynamicFilter(g);
  if (g.order_dynamic_filter) {
    const auto cmp = g.order_dynamic_filter->comparison_type;
    g.order_strict = cmp == duckdb::ExpressionType::COMPARE_LESSTHAN ||
                     cmp == duckdb::ExpressionType::COMPARE_GREATERTHAN;
  }
  g.ordered = true;
  auto& heap = g.ordered_heap;
  heap.reserve(g.segment_order.size());
  for (const auto seg : g.segment_order) {
    if (g.Segment(seg).LiveCount() != 0) {
      heap.push_back({SegmentKey(g, seg), seg});
    }
  }
  absl::c_make_heap(heap, std::greater{});
  if (heap.empty()) {
    return;
  }
  absl::c_pop_heap(heap, std::greater{});
  auto& top = heap.back();
  auto& work = g.Segment(top.id);
  work.ordered = BuildSegmentOrderedUnits(g, work, top.id, top.key);
  if (work.ordered->units.empty()) {
    heap.pop_back();
    return;
  }
  top.key = work.ordered->keys.front();
  absl::c_push_heap(heap, std::greater{});
}

bool ConstantCount(const irs::Filter& filter) {
  if (filter.type() == irs::Type<irs::ByTerm>::id()) {
    return true;
  }
  if (filter.type() != irs::Type<irs::BooleanFilter>::id()) {
    return false;
  }
  const auto& boolean = irs::utils::downCast<irs::BooleanFilter>(filter);
  size_t terms = 0;
  size_t others = 0;
  for (const auto occur :
       {irs::Occur::Must, irs::Occur::Should, irs::Occur::MustNot}) {
    terms += boolean.Terms(occur).size();
    for (const auto& child : boolean.Filters(occur)) {
      if (child && child->type() == irs::Type<irs::ByTerm>::id()) {
        ++terms;
      } else {
        ++others;
      }
      if (terms > 1 || others > 0) {
        return false;
      }
    }
  }
  return terms == 1 && others == 0;
}

bool CheapToPrepare(const irs::Filter& filter) {
  const auto type = filter.type();
  if (type == irs::Type<irs::ByTerm>::id() ||
      type == irs::Type<irs::ByGranularRange>::id() ||
      type == irs::Type<irs::All>::id() ||
      type == irs::Type<irs::Empty>::id()) {
    return true;
  }
  if (type != irs::Type<irs::BooleanFilter>::id()) {
    return false;
  }
  const auto& boolean = irs::utils::downCast<irs::BooleanFilter>(filter);
  for (const auto occur :
       {irs::Occur::Must, irs::Occur::Should, irs::Occur::MustNot}) {
    for (const auto& child : boolean.Filters(occur)) {
      if (!child || !CheapToPrepare(*child)) {
        return false;
      }
    }
  }
  return true;
}

std::optional<uint64_t> UniqueBound(const ScanGlobalState& g,
                                    const irs::Filter* where) {
  const auto& bind = g.Bind();
  std::optional<uint64_t> bound;
  const auto tighten = [&](std::optional<uint64_t> rows) {
    if (rows) {
      bound = bound ? std::min(*bound, *rows) : *rows;
    }
  };
  if (where) {
    tighten(UniqueMatchBound(bind, *where));
  }
  for (const auto& cf : g.col_filters) {
    if (!cf.is_score && !cf.optional && !cf.row_gather &&
        cf.extract_path.empty() && cf.row_fields.empty()) {
      tighten(UniqueColumnBound(bind, cf.field,
                                *duckdb::ExpressionFilter::GetExpressionFilter(
                                   *cf.filter, "UniqueBound")
                                   .expr));
    }
  }
  return bound;
}

void TakeUnit(ScanLocalState& l, ScanUnit unit) {
  l.unit = unit;
  l.has_unit = true;
  l.current_seg = unit.seg;
  l.rg_units += unit.live;
}

constexpr uint64_t PackRange(uint32_t next, uint32_t end) noexcept {
  return uint64_t{end} << 32 | next;
}

constexpr uint32_t RangeNext(uint64_t range) noexcept {
  return static_cast<uint32_t>(range);
}

constexpr uint32_t RangeEnd(uint64_t range) noexcept {
  return static_cast<uint32_t>(range >> 32);
}

std::atomic_uint64_t& OwnRange(ScanGlobalState& g, ScanLocalState& l) noexcept {
  return l.worker < g.workers ? g.ranges[l.worker].range : l.private_range;
}

void SetOwned(ScanGlobalState& g, ScanLocalState& l, uint32_t seg,
              uint64_t range) noexcept {
  l.owned_seg = seg;
  OwnRange(g, l).store(range, std::memory_order_relaxed);
}

ScanUnit UnitOf(uint32_t seg, uint32_t begin, uint32_t end) noexcept {
  return {
    .seg = seg,
    .rg_begin = begin,
    .rg_end = end,
    .live = end - begin,
    .whole = false,
  };
}

bool ClaimOwned(ScanGlobalState& g, ScanLocalState& l) {
  auto& own = OwnRange(g, l);
  auto range = own.load(std::memory_order_relaxed);
  for (;;) {
    const auto next = RangeNext(range);
    const auto end = RangeEnd(range);
    if (next >= end) {
      return false;
    }
    if (own.compare_exchange_weak(range, PackRange(next + 1, end),
                                  std::memory_order_relaxed,
                                  std::memory_order_relaxed)) {
      const auto base = g.Segment(l.owned_seg).base;
      l.claimed_end = next + 1;
      TakeUnit(l, UnitOf(l.owned_seg, next - base, next + 1 - base));
      return true;
    }
  }
}

bool Own(ScanGlobalState& g, ScanLocalState& l, uint32_t seg) {
  const auto& work = g.Segment(seg);
  SetOwned(g, l, seg, PackRange(work.base, work.base + work.Units()));
  return ClaimOwned(g, l);
}

uint32_t SegmentAt(const ScanGlobalState& g, uint32_t pos) noexcept {
  const auto it = absl::c_upper_bound(g.split_bases, pos);
  SDB_ASSERT(it != g.split_bases.begin());
  return g.split_segments[std::distance(g.split_bases.begin(), it) - 1];
}

uint32_t LargestRange(const ScanGlobalState& g,
                      const ScanLocalState& l) noexcept {
  uint32_t own_begin = 0;
  uint32_t ahead_begin = 0;
  uint32_t ahead_end = 0;
  if (l.owned_seg != std::numeric_limits<uint32_t>::max()) {
    const auto& own = g.segments[l.owned_seg];
    own_begin = own.base;
    ahead_begin = l.claimed_end;
    ahead_end = own.base + own.Units();
  }
  auto victim = g.workers;
  auto ahead = g.workers;
  uint32_t most = 0;
  uint32_t most_ahead = 0;
  for (uint32_t w = 0; w != g.workers; ++w) {
    const auto r = g.ranges[w].range.load(std::memory_order_relaxed);
    const auto next = RangeNext(r);
    const auto end = RangeEnd(r);
    if (end <= next || (next >= own_begin && next < ahead_begin)) {
      continue;
    }
    if (end - next > most) {
      most = end - next;
      victim = w;
    }
    if (next >= ahead_begin && next < ahead_end && end - next > most_ahead) {
      most_ahead = end - next;
      ahead = w;
    }
  }
  return ahead != g.workers ? ahead : victim;
}

bool StealFrom(ScanGlobalState& g, ScanLocalState& l, uint32_t victim) {
  auto& slot = g.ranges[victim].range;
  auto range = slot.load(std::memory_order_relaxed);
  for (;;) {
    const auto next = RangeNext(range);
    const auto end = RangeEnd(range);
    if (next >= end) {
      return false;
    }
    const auto mid = next + (end - next) / 2;
    if (slot.compare_exchange_weak(range, PackRange(next, mid),
                                   std::memory_order_relaxed,
                                   std::memory_order_relaxed)) {
      SetOwned(g, l, SegmentAt(g, mid), PackRange(mid, end));
      return true;
    }
  }
}

bool Steal(ScanGlobalState& g, ScanLocalState& l) {
  for (auto victim = LargestRange(g, l); victim != g.workers;
       victim = LargestRange(g, l)) {
    if (StealFrom(g, l, victim)) {
      return true;
    }
  }
  return false;
}

bool ClaimOrderedUnit(ScanGlobalState& g, ScanLocalState& l) {
  if (g.ordered_exhausted.load(std::memory_order_relaxed)) {
    return false;
  }
  auto& heap = g.ordered_heap;
  absl::MutexLock lock{&g.ordered_mutex};
  for (;;) {
    if (DropPruned(g, OrderBound(g))) {
      g.ordered_exhausted.store(true, std::memory_order_relaxed);
      return false;
    }
    absl::c_pop_heap(heap, std::greater{});
    const auto top = heap.back();
    heap.pop_back();
    auto& work = g.Segment(top.id);
    if (!work.ordered) {
      work.ordered = BuildSegmentOrderedUnits(g, work, top.id, top.key);
      if (!work.ordered->units.empty()) {
        heap.push_back({work.ordered->keys.front(), top.id});
        absl::c_push_heap(heap, std::greater{});
      }
      continue;
    }
    auto& ordered = *work.ordered;
    TakeUnit(l, ordered.units[ordered.next++]);
    if (ordered.next < ordered.units.size()) {
      heap.push_back({ordered.keys[ordered.next], top.id});
      absl::c_push_heap(heap, std::greater{});
    }
    return true;
  }
}

void PublishFinished(ScanGlobalState& g, ScanLocalState& l) {
  if (l.finished_segments != 0) {
    g.done_segments.fetch_add(l.finished_segments, std::memory_order_acq_rel);
    l.finished_segments = 0;
  }
}

bool ClaimBatch(ScanGlobalState& g, ScanLocalState& l) {
  auto begin = g.next_segment.load(std::memory_order_relaxed);
  for (;;) {
    if (begin >= g.live_segments) {
      return false;
    }
    const auto& first = g.Segment(g.segment_order[begin]);
    const auto take =
      std::max<uint32_t>(1, (g.live_segments - begin) / (2 * g.workers));
    const auto end =
      first.claim.load(std::memory_order_relaxed) == SegmentWork::kSplit
        ? begin + 1
        : std::min(first.run_end, begin + take);
    if (g.next_segment.compare_exchange_weak(
          begin, end, std::memory_order_relaxed, std::memory_order_relaxed)) {
      l.batch_next = begin;
      l.batch_end = end;
      return true;
    }
  }
}

void CutStarts(ScanGlobalState& g, uint64_t positions) {
  const auto segments = g.split_segments.size();
  if (segments == 0 || segments >= g.workers) {
    return;
  }
  const auto units = [&](size_t i) {
    return uint64_t{g.Segment(g.split_segments[i]).Units()};
  };
  std::vector<uint64_t> parts(segments);
  uint64_t cut = 0;
  for (size_t i = 0; i != segments; ++i) {
    parts[i] =
      std::clamp<uint64_t>(units(i) * g.workers / positions, 1, units(i));
    cut += parts[i];
  }
  for (; cut < g.workers; ++cut) {
    auto best = segments;
    for (size_t i = 0; i != segments; ++i) {
      if (parts[i] < units(i) &&
          (best == segments ||
           units(i) * parts[best] > units(best) * parts[i])) {
        best = i;
      }
    }
    if (best == segments) {
      break;
    }
    ++parts[best];
  }
  for (size_t i = 0; i != segments; ++i) {
    auto& work = g.Segment(g.split_segments[i]);
    const auto base = uint64_t{work.base};
    work.first_start = static_cast<uint32_t>(g.starts.size());
    work.start_count = static_cast<uint32_t>(parts[i]);
    for (uint64_t j = 0; j != parts[i]; ++j) {
      g.starts.push_back(
        PackRange(static_cast<uint32_t>(base + units(i) * j / parts[i]),
                  static_cast<uint32_t>(base + units(i) * (j + 1) / parts[i])));
    }
  }
}

bool TakeStart(ScanGlobalState& g, ScanLocalState& l, uint32_t seg) {
  auto& work = g.Segment(seg);
  if (work.next_start.load(std::memory_order_relaxed) >= work.start_count) {
    return false;
  }
  const auto j = work.next_start.fetch_add(1, std::memory_order_relaxed);
  if (j >= work.start_count) {
    return false;
  }
  SetOwned(g, l, seg, g.starts[work.first_start + j]);
  return true;
}

bool ClaimStart(ScanGlobalState& g, ScanLocalState& l) {
  if (g.starts.empty() || l.starts_exhausted) {
    return false;
  }
  if (l.owned_seg != std::numeric_limits<uint32_t>::max() &&
      TakeStart(g, l, l.owned_seg)) {
    return true;
  }
  const auto segments = static_cast<uint32_t>(g.split_segments.size());
  const auto first = g.arrivals.fetch_add(1, std::memory_order_relaxed);
  for (uint32_t k = 0; k != segments; ++k) {
    if (TakeStart(g, l, g.split_segments[(first + k) % segments])) {
      return true;
    }
  }
  l.starts_exhausted = true;
  return false;
}

bool SegmentStaysWhole(ScanShape shape, const irs::QueryBuilder& query,
                       bool exact_counts, uint64_t rg_size) {
  if (query.Kind() == irs::QueryKind::Empty) {
    return true;
  }
  if (shape == ScanShape::Count) {
    return (exact_counts && irs::count::IsConstant(query)) ||
           query.Postings() < rg_size;
  }
  return std::max<uint64_t>(query.Postings(), query.EstimateMax()) <
         STANDARD_VECTOR_SIZE;
}

}  // namespace

void BuildClaimPlan(ScanGlobalState& g, duckdb::ClientContext& context) {
  const auto& bind = g.Bind();
  bool ordered = bind.scan_order && (g.shape == ScanShape::Stream ||
                                     g.shape == ScanShape::ColScan);
  if (ordered) {
    g.order_type = bind.ColumnTypeById(bind.scan_order->column);
  }

  g.rg_size = bind.relation.RowGroupSize();

  const auto threads =
    static_cast<uint64_t>(duckdb::TaskScheduler::QueryThreads(context));

  g.segments = std::make_unique<SegmentWork[]>(g.total_segments);
  g.live_segments = static_cast<uint32_t>(g.segment_order.size());
  if (ordered && bind.scan_order->Skips()) {
    for (const auto [seg, rg] : OffsetSkippedRowGroups(bind, *g.reader)) {
      g.Segment(seg).skipped_rgs.push_back(rg);
    }
  }

  if (!ordered) {
    const auto& reader = *g.reader;
    absl::c_sort(g.segment_order, [&](uint32_t l, uint32_t r) {
      return reader[l].live_docs_count() > reader[r].live_docs_count();
    });
  }

  const auto where = g.ClaimedWhere();
  const bool eager =
    !ordered && (g.shape == ScanShape::Count || g.shape == ScanShape::Stream) &&
    !g.scorer_obj && g.vector_scorer == nullptr && g.splittable &&
    g.live_segments < threads && CheapToPrepare(*g.filter);
  const bool constant_count = !eager && g.shape == ScanShape::Count &&
                              g.col_filters.empty() && where &&
                              ConstantCount(*where);
  const auto unique = UniqueBound(g, where.get());
  const bool single = unique && *unique <= STANDARD_VECTOR_SIZE;
  irs::ColFilterStateCache states;
  irs::ColFilterClassification cls;
  uint64_t parallel = 0;
  uint64_t rgs = 0;
  uint64_t whole = 0;
  uint64_t whole_docs = 0;
  auto run_end = g.live_segments;
  for (auto i = g.live_segments; i-- != 0;) {
    const auto seg = g.segment_order[i];
    auto& work = g.Segment(seg);
    const auto& sub = (*g.reader)[seg];
    work.groups = SegmentRowGroups{sub, g.rg_size};
    work.rg_count = work.groups.Count();
    const bool exact_counts =
      g.col_filters.empty() && sub.live_docs_count() == sub.docs_count();
    bool split =
      g.splittable && work.rg_count > 1 && !(constant_count && exact_counts);
    if (split && eager) {
      PrepareSegmentNow(g, seg);
      split =
        !SegmentStaysWhole(g.shape, *g.queries[seg], exact_counts, g.rg_size);
    }
    SDB_ASSERT(split || work.LiveCount() == 0 || work.skipped_rgs.empty());
    work.done_rgs.store(static_cast<uint32_t>(work.skipped_rgs.size()),
                        std::memory_order_relaxed);
    work.claim.store(split ? SegmentWork::kSplit : SegmentWork::kWhole,
                     std::memory_order_relaxed);
    if (split) {
      run_end = i;
      if (!ordered && !g.col_filters.empty() && threads > 1) {
        ClassifySegmentColFilters(sub, g, g.col_filters, states, cls, false);
        work.dead = cls.segment_dead;
      }
    }
    work.run_end = run_end;
  }
  uint32_t skipped = 0;
  uint64_t positions = 0;
  for (uint32_t i = 0; i != g.live_segments; ++i) {
    const auto seg = g.segment_order[i];
    auto& work = g.Segment(seg);
    const bool split =
      work.claim.load(std::memory_order_relaxed) == SegmentWork::kSplit;
    const auto live = split ? work.LiveCount() : std::min(work.LiveCount(), 1U);
    if (live == 0) {
      ++skipped;
      continue;
    }
    rgs += work.rg_count;
    if (const auto& query = g.queries[seg];
        query && query->Kind() == irs::QueryKind::Empty) {
      continue;
    }
    const auto& sub = (*g.reader)[seg];
    if (split) {
      parallel += live;
      work.base = static_cast<uint32_t>(positions);
      g.split_bases.push_back(work.base);
      g.split_segments.push_back(seg);
      positions += work.Units();
      SDB_ASSERT(positions <= std::numeric_limits<uint32_t>::max());
    } else {
      ++whole;
      whole_docs += sub.Meta().docs_count;
    }
  }
  g.done_segments.store(skipped, std::memory_order_relaxed);
  parallel += std::min(whole, (whole_docs + g.rg_size - 1) / g.rg_size);
  if (g.stats_stage) {
    parallel = std::max<uint64_t>(parallel, g.total_segments);
  }
  if (single) {
    parallel = 1;
  }
  g.workers = static_cast<uint32_t>(std::clamp<uint64_t>(
    std::min(threads, parallel), 1, std::numeric_limits<uint32_t>::max()));
  g.ranges = std::make_unique<ScanGlobalState::RangeSlot[]>(g.workers);
  if (!ordered) {
    CutStarts(g, positions);
  }
  constexpr uint64_t kFoldSharers = 4;
  g.fold_rgs = kFoldSharers * rgs / g.workers;

  if (ordered) {
    BuildOrderedHeap(g);
  }
}

bool ClaimUnit(ScanGlobalState& g, ScanLocalState& l) {
  l.has_unit = false;
  if (l.units_exhausted) {
    return false;
  }
  if (g.Ordered()) {
    PublishFinished(g, l);
    if (ClaimOrderedUnit(g, l)) {
      return true;
    }
    l.units_exhausted = true;
    return false;
  }
  if (ClaimOwned(g, l)) {
    return true;
  }
  while (ClaimStart(g, l)) {
    if (ClaimOwned(g, l)) {
      return true;
    }
  }
  for (;;) {
    while (l.batch_next != l.batch_end) {
      const auto seg = g.segment_order[l.batch_next++];
      auto& work = g.Segment(seg);
      if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
        TakeUnit(l, WholeUnit(seg, work));
        return true;
      }
      if (g.starts.empty() && Own(g, l, seg)) {
        return true;
      }
    }
    PublishFinished(g, l);
    if (!ClaimBatch(g, l)) {
      break;
    }
  }
  while (Steal(g, l)) {
    if (ClaimOwned(g, l)) {
      return true;
    }
  }
  PublishFinished(g, l);
  l.units_exhausted = true;
  return false;
}

namespace {

void BuildRowGroupChecks(const ScanGlobalState& g, ScanLocalState& l,
                         uint32_t seg) {
  l.rg_checks.clear();
  l.checked_seg = seg;
  const auto* col_reader = (*g.reader)[seg].GetColReader();
  if (!col_reader) {
    return;
  }
  for (const auto& spec : l.seg_cls.active) {
    if (spec.is_score || spec.row_gather || !spec.row_fields.empty()) {
      continue;
    }
    const auto* column = col_reader->Column(spec.field);
    if (!column) {
      continue;
    }
    const auto* reader = irs::FilteredColumn(*column, spec);
    if (!reader || reader->Type().IsNested()) {
      continue;
    }
    l.rg_checks.push_back({
      .reader = reader,
      .filter = &spec.filter->Cast<duckdb::ExpressionFilter>(),
      .state = &l.filter_states.State(*g.client_context, *spec.filter),
      .window = {},
    });
  }
}

bool RowGroupDead(RowGroupCheck& check, uint64_t begin, uint64_t end) {
  for (auto row = begin; row < end; row = check.window.end) {
    check.window = check.reader->Locate(row, check.window);
    if (!irs::ZoneDead(check.filter->CheckStatistics(
          check.reader->RowGroupStatistics(check.window.block),
          *check.state))) {
      return false;
    }
  }
  return true;
}

void AddDone(ScanGlobalState& g, ScanLocalState& l, uint32_t seg,
             uint32_t rgs) {
  auto& work = g.Segment(seg);
  if (work.done_rgs.fetch_add(rgs, std::memory_order_acq_rel) + rgs ==
      work.rg_count) {
    ++l.finished_segments;
  }
}

bool TrimDeadRowGroups(ScanGlobalState& g, ScanLocalState& l) {
  auto& unit = l.unit;
  if (unit.whole) {
    return true;
  }
  if (l.checked_seg != unit.seg) {
    BuildRowGroupChecks(g, l, unit.seg);
  }
  if (l.rg_checks.empty()) {
    return true;
  }
  const auto& groups = g.Segment(unit.seg).groups;
  auto first = unit.rg_end;
  auto last = unit.rg_begin;
  uint32_t live = 0;
  for (auto rg = unit.rg_begin; rg != unit.rg_end; ++rg) {
    const auto begin = groups.Begin(rg);
    const auto end = groups.End(rg);
    bool dead = false;
    for (auto& check : l.rg_checks) {
      if (RowGroupDead(check, begin, end)) {
        dead = true;
        break;
      }
    }
    if (!dead) {
      ++live;
      first = std::min(first, rg);
      last = rg + 1;
    }
  }
  l.rg_units -= unit.live - live;
  if (live == 0) {
    return false;
  }
  const auto trimmed = (first - unit.rg_begin) + (unit.rg_end - last);
  if (trimmed != 0) {
    AddDone(g, l, unit.seg, trimmed);
    unit.rg_begin = first;
    unit.rg_end = last;
  }
  unit.live = live;
  return true;
}

}  // namespace

bool NextLiveUnit(ScanGlobalState& g, ScanLocalState& l) {
  while (ClaimUnit(g, l)) {
    l.Classify(g, l.unit.seg);
    if (!l.seg_cls.segment_dead && TrimDeadRowGroups(g, l)) {
      return true;
    }
    FinishUnit(g, l);
  }
  return false;
}

bool FinishUnit(ScanGlobalState& g, ScanLocalState& l) {
  SDB_ASSERT(l.has_unit);
  l.has_unit = false;
  const auto& unit = l.unit;
  if (!unit.whole) {
    auto& work = g.Segment(unit.seg);
    const auto rgs = unit.rg_end - unit.rg_begin;
    if (work.done_rgs.fetch_add(rgs, std::memory_order_acq_rel) + rgs !=
        work.rg_count) {
      return false;
    }
  }
  ++l.finished_segments;
  return true;
}

}  // namespace sdb::connector
