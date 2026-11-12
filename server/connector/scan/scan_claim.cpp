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
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <optional>
#include <vector>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

UnitRows ScanGlobalState::RowsOf(const ScanUnit& unit) const noexcept {
  const uint64_t docs = (*reader)[unit.seg].Meta().docs_count;
  return {.begin = std::min(docs, uint64_t{unit.rg_begin} * rg_size),
          .end = std::min(docs, uint64_t{unit.rg_end} * rg_size)};
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

bool OrderKeyBefore(const ScanOrderKey& l, const ScanOrderKey& r,
                    const ScanOrderSpec& order) {
  const bool ln = l.value.IsNull();
  const bool rn = r.value.IsNull();
  if (ln || rn) {
    const bool nulls_first =
      order.null_order == duckdb::OrderByNullType::NULLS_FIRST;
    return ln != rn ? (ln ? nulls_first : !nulls_first) : l.id < r.id;
  }
  const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
  const auto& lo = asc ? l.value : r.value;
  const auto& hi = asc ? r.value : l.value;
  return lo != hi ? lo < hi : l.id < r.id;
}

void SortScanOrderKeys(std::vector<ScanOrderKey>& keys,
                       const ScanOrderSpec& order) {
  absl::c_sort(keys, [&](const ScanOrderKey& l, const ScanOrderKey& r) {
    return OrderKeyBefore(l, r, order);
  });
}

auto HeapOrder(const ScanOrderSpec& order) {
  return [&order](const ScanOrderKey& l, const ScanOrderKey& r) {
    return OrderKeyBefore(r, l, order);
  };
}

struct RawKey {
  uint32_t rg = 0;
  size_t block = 0;
  std::optional<long double> key;
};

bool RawStatType(const duckdb::BaseStatistics& stats) noexcept {
  switch (stats.GetType().InternalType()) {
    case duckdb::PhysicalType::INT8:
    case duckdb::PhysicalType::INT16:
    case duckdb::PhysicalType::INT32:
    case duckdb::PhysicalType::INT64:
    case duckdb::PhysicalType::UINT8:
    case duckdb::PhysicalType::UINT16:
    case duckdb::PhysicalType::UINT32:
    case duckdb::PhysicalType::UINT64:
    case duckdb::PhysicalType::FLOAT:
    case duckdb::PhysicalType::DOUBLE:
      return true;
    default:
      return false;
  }
}

template<typename T>
long double RawStat(const duckdb::BaseStatistics& stats, bool min) {
  return min ? duckdb::NumericStats::GetMinUnsafe<T>(stats)
             : duckdb::NumericStats::GetMaxUnsafe<T>(stats);
}

std::optional<long double> RawStat(const duckdb::BaseStatistics& stats,
                                   bool min) {
  if (!duckdb::NumericStats::HasMinMax(stats)) {
    return std::nullopt;
  }
  switch (stats.GetType().InternalType()) {
    case duckdb::PhysicalType::INT8:
      return RawStat<int8_t>(stats, min);
    case duckdb::PhysicalType::INT16:
      return RawStat<int16_t>(stats, min);
    case duckdb::PhysicalType::INT32:
      return RawStat<int32_t>(stats, min);
    case duckdb::PhysicalType::INT64:
      return RawStat<int64_t>(stats, min);
    case duckdb::PhysicalType::UINT8:
      return RawStat<uint8_t>(stats, min);
    case duckdb::PhysicalType::UINT16:
      return RawStat<uint16_t>(stats, min);
    case duckdb::PhysicalType::UINT32:
      return RawStat<uint32_t>(stats, min);
    case duckdb::PhysicalType::UINT64:
      return RawStat<uint64_t>(stats, min);
    case duckdb::PhysicalType::FLOAT:
      return RawStat<float>(stats, min);
    case duckdb::PhysicalType::DOUBLE:
      return RawStat<double>(stats, min);
    default:
      return std::nullopt;
  }
}

RawKey UnitRawKey(const irs::ColumnReader& reader, uint32_t rg, bool asc,
                  bool min, uint64_t begin, uint64_t end) {
  RawKey best{.rg = rg};
  irs::BlockWindow w;
  for (uint64_t row = begin; row < end; row = w.end) {
    w = reader.Locate(row, w);
    const auto v = RawStat(reader.RowGroupStatistics(w.block), min);
    if (v && (!best.key || (asc ? *v < *best.key : *best.key < *v))) {
      best.key = v;
      best.block = w.block;
    }
  }
  return best;
}

duckdb::Value UnitOrderKey(const irs::ColumnReader& reader,
                           const ScanOrderSpec& order, uint64_t begin,
                           uint64_t end) {
  const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
  duckdb::Value best;
  irs::BlockWindow w;
  for (uint64_t row = begin; row < end; row = w.end) {
    w = reader.Locate(row, w);
    auto v = duckdb::RowGroupReorderer::RetrieveStat(
      reader.RowGroupStatistics(w.block), order.order_by, order.column_type);
    if (v.IsNull()) {
      continue;
    }
    if (best.IsNull() || (asc ? v < best : best < v)) {
      best = std::move(v);
    }
  }
  return best;
}

const irs::ColumnReader* OrderColumn(const ScanGlobalState& g, uint32_t seg) {
  const auto* col_reader = (*g.reader)[seg].GetColReader();
  return col_reader ? col_reader->Column(g.Bind().scan_order->column) : nullptr;
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
    const auto& expr = *duckdb::ExpressionFilter::GetExpressionFilter(
                          *cf.filter, "FindOrderDynamicFilter")
                          .expr;
    auto dyn = duckdb::ExpressionFilter::GetOptionalDynamicFilterData(expr);
    if (!dyn &&
        expr.GetExpressionClass() ==
          duckdb::ExpressionClass::BOUND_CONJUNCTION &&
        expr.GetExpressionType() == duckdb::ExpressionType::CONJUNCTION_AND) {
      for (const auto& child :
           expr.Cast<duckdb::BoundConjunctionExpression>().GetChildren()) {
        if ((dyn = duckdb::ExpressionFilter::GetOptionalDynamicFilterData(
               *child))) {
          break;
        }
      }
    }
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

duckdb::Value OrderBound(const ScanGlobalState& g) {
  const auto& dyn = g.order_dynamic_filter;
  if (!dyn || dyn->version.load(std::memory_order_acquire) == 0) {
    return duckdb::Value{};
  }
  duckdb::lock_guard<duckdb::mutex> lock{dyn->lock};
  return dyn->constant;
}

bool OrderPruned(const ScanGlobalState& g, const duckdb::Value& bound,
                 const ScanOrderKey& best) {
  return !bound.IsNull() && !best.value.IsNull() &&
         !duckdb::DynamicFilterData::CompareValue(
           g.order_dynamic_filter->comparison_type, bound, best.value);
}

bool DropPruned(ScanGlobalState& g, const duckdb::Value& bound) {
  auto& heap = g.ordered_heap;
  if (!heap.empty() && OrderPruned(g, bound, heap.front())) {
    std::erase_if(heap,
                  [](const ScanOrderKey& key) { return !key.value.IsNull(); });
    absl::c_make_heap(heap, HeapOrder(*g.Bind().scan_order));
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

std::unique_ptr<OrderedUnits> BuildSegmentOrderedUnits(ScanGlobalState& g,
                                                       SegmentWork& work,
                                                       uint32_t seg) {
  const auto& order = *g.Bind().scan_order;
  auto built = std::make_unique<OrderedUnits>();
  if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
    duckdb::Value segment_key;
    if (const auto* column = OrderColumn(g, seg)) {
      segment_key = duckdb::RowGroupReorderer::RetrieveStat(
        column->MergedStatistics(), order.order_by, order.column_type);
    }
    built->units.push_back(WholeUnit(seg, work));
    built->keys.push_back(std::move(segment_key));
    return built;
  }
  const auto* column = OrderColumn(g, seg);
  const uint64_t docs = (*g.reader)[seg].Meta().docs_count;
  if (column && order.column_type == duckdb::OrderByColumnType::NUMERIC &&
      RawStatType(column->MergedStatistics())) {
    const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
    const bool min = order.order_by == duckdb::OrderByStatistics::MIN;
    const bool nulls_first =
      order.null_order == duckdb::OrderByNullType::NULLS_FIRST;
    std::vector<RawKey> raw;
    raw.reserve(work.rg_count);
    for (uint32_t rg = 0; rg < work.rg_count; ++rg) {
      const auto begin = uint64_t{rg} * g.rg_size;
      raw.push_back(UnitRawKey(*column, rg, asc, min, begin,
                               std::min<uint64_t>(docs, begin + g.rg_size)));
    }
    absl::c_sort(raw, [&](const RawKey& l, const RawKey& r) {
      if (!l.key || !r.key) {
        return bool(l.key) != bool(r.key)
                 ? (!l.key ? nulls_first : !nulls_first)
                 : l.rg < r.rg;
      }
      if (*l.key != *r.key) {
        return asc ? *l.key < *r.key : *r.key < *l.key;
      }
      return l.rg < r.rg;
    });
    built->units.reserve(raw.size());
    built->keys.reserve(raw.size());
    for (const auto& key : raw) {
      built->units.push_back({.seg = seg,
                              .rg_begin = key.rg,
                              .rg_end = key.rg + 1,
                              .live = 1,
                              .whole = false});
      built->keys.push_back(key.key ? duckdb::RowGroupReorderer::RetrieveStat(
                                        column->RowGroupStatistics(key.block),
                                        order.order_by, order.column_type)
                                    : duckdb::Value{});
    }
    return built;
  }
  std::vector<ScanOrderKey> keys;
  keys.reserve(work.rg_count);
  for (uint32_t rg = 0; rg < work.rg_count; ++rg) {
    const auto begin = uint64_t{rg} * g.rg_size;
    const auto end = std::min<uint64_t>(docs, begin + g.rg_size);
    duckdb::Value v;
    if (column) {
      v = UnitOrderKey(*column, order, begin, end);
    }
    keys.push_back({rg, std::move(v)});
  }
  SortScanOrderKeys(keys, order);
  built->units.reserve(keys.size());
  built->keys.reserve(keys.size());
  for (auto& key : keys) {
    built->units.push_back({.seg = seg,
                            .rg_begin = key.id,
                            .rg_end = key.id + 1,
                            .live = 1,
                            .whole = false});
    built->keys.push_back(std::move(key.value));
  }
  return built;
}

void BuildOrderedHeap(ScanGlobalState& g) {
  g.order_dynamic_filter = FindOrderDynamicFilter(g);
  g.ordered = true;
  g.ordered_heap.reserve(g.segment_order.size());
  for (const auto seg : g.segment_order) {
    auto& work = g.Segment(seg);
    work.ordered = BuildSegmentOrderedUnits(g, work, seg);
    g.ordered_heap.push_back({seg, work.ordered->keys.front()});
  }
  absl::c_make_heap(g.ordered_heap, HeapOrder(*g.Bind().scan_order));
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
    if (!cf.is_score && !cf.zonemap_only && !cf.row_gather &&
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
  return {.seg = seg,
          .rg_begin = begin,
          .rg_end = end,
          .live = end - begin,
          .whole = false};
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
    const auto& work = g.Segment(l.owned_seg);
    const auto take = std::min(end - next, work.chunk);
    if (own.compare_exchange_weak(range, PackRange(next + take, end),
                                  std::memory_order_relaxed,
                                  std::memory_order_relaxed)) {
      l.claimed_end = next + take;
      TakeUnit(l,
               UnitOf(l.owned_seg, next - work.base, next + take - work.base));
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
  uint32_t ahead_begin = 0;
  uint32_t ahead_end = 0;
  if (l.owned_seg != std::numeric_limits<uint32_t>::max()) {
    const auto& own = g.segments[l.owned_seg];
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
    if (end <= next) {
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
  const auto heap_order = HeapOrder(*g.Bind().scan_order);
  auto& heap = g.ordered_heap;
  if (g.ordered_exhausted.load(std::memory_order_relaxed)) {
    return false;
  }
  const auto bound = OrderBound(g);
  absl::MutexLock lock{&g.ordered_mutex};
  if (DropPruned(g, bound)) {
    g.ordered_exhausted.store(true, std::memory_order_relaxed);
    return false;
  }
  absl::c_pop_heap(heap, heap_order);
  const auto seg = heap.back().id;
  heap.pop_back();
  auto& ordered = *g.Segment(seg).ordered;
  TakeUnit(l, ordered.units[ordered.next++]);
  if (ordered.next < ordered.units.size()) {
    heap.push_back({seg, ordered.keys[ordered.next]});
    absl::c_push_heap(heap, heap_order);
  }
  return true;
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

}  // namespace

void BuildClaimPlan(ScanGlobalState& g, duckdb::ClientContext& context) {
  const auto& bind = g.Bind();
  const bool ordered =
    bind.scan_order.has_value() &&
    (g.shape == ScanShape::Stream || g.shape == ScanShape::ColScan);

  g.rg_size = bind.relation.row_group_size != 0 ? bind.relation.row_group_size
                                                : DEFAULT_ROW_GROUP_SIZE;

  const auto threads =
    static_cast<uint64_t>(duckdb::TaskScheduler::QueryThreads(context));

  g.segments = std::make_unique<SegmentWork[]>(g.total_segments);
  g.live_segments = static_cast<uint32_t>(g.segment_order.size());

  if (!ordered) {
    const auto& reader = *g.reader;
    absl::c_sort(g.segment_order, [&](uint32_t l, uint32_t r) {
      return reader[l].live_docs_count() > reader[r].live_docs_count();
    });
  }

  const auto where = g.ClaimedWhere();
  const bool constant_count = g.shape == ScanShape::Count &&
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
    const uint64_t docs = sub.Meta().docs_count;
    work.rg_count = static_cast<uint32_t>(
      std::max<uint64_t>(1, (docs + g.rg_size - 1) / g.rg_size));
    const bool split =
      g.splittable && work.rg_count > 1 &&
      !(constant_count && sub.live_docs_count() == sub.docs_count());
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
    const auto live = split ? work.LiveCount() : 1;
    if (live == 0) {
      ++skipped;
      continue;
    }
    rgs += work.rg_count;
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
  for (const auto seg : g.split_segments) {
    auto& work = g.Segment(seg);
    work.chunk = std::max<uint32_t>(1, work.rg_count / g.workers);
  }
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
    l.rg_checks.push_back(
      {.reader = reader,
       .filter = &spec.filter->Cast<duckdb::ExpressionFilter>(),
       .state = &l.filter_states.State(*g.client_context, *spec.filter),
       .window = {}});
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
  const uint64_t docs = (*g.reader)[unit.seg].Meta().docs_count;
  auto first = unit.rg_end;
  auto last = unit.rg_begin;
  uint32_t live = 0;
  for (auto rg = unit.rg_begin; rg != unit.rg_end; ++rg) {
    const auto begin = uint64_t{rg} * g.rg_size;
    const auto end = std::min(docs, begin + g.rg_size);
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
