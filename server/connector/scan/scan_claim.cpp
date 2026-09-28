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
#include <array>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <optional>

#include "connector/scan/scan_state.h"
#include "query/config.h"

namespace sdb::connector {

UnitRows ScanGlobalState::RowsOf(const ScanUnit& unit) const noexcept {
  const uint64_t visible = irs::VisibleCount((*reader)[unit.seg].Meta());
  return {.begin = std::min(visible, uint64_t{unit.rg_begin} * rg_size),
          .end = std::min(visible, uint64_t{unit.rg_end} * rg_size)};
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

constexpr std::array<std::string_view, 3> kSplitModes{"tail", "always",
                                                      "never"};
constexpr std::array<std::string_view, 3> kOrderModes{"smallest_first",
                                                      "largest_first", "order"};

std::optional<SplitMode> ReadSplit(duckdb::ClientContext& context) {
  static constinit SettingRef gSplit{"sdb_scan_split"};
  const auto i = gSplit.Enum(context, kSplitModes);
  if (i == kSplitModes.size()) {
    return std::nullopt;
  }
  return static_cast<SplitMode>(i);
}

OrderMode ReadOrder(duckdb::ClientContext& context, bool scan_ordered) {
  static constinit SettingRef gOrder{"sdb_scan_order"};
  const auto i = gOrder.Enum(context, kOrderModes);
  const auto mode =
    i == kOrderModes.size()
      ? (scan_ordered ? OrderMode::Order : OrderMode::LargestFirst)
      : static_cast<OrderMode>(i);
  return mode == OrderMode::Order && !scan_ordered ? OrderMode::LargestFirst
                                                   : mode;
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
  if (!dyn || !dyn->initialized.load(std::memory_order_acquire)) {
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

std::unique_ptr<OrderedUnits> BuildSegmentOrderedUnits(
  ScanGlobalState& g, SegmentWork& work, uint32_t seg,
  duckdb::Value segment_key);

void BuildOrderedHeap(ScanGlobalState& g) {
  const auto& order = *g.Bind().scan_order;
  g.order_dynamic_filter = FindOrderDynamicFilter(g);
  g.ordered_heap.reserve(g.segment_order.size());
  for (const auto seg : g.segment_order) {
    duckdb::Value v;
    if (const auto* column = OrderColumn(g, seg)) {
      v = duckdb::RowGroupReorderer::RetrieveStat(
        column->MergedStatistics(), order.order_by, order.column_type);
    }
    auto& work = g.Segment(seg);
    work.ordered = BuildSegmentOrderedUnits(g, work, seg, std::move(v));
    g.ordered_heap.push_back({seg, work.ordered->keys.front()});
  }
  absl::c_make_heap(g.ordered_heap, HeapOrder(order));
  g.ordered = true;
}

std::unique_ptr<OrderedUnits> BuildSegmentOrderedUnits(
  ScanGlobalState& g, SegmentWork& work, uint32_t seg,
  duckdb::Value segment_key) {
  const auto& order = *g.Bind().scan_order;
  auto built = std::make_unique<OrderedUnits>();
  if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
    built->units.push_back(
      {.seg = seg, .rg_begin = 0, .rg_end = work.rg_count, .whole = true});
    built->keys.push_back(std::move(segment_key));
    return built;
  }
  const auto* column = OrderColumn(g, seg);
  const uint64_t docs = irs::VisibleCount((*g.reader)[seg].Meta());
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
    built->units.push_back(
      {.seg = seg, .rg_begin = key.id, .rg_end = key.id + 1, .whole = false});
    built->keys.push_back(std::move(key.value));
  }
  return built;
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
    }
  }
  return terms == 1 && others == 0;
}

void TakeUnit(ScanLocalState& l, ScanUnit unit) {
  l.unit = unit;
  l.has_unit = true;
  l.current_seg = unit.seg;
  l.rg_units += unit.rg_end - unit.rg_begin;
}

bool ClaimRowGroups(ScanGlobalState& g, ScanLocalState& l, uint32_t seg) {
  auto& work = g.Segment(seg);
  auto begin = work.next_rg.load(std::memory_order_relaxed);
  for (;;) {
    if (begin >= work.rg_count) {
      return false;
    }
    const auto take =
      std::max<uint32_t>(1, (work.rg_count - begin) / g.workers);
    if (work.next_rg.compare_exchange_weak(begin, begin + take,
                                           std::memory_order_relaxed,
                                           std::memory_order_relaxed)) {
      TakeUnit(l, {.seg = seg,
                   .rg_begin = begin,
                   .rg_end = begin + take,
                   .whole = false});
      return true;
    }
  }
}

bool ClaimOrderedUnit(ScanGlobalState& g, ScanLocalState& l) {
  const auto heap_order = HeapOrder(*g.Bind().scan_order);
  auto& heap = g.ordered_heap;
  for (;;) {
    if (g.ordered_exhausted.load(std::memory_order_relaxed)) {
      return false;
    }
    const auto bound = OrderBound(g);
    ScanOrderKey head;
    {
      absl::MutexLock lock{&g.ordered_mutex};
      if (DropPruned(g, bound)) {
        g.ordered_exhausted.store(true, std::memory_order_relaxed);
        return false;
      }
      absl::c_pop_heap(heap, heap_order);
      head = std::move(heap.back());
      heap.pop_back();
      if (auto* ordered = g.Segment(head.id).ordered.get()) {
        const auto k = ordered->next++;
        TakeUnit(l, ordered->units[k]);
        if (ordered->next < ordered->units.size()) {
          heap.push_back({head.id, ordered->keys[ordered->next]});
          absl::c_push_heap(heap, heap_order);
        }
        return true;
      }
    }
    auto& work = g.Segment(head.id);
    auto built =
      BuildSegmentOrderedUnits(g, work, head.id, std::move(head.value));
    absl::MutexLock lock{&g.ordered_mutex};
    heap.push_back({head.id, built->keys.front()});
    work.ordered = std::move(built);
    absl::c_push_heap(heap, heap_order);
  }
}

void PublishFinished(ScanGlobalState& g, ScanLocalState& l) {
  if (l.finished_segments != 0) {
    g.done_segments.fetch_add(l.finished_segments, std::memory_order_acq_rel);
    l.finished_segments = 0;
  }
}

void Share(ScanGlobalState& g, const ScanLocalState& l, uint32_t seg) {
  if (l.worker < g.workers) {
    g.joinable[l.worker].store(seg, std::memory_order_relaxed);
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

bool Join(ScanGlobalState& g, ScanLocalState& l) {
  for (;;) {
    uint32_t best = std::numeric_limits<uint32_t>::max();
    uint32_t most = 0;
    for (uint32_t w = 0; w != g.workers; ++w) {
      const auto seg = g.joinable[w].load(std::memory_order_relaxed);
      if (seg == std::numeric_limits<uint32_t>::max()) {
        continue;
      }
      const auto& work = g.Segment(seg);
      const auto next = work.next_rg.load(std::memory_order_relaxed);
      if (next < work.rg_count && work.rg_count - next > most) {
        most = work.rg_count - next;
        best = seg;
      }
    }
    if (best == std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    if (ClaimRowGroups(g, l, best)) {
      Share(g, l, best);
      return true;
    }
  }
}

}  // namespace

void BuildClaimPlan(ScanGlobalState& g, duckdb::ClientContext& context) {
  const auto& bind = g.Bind();
  const bool scan_ordered =
    bind.scan_order.has_value() &&
    (g.shape == ScanShape::Stream || g.shape == ScanShape::ColScan);

  g.rg_size = bind.relation.row_group_size != 0 ? bind.relation.row_group_size
                                                : DEFAULT_ROW_GROUP_SIZE;

  g.order = ReadOrder(context, scan_ordered);
  const bool ordered = g.order == OrderMode::Order;

  const auto threads =
    static_cast<uint64_t>(duckdb::TaskScheduler::QueryThreads(context));

  g.segments = std::make_unique<SegmentWork[]>(g.total_segments);
  g.live_segments = static_cast<uint32_t>(g.segment_order.size());

  g.split = !g.splittable
              ? SplitMode::Never
              : ReadSplit(context).value_or(scan_ordered ? SplitMode::Always
                                                         : SplitMode::Tail);
  static constinit SettingRef gNoSplit{"sdb_scan_no_split_row_groups"};
  g.no_split_rgs = g.split == SplitMode::Never ? 0 : gNoSplit.Int(context);

  if (g.order == OrderMode::SmallestFirst ||
      g.order == OrderMode::LargestFirst) {
    const auto& reader = *g.reader;
    const bool largest_first = g.order == OrderMode::LargestFirst;
    absl::c_sort(g.segment_order, [&](uint32_t l, uint32_t r) {
      const auto lhs = reader[l].live_docs_count();
      const auto rhs = reader[r].live_docs_count();
      return largest_first ? lhs > rhs : lhs < rhs;
    });
  }

  const bool constant_count = g.shape == ScanShape::Count &&
                              g.col_filters.empty() && bind.search.filter &&
                              ConstantCount(*bind.search.filter);
  uint64_t parallel = 0;
  uint64_t rgs = 0;
  auto run_end = g.live_segments;
  for (auto i = g.live_segments; i-- != 0;) {
    const auto seg = g.segment_order[i];
    auto& work = g.Segment(seg);
    const auto& sub = (*g.reader)[seg];
    const uint64_t docs = irs::VisibleCount(sub.Meta());
    work.rg_count = static_cast<uint32_t>(
      std::max<uint64_t>(1, (docs + g.rg_size - 1) / g.rg_size));
    const bool split =
      g.split != SplitMode::Never && work.rg_count > g.no_split_rgs &&
      !(constant_count && sub.live_docs_count() == sub.docs_count());
    work.claim.store(split ? SegmentWork::kSplit : SegmentWork::kWhole,
                     std::memory_order_relaxed);
    if (split) {
      run_end = i;
    }
    work.run_end = run_end;
    parallel += split ? work.rg_count : 1;
    rgs += work.rg_count;
  }
  if (g.stats_stage) {
    parallel = std::max<uint64_t>(parallel, g.total_segments);
  }
  g.workers = static_cast<uint32_t>(std::clamp<uint64_t>(
    std::min(threads, parallel), 1, std::numeric_limits<uint32_t>::max()));
  g.joinable = std::make_unique<std::atomic_uint32_t[]>(g.workers);
  for (uint32_t w = 0; w != g.workers; ++w) {
    g.joinable[w].store(std::numeric_limits<uint32_t>::max(),
                        std::memory_order_relaxed);
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
  if (g.split == SplitMode::Always) {
    PublishFinished(g, l);
    for (;;) {
      auto i = g.next_segment.load(std::memory_order_relaxed);
      if (i >= g.live_segments) {
        break;
      }
      const auto seg = g.segment_order[i];
      auto& work = g.Segment(seg);
      if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
        if (g.next_segment.compare_exchange_weak(
              i, i + 1, std::memory_order_relaxed, std::memory_order_relaxed)) {
          TakeUnit(l, {.seg = seg,
                       .rg_begin = 0,
                       .rg_end = work.rg_count,
                       .whole = true});
          return true;
        }
        continue;
      }
      if (ClaimRowGroups(g, l, seg)) {
        return true;
      }
      g.next_segment.compare_exchange_strong(
        i, i + 1, std::memory_order_relaxed, std::memory_order_relaxed);
    }
    l.units_exhausted = true;
    return false;
  }
  if (l.current_seg != std::numeric_limits<uint32_t>::max() &&
      g.Segment(l.current_seg).claim.load(std::memory_order_relaxed) ==
        SegmentWork::kSplit &&
      ClaimRowGroups(g, l, l.current_seg)) {
    return true;
  }
  for (;;) {
    while (l.batch_next != l.batch_end) {
      const auto seg = g.segment_order[l.batch_next++];
      auto& work = g.Segment(seg);
      if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
        TakeUnit(
          l,
          {.seg = seg, .rg_begin = 0, .rg_end = work.rg_count, .whole = true});
        return true;
      }
      Share(g, l, seg);
      if (ClaimRowGroups(g, l, seg)) {
        return true;
      }
    }
    PublishFinished(g, l);
    if (!ClaimBatch(g, l)) {
      break;
    }
  }
  if (Join(g, l)) {
    return true;
  }
  PublishFinished(g, l);
  l.units_exhausted = true;
  return false;
}

bool NextLiveUnit(ScanGlobalState& g, ScanLocalState& l) {
  while (ClaimUnit(g, l)) {
    l.Classify(g, l.unit.seg);
    if (!l.seg_cls.segment_dead) {
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
