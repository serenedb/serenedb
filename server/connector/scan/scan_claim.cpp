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

irs::DocRange ScanGlobalState::RangeOf(const ScanUnit& unit) const noexcept {
  if (unit.whole) {
    return {};
  }
  const auto docs = (*reader)[unit.seg].docs_count();
  const auto begin =
    std::min<uint64_t>(docs, uint64_t{unit.rg_begin} * rg_size);
  const auto end = uint64_t{unit.rg_end} * rg_size;
  return {.begin = irs::doc_limits::min() + static_cast<irs::doc_id_t>(begin),
          .end = end >= docs
                   ? irs::doc_limits::eof()
                   : irs::doc_limits::min() + static_cast<irs::doc_id_t>(end)};
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
      ? (scan_ordered ? OrderMode::Order : OrderMode::SmallestFirst)
      : static_cast<OrderMode>(i);
  return mode == OrderMode::Order && !scan_ordered ? OrderMode::SmallestFirst
                                                   : mode;
}

const irs::ColumnReader* OrderColumn(const ScanGlobalState& g, uint32_t seg) {
  const auto* col_reader = (*g.reader)[seg].GetColReader();
  return col_reader ? col_reader->Column(g.Bind().scan_order->column)
                    : nullptr;
}

void BuildOrderedHeap(ScanGlobalState& g) {
  const auto& order = *g.Bind().scan_order;
  g.ordered_heap.reserve(g.segment_order.size());
  for (const auto seg : g.segment_order) {
    duckdb::Value v;
    if (const auto* column = OrderColumn(g, seg)) {
      v = duckdb::RowGroupReorderer::RetrieveStat(
        column->MergedStatistics(), order.order_by, order.column_type);
    }
    g.ordered_heap.push_back({seg, std::move(v)});
  }
  absl::c_make_heap(g.ordered_heap, HeapOrder(order));
  g.ordered = true;
}

void BuildSegmentOrderedUnits(ScanGlobalState& g, SegmentWork& work,
                              uint32_t seg, duckdb::Value segment_key) {
  const auto& order = *g.Bind().scan_order;
  if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
    work.ordered_units.push_back(
      {.seg = seg, .rg_begin = 0, .rg_end = work.rg_count, .whole = true});
    work.ordered_keys.push_back(std::move(segment_key));
    return;
  }
  const auto* column = OrderColumn(g, seg);
  const auto docs = (*g.reader)[seg].docs_count();
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
  work.ordered_units.reserve(keys.size());
  work.ordered_keys.reserve(keys.size());
  for (auto& key : keys) {
    work.ordered_units.push_back(
      {.seg = seg, .rg_begin = key.id, .rg_end = key.id + 1, .whole = false});
    work.ordered_keys.push_back(std::move(key.value));
  }
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
  if (unit.whole) {
    ++l.whole_units;
  } else {
    ++l.rg_units;
  }
}

void Exhaust(ScanGlobalState& g, ScanLocalState& l) {
  l.units_exhausted = true;
  g.metrics.whole_units.fetch_add(l.whole_units, std::memory_order_relaxed);
  g.metrics.rg_units.fetch_add(l.rg_units, std::memory_order_relaxed);
}

bool ClaimRowGroups(ScanGlobalState& g, ScanLocalState& l, uint32_t seg,
                    bool front) {
  auto& work = g.Segment(seg);
  auto packed = work.rgs.load(std::memory_order_relaxed);
  for (;;) {
    const auto begin = SegmentWork::Front(packed);
    const auto end = SegmentWork::Back(packed);
    if (begin >= end) {
      return false;
    }
    const auto take = std::min(g.unit_rgs, end - begin);
    const auto next = front ? SegmentWork::Pack(begin + take, end)
                            : SegmentWork::Pack(begin, end - take);
    if (work.rgs.compare_exchange_weak(packed, next, std::memory_order_relaxed,
                                       std::memory_order_relaxed)) {
      TakeUnit(l, {.seg = seg,
                   .rg_begin = front ? begin : end - take,
                   .rg_end = front ? begin + take : end,
                   .whole = false});
      l.owner = front;
      return true;
    }
  }
}

bool ClaimOrderedUnit(ScanGlobalState& g, ScanLocalState& l) {
  const auto heap_order = HeapOrder(*g.Bind().scan_order);
  auto& heap = g.ordered_heap;
  for (;;) {
    ScanOrderKey head;
    {
      absl::MutexLock lock{&g.ordered_mutex};
      if (heap.empty()) {
        return false;
      }
      absl::c_pop_heap(heap, heap_order);
      head = std::move(heap.back());
      heap.pop_back();
      auto& work = g.Segment(head.id);
      if (work.ordered_built) {
        const auto k = work.ordered_next++;
        TakeUnit(l, work.ordered_units[k]);
        l.owner = true;
        if (work.ordered_next < work.ordered_units.size()) {
          heap.push_back({head.id, work.ordered_keys[work.ordered_next]});
          absl::c_push_heap(heap, heap_order);
        }
        return true;
      }
    }
    auto& work = g.Segment(head.id);
    BuildSegmentOrderedUnits(g, work, head.id, std::move(head.value));
    absl::MutexLock lock{&g.ordered_mutex};
    work.ordered_built = true;
    heap.push_back({head.id, work.ordered_keys.front()});
    absl::c_push_heap(heap, heap_order);
  }
}

bool Steal(ScanGlobalState& g, ScanLocalState& l) {
  for (;;) {
    auto i = g.next_steal.load(std::memory_order_relaxed);
    if (i >= g.live_segments) {
      return false;
    }
    const auto seg = g.segment_order[i];
    auto& work = g.Segment(seg);
    if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kSplit &&
        ClaimRowGroups(g, l, seg, false)) {
      return true;
    }
    g.next_steal.compare_exchange_strong(i, i + 1, std::memory_order_relaxed,
                                         std::memory_order_relaxed);
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

  const auto threads = static_cast<uint64_t>(
    duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads());

  g.segments = std::make_unique<SegmentWork[]>(g.total_segments);
  uint64_t total_rgs = 0;
  for (const auto seg : g.segment_order) {
    auto& work = g.Segment(seg);
    const auto docs = (*g.reader)[seg].docs_count();
    work.rg_count = static_cast<uint32_t>(
      std::max<uint64_t>(1, (docs + g.rg_size - 1) / g.rg_size));
    work.rgs.store(SegmentWork::Pack(0, work.rg_count),
                   std::memory_order_relaxed);
    total_rgs += work.rg_count;
  }
  g.live_segments = static_cast<uint32_t>(g.segment_order.size());

  g.split = !g.splittable
              ? SplitMode::Never
              : ReadSplit(context).value_or(scan_ordered ? SplitMode::Always
                                                         : SplitMode::Tail);
  static constinit SettingRef gNoSplit{"sdb_scan_no_split_row_groups"};
  g.no_split_rgs = g.split == SplitMode::Never ? 0 : gNoSplit.Int(context);

  if (!ordered) {
    const auto& reader = *g.reader;
    const bool largest_first = g.order == OrderMode::LargestFirst;
    absl::c_sort(g.segment_order, [&](uint32_t l, uint32_t r) {
      const auto lhs = reader[l].live_docs_count();
      const auto rhs = reader[r].live_docs_count();
      return largest_first ? lhs > rhs : lhs < rhs;
    });
  }

  uint64_t parallel = g.split == SplitMode::Never
                        ? g.live_segments
                        : std::max<uint64_t>(g.live_segments, total_rgs);
  if (g.stats_stage) {
    parallel = std::max<uint64_t>(parallel, g.total_segments);
  }
  g.workers = static_cast<uint32_t>(std::clamp<uint64_t>(
    std::min(threads, parallel), 1, std::numeric_limits<uint32_t>::max()));
  g.unit_rgs = static_cast<uint32_t>(
    std::max<uint64_t>(1, total_rgs / (uint64_t{8} * g.workers)));

  uint32_t whole_prefix = 0;
  switch (g.split) {
    case SplitMode::Never:
      whole_prefix = g.live_segments;
      break;
    case SplitMode::Always:
      break;
    case SplitMode::Tail:
      whole_prefix = g.workers <= 1                ? g.live_segments
                     : g.live_segments > g.workers ? g.live_segments - g.workers
                                                   : 0;
      break;
  }
  const bool constant_count = g.shape == ScanShape::Count &&
                              g.col_filters.empty() && bind.search.filter &&
                              ConstantCount(*bind.search.filter);
  for (uint32_t i = 0; i != g.live_segments; ++i) {
    const auto seg = g.segment_order[i];
    auto& work = g.Segment(seg);
    const auto& sub = (*g.reader)[seg];
    const bool whole =
      i < whole_prefix || work.rg_count <= g.no_split_rgs ||
      (constant_count && sub.live_docs_count() == sub.docs_count());
    work.claim.store(whole ? SegmentWork::kWhole : SegmentWork::kSplit,
                     std::memory_order_relaxed);
  }

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
    if (ClaimOrderedUnit(g, l)) {
      return true;
    }
    Exhaust(g, l);
    return false;
  }
  if (l.current_seg != std::numeric_limits<uint32_t>::max() &&
      g.Segment(l.current_seg).claim.load(std::memory_order_relaxed) ==
        SegmentWork::kSplit &&
      ClaimRowGroups(g, l, l.current_seg, l.owner)) {
    return true;
  }
  for (;;) {
    const auto i = g.next_segment.fetch_add(1, std::memory_order_relaxed);
    if (i >= g.live_segments) {
      break;
    }
    const auto seg = g.segment_order[i];
    auto& work = g.Segment(seg);
    if (work.claim.load(std::memory_order_relaxed) == SegmentWork::kWhole) {
      TakeUnit(
        l, {.seg = seg, .rg_begin = 0, .rg_end = work.rg_count, .whole = true});
      l.owner = true;
      return true;
    }
    if (ClaimRowGroups(g, l, seg, true)) {
      return true;
    }
  }
  if (Steal(g, l)) {
    return true;
  }
  Exhaust(g, l);
  return false;
}

bool NextLiveUnit(ScanGlobalState& g, ScanLocalState& l) {
  while (ClaimUnit(g, l)) {
    l.Classify(g, l.unit.seg);
    if (!l.seg_cls.segment_dead) {
      return true;
    }
    if (FinishUnit(g, l)) {
      FinishSegments(g, 1);
    }
  }
  return false;
}

bool FinishUnit(ScanGlobalState& g, ScanLocalState& l) {
  SDB_ASSERT(l.has_unit);
  l.has_unit = false;
  const auto& unit = l.unit;
  if (unit.whole) {
    return true;
  }
  auto& work = g.Segment(unit.seg);
  const auto rgs = unit.rg_end - unit.rg_begin;
  const auto done =
    work.done_rgs.fetch_add(rgs, std::memory_order_acq_rel) + rgs;
  return done == work.rg_count;
}

bool FinishSegments(ScanGlobalState& g, uint32_t count) {
  SDB_ASSERT(count != 0);
  return g.done_segments.fetch_add(count, std::memory_order_acq_rel) + count ==
         g.live_segments;
}

}  // namespace sdb::connector
