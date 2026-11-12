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
#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <array>
#include <bit>
#include <duckdb/parallel/task_executor.hpp>
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
#include <iresearch/utils/bit_utils.hpp>
#include <iresearch/utils/containers/bitset.hpp>
#include <iresearch/utils/down_cast.hpp>
#include <optional>
#include <span>
#include <vector>

#include "connector/scan/scan_state.h"
#include "query/config.h"

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
  const auto i = GetEnumSetting(context, "sdb_scan_split", kSplitModes);
  if (i == kSplitModes.size()) {
    return std::nullopt;
  }
  return static_cast<SplitMode>(i);
}

OrderMode ReadOrder(duckdb::ClientContext& context, bool scan_ordered) {
  const auto i = GetEnumSetting(context, "sdb_scan_order", kOrderModes);
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

std::unique_ptr<OrderedUnits> BuildSegmentOrderedUnits(
  ScanGlobalState& g, SegmentWork& work, uint32_t seg,
  duckdb::Value segment_key);

void BuildOrderedUnits(ScanGlobalState& g, uint32_t seg) {
  const auto& order = *g.Bind().scan_order;
  duckdb::Value v;
  if (const auto* column = OrderColumn(g, seg)) {
    v = duckdb::RowGroupReorderer::RetrieveStat(
      column->MergedStatistics(), order.order_by, order.column_type);
  }
  auto& work = g.Segment(seg);
  work.ordered = BuildSegmentOrderedUnits(g, work, seg, std::move(v));
}

void BuildNextOrderedUnits(ScanGlobalState& g, std::atomic_uint32_t& next) {
  const auto size = static_cast<uint32_t>(g.segment_order.size());
  for (auto i = next.fetch_add(1, std::memory_order_relaxed); i < size;
       i = next.fetch_add(1, std::memory_order_relaxed)) {
    BuildOrderedUnits(g, g.segment_order[i]);
  }
}

struct OrderedSlice final : duckdb::BaseExecutorTask {
  OrderedSlice(duckdb::TaskExecutor& executor, ScanGlobalState& g,
               std::atomic_uint32_t& next)
    : BaseExecutorTask{executor}, g{g}, next{next} {}

  void ExecuteTask() final { BuildNextOrderedUnits(g, next); }

  ScanGlobalState& g;
  std::atomic_uint32_t& next;
};

void BuildOrderedHeap(ScanGlobalState& g, duckdb::ClientContext& context,
                      uint64_t threads) {
  const auto& order = *g.Bind().scan_order;
  g.order_dynamic_filter = FindOrderDynamicFilter(g);
  const auto helpers = std::min<uint64_t>(threads, g.segment_order.size());
  std::atomic_uint32_t next{0};
  if (helpers <= 1) {
    BuildNextOrderedUnits(g, next);
  } else {
    duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(context),
                                  context.GetSchedulerAccount()};
    for (uint64_t i = 0; i != helpers; ++i) {
      executor.ScheduleTask(duckdb::make_uniq<OrderedSlice>(executor, g, next));
    }
    executor.WorkOnTasks();
  }
  g.ordered_heap.reserve(g.segment_order.size());
  for (const auto seg : g.segment_order) {
    g.ordered_heap.push_back({seg, g.Segment(seg).ordered->keys.front()});
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
    built->units.push_back(WholeUnit(seg, work));
    built->keys.push_back(std::move(segment_key));
    return built;
  }
  const auto* column = OrderColumn(g, seg);
  const uint64_t docs = (*g.reader)[seg].Meta().docs_count;
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

ScanUnit UnitOf(const SegmentWork& work, uint32_t seg, uint32_t begin,
                uint32_t end) noexcept {
  if (const auto& claims = work.claims; !claims.empty()) {
    return {.seg = seg,
            .rg_begin = claims[begin].rg,
            .rg_end = claims[end].rg,
            .live = claims[end].live - claims[begin].live,
            .whole = false};
  }
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
      TakeUnit(l, UnitOf(work, l.owned_seg, next - work.base,
                         next + take - work.base));
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

uint32_t LargestRange(const ScanGlobalState& g) noexcept {
  auto victim = g.workers;
  uint32_t most = 0;
  for (uint32_t w = 0; w != g.workers; ++w) {
    const auto r = g.ranges[w].range.load(std::memory_order_relaxed);
    if (RangeEnd(r) > RangeNext(r) && RangeEnd(r) - RangeNext(r) > most) {
      most = RangeEnd(r) - RangeNext(r);
      victim = w;
    }
  }
  return victim;
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
  for (auto victim = LargestRange(g); victim != g.workers;
       victim = LargestRange(g)) {
    if (StealFrom(g, l, victim)) {
      return true;
    }
  }
  return false;
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

constexpr uint32_t kLiveSamples = 16;
constexpr uint64_t kParallelLiveRowGroups = 1024;
constexpr uint64_t kLiveSlicesPerThread = 2;
constexpr uint64_t kMinLiveSlice = 256;

bool RowGroupDead(const irs::ColumnReader& reader,
                  const duckdb::ExpressionFilter& filter,
                  duckdb::TableFilterState& state, irs::BlockWindow& window,
                  uint64_t begin, uint64_t end) {
  for (auto row = begin; row < end; row = window.end) {
    window = reader.Locate(row, window);
    if (!irs::ZoneDead(filter.CheckStatistics(
          reader.RowGroupStatistics(window.block), state))) {
      return false;
    }
  }
  return true;
}

struct RowGroupCheck {
  const irs::ColumnReader* reader;
  const duckdb::ExpressionFilter* filter;
  duckdb::TableFilterState* state;
  mutable irs::BlockWindow window;
};

using RowGroupChecks = absl::InlinedVector<RowGroupCheck, 4>;

struct LivePass {
  SegmentWork* work;
  const irs::ColReader* col_reader;
  std::vector<irs::ColFilterSpec> specs;
  duckdb::ClientContext* context;
  uint64_t rows;
  uint64_t rg_size;
  irs::bitset live;
};

RowGroupChecks MakeChecks(const LivePass& pass,
                          irs::ColFilterStateCache& states) {
  RowGroupChecks checks;
  for (const auto& spec : pass.specs) {
    if (spec.is_score || spec.row_gather) {
      continue;
    }
    SDB_ASSERT(spec.row_fields.empty());
    const auto* column = pass.col_reader->Column(spec.field);
    SDB_ASSERT(column);
    const auto* reader = irs::FilteredColumn(*column, spec);
    if (!reader || reader->Type().IsNested()) {
      continue;
    }
    SDB_ASSERT(reader->RowCount() == pass.rows);
    checks.emplace_back(reader, &spec.filter->Cast<duckdb::ExpressionFilter>(),
                        &states.State(*pass.context, *spec.filter));
  }
  return checks;
}

bool RowGroupLive(const LivePass& pass, const RowGroupChecks& checks,
                  uint32_t rg) {
  const auto begin = uint64_t{rg} * pass.rg_size;
  const auto end = std::min(pass.rows, begin + pass.rg_size);
  return absl::c_none_of(checks, [&](const RowGroupCheck& c) {
    return RowGroupDead(*c.reader, *c.filter, *c.state, c.window, begin, end);
  });
}

void MarkLive(LivePass& pass, const RowGroupChecks& checks, uint32_t begin,
              uint32_t end) {
  for (auto rg = begin; rg != end; ++rg) {
    if (RowGroupLive(pass, checks, rg)) {
      pass.live.set(rg);
    }
  }
}

bool SampleRuledOut(const LivePass& pass, irs::ColFilterStateCache& states) {
  const auto checks = MakeChecks(pass, states);
  if (checks.empty()) {
    return false;
  }
  const auto rg_count = pass.work->rg_count;
  const auto samples = std::min(kLiveSamples, rg_count);
  for (uint32_t i = 0; i != samples; ++i) {
    if (!RowGroupLive(pass, checks,
                      static_cast<uint32_t>((uint64_t{2} * i + 1) * rg_count /
                                            (uint64_t{2} * samples)))) {
      return true;
    }
  }
  return false;
}

struct LiveSlice final : duckdb::BaseExecutorTask {
  LiveSlice(duckdb::TaskExecutor& executor, LivePass& pass, uint32_t begin,
            uint32_t end)
    : BaseExecutorTask{executor}, pass{pass}, begin{begin}, end{end} {}

  void ExecuteTask() final {
    irs::ColFilterStateCache states;
    MarkLive(pass, MakeChecks(pass, states), begin, end);
  }

  LivePass& pass;
  uint32_t begin;
  uint32_t end;
};

void MarkLiveRowGroups(std::span<LivePass> passes,
                       irs::ColFilterStateCache& states,
                       duckdb::ClientContext& context, uint64_t threads) {
  uint64_t total = 0;
  for (const auto& pass : passes) {
    total += pass.work->rg_count;
  }
  if (total < kParallelLiveRowGroups) {
    for (auto& pass : passes) {
      MarkLive(pass, MakeChecks(pass, states), 0, pass.work->rg_count);
    }
    return;
  }
  constexpr uint64_t kWord = irs::BitsRequired<irs::bitset::word_t>();
  const auto per = (total + kLiveSlicesPerThread * threads - 1) /
                   (kLiveSlicesPerThread * threads);
  const auto slice = static_cast<uint32_t>(
    (std::max(per, kMinLiveSlice) + kWord - 1) / kWord * kWord);
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(context),
                                context.GetSchedulerAccount()};
  for (auto& pass : passes) {
    const auto rg_count = pass.work->rg_count;
    for (uint32_t begin = 0; begin < rg_count; begin += slice) {
      executor.ScheduleTask(duckdb::make_uniq<LiveSlice>(
        executor, pass, begin, std::min(rg_count, begin + slice)));
    }
  }
  executor.WorkOnTasks();
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

std::vector<LiveClaim> CutClaims(const irs::bitset& live, uint32_t threads) {
  const auto live_count = static_cast<uint32_t>(live.count());
  if (live_count == 0) {
    return {{.rg = static_cast<uint32_t>(live.size())}};
  }
  const auto step = [&](uint32_t claimed) {
    return claimed + std::max<uint32_t>(1, (live_count - claimed) / threads);
  };
  std::vector<LiveClaim> claims{LiveClaim{}};
  auto next = step(0);
  uint32_t seen = 0;
  uint32_t word_rg = 0;
  for (const auto bits : live) {
    const auto ones = static_cast<uint32_t>(std::popcount(bits));
    for (; next < seen + ones; next = step(next)) {
      const auto bit = irs::SelectInWord(bits, next - seen);
      claims.push_back(
        {.rg = word_rg + static_cast<uint32_t>(bit), .live = next});
    }
    seen += ones;
    word_rg += irs::BitsRequired<irs::bitset::word_t>();
  }
  claims.push_back(
    {.rg = static_cast<uint32_t>(live.size()), .live = live_count});
  return claims;
}

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
  g.no_split_rgs = g.split == SplitMode::Never
                     ? 0
                     : GetIntSetting(context, "sdb_scan_no_split_row_groups");

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

  const auto where = g.ClaimedWhere();
  const bool constant_count = g.shape == ScanShape::Count &&
                              g.col_filters.empty() && where &&
                              ConstantCount(*where);
  const auto unique = UniqueBound(g, where.get());
  const bool single = unique && *unique <= STANDARD_VECTOR_SIZE;
  irs::ColFilterStateCache states;
  irs::ColFilterClassification cls;
  std::vector<LivePass> passes;
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
      g.split != SplitMode::Never && work.rg_count > g.no_split_rgs &&
      !(constant_count && sub.live_docs_count() == sub.docs_count());
    work.claim.store(split ? SegmentWork::kSplit : SegmentWork::kWhole,
                     std::memory_order_relaxed);
    if (split) {
      run_end = i;
      if (!ordered && !g.col_filters.empty() && threads > 1) {
        ClassifySegmentColFilters(sub, g, g.col_filters, states, cls, false);
        if (cls.segment_dead) {
          work.claims = {{.rg = work.rg_count}};
        } else {
          LivePass pass{.work = &work,
                        .col_reader = sub.GetColReader(),
                        .specs = cls.active,
                        .context = &context,
                        .rows = docs,
                        .rg_size = g.rg_size};
          SDB_ASSERT(pass.col_reader);
          if (SampleRuledOut(pass, states)) {
            pass.live.reset(work.rg_count);
            passes.push_back(std::move(pass));
          }
        }
      }
    }
    work.run_end = run_end;
  }
  MarkLiveRowGroups(passes, states, context, threads);
  for (const auto& pass : passes) {
    pass.work->claims = CutClaims(pass.live, static_cast<uint32_t>(threads));
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
    if (work.claims.empty()) {
      work.chunk = std::max<uint32_t>(1, work.rg_count / g.workers);
    }
  }
  if (g.split == SplitMode::Tail && !ordered) {
    CutStarts(g, positions);
  }
  constexpr uint64_t kFoldSharers = 4;
  g.fold_rgs = kFoldSharers * rgs / g.workers;

  if (ordered) {
    BuildOrderedHeap(g, context, threads);
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
          TakeUnit(l, WholeUnit(seg, work));
          return true;
        }
        continue;
      }
      if (!work.owned.exchange(true, std::memory_order_relaxed) &&
          Own(g, l, seg)) {
        return true;
      }
      if (Steal(g, l)) {
        if (ClaimOwned(g, l)) {
          return true;
        }
        continue;
      }
      g.next_segment.compare_exchange_strong(
        i, i + 1, std::memory_order_relaxed, std::memory_order_relaxed);
    }
  } else {
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
