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
#include <absl/strings/match.h>

#include <algorithm>
#include <array>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/storage_info.hpp>
#include <duckdb/storage/table/row_group_reorderer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/assert.hpp>
#include <optional>

#include "catalog/inverted_index.h"
#include "connector/scan/scan_state.h"
#include "query/config.h"

namespace sdb::connector {

std::string_view ToString(SplitMode mode) noexcept {
  switch (mode) {
    case SplitMode::Tail:
      return "tail";
    case SplitMode::Always:
      return "always";
    case SplitMode::Never:
      return "never";
  }
  return "";
}

std::string_view ToString(OrderMode mode) noexcept {
  switch (mode) {
    case OrderMode::Size:
      return "size";
    case OrderMode::Order:
      return "order";
  }
  return "";
}

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

struct ScanOrderKey {
  uint32_t id;
  duckdb::Value value;
};

void SortScanOrderKeys(std::vector<ScanOrderKey>& keys,
                       const ScanOrderSpec& order) {
  const bool nulls_first =
    order.null_order == duckdb::OrderByNullType::NULLS_FIRST;
  const bool asc = order.order_type == duckdb::OrderType::ASCENDING;
  absl::c_sort(keys, [&](const ScanOrderKey& l, const ScanOrderKey& r) {
    const bool ln = l.value.IsNull();
    const bool rn = r.value.IsNull();
    if (ln || rn) {
      return ln != rn ? (ln ? nulls_first : !nulls_first) : l.id < r.id;
    }
    const auto& lo = asc ? l.value : r.value;
    const auto& hi = asc ? r.value : l.value;
    return lo != hi ? lo < hi : l.id < r.id;
  });
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
constexpr std::array<std::string_view, 2> kOrderModes{"size", "order"};

std::optional<SplitMode> ReadSplit(duckdb::ClientContext& context) {
  static constinit SettingRef gSplit{"sdb_scan_split"};
  const auto i = gSplit.Enum(context, kSplitModes);
  if (i == kSplitModes.size()) {
    return std::nullopt;
  }
  return static_cast<SplitMode>(i);
}

SplitMode DefaultSplit(const ScanGlobalState& g, bool scan_ordered) {
  if (scan_ordered) {
    return SplitMode::Always;
  }
  return SplitMode::Tail;
}

OrderMode ReadOrder(duckdb::ClientContext& context, bool scan_ordered) {
  if (!scan_ordered) {
    return OrderMode::Size;
  }
  static constinit SettingRef gOrder{"sdb_scan_order"};
  const auto i = gOrder.Enum(context, kOrderModes);
  return i == kOrderModes.size() ? OrderMode::Order : static_cast<OrderMode>(i);
}

void BuildOrderedUnits(ScanGlobalState& g, const ScanOrderSpec& order) {
  const auto field = static_cast<irs::field_id>(order.column.id());
  std::vector<ScanUnit> units;
  std::vector<ScanOrderKey> keys;
  for (const auto seg : g.segment_order) {
    const auto& sub = (*g.reader)[seg];
    const auto* col_reader = sub.GetColReader();
    const auto* column = col_reader ? col_reader->Column(field) : nullptr;
    const auto& work = g.Segment(seg);
    if (g.split == SplitMode::Never || work.rg_count <= g.no_split_rgs) {
      duckdb::Value v;
      if (column) {
        v = duckdb::RowGroupReorderer::RetrieveStat(
          column->MergedStatistics(), order.order_by, order.column_type);
      }
      keys.push_back({static_cast<uint32_t>(units.size()), std::move(v)});
      units.push_back(
        {.seg = seg, .rg_begin = 0, .rg_end = work.rg_count, .whole = true});
      continue;
    }
    const auto docs = sub.docs_count();
    for (uint32_t rg = 0; rg < work.rg_count; ++rg) {
      const auto begin = uint64_t{rg} * g.rg_size;
      const auto end = std::min<uint64_t>(docs, begin + g.rg_size);
      duckdb::Value v;
      if (column) {
        v = UnitOrderKey(*column, order, begin, end);
      }
      keys.push_back({static_cast<uint32_t>(units.size()), std::move(v)});
      units.push_back(
        {.seg = seg, .rg_begin = rg, .rg_end = rg + 1, .whole = false});
    }
  }
  SortScanOrderKeys(keys, order);
  g.ordered_units.reserve(units.size());
  for (const auto& key : keys) {
    g.ordered_units.push_back(units[key.id]);
  }
}

}  // namespace

void BuildClaimPlan(ScanGlobalState& g, duckdb::ClientContext& context) {
  const auto& bind = g.Bind();
  const bool scan_ordered =
    bind.scan_order.has_value() &&
    (g.shape == ScanShape::Stream || g.shape == ScanShape::ColScan);

  uint64_t rg_rows =
    bind.relation.IsIndexRelation()
      ? bind.relation.ScannedIndex().GetOptions().row_group_size
      : 0;
  if (rg_rows == 0) {
    rg_rows = DEFAULT_ROW_GROUP_SIZE;
  }
  g.rg_size = rg_rows >= DEFAULT_ROW_GROUP_SIZE
                ? rg_rows
                : DEFAULT_ROW_GROUP_SIZE / rg_rows * rg_rows;

  g.order = ReadOrder(context, scan_ordered);

  const auto threads = static_cast<uint64_t>(
    duckdb::TaskScheduler::GetScheduler(context).NumberOfThreads());

  g.segments = std::make_unique<SegmentWork[]>(g.total_segments);
  uint64_t total_rgs = 0;
  for (const auto seg : g.segment_order) {
    auto& work = g.Segment(seg);
    const auto docs = (*g.reader)[seg].docs_count();
    work.rg_count = static_cast<uint32_t>(
      std::max<uint64_t>(1, (docs + g.rg_size - 1) / g.rg_size));
    work.live = true;
    total_rgs += work.rg_count;
  }
  g.live_segments = static_cast<uint32_t>(g.segment_order.size());

  g.split = !g.splittable
              ? SplitMode::Never
              : ReadSplit(context).value_or(DefaultSplit(g, scan_ordered));
  static constinit SettingRef gNoSplit{"sdb_scan_no_split_row_groups"};
  g.no_split_rgs = g.split == SplitMode::Never ? 0 : gNoSplit.Int(context);

  if (g.order == OrderMode::Order) {
    BuildOrderedUnits(g, *bind.scan_order);
  } else {
    const auto& reader = *g.reader;
    absl::c_sort(g.segment_order, [&](uint32_t l, uint32_t r) {
      return reader[l].live_docs_count() < reader[r].live_docs_count();
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
  for (uint32_t i = 0; i != g.live_segments; ++i) {
    auto& work = g.Segment(g.segment_order[i]);
    const bool whole = i < whole_prefix || work.rg_count <= g.no_split_rgs;
    work.claim.store(whole ? SegmentWork::kWhole : SegmentWork::kSplit,
                     std::memory_order_relaxed);
  }
}

namespace {

void TakeUnit(ScanGlobalState& g, ScanLocalState& l, ScanUnit unit) {
  l.unit = unit;
  l.has_unit = true;
  l.current_seg = unit.seg;
  if (unit.whole) {
    g.metrics.whole_units.fetch_add(1, std::memory_order_relaxed);
  } else {
    g.metrics.rg_units.fetch_add(1, std::memory_order_relaxed);
  }
}

bool ClaimRowGroup(ScanGlobalState& g, ScanLocalState& l, uint32_t seg) {
  auto& work = g.Segment(seg);
  const auto rg = work.next_rg.fetch_add(1, std::memory_order_relaxed);
  if (rg >= work.rg_count) {
    return false;
  }
  TakeUnit(g, l,
           {.seg = seg, .rg_begin = rg, .rg_end = rg + 1, .whole = false});
  return true;
}

bool ClaimOrderedUnit(ScanGlobalState& g, ScanLocalState& l) {
  const auto i = g.next_ordered_unit.fetch_add(1, std::memory_order_relaxed);
  if (i >= g.ordered_units.size()) {
    return false;
  }
  const auto& unit = g.ordered_units[i];
  auto& work = g.Segment(unit.seg);
  work.claim.store(unit.whole ? SegmentWork::kWhole : SegmentWork::kSplit,
                   std::memory_order_relaxed);
  TakeUnit(g, l, unit);
  return true;
}

bool Steal(ScanGlobalState& g, ScanLocalState& l) {
  for (;;) {
    uint32_t best = std::numeric_limits<uint32_t>::max();
    uint32_t best_left = 0;
    for (const auto seg : g.segment_order) {
      auto& work = g.Segment(seg);
      if (work.claim.load(std::memory_order_relaxed) != SegmentWork::kSplit) {
        continue;
      }
      const auto next = work.next_rg.load(std::memory_order_relaxed);
      const auto left = next < work.rg_count ? work.rg_count - next : 0;
      if (left > best_left) {
        best_left = left;
        best = seg;
      }
    }
    if (best == std::numeric_limits<uint32_t>::max()) {
      return false;
    }
    if (ClaimRowGroup(g, l, best)) {
      return true;
    }
  }
}

}  // namespace

bool ClaimUnit(ScanGlobalState& g, ScanLocalState& l) {
  l.has_unit = false;
  if (l.units_exhausted) {
    return false;
  }
  if (g.Ordered()) {
    if (ClaimOrderedUnit(g, l)) {
      return true;
    }
    l.units_exhausted = true;
    return false;
  }
  if (l.current_seg != std::numeric_limits<uint32_t>::max() &&
      g.Segment(l.current_seg).claim.load(std::memory_order_relaxed) ==
        SegmentWork::kSplit &&
      ClaimRowGroup(g, l, l.current_seg)) {
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
        g, l,
        {.seg = seg, .rg_begin = 0, .rg_end = work.rg_count, .whole = true});
      return true;
    }
    if (ClaimRowGroup(g, l, seg)) {
      return true;
    }
  }
  if (Steal(g, l)) {
    return true;
  }
  l.units_exhausted = true;
  return false;
}

bool UnitFinished(ScanGlobalState& g, ScanLocalState& l) {
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

bool SegmentsDone(ScanGlobalState& g, uint32_t count) {
  SDB_ASSERT(count != 0);
  return g.done_segments.fetch_add(count, std::memory_order_acq_rel) + count ==
         g.live_segments;
}

}  // namespace sdb::connector
