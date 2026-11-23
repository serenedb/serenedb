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
#include <atomic>
#include <duckdb/storage/external_file_cache/external_file_cache.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

constexpr uint64_t kScanStart = duckdb::ExternalFileCache::MAX_FILE_IN_FLIGHT;
constexpr uint64_t kSampleRowGroups = 8;
constexpr uint64_t kScanAhead = uint64_t{256} << 20;
constexpr uint64_t kMinWorkerAhead = uint64_t{4} << 20;
constexpr uint64_t kMaxWorkerAhead = uint64_t{32} << 20;
constexpr uint64_t kMaxUnitAhead = uint64_t{64} << 20;
constexpr uint64_t kMaxAheadRowGroups = 256;
constexpr uint64_t kProjectedPass = 1024;

struct AheadScratch {
  irs::ColFilterStateCache& states;
  irs::ColFilterClassification& cls;
  RowGroupChecks& checks;
  duckdb::ClientContext& context;
  std::vector<duckdb::FileRange>& extents;
};

uint32_t Claim(std::atomic_uint32_t& mark, uint32_t end) noexcept {
  auto declared = mark.load(std::memory_order_relaxed);
  while (declared < end &&
         !mark.compare_exchange_weak(declared, end, std::memory_order_relaxed,
                                     std::memory_order_relaxed)) {
  }
  return std::min(declared, end);
}

UnitRows RowGroupRows(const SegmentWork& work, uint32_t rg) {
  return {.begin = work.groups.Begin(rg), .end = work.groups.End(rg)};
}

uint64_t CollectRowGroup(const irs::ColReader& col_reader,
                         std::span<const irs::field_id> fields, UnitRows group,
                         std::vector<duckdb::FileRange>& extents) {
  const auto from = extents.size();
  for (const auto field : fields) {
    if (const auto* column = col_reader.Column(field)) {
      column->CollectBlocks(group.begin, group.end - 1, extents);
    }
  }
  uint64_t bytes = 0;
  for (auto i = from; i != extents.size(); ++i) {
    bytes += extents[i].size;
  }
  return bytes;
}

bool Cached(const irs::ColReader& col_reader,
            std::span<const duckdb::FileRange> extents) {
  return absl::c_all_of(extents, [&](const auto& extent) {
    return col_reader.File()->Warm(extent.offset, extent.size) == extent.size;
  });
}

void DeclareRowGroups(ScanGlobalState& g, const AheadScratch& scratch,
                      uint32_t seg, uint32_t from, uint32_t to) {
  const auto& sub = (*g.reader)[seg];
  const auto* col_reader = sub.GetColReader();
  SDB_ASSERT(col_reader && sub.Meta().docs_count != 0);
  const auto& work = g.Segment(seg);
  if (from >= to || work.dead) {
    return;
  }
  bool classified = g.col_filters.empty() && !g.pushed_filters;
  scratch.checks.Clear();
  auto& extents = scratch.extents;
  extents.clear();
  for (auto rg = from; rg != to; ++rg) {
    const auto kept = extents.size();
    const auto group = RowGroupRows(work, rg);
    CollectRowGroup(*col_reader, g.ahead_fields, group, extents);
    if (Cached(*col_reader, std::span{extents}.subspan(kept))) {
      extents.resize(kept);
      continue;
    }
    if (!classified) {
      ClassifySegmentColFilters(sub, g, g.col_filters, scratch.states,
                                scratch.cls, false);
      if (scratch.cls.segment_dead) {
        return;
      }
      BuildRowGroupChecks(g, *col_reader, scratch.cls.active, scratch.context,
                          scratch.states, scratch.checks);
      classified = true;
    }
    if (!RowGroupLive(scratch.context, scratch.checks, group.begin,
                      group.end)) {
      extents.resize(kept);
    }
  }
  if (!extents.empty()) {
    col_reader->File()->Prefetch(extents);
  }
}

void DeclareNextSegments(ScanGlobalState& g, const AheadScratch& scratch) {
  const auto claimed =
    std::min(g.next_segment.load(std::memory_order_relaxed), g.live_segments);
  auto end = claimed;
  while (end != g.live_segments &&
         g.ahead_before[end] - g.ahead_before[claimed] < g.ahead_rgs) {
    ++end;
  }
  for (auto i = std::max(Claim(g.ahead_segment, end), claimed); i < end; ++i) {
    const auto seg = g.segment_order[i];
    const auto& work = g.Segment(seg);
    if (!g.starts.empty() &&
        work.claim.load(std::memory_order_relaxed) == SegmentWork::kSplit) {
      continue;
    }
    const auto count = static_cast<uint32_t>(std::min<uint64_t>(
      work.rg_count,
      g.ahead_rgs - (g.ahead_before[i] - g.ahead_before[claimed])));
    DeclareRowGroups(g, scratch, seg, 0, count);
  }
}

void DeclareStarts(ScanGlobalState& g, const AheadScratch& scratch) {
  if (g.starts.empty()) {
    return;
  }
  const auto rgs = std::max<uint64_t>(1, g.ahead_rgs / g.starts.size());
  for (const auto seg : g.split_segments) {
    const auto& work = g.Segment(seg);
    for (uint32_t j = 0; j != work.start_count; ++j) {
      const auto range = g.starts[work.first_start + j];
      const auto begin = RangeNext(range) - work.base;
      const auto end = RangeEnd(range) - work.base;
      DeclareRowGroups(
        g, scratch, seg, begin,
        static_cast<uint32_t>(std::min<uint64_t>(end, begin + rgs)));
    }
  }
}

uint32_t AheadEnd(ScanGlobalState& g, ScanLocalState& l) {
  const auto& unit = l.unit;
  if (unit.whole) {
    return g.Segment(unit.seg).rg_count;
  }
  if (!g.Ordered() && l.owned_seg == unit.seg) {
    return std::max(unit.rg_end, OwnedEnd(g, l));
  }
  return unit.rg_end;
}

void CollectProjected(const ScanGlobalState& g, ScanLocalState& l,
                      const irs::ColReader& col_reader, uint64_t row) {
  if (row < l.projected_until) {
    return;
  }
  const auto& work = g.segments[l.unit.seg];
  const auto rg = work.groups.Of(row);
  auto group = RowGroupRows(work, rg);
  const bool whole = group.begin == row;
  group.begin = row;
  const auto bytes =
    CollectRowGroup(col_reader, g.ahead_projections, group, l.ahead_extents);
  if (l.projected_rgs == 0 && whole) {
    l.projected_rgs = std::clamp<uint64_t>(
      g.ahead_bytes / std::max<uint64_t>(bytes, 1), 1, kMaxAheadRowGroups);
  }
  l.projected_until = group.end;
}

void CollectProjectedAhead(const ScanGlobalState& g, ScanLocalState& l,
                           const irs::ColReader& col_reader, uint32_t rg,
                           uint32_t end) {
  const auto& work = g.segments[l.unit.seg];
  const auto stop = static_cast<uint32_t>(
    std::min<uint64_t>(end, rg + 1 + std::max<uint64_t>(l.projected_rgs, 1)));
  for (auto next = std::max(l.projected_rg, rg + 1); next < stop; ++next) {
    const auto group = RowGroupRows(work, next);
    if (RowGroupLive(*g.client_context, l.ahead_checks, group.begin,
                     group.end)) {
      CollectRowGroup(col_reader, g.ahead_projections, group, l.ahead_extents);
    }
  }
  l.projected_rg = std::max(l.projected_rg, stop);
}

void DeclareBatch(ScanGlobalState& g, ScanLocalState& l,
                  const AheadScratch& scratch) {
  const auto begin = std::max(
    {l.ahead_batch, l.batch_next,
     std::min(g.ahead_segment.load(std::memory_order_relaxed), l.batch_end)});
  auto end = begin;
  while (end < l.batch_end &&
         g.ahead_before[end] - g.ahead_before[l.batch_next] <
           g.ahead_worker_rgs) {
    ++end;
  }
  for (auto i = begin; i < end; ++i) {
    const auto seg = g.segment_order[i];
    DeclareRowGroups(g, scratch, seg, 0, g.Segment(seg).rg_count);
  }
  l.ahead_batch = std::max(l.ahead_batch, end);
}

}  // namespace

void PlanReadAhead(ScanGlobalState& g, duckdb::ClientContext& context) {
  for (const auto& filter : g.col_filters) {
    if (!filter.is_score && (!filter.optional || filter.is_dynamic)) {
      g.ahead_fields.push_back(filter.field);
    }
  }
  for (const auto& projection : g.cs_projections) {
    const auto field = static_cast<irs::field_id>(projection.column_id);
    if (g.col_filters.empty()) {
      g.ahead_fields.push_back(field);
    } else if (!absl::c_linear_search(g.ahead_fields, field)) {
      g.ahead_projections.push_back(field);
    }
  }
  if (g.ahead_fields.empty()) {
    return;
  }
  g.ahead_projected =
    !g.Ordered() && absl::c_none_of(g.col_filters, [](const auto& filter) {
      return filter.is_dynamic;
    });
  g.ahead_bytes =
    std::clamp(kScanAhead / g.workers, kMinWorkerAhead, kMaxWorkerAhead);
  g.ahead_before.assign(g.live_segments + 1, 0);
  for (uint32_t i = 0; i != g.live_segments; ++i) {
    g.ahead_before[i + 1] =
      g.ahead_before[i] + g.Segment(g.segment_order[i]).rg_count;
  }
  irs::ColFilterStateCache states;
  irs::ColFilterClassification cls;
  RowGroupChecks checks;
  std::vector<duckdb::FileRange> extents;
  uint64_t rg_bytes = 0;
  for (uint32_t i = 0; i != g.live_segments && rg_bytes == 0; ++i) {
    const auto seg = g.segment_order[i];
    const auto& work = g.Segment(seg);
    const auto* col_reader = (*g.reader)[seg].GetColReader();
    SDB_ASSERT(col_reader);
    const auto rgs = static_cast<uint32_t>(
      std::min<uint64_t>(work.rg_count, kSampleRowGroups));
    extents.clear();
    uint64_t bytes = 0;
    for (uint32_t rg = 0; rg != rgs; ++rg) {
      bytes += CollectRowGroup(*col_reader, g.ahead_fields,
                               RowGroupRows(work, rg), extents);
    }
    rg_bytes = bytes / rgs;
  }
  rg_bytes = std::max<uint64_t>(rg_bytes, 1);
  g.ahead_rgs = static_cast<uint32_t>(
    std::clamp<uint64_t>(kScanStart / rg_bytes, 1, kMaxAheadRowGroups));
  g.ahead_worker_rgs = static_cast<uint32_t>(
    std::clamp<uint64_t>(g.ahead_bytes / rg_bytes, 1, kMaxAheadRowGroups));
  const AheadScratch scratch{states, cls, checks, context, extents};
  DeclareStarts(g, scratch);
  DeclareNextSegments(g, scratch);
}

void StartReadAhead(ScanGlobalState& g, ScanLocalState& l) {
  l.ahead_check = std::numeric_limits<uint64_t>::max();
  l.projected_until = std::numeric_limits<uint64_t>::max();
  const auto seg = l.unit.seg;
  const auto* col_reader = (*g.reader)[seg].GetColReader();
  if (const auto rows = g.RowsOf(l.unit);
      col_reader && rows.begin != rows.end) {
    const auto rg = g.Segment(seg).groups.Of(rows.begin);
    if (l.ahead_seg != seg || rg < l.ahead_from || rg > l.ahead_rg) {
      l.ahead_seg = seg;
      l.ahead_checks.Clear();
      BuildRowGroupChecks(g, *col_reader, l.seg_cls.active, *g.client_context,
                          l.filter_states, l.ahead_checks);
      l.ahead_live.clear();
      l.ahead_rg = rg;
      l.ahead_rgs = 0;
      l.projected_rg = rg + 1;
      l.projected_rgs = 0;
    }
    if (!g.ahead_projections.empty()) {
      l.projected_until = rows.begin;
    }
    MoveReadAhead(g, l, rows.begin);
  }
  if (g.Ordered()) {
    return;
  }
  const AheadScratch scratch{l.filter_states, l.ahead_cls, l.declare_checks,
                             *g.client_context, l.ahead_extents};
  if (l.unit.whole) {
    DeclareBatch(g, l, scratch);
  }
  DeclareNextSegments(g, scratch);
}

void MoveReadAhead(ScanGlobalState& g, ScanLocalState& l, uint64_t row) {
  const auto* col_reader = (*g.reader)[l.unit.seg].GetColReader();
  SDB_ASSERT(col_reader);
  const auto& work = g.Segment(l.unit.seg);
  const auto rg = work.groups.Of(row);
  const auto end = AheadEnd(g, l);
  l.ahead_from = rg;
  l.ahead_live.erase(l.ahead_live.begin(),
                     absl::c_lower_bound(l.ahead_live, rg));
  l.ahead_rg = std::max(l.ahead_rg, rg);
  l.ahead_extents.clear();
  while (l.ahead_rg < end &&
         (l.ahead_rgs == 0 || l.ahead_live.size() < l.ahead_rgs)) {
    const auto next = l.ahead_rg++;
    const auto group = RowGroupRows(work, next);
    const auto kept = l.ahead_extents.size();
    const auto bytes =
      CollectRowGroup(*col_reader, g.ahead_fields, group, l.ahead_extents);
    if (Cached(*col_reader, std::span{l.ahead_extents}.subspan(kept))) {
      l.ahead_extents.resize(kept);
    } else if (!RowGroupLive(*g.client_context, l.ahead_checks, group.begin,
                             group.end)) {
      l.ahead_extents.resize(kept);
      continue;
    }
    l.ahead_live.push_back(next);
    if (l.ahead_rgs == 0) {
      const auto budget =
        g.shape == ScanShape::Count ? kMaxUnitAhead : g.ahead_bytes;
      l.ahead_rgs = std::clamp<uint64_t>(budget / std::max<uint64_t>(bytes, 1),
                                         1, kMaxAheadRowGroups);
    }
  }
  const auto passed =
    l.produced_rows - std::exchange(l.ahead_produced, l.produced_rows);
  if (passed * kProjectedPass >= g.rg_size && !g.ahead_projections.empty()) {
    CollectProjected(g, l, *col_reader, row);
    if (g.ahead_projected) {
      CollectProjectedAhead(g, l, *col_reader, rg, end);
    }
  }
  if (!l.ahead_extents.empty()) {
    col_reader->File()->Prefetch(l.ahead_extents);
  }
  l.ahead_check = l.ahead_rg >= end && g.ahead_projections.empty()
                    ? std::numeric_limits<uint64_t>::max()
                    : work.groups.End(rg);
}

void DeclareProjected(const ScanGlobalState& g, ScanLocalState& l,
                      uint64_t row) {
  const auto* col_reader = (*g.reader)[l.unit.seg].GetColReader();
  SDB_ASSERT(col_reader);
  l.ahead_extents.clear();
  CollectProjected(g, l, *col_reader, row);
  if (!l.ahead_extents.empty()) {
    col_reader->File()->Prefetch(l.ahead_extents);
  }
}

}  // namespace sdb::connector
