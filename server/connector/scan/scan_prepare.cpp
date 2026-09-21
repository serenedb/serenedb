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

#include <iresearch/index/index_reader.hpp>
#include <iresearch/utils/assert.hpp>

#include "connector/scan/scan_state.h"

namespace sdb::connector {

const irs::QueryBuilder& EnsureSegmentQuery(ScanGlobalState& g,
                                            ScanLocalState& l,
                                            uint32_t seg_idx) {
  auto& work = g.Segment(seg_idx);
  auto state = work.prepare.load(std::memory_order_acquire);
  if (state == SegmentWork::kReady) {
    return *g.queries[seg_idx];
  }
  if (state == SegmentWork::kUnprepared &&
      work.prepare.compare_exchange_strong(state, SegmentWork::kPreparing,
                                           std::memory_order_acq_rel)) {
    irs::PrepareCollector* collector = nullptr;
    if (g.collector) {
      if (l.thread_slot == std::numeric_limits<uint32_t>::max()) {
        l.thread_slot = g.thread_slots.fetch_add(1, std::memory_order_relaxed);
        SDB_ASSERT(l.thread_slot < g.collect_threads);
      }
      collector = g.collector->Get();
    }
    g.queries[seg_idx] = g.filter->PrepareSegment(
      (*g.reader)[seg_idx], {.collector = collector,
                             .thread = collector != nullptr ? l.thread_slot : 0,
                             .needs_terms = g.needs_terms});
    work.prepare.store(SegmentWork::kReady, std::memory_order_release);
    work.prepare.notify_all();
    return *g.queries[seg_idx];
  }
  while (state != SegmentWork::kReady) {
    work.prepare.wait(state, std::memory_order_acquire);
    state = work.prepare.load(std::memory_order_acquire);
  }
  return *g.queries[seg_idx];
}

bool RunPrepareStage(duckdb::TableFunctionInput& input, ScanGlobalState& g,
                     ScanLocalState& l) {
  if (!g.stats_stage || g.stats_barrier.Released()) {
    return true;
  }
  for (;;) {
    const auto seg = g.prepare_next.fetch_add(1, std::memory_order_relaxed);
    if (seg >= g.total_segments) {
      break;
    }
    EnsureSegmentQuery(g, l, seg);
    if (g.stats_barrier.Arrive()) {
      if (g.collector) {
        g.collector->Finish();
      }
      g.stats_barrier.Release(input);
      return true;
    }
  }
  if (g.stats_barrier.Park(input)) {
    g.metrics.parked.fetch_add(1, std::memory_order_relaxed);
    return false;
  }
  g.stats_barrier.Wait();
  return true;
}

}  // namespace sdb::connector
