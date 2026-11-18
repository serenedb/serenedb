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

#include <absl/cleanup/cleanup.h>

#include <duckdb/common/fault_points.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {

const irs::QueryBuilder& EnsureSegmentQuery(ScanGlobalState& g,
                                            ScanLocalState* l,
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
      SDB_ASSERT(l);
      if (l->thread_slot == std::numeric_limits<uint32_t>::max()) {
        l->thread_slot = g.thread_slots.fetch_add(1, std::memory_order_relaxed);
        SDB_ASSERT(l->thread_slot < g.collect_threads);
      }
      collector = g.collector->Get();
    }
    const auto& segment = (*g.reader)[seg_idx];
    const irs::PrepareContext ctx{
      .collector = collector,
      .thread = collector != nullptr ? l->thread_slot : 0,
      .needs_terms = g.needs_terms};
    const auto index_only = g.ClaimedIndexOnly();
    const auto& filter = !UsesColumnForms(g, segment) ? *g.filter
                         : index_only                 ? *index_only
                                                      : MatchAllFilter();
    g.queries[seg_idx] = g.vector_scorer == nullptr
                           ? irs::PrepareMasked(filter, segment, ctx)
                           : g.filter->PrepareSegment(segment, ctx);
    if (work.prepare.exchange(SegmentWork::kReady, std::memory_order_acq_rel) ==
        SegmentWork::kAwaited) {
      work.prepare.notify_all();
    }
    return *g.queries[seg_idx];
  }
  while (state != SegmentWork::kReady) {
    if (state == SegmentWork::kPreparing &&
        !work.prepare.compare_exchange_weak(state, SegmentWork::kAwaited,
                                            std::memory_order_acq_rel)) {
      continue;
    }
    work.prepare.wait(SegmentWork::kAwaited, std::memory_order_acquire);
    state = work.prepare.load(std::memory_order_acquire);
  }
  return *g.queries[seg_idx];
}

bool RunPrepareStage(duckdb::TableFunctionInput& input, ScanGlobalState& g,
                     ScanLocalState& l) {
  if (!g.stats_stage) {
    return true;
  }
  if (!g.stats_barrier.Released()) {
    absl::Cleanup abandon = [&g] { g.stats_barrier.Abandon(); };
    for (;;) {
      const auto seg = g.prepare_next.fetch_add(1, std::memory_order_relaxed);
      if (seg >= g.total_segments) {
        break;
      }
      EnsureSegmentQuery(g, &l, seg);
      SDB_IF_FAILURE("scan_prepare_fails_last_segment") {
        if (seg + 1 == g.total_segments) {
          THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
        }
      }
      if (g.stats_barrier.Arrive()) {
        if (g.collector) {
          g.collector->Finish();
        }
        g.stats_barrier.Release(input);
      }
    }
    std::move(abandon).Cancel();
    if (g.stats_barrier.Park(input)) {
      return false;
    }
    g.stats_barrier.Wait();
  }
  return !g.stats_barrier.Failed();
}

}  // namespace sdb::connector
