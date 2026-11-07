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

#include <duckdb/common/fault_points.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/queries/docs_mask_query.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <utility>

#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

uint32_t NextGeneration(uint32_t state) noexcept {
  return state == std::numeric_limits<uint32_t>::max() ? SegmentWork::kPreparing
                                                       : state + 1;
}

const irs::QueryBuilder& PrepareSegment(ScanGlobalState& g, ScanLocalState& l,
                                        uint32_t seg_idx, SegmentWork& work) {
  irs::PrepareCollector* collector = nullptr;
  if (g.collector) {
    if (l.thread_slot == std::numeric_limits<uint32_t>::max()) {
      l.thread_slot = g.thread_slots.fetch_add(1, std::memory_order_relaxed);
      SDB_ASSERT(l.thread_slot < g.collect_threads);
    }
    collector = g.collector->Get();
  }
  const auto& segment = (*g.reader)[seg_idx];
  work.job.Hire(g.workers);
  const irs::PrepareContext ctx{
    .collector = collector,
    .thread = collector != nullptr ? l.thread_slot : 0,
    .needs_terms = g.needs_terms,
    .tasks = g.workers > 1 ? &work.job : nullptr};
  try {
    SDB_IF_FAILURE("ScanPrepareFault") {
      THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
    }
    g.queries[seg_idx] = g.vector_scorer == nullptr
                           ? irs::PrepareMasked(*g.filter, segment, ctx)
                           : g.filter->PrepareSegment(segment, ctx);
  } catch (...) {
    work.prepare.store(SegmentWork::kUnprepared, std::memory_order_release);
    work.prepare.notify_all();
    throw;
  }
  work.prepare.store(SegmentWork::kReady, std::memory_order_release);
  work.prepare.notify_all();
  return *g.queries[seg_idx];
}

}  // namespace

void PrepareJob::Run(uint32_t count, absl::FunctionRef<void(Next)> worker) {
  if (count == 0) {
    return;
  }
  _worker = &worker;
  _error = nullptr;
  _failed.clear(std::memory_order_relaxed);
  _left.store(count, std::memory_order_relaxed);
  _cursor.store(uint64_t{count} << 32, std::memory_order_release);
  _state.store(NextGeneration(_state.load(std::memory_order_relaxed)),
               std::memory_order_release);
  _state.notify_all();
  uint32_t task = 0;
  if (Claim(task, count)) {
    Work(task, count);
  }
  for (auto left = _left.load(std::memory_order_acquire); left != 0;
       left = _left.load(std::memory_order_acquire)) {
    _left.wait(left, std::memory_order_acquire);
  }
  for (auto inside = _inside.load(std::memory_order_acquire); inside != 0;
       inside = _inside.load(std::memory_order_acquire)) {
    _inside.wait(inside, std::memory_order_acquire);
  }
  _worker = nullptr;
  if (_error) {
    std::rethrow_exception(std::exchange(_error, nullptr));
  }
}

void PrepareJob::Help() {
  _inside.fetch_add(1, std::memory_order_acq_rel);
  uint32_t task = 0;
  uint32_t count = 0;
  if (Claim(task, count)) {
    Work(task, count);
  }
  if (_inside.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    _inside.notify_all();
  }
}

bool PrepareJob::Claim(uint32_t& task, uint32_t& count) noexcept {
  const auto cursor = _cursor.fetch_add(1, std::memory_order_acq_rel);
  task = static_cast<uint32_t>(cursor);
  count = static_cast<uint32_t>(cursor >> 32);
  return task < count;
}

void PrepareJob::Work(uint32_t task, uint32_t count) {
  bool first = true;
  bool held = true;
  const auto next = [&]() -> uint32_t {
    if (first) {
      first = false;
      return task;
    }
    if (held) {
      held = false;
      Finish();
    }
    uint32_t total = 0;
    if (Claim(task, total)) {
      held = true;
      return task;
    }
    return count;
  };
  try {
    (*_worker)(next);
  } catch (...) {
    if (!_failed.test_and_set(std::memory_order_acq_rel)) {
      _error = std::current_exception();
    }
    if (held) {
      Finish();
    }
    Drain();
  }
}

void PrepareJob::Finish() noexcept {
  if (_left.fetch_sub(1, std::memory_order_acq_rel) == 1) {
    _left.notify_all();
  }
}

void PrepareJob::Drain() noexcept {
  uint32_t task = 0;
  uint32_t count = 0;
  while (Claim(task, count)) {
    Finish();
  }
}

const irs::QueryBuilder& EnsureSegmentQuery(ScanGlobalState& g,
                                            ScanLocalState& l,
                                            uint32_t seg_idx) {
  auto& work = g.Segment(seg_idx);
  for (;;) {
    auto state = work.prepare.load(std::memory_order_acquire);
    if (state == SegmentWork::kReady) {
      return *g.queries[seg_idx];
    }
    if (state == SegmentWork::kUnprepared) {
      if (work.prepare.compare_exchange_strong(state, SegmentWork::kPreparing,
                                               std::memory_order_acq_rel)) {
        return PrepareSegment(g, l, seg_idx, work);
      }
      continue;
    }
    work.job.Help();
    work.prepare.wait(state, std::memory_order_acquire);
  }
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
  if (g.stats_barrier.Park(input, *g.client_context)) {
    l.parked_on = &g.stats_barrier;
    return false;
  }
  g.stats_barrier.Wait();
  return true;
}

}  // namespace sdb::connector
