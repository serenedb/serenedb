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

#include "iresearch/index/merge/merge_executor.hpp"

#include <absl/algorithm/container.h>
#include <absl/base/thread_annotations.h>
#include <absl/cleanup/cleanup.h>
#include <absl/random/distributions.h>
#include <absl/random/random.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/utils/assert.hpp"

namespace irs {
namespace {

class MergeRun {
 public:
  explicit MergeRun(MergePlan& plan);

  MergeRun(const MergeRun&) = delete;
  MergeRun& operator=(const MergeRun&) = delete;

  void Start(duckdb::TaskExecutor& executor) { Submit(executor, _roots); }

  void RunNext(duckdb::TaskExecutor& executor);

  bool Failed() const noexcept {
    return _failed.load(std::memory_order_relaxed);
  }

 private:
  bool Lower(size_t lhs, size_t rhs) const noexcept {
    return _rank[lhs] != _rank[rhs] ? _rank[lhs] < _rank[rhs] : lhs > rhs;
  }

  void Submit(duckdb::TaskExecutor& executor, std::span<const size_t> ready);
  size_t Take();

  MergePlan& _plan;
  std::vector<std::vector<size_t>> _dependents;
  std::vector<std::atomic<size_t>> _waiting;
  std::vector<uint64_t> _rank;
  std::vector<size_t> _roots;
  absl::Mutex _mutex;
  std::vector<size_t> _ready ABSL_GUARDED_BY(_mutex);
  std::atomic_bool _failed{false};
};

class UnitTask final : public duckdb::BaseExecutorTask {
 public:
  UnitTask(duckdb::TaskExecutor& executor, MergeRun& run) noexcept
    : BaseExecutorTask{executor}, _run{run} {}

  void ExecuteTask() final { _run.RunNext(executor); }

 private:
  MergeRun& _run;
};

MergeRun::MergeRun(MergePlan& plan)
  : _plan{plan},
    _dependents(plan.Units().size()),
    _waiting(plan.Units().size()),
    _rank(plan.Units().size()) {
  const auto units = plan.Units();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    const auto& deps = units[unit].deps;
    _waiting[unit].store(deps.size(), std::memory_order_relaxed);
    for (const auto dep : deps) {
      SDB_ASSERT(dep < unit);
      _dependents[dep].push_back(unit);
    }
  }
  for (size_t unit = units.size(); unit-- != 0;) {
    uint64_t after = 0;
    for (const auto next : _dependents[unit]) {
      after = std::max(after, _rank[next]);
    }
    _rank[unit] = units[unit].cost + after;
    if (units[unit].deps.empty()) {
      _roots.push_back(unit);
    }
  }
  SDB_IF_FAILURE("irs::ExecuteMerge::Shuffle") {
    absl::BitGen random;
    for (auto& rank : _rank) {
      rank = absl::Uniform<uint64_t>(random);
    }
  }
}

void MergeRun::RunNext(duckdb::TaskExecutor& executor) {
  if (Failed()) {
    return;
  }
  const auto unit = Take();
  absl::Cleanup abort = [this]() noexcept {
    _failed.store(true, std::memory_order_relaxed);
    _plan.Abort();
  };
  if (!_plan.RunUnit(unit)) {
    return;
  }
  std::vector<size_t> ready;
  for (const auto next : _dependents[unit]) {
    if (_waiting[next].fetch_sub(1, std::memory_order_acq_rel) == 1) {
      ready.push_back(next);
    }
  }
  Submit(executor, ready);
  std::move(abort).Cancel();
}

void MergeRun::Submit(duckdb::TaskExecutor& executor,
                      std::span<const size_t> ready) {
  if (ready.empty() || Failed()) {
    return;
  }
  const auto lower = [this](size_t lhs, size_t rhs) { return Lower(lhs, rhs); };
  {
    absl::MutexLock lock{&_mutex};
    for (const auto unit : ready) {
      _ready.push_back(unit);
      absl::c_push_heap(_ready, lower);
    }
  }
  for (size_t task = 0; task != ready.size(); ++task) {
    executor.ScheduleTask(duckdb::make_uniq<UnitTask>(executor, *this));
  }
}

size_t MergeRun::Take() {
  const auto lower = [this](size_t lhs, size_t rhs) { return Lower(lhs, rhs); };
  absl::MutexLock lock{&_mutex};
  SDB_ASSERT(!_ready.empty());
  absl::c_pop_heap(_ready, lower);
  const auto unit = _ready.back();
  _ready.pop_back();
  return unit;
}

}  // namespace

bool ExecuteMerge(MergePlan& plan) {
  SDB_IF_FAILURE("irs::ExecuteMerge") { return false; }
  MergeRun run{plan};
  const auto workers = plan.Workers();
  duckdb::TaskExecutor executor{workers.scheduler, MergeAccount(workers)};
  const duckdb::TaskScheduler::BlockingScope blocking;
  run.Start(executor);
  executor.AwaitTasks();
  return !run.Failed();
}

bool MergeSegments(Directory& dir, MergeDocMap& doc_map, SegmentMeta& segment,
                   const SegmentWriterOptions& options,
                   duckdb::shared_ptr<duckdb::DedicatedPool> pool,
                   const MergeProgress& progress, const AnnBuildEnv* env,
                   MergeProfile* profile) {
  SDB_ASSERT(options.db);
  bool merged = false;
  absl::Cleanup invalidate = [&merged, &segment]() noexcept {
    if (!merged) [[unlikely]] {
      segment.files.clear();
      static_cast<SegmentInfo&>(segment) = SegmentInfo{};
    }
  };
  MergePlan plan{dir,          doc_map,
                 segment,      options.scorer,
                 *options.db,  options.field_options,
                 progress,     env,
                 options.sort, options.hnsw_bases};
  absl::Cleanup report = [&plan, profile]() noexcept {
    if (profile) {
      *profile = plan.Profile();
    }
  };
  merged = plan.Prepare(std::move(pool)) && ExecuteMerge(plan);
  return merged;
}

}  // namespace irs
