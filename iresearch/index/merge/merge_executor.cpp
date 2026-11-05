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
#include <absl/cleanup/cleanup.h>
#include <absl/random/random.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
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

  void Start(duckdb::TaskExecutor& executor) {
    Submit(executor, std::move(_roots));
  }

  void Run(duckdb::TaskExecutor& executor, size_t unit);

  bool Failed() const noexcept {
    return _failed.load(std::memory_order_relaxed);
  }

 private:
  void Submit(duckdb::TaskExecutor& executor, std::vector<size_t> ready);

  MergePlan& _plan;
  std::vector<std::vector<size_t>> _dependents;
  std::vector<std::atomic<size_t>> _waiting;
  std::vector<size_t> _roots;
  std::atomic_bool _failed{false};
};

class UnitTask final : public duckdb::BaseExecutorTask {
 public:
  UnitTask(duckdb::TaskExecutor& executor, MergeRun& run, size_t unit) noexcept
    : BaseExecutorTask{executor}, _run{run}, _unit{unit} {}

  void ExecuteTask() final { _run.Run(executor, _unit); }

 private:
  MergeRun& _run;
  size_t _unit;
};

MergeRun::MergeRun(MergePlan& plan)
  : _plan{plan},
    _dependents(plan.Units().size()),
    _waiting(plan.Units().size()) {
  const auto units = plan.Units();
  for (size_t unit = 0; unit != units.size(); ++unit) {
    const auto& deps = units[unit].deps;
    _waiting[unit].store(deps.size(), std::memory_order_relaxed);
    for (const auto dep : deps) {
      SDB_ASSERT(dep < unit);
      _dependents[dep].push_back(unit);
    }
  }
  std::vector<uint64_t> rank(units.size());
  for (size_t unit = units.size(); unit-- != 0;) {
    uint64_t after = 0;
    for (const auto next : _dependents[unit]) {
      after = std::max(after, rank[next]);
    }
    rank[unit] = units[unit].cost + after;
    if (units[unit].deps.empty()) {
      _roots.push_back(unit);
    }
  }
  const auto longer = [&rank](size_t lhs, size_t rhs) {
    return rank[lhs] != rank[rhs] ? rank[lhs] > rank[rhs] : lhs < rhs;
  };
  absl::c_sort(_roots, longer);
  for (auto& dependents : _dependents) {
    absl::c_sort(dependents, longer);
  }
}

void MergeRun::Run(duckdb::TaskExecutor& executor, size_t unit) {
  if (Failed()) {
    return;
  }
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
  Submit(executor, std::move(ready));
  std::move(abort).Cancel();
}

void MergeRun::Submit(duckdb::TaskExecutor& executor,
                      std::vector<size_t> ready) {
  SDB_IF_FAILURE("irs::ExecuteMerge::Shuffle") {
    absl::c_shuffle(ready, absl::BitGen{});
  }
  for (const auto unit : ready) {
    if (Failed()) {
      return;
    }
    executor.ScheduleTask(duckdb::make_uniq<UnitTask>(executor, *this, unit));
  }
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
