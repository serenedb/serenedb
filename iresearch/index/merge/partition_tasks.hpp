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

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <duckdb/parallel/task_executor.hpp>

#include "iresearch/index/merge/merge_workers.hpp"

namespace irs {

template<typename Work>
class PartitionTask final : public duckdb::BaseExecutorTask {
 public:
  PartitionTask(duckdb::TaskExecutor& executor, std::atomic_size_t& next,
                size_t count, const Work& work) noexcept
    : BaseExecutorTask{executor}, _next{next}, _count{count}, _work{work} {}

  void ExecuteTask() final {
    for (auto partition = _next.fetch_add(1, std::memory_order_relaxed);
         partition < _count && !executor.HasError();
         partition = _next.fetch_add(1, std::memory_order_relaxed)) {
      _work(partition);
    }
  }

 private:
  std::atomic_size_t& _next;
  size_t _count;
  const Work& _work;
};

template<typename Work>
void ForEachPartition(size_t count, const MergeWorkers& workers,
                      const Work& work) {
  const auto tasks = std::min(workers.threads, count);
  if (tasks < 2) {
    for (size_t partition = 0; partition != count; ++partition) {
      work(partition);
    }
    return;
  }
  std::atomic_size_t next{0};
  duckdb::TaskExecutor executor{workers.scheduler, MergeAccount(workers)};
  for (size_t task = 0; task != tasks; ++task) {
    executor.ScheduleTask(
      duckdb::make_uniq<PartitionTask<Work>>(executor, next, count, work));
  }
  executor.WorkOnTasks();
}

}  // namespace irs
