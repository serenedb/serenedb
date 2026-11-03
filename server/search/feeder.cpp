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

#include "search/feeder.h"

#include <duckdb/common/fault_points.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string>
#include <utility>

namespace sdb::search {

class Feeder::Task final : public duckdb::BaseExecutorTask {
 public:
  Task(Feeder& feeder, size_t slot)
    : BaseExecutorTask{feeder._executor}, _feeder{feeder}, _slot{slot} {}

  duckdb::TaskExecutionResult ExecuteTaskStep() final {
    auto& queue = _feeder._queues[_slot];
    std::unique_ptr<duckdb::DataChunk> batch;
    {
      absl::MutexLock lock{&queue.mutex};
      if (queue.batches.empty()) {
        queue.running = false;
        return duckdb::TaskExecutionResult::TASK_FINISHED;
      }
      batch = std::move(queue.batches.front());
      queue.batches.pop_front();
    }
    try {
      SDB_IF_FAILURE("inverted_feed_fails") {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_IO_ERROR),
                        ERR_MSG("inverted index feed failed"));
      }
      _feeder._feed(_slot, *batch);
    } catch (...) {
      Cancel();
      throw;
    }
    return duckdb::TaskExecutionResult::TASK_NOT_FINISHED;
  }

  void Cancel() final {
    auto& queue = _feeder._queues[_slot];
    absl::MutexLock lock{&queue.mutex};
    queue.batches.clear();
    queue.running = false;
  }

  std::string TaskType() const final { return "SearchFeed"; }

 private:
  Feeder& _feeder;
  size_t _slot;
};

Feeder::Feeder(duckdb::DatabaseInstance& db, size_t slots, size_t depth,
               Feed feed)
  : _executor{duckdb::TaskScheduler::GetScheduler(db)}, _feed{std::move(feed)} {
  for (size_t i = 0; i < slots; ++i) {
    _queues.emplace_back().depth = depth;
  }
}

Feeder::~Feeder() {
  Clear();
  try {
    _executor.WorkOnTasks();
  } catch (...) {
  }
}

void Feeder::Clear() noexcept {
  for (auto& queue : _queues) {
    absl::MutexLock lock{&queue.mutex};
    queue.batches.clear();
  }
}

void Feeder::Push(std::unique_ptr<duckdb::DataChunk> batch) {
  if (_executor.HasError()) {
    return;
  }
  const auto slot = _next++ % _queues.size();
  auto& queue = _queues[slot];
  bool schedule = false;
  {
    absl::MutexLock lock{&queue.mutex};
    queue.batches.push_back(std::move(batch));
    schedule = !std::exchange(queue.running, true);
    if (!schedule && queue.NotFull()) {
      return;
    }
  }
  if (schedule) {
    _executor.ScheduleTask(duckdb::make_uniq<Task>(*this, slot));
  }
  while (true) {
    {
      absl::MutexLock lock{&queue.mutex};
      if (queue.NotFull()) {
        return;
      }
    }
    duckdb::shared_ptr<duckdb::Task> task;
    if (!_executor.GetTask(task)) {
      break;
    }
    task->Execute(duckdb::TaskExecutionMode::PROCESS_ALL);
  }
  duckdb::TaskScheduler::BlockingScope blocking;
  absl::MutexLock lock{&queue.mutex};
  queue.mutex.Await(absl::Condition(&queue, &Queue::NotFull));
}

void Feeder::Finish() { _executor.WorkOnTasks(); }

}  // namespace sdb::search
