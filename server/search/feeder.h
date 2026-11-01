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

#include <absl/functional/any_invocable.h>
#include <absl/synchronization/mutex.h>

#include <deque>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <memory>

namespace sdb::search {

class Feeder {
 public:
  using Feed =
    absl::AnyInvocable<void(size_t slot, duckdb::DataChunk& batch) const>;

  Feeder(duckdb::DatabaseInstance& db, size_t slots, size_t depth, Feed feed);
  ~Feeder();

  void Push(std::unique_ptr<duckdb::DataChunk> batch);
  void Finish();

 private:
  struct Queue {
    bool NotFull() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(mutex) {
      return batches.size() < depth;
    }

    size_t depth = 0;
    absl::Mutex mutex;
    std::deque<std::unique_ptr<duckdb::DataChunk>> batches
      ABSL_GUARDED_BY(mutex);
    bool running ABSL_GUARDED_BY(mutex) = false;
  };
  class Task;

  void Clear() noexcept;

  duckdb::TaskExecutor _executor;
  Feed _feed;
  std::deque<Queue> _queues;
  size_t _next = 0;
};

}  // namespace sdb::search
