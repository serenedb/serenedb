////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include <yaclib/async/future.hpp>

#include "catalog/index.h"
#include "catalog/object_dependency.h"
#include "general_server/scheduler.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "storage_engine/engine_feature.h"

namespace sdb::catalog {
using AsyncResult = yaclib::Future<Result>;

static constexpr uint32_t kInitialDelay = 125;
static constexpr uint32_t kMaxDelay = kInitialDelay << 7;

template<typename T>
AsyncResult QueueDropTask(std::shared_ptr<T> task) {
  auto* scheduler = GetScheduler();
  if (!scheduler) {
    return yaclib::MakeFuture<Result>();
  }

  try {
    auto [f, p] = yaclib::MakeContract<Result>();
    scheduler->queue(RequestLane::InternalLow,
                     [task, p = std::move(p)]() mutable {
                       (*task)()
                         .ThenInline([p = std::move(p)](Result&& r) mutable {
                           std::move(p).Set(std::move(r));
                         })
                         .Detach();
                     });
    return std::move(f).ThenInline(
      [task = std::move(task)](Result&& r) mutable -> AsyncResult {
        if (r.errorNumber() == ERROR_LOCKED) {
          auto* scheduler = GetScheduler();
          if (!scheduler) {
            return yaclib::MakeFuture<Result>();
          }
          task->delay = std::min(kMaxDelay, task->delay << 1);
          return scheduler
            ->delay(T::kName, std::chrono::microseconds{task->delay})
            .ThenInline([task] { return QueueDropTask(std::move(task)); });
        }
        if (!r.ok()) {
          SDB_FATAL("xxxxx", Logger::THREADS, "Failed to execute ",
                    task->GetContext(), ", error: ", r.errorMessage());
        }
        return yaclib::MakeFuture<Result>();
      });
  } catch (...) {
    SDB_FATAL("xxxxx", Logger::THREADS, "Unable to schedule ", T::kName,
              ", shutting down");
  }
}

struct DropTask {
  ObjectId parent_id;
  ObjectId id;
  bool is_root = false;
  uint32_t delay = kInitialDelay;  // delay in microseconds
};

struct TableShardDrop : DropTask, std::enable_shared_from_this<TableShardDrop> {
  static constexpr std::string_view kName = "table shard drop";

  TableShardDrop(DropTask&& task) : DropTask{std::move(task)} {}

  std::string GetContext() const {
    return absl::StrCat("table ", parent_id, " shard ", id);
  }

  AsyncResult operator()();
};

struct IndexShardDrop : DropTask, std::enable_shared_from_this<IndexShardDrop> {
  static constexpr std::string_view kName = "index shard drop";

  ObjectId db_id;
  ObjectId schema_id;
  IndexType type;

  IndexShardDrop(DropTask&& task, ObjectId db_id, ObjectId schema_id,
                 IndexType type)
    : DropTask{std::move(task)},
      db_id{db_id},
      schema_id{schema_id},
      type{type} {}

  std::string GetContext() const {
    return absl::StrCat("index ", parent_id, " shard ", id);
  }

  AsyncResult operator()();
};

struct IndexDrop : DropTask, std::enable_shared_from_this<IndexDrop> {
  static constexpr std::string_view kName = "index drop";

  ObjectId db_id;
  ObjectId schema_id;
  ObjectId shard_id;
  IndexType type;

  std::string GetContext() const { return absl::StrCat("index ", id); }

  AsyncResult operator()();
  Result Finalize();
};

struct TableDrop : DropTask, std::enable_shared_from_this<TableDrop> {
  static constexpr std::string_view kName = "table drop";

  ObjectId shard_id;
  std::vector<std::shared_ptr<IndexDrop>> indexes;

  std::string GetContext() const { return absl::StrCat("table ", id); }

  AsyncResult operator()();
  Result Finalize();
};

struct SchemaDrop : DropTask, std::enable_shared_from_this<SchemaDrop> {
  static constexpr std::string_view kName = "scope drop";

  std::vector<std::shared_ptr<TableDrop>> tables;

  std::string GetContext() const {
    return absl::StrCat("schema ", parent_id, ".", id);
  }

  AsyncResult operator()();
  Result Finalize();
};

struct DatabaseDrop : DropTask, std::enable_shared_from_this<DatabaseDrop> {
  static constexpr std::string_view kName = "database drop";

  std::vector<std::shared_ptr<SchemaDrop>> schemas;

  std::string GetContext() const { return absl::StrCat("database ", id); }

  AsyncResult operator()();
  Result Finalize();
};

}  // namespace sdb::catalog
