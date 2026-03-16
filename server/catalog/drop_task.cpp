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

#include "catalog/drop_task.h"

#include <absl/strings/str_cat.h>
#include <rocksdb/options.h>

#include <filesystem>
#include <yaclib/async/make.hpp>
#include <yaclib/async/when_all.hpp>

#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/types.h"
#include "connector/key_utils.hpp"
#include "general_server/scheduler.h"
#include "rocksdb_engine_catalog/rocksdb_column_family_manager.h"
#include "rocksdb_engine_catalog/rocksdb_common.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {

namespace {

Result RemoveIndexShards(ObjectId db_id, ObjectId schema_id = ObjectId{0},
                         ObjectId table_id = ObjectId{0},
                         ObjectId index_id = ObjectId{0},
                         ObjectId shard_id = ObjectId{0}) {
  auto path = search::InvertedIndexShard::GetPath(db_id, schema_id, table_id,
                                                  index_id, shard_id);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return Result{ERROR_FAILED,
                  "Failed to remove index shards: " + ec.message()};
  }
  return {};
}

}  // namespace

AsyncResult DropTask::Schedule(std::shared_ptr<DropTask> task) noexcept {
  try {
    while (true) {
      auto* scheduler = GetScheduler();
      if (!scheduler) {
        co_return {};
      }
      co_await scheduler->delay(task->GetName(),
                                std::chrono::microseconds{task->delay});
      auto r = co_await scheduler->queueWithFuture(
        RequestLane::InternalLow, [task] { return (*task)(); });
      if (r.errorNumber() == ERROR_LOCKED) {
        task->delay = std::min(kMaxDelay, task->delay * 2);
        continue;
      }
      if (!r.ok()) {
        SDB_ERROR("xxxxx", Logger::THREADS, "Failed to execute ",
                  task->GetContext(), ", error: ", r.errorMessage());
      }
      co_return r;
    }
  } catch (std::exception& e) {
    SDB_ERROR("xxxxx", Logger::THREADS, "Unable to schedule ", task->GetName(),
              ": \"", e.what(), "\"");
    co_return Result{ERROR_INTERNAL,  "Unable to schedule ",
                     task->GetName(), ": ",
                     e.what(),        "\""};
  }
}

AsyncResult TableShardDrop::operator()() {
  SDB_ASSERT(!is_root);
  auto& server = GetServerEngine();
  // TODO(codeworse): Probably we should store data by table shard id, not
  // table id. So, in that way here we would use id(not parent_id)
  auto [start, end] = connector::key_utils::CreateTableRange(parent_id);
  auto* cf = RocksDBColumnFamilyManager::get(
    RocksDBColumnFamilyManager::Family::Default);
  // Drop table data
  // TODO(codeworse): add some parameter for large range(not just >= 1000)
  auto r =
    rocksutils::RemoveLargeRange(server.db(), rocksdb::Slice{start},
                                 rocksdb::Slice{end}, cf, true, (size >= 1000));
  if (!r.ok()) {
    return Schedule(shared_from_this());
  }
  return yaclib::MakeFuture<Result>();
}

Result IndexDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::IndexShard);
  if (!r.ok()) {
    return r;
  }
  if (is_root) {
    r = server.DropDefinition(parent_id, RocksDBEntryType::Index, id);
    if (!r.ok()) {
      return r;
    }

    return server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
  }
  return {};
}

AsyncResult IndexDrop::operator()() {
  Result r;
  if (type == IndexType::Inverted && is_root) {
    r = RemoveIndexShards(db_id, schema_id, parent_id, id);
  }
  if (!r.ok() || !Finalize().ok()) {
    return Schedule(shared_from_this());
  }
  return yaclib::MakeFuture<Result>();
}

Result TableDrop::Finalize() {
  auto& server = GetServerEngine();

  auto r = server.DropEntry(id);
  if (!r.ok()) {
    return r;
  }

  if (is_root) {
    r = server.DropDefinition(parent_id, RocksDBEntryType::Table, id);
    if (!r.ok()) {
      return r;
    }
    return server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
  }
  return {};
}

AsyncResult TableDrop::operator()() {
  std::vector<AsyncResult> async_results;
  async_results.reserve(indexes.size());
  if (is_root && !indexes.empty()) {
    ObjectId db_id = indexes.back()->db_id;
    ObjectId schema_id = indexes.back()->schema_id;
    auto r = RemoveIndexShards(db_id, schema_id, id);
    if (!r.ok()) {
      co_return co_await Schedule(shared_from_this());
    }
  }
  for (auto& index : indexes) {
    async_results.push_back(Schedule(index));
  }
  if (!async_results.empty()) {
    co_await yaclib::Await(async_results.begin(), async_results.end());
  }
  SDB_ASSERT(type != TableType::Unknown);
  if (type == TableType::Document) {
    auto shard_task =
      std::make_shared<TableShardDrop>(id, shard_id, table_size);
    auto r = co_await Schedule(std::move(shard_task));
    if (!r.ok() || !Finalize().ok()) {
      co_return co_await Schedule(shared_from_this());
    }
  }
  co_return {};
}

Result SchemaDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id);
  if (!r.ok()) {
    return r;
  }

  if (is_root) {
    auto r = server.DropDefinition(parent_id, RocksDBEntryType::Schema, id);
    if (!r.ok()) {
      return r;
    }
    r = server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
    if (!r.ok()) {
      return r;
    }
  }
  return {};
}

AsyncResult SchemaDrop::operator()() {
  std::vector<AsyncResult> async_results;
  if (is_root) {
    auto r = RemoveIndexShards(parent_id, id);
    if (!r.ok()) {
      co_return co_await Schedule(shared_from_this());
    }
  }
  async_results.reserve(tables.size());
  for (auto& table : tables) {
    async_results.push_back(Schedule(table));
  }
  if (!async_results.empty()) {
    co_await yaclib::Await(async_results.begin(), async_results.end());
  }
  if (!Finalize().ok()) {
    co_return co_await Schedule(shared_from_this());
  }
  co_return {};
}

Result DatabaseDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::Schema);
  if (!r.ok()) {
    return r;
  }
  r = server.DropDefinition(id::kInstance, RocksDBEntryType::Database, id);
  if (!r.ok()) {
    return r;
  }
  return server.DropDefinition(id::kInstance, RocksDBEntryType::Tombstone, id);
}

AsyncResult DatabaseDrop::operator()() {
  SDB_ASSERT(is_root);
  auto r = RemoveIndexShards(id);
  if (!r.ok()) {
    co_return co_await Schedule(shared_from_this());
  }
  std::vector<AsyncResult> async_results;
  async_results.reserve(schemas.size());
  for (auto& schema : schemas) {
    async_results.push_back(Schedule(schema));
  }
  if (!async_results.empty()) {
    co_await yaclib::Await(async_results.begin(), async_results.end());
  }
  if (!Finalize().ok()) {
    co_return co_await Schedule(shared_from_this());
  }
  co_return {};
}

}  // namespace sdb::catalog
