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

#include <rocksdb/options.h>

#include <yaclib/async/join.hpp>
#include <yaclib/async/make.hpp>
#include <yaclib/async/when_all.hpp>

#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/types.h"
#include "connector/key_utils.hpp"
#include "general_server/scheduler.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "rocksdb_engine_catalog/rocksdb_utils.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"

namespace sdb::catalog {

namespace {
bool CheckResult(const Result& result) {
  return result == ERROR_OK || result == ERROR_SERVER_DATA_SOURCE_NOT_FOUND ||
         result == ERROR_SERVER_DATABASE_NOT_FOUND ||
         result == ERROR_SERVER_DOCUMENT_NOT_FOUND ||
         result == ERROR_SERVER_INDEX_NOT_FOUND;
}

template<typename T>
AsyncResult QueueDropTask(std::shared_ptr<T> task) {
  auto* scheduler = GetScheduler();
  if (SerenedServer::Instance().isStopping()) {
    co_return {};
  }
  SDB_ASSERT(scheduler);
  GetServerEngine().AddDropTask();

  try {
    auto r = co_await scheduler->queueWithFuture(RequestLane::InternalLow,
                                                 [task] { return (*task)(); });
    while (r.errorNumber() == ERROR_LOCKED) {
      auto* scheduler = GetScheduler();
      if (!scheduler) {
        co_return {};
      }
      task->delay = std::min(kMaxDelay, task->delay << 1);
      co_await scheduler->delay(T::kName,
                                std::chrono::microseconds{task->delay});
      r = co_await scheduler->queueWithFuture(RequestLane::InternalLow,
                                              [task] { return (*task)(); });
    }
    GetServerEngine().RemoveDropTask();
    if (!r.ok()) {
      SDB_FATAL("xxxxx", Logger::THREADS, "Failed to execute ",
                task->GetContext(), ", error: ", r.errorMessage());
    }
    co_return r;
  } catch (std::exception& e) {
    SDB_FATAL("xxxxx", Logger::THREADS, "Unable to schedule ", T::kName, ": \"",
              e.what(), "\", shutting down");
  }
}
}  // namespace

AsyncResult TableShardDrop::operator()() {
  SDB_ASSERT(!is_root);
  auto& server = GetServerEngine();
  // TODO(codeworse): Probably we should store data by table shard id, not table
  // id. So, in that way here we would use id(not parent_id)
  auto [start, end] = connector::key_utils::CreateTableRange(parent_id);
  // Drop table data
  auto r = server.DropRange(start, end,
                            RocksDBColumnFamilyManager::get(
                              RocksDBColumnFamilyManager::Family::Default));
  if (!CheckResult(r)) {
    return QueueDropTask(shared_from_this());
  }
  rocksdb::Slice start_slice{start}, end_slice{end};
  r = rocksutils::ConvertStatus(server.db()->CompactRange(
    rocksdb::CompactRangeOptions{}, &start_slice, &end_slice));
  if (!CheckResult(r)) {
    return QueueDropTask(shared_from_this());
  }
  return yaclib::MakeFuture<Result>();
}

AsyncResult IndexShardDrop::operator()() {
  SDB_ASSERT(!is_root);
  // TODO(codeworse): in case of table/schema/database drop delete the entire
  // folder at once
  if (type == IndexType::Inverted) {
    auto path = search::InvertedIndexShard::GetPath(db_id, schema_id, id);
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
      return QueueDropTask(shared_from_this());
    }
  }
  return yaclib::MakeFuture<Result>();
}

Result IndexDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::IndexShard);
  if (!CheckResult(r)) {
    return r;
  }
  r = server.DropDefinition(parent_id, RocksDBEntryType::Index, id);
  if (!CheckResult(r)) {
    return r;
  }
  if (is_root) {
    return server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
  }
  return {};
}

AsyncResult IndexDrop::operator()() {
  auto shard_task = std::make_shared<IndexShardDrop>(
    DropTask{.parent_id = id, .id = shard_id}, db_id, schema_id, type);
  auto r = co_await QueueDropTask(std::move(shard_task));
  if (!CheckResult(r) || !CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

AsyncResult IndexDrop::Schedule() { return QueueDropTask(shared_from_this()); }

Result TableDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::TableShard);
  if (!CheckResult(r)) {
    return r;
  }
  r = server.DropDefinition(parent_id, RocksDBEntryType::Table, id);
  if (!CheckResult(r)) {
    return r;
  }
  if (is_root) {
    return server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
  }
  return {};
}

AsyncResult TableDrop::operator()() {
  std::vector<AsyncResult> async_results;
  async_results.reserve(indexes.size());
  for (auto& index : indexes) {
    async_results.push_back(QueueDropTask(index));
  }
  if (!async_results.empty()) {
    co_await yaclib::Join(async_results.begin(), async_results.end());
  }
  auto shard_task =
    std::make_shared<TableShardDrop>(DropTask{.parent_id = id, .id = shard_id});
  auto r = co_await QueueDropTask(std::move(shard_task));
  if (!CheckResult(r) || !CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

AsyncResult TableDrop::Schedule() { return QueueDropTask(shared_from_this()); }

Result SchemaDrop::Finalize() {
  auto& server = GetServerEngine();
  for (auto entry_type : {RocksDBEntryType::Table, RocksDBEntryType::View,
                          RocksDBEntryType::Function}) {
    auto r = server.DropEntry(id, entry_type);
    if (!CheckResult(r)) {
      return r;
    }
  }
  auto r = server.DropDefinition(parent_id, RocksDBEntryType::Schema, id);
  if (!CheckResult(r)) {
    return r;
  }
  if (is_root) {
    r = server.DropDefinition(parent_id, RocksDBEntryType::Tombstone, id);
    if (!CheckResult(r)) {
      return r;
    }
  }
  return {};
}

AsyncResult SchemaDrop::operator()() {
  std::vector<AsyncResult> async_results;
  async_results.reserve(tables.size());
  for (auto& table : tables) {
    async_results.push_back(QueueDropTask(table));
  }
  if (!async_results.empty()) {
    co_await yaclib::Join(async_results.begin(), async_results.end());
  }
  if (!CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

AsyncResult SchemaDrop::Schedule() { return QueueDropTask(shared_from_this()); }

Result DatabaseDrop::Finalize() {
  auto& server = GetServerEngine();
  auto r = server.DropEntry(id, RocksDBEntryType::Schema);
  if (!CheckResult(r)) {
    return r;
  }
  r = server.DropDefinition(id::kInstance, RocksDBEntryType::Database, id);
  if (!CheckResult(r)) {
    return r;
  }
  return server.DropDefinition(id::kInstance, RocksDBEntryType::Tombstone, id);
}

AsyncResult DatabaseDrop::operator()() {
  SDB_ASSERT(is_root);
  std::vector<AsyncResult> async_results;
  async_results.reserve(schemas.size());
  for (auto& schema : schemas) {
    async_results.push_back(QueueDropTask(schema));
  }
  if (!async_results.empty()) {
    co_await yaclib::Join(async_results.begin(), async_results.end());
  }
  if (!CheckResult(Finalize())) {
    co_return co_await QueueDropTask(shared_from_this());
  }
  co_return {};
}

AsyncResult DatabaseDrop::Schedule() {
  return QueueDropTask(shared_from_this());
}

}  // namespace sdb::catalog
