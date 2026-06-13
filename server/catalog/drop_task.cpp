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

#include <filesystem>
#include <span>
#include <yaclib/async/make.hpp>
#include <yaclib/async/when_all.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/store/store.h"
#include "catalog/types.h"
#include "general_server/scheduler.h"
#include "search/inverted_index_shard.h"
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
template<typename T>
yaclib::Future<> RunChildrenTasks(std::span<std::shared_ptr<T>> tasks) {
  static_assert(std::is_base_of_v<DropTask, T>);
  if (tasks.empty()) {
    co_return {};
  }
  std::vector<AsyncResult> async_results;
  async_results.reserve(tasks.size());
  for (const auto& task : tasks) {
    async_results.push_back(DropTask::Schedule(task));
  }
  co_await yaclib::Await(async_results.begin(), async_results.end());
  co_return {};
}

}  // namespace

AsyncResult DropTask::Schedule(std::shared_ptr<DropTask> task) noexcept {
  try {
    while (true) {
      auto* scheduler = GetScheduler();
      if (!scheduler) {
        co_return {};
      }
      co_await scheduler->delay(task->GetName(), task->_delay);
      auto r = co_await scheduler->queueWithFuture(
        RequestLane::InternalLow,
        [task] { return DropTask::ExecuteTask(std::move(task)); });
      if (r.errorNumber() == ERROR_LOCKED) {
        task->_delay = std::min(kMaxDelay, task->_delay * 2);
        continue;
      }
      if (!r.ok()) {
        SDB_ERROR(GENERAL, "Failed to execute ", task->GetContext(),
                  ", error: ", r.errorMessage());
      }
      co_return r;
    }
  } catch (std::exception& e) {
    SDB_ERROR(GENERAL, "Unable to schedule ", task->GetName(), ": \"", e.what(),
              "\"");
    co_return Result{ERROR_INTERNAL,
                     "Unable to schedule ",
                     task->GetName(),
                     ": ",
                     "\"",
                     e.what(),
                     "\""};
  }
}

AsyncResult TableShardDrop::Execute() {
  SDB_ASSERT(!_is_root);
  // Table rows live in the store table, dropped by TableDrop::Finalize;
  // this task only exists to wait until catalog snapshots release the
  // shard object (AllowToDrop) before storage teardown proceeds.
  return yaclib::MakeFuture<Result>();
}

Result IndexDrop::Finalize() {
  if (_type == catalog::ObjectType::InvertedIndex ||
      _type == catalog::ObjectType::SecondaryIndex) {
    auto r =
      GetCatalogStore().Write([&](auto& ctx) { ctx.DropStoreIndex(_id); });
    if (!r.ok()) {
      return r;
    }
  }
  auto& server = GetCatalogStore();
  auto shard_type = catalog::IndexShardType(_type);
  auto r = server.DropEntry(_id, shard_type);
  if (!r.ok()) {
    return r;
  }
  if (_is_root) {
    r = server.DropDefinition(_parent_id, _type, _id);
    if (!r.ok()) {
      return r;
    }

    return server.DropDefinition(_parent_id, catalog::ObjectType::Tombstone,
                                 _id);
  }
  return {};
}

AsyncResult IndexDrop::Execute() {
  Result r;
  if (_type == catalog::ObjectType::InvertedIndex && _is_root) {
    r = RemoveIndexShards(_db_id, _schema_id, _parent_id, _id);
  }
  if (!r.ok() || !Finalize().ok()) {
    return yaclib::MakeFuture<Result>(ERROR_LOCKED);
  }
  return yaclib::MakeFuture<Result>();
}

Result TableDrop::Finalize() {
  SDB_IF_FAILURE("crash_before_seq_counter_wipe") { SDB_IMMEDIATE_ABORT(); }
  auto& server = GetCatalogStore();
  // Indexes, shards.
  auto r = server.DropEntry(_id);
  if (!r.ok()) {
    return r;
  }
  // Owned seqs (def + counter) + the table's own catalog rows in one batch.
  return server.Write([&](auto& ctx) {
    for (auto seq_id : _owned_sequences) {
      ctx.DropDefinition(_parent_id, catalog::ObjectType::Sequence, seq_id);
      ctx.DropSequence(seq_id);
    }
    if (_is_root) {
      ctx.DropDefinition(_parent_id, catalog::ObjectType::Table, _id);
      ctx.DropDefinition(_parent_id, catalog::ObjectType::Tombstone, _id);
    }
    ctx.DropStoreTable(catalog::DroppedStoreTableName(_id));
  });
}

AsyncResult TableDrop::Execute() {
  if (_is_root && !_indexes.empty()) {
    ObjectId db_id = _indexes.back()->GetDatabaseId();
    ObjectId schema_id = _parent_id;
    auto r = RemoveIndexShards(db_id, schema_id, _id);
    if (!r.ok()) {
      co_return Result{ERROR_LOCKED};
    }
  }
  co_await RunChildrenTasks(std::span{_indexes});
  auto r = co_await Schedule(_shard_drop);
  if (!r.ok() || !Finalize().ok()) {
    co_return Result{ERROR_LOCKED};
  }
  co_return {};
}

Result SchemaDrop::Finalize() {
  auto& server = GetCatalogStore();
  // Standalone seq counter rows -- DropEntry only sweeps definition rows.
  auto r = server.VisitDefinitions(
    _id, catalog::ObjectType::Sequence,
    [&](CatalogStore::Key key, std::string_view) -> Result {
      return server.DropSequence(key.id);
    });
  if (!r.ok()) {
    return r;
  }
  r = server.DropEntry(_id);
  if (!r.ok()) {
    return r;
  }
  if (_is_root) {
    r = server.DropDefinition(_parent_id, catalog::ObjectType::Schema, _id);
    if (!r.ok()) {
      return r;
    }
    return server.DropDefinition(_parent_id, catalog::ObjectType::Tombstone,
                                 _id);
  }
  return {};
}

AsyncResult SchemaDrop::Execute() {
  if (_is_root) {
    auto r = RemoveIndexShards(_parent_id, _id);
    if (!r.ok()) {
      co_return Result{ERROR_LOCKED};
    }
  }
  co_await RunChildrenTasks(std::span{_tables});
  if (!Finalize().ok()) {
    co_return Result{ERROR_LOCKED};
  }
  co_return {};
}

Result DatabaseDrop::Finalize() {
  auto& server = GetCatalogStore();
  // Range-delete schema Definitions under db. Per-schema standalone-seq
  // counter wipes already ran inside each SchemaDrop::Finalize above.
  auto r = server.DropEntry(_id, catalog::ObjectType::Schema);
  if (!r.ok()) {
    return r;
  }
  r = server.DropDefinition(id::kInstance, catalog::ObjectType::Database, _id);
  if (!r.ok()) {
    return r;
  }
  return server.DropDefinition(id::kInstance, catalog::ObjectType::Tombstone,
                               _id);
}

AsyncResult DatabaseDrop::Execute() {
  SDB_ASSERT(_is_root);
  auto r = RemoveIndexShards(_id);
  if (!r.ok()) {
    co_return Result{ERROR_LOCKED};
  }
  co_await RunChildrenTasks(std::span{_schemas});
  if (!Finalize().ok()) {
    co_return Result{ERROR_LOCKED};
  }
  co_return {};
}

}  // namespace sdb::catalog
