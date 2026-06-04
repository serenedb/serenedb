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
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/store/store.h"
#include "catalog/types.h"
#include "scheduler/background_scheduler.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {
namespace {

Result RemoveIndexStorage(ObjectId db_id, ObjectId schema_id = ObjectId{0},
                          ObjectId table_id = ObjectId{0},
                          ObjectId index_id = ObjectId{0}) {
  auto path =
    search::InvertedIndexStorage::GetPath(db_id, schema_id, table_id, index_id);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return Result{ERROR_FAILED,
                  "Failed to remove index storage: " + ec.message()};
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
      auto& scheduler = BackgroundScheduler::instance();
      co_await scheduler.Delay(task->_delay);
      auto r = co_await scheduler.Run(
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
  if (_is_root) {
    auto r = server.DropDefinition(_parent_id, _type, _id);
    if (!r.ok()) {
      return r;
    }

    return server.DropDefinition(_parent_id, catalog::ObjectType::Tombstone,
                                 _id);
  }
  return {};
}

AsyncResult IndexDrop::Execute() {
  if (_type == catalog::ObjectType::InvertedIndex && _is_root) {
    // The storage is guaranteed released here: AllowToDropDependencies() gates
    // on _data.expired(), so neither this drop nor any ancestor reaches Execute
    // while a catalog snapshot, query, replay session, or background task still
    // holds the iresearch storage -- no live holder touches the removed dir.
    auto r = RemoveIndexStorage(_db_id, _schema_id, _parent_id, _id);
    if (!r.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
      return yaclib::MakeFuture<Result>(ERROR_LOCKED);
    }
  }
  auto r = Finalize();
  if (!r.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
    return yaclib::MakeFuture<Result>(ERROR_LOCKED);
  }
  return yaclib::MakeFuture<Result>();
}

Result TableDrop::Finalize() {
  SDB_IF_FAILURE("crash_before_seq_counter_wipe") { SDB_IMMEDIATE_ABORT(); }
  auto& server = GetCatalogStore();
  // Indexes and their storage.
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
  if (_db_id.isSet()) {
    // Search table: wait until every holder of the iresearch store has
    // released it (mirrors IndexDrop's weak_ptr drain), then remove the
    // directory + WAL chunks. _db_id is set only for Search tables.
    if (!_search_data.expired()) {
      co_return Result{ERROR_LOCKED};
    }
    if (auto r = search::SearchTable::DropArtifacts(_db_id, _id); !r.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
      co_return Result{ERROR_LOCKED};
    }
  }
  if (_is_root && !_indexes.empty()) {
    ObjectId db_id = _indexes.back()->GetDatabaseId();
    ObjectId schema_id = _parent_id;
    auto r = RemoveIndexStorage(db_id, schema_id, _id);
    if (!r.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
      co_return Result{ERROR_LOCKED};
    }
  }
  co_await RunChildrenTasks(std::span{_indexes});
  Result r = Finalize();
  if (!r.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
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
    auto r = RemoveIndexStorage(_parent_id, _id);
    if (!r.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
      co_return Result{ERROR_LOCKED};
    }
  }
  co_await RunChildrenTasks(std::span{_tables});
  if (auto r = Finalize(); !r.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
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
  r = server.DropEntry(_id, catalog::ObjectType::Subscription);
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
  auto r = RemoveIndexStorage(_id);
  if (!r.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
    co_return Result{ERROR_LOCKED};
  }
  co_await RunChildrenTasks(std::span{_schemas});
  if (auto r = Finalize(); !r.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.errorMessage());
    co_return Result{ERROR_LOCKED};
  }
  co_return {};
}

}  // namespace sdb::catalog
