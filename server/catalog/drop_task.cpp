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
#include <optional>
#include <span>
#include <yaclib/async/make.hpp>
#include <yaclib/async/when_all.hpp>
#include <yaclib/coro/await.hpp>
#include <yaclib/coro/future.hpp>
#include <yaclib/coro/on.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
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

absl::Status RemoveIndexStorage(ObjectId db_id,
                                ObjectId schema_id = ObjectId{0},
                                ObjectId table_id = ObjectId{0},
                                ObjectId index_id = ObjectId{0}) {
  auto path =
    search::InvertedIndexStorage::GetPath(db_id, schema_id, table_id, index_id);
  std::error_code ec;
  std::filesystem::remove_all(path, ec);
  if (ec) {
    return absl::InternalError(
      absl::StrCat("Failed to remove index storage: ", ec.message()));
  }
  return absl::OkStatus();
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
      auto outcome = co_await scheduler.Run(
        [task] { return DropTask::ExecuteTask(std::move(task)); });
      if (outcome == DropOutcome::Retry) {
        task->_delay = std::min(kMaxDelay, task->_delay * 2);
        continue;
      }
      co_return outcome;
    }
  } catch (std::exception& e) {
    SDB_ERROR(GENERAL, "Unable to schedule ", task->GetName(), ": \"", e.what(),
              "\"");
    co_return DropOutcome::Done;
  }
}

void IndexDrop::Finalize() {
  if (_type == catalog::ObjectType::InvertedIndex ||
      _type == catalog::ObjectType::SecondaryIndex) {
    GetCatalogStore().Write([&](auto& ctx) { ctx.DropStoreIndex(_id); });
  }
  auto& server = GetCatalogStore();
  if (_is_root) {
    server.DropDefinition(_parent_id, _type, _id);
    server.DropDefinition(_parent_id, catalog::ObjectType::Tombstone, _id);
  }
}

AsyncResult IndexDrop::Execute() {
  if (_type == catalog::ObjectType::InvertedIndex && _is_root) {
    // The storage is guaranteed released here: AllowToDropDependencies() gates
    // on _data.expired(), so neither this drop nor any ancestor reaches Execute
    // while a catalog snapshot, query, replay session, or background task still
    // holds the iresearch storage -- no live holder touches the removed dir.
    if (auto s = RemoveIndexStorage(_db_id, _schema_id, _parent_id, _id);
        !s.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
      return yaclib::MakeFuture<DropOutcome>(DropOutcome::Retry);
    }
  }
  Finalize();
  return yaclib::MakeFuture<DropOutcome>(DropOutcome::Done);
}

void TableDropBase::Finalize() {
  SDB_IF_FAILURE("crash_before_seq_counter_wipe") { SDB_IMMEDIATE_ABORT(); }
  auto& server = GetCatalogStore();
  // Indexes and their storage.
  server.DropEntry(_id);
  // Owned seqs (def + counter) + the table's own catalog rows in one batch.
  server.Write([&](auto& ctx) {
    for (auto seq_id : _owned_sequences) {
      ctx.DropDefinition(_parent_id, catalog::ObjectType::Sequence, seq_id);
      ctx.DropSequence(seq_id);
    }
    if (_is_root) {
      ctx.DropDefinition(_parent_id, catalog::ObjectType::Table, _id);
      ctx.DropDefinition(_parent_id, catalog::ObjectType::Tombstone, _id);
    }
    FinalizeStore(ctx);
  });
}

AsyncResult TableDrop::Execute() {
  if (_is_root && !_indexes.empty()) {
    ObjectId db_id = _indexes.back()->GetDatabaseId();
    ObjectId schema_id = _parent_id;
    if (auto s = RemoveIndexStorage(db_id, schema_id, _id); !s.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
      co_return DropOutcome::Retry;
    }
  }
  co_await RunChildrenTasks(std::span{_indexes});
  Finalize();
  co_return DropOutcome::Done;
}

AsyncResult SearchTableDrop::Execute() {
  // The WAL chunk dir + shard registration live under the per-database WAL,
  // which no ancestor schema/database drop reaches, so they are always removed
  // per-table here.
  if (auto r = search::SearchTable::DropWalShard(_db_id, _id); !r.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.message());
    co_return DropOutcome::Retry;
  }

  if (_is_root) {
    if (auto r = search::SearchTable::DropIndexDir(_db_id, _parent_id, _id);
        !r.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", r.message());
      co_return DropOutcome::Retry;
    }
  }
  Finalize();
  co_return DropOutcome::Done;
}

void SchemaDrop::Finalize() {
  auto& server = GetCatalogStore();
  // Standalone seq counter rows -- DropEntry only sweeps definition rows.
  server.VisitDefinitions(_id, catalog::ObjectType::Sequence,
                          [&](CatalogStore::Key key, std::string_view) {
                            server.DropSequence(key.id);
                            return true;
                          });
  server.DropEntry(_id);
  if (_is_root) {
    server.DropDefinition(_parent_id, catalog::ObjectType::Schema, _id);
    server.DropDefinition(_parent_id, catalog::ObjectType::Tombstone, _id);
  }
}

AsyncResult SchemaDrop::Execute() {
  if (_is_root) {
    if (auto s = RemoveIndexStorage(_parent_id, _id); !s.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
      co_return DropOutcome::Retry;
    }
  }
  co_await RunChildrenTasks(std::span{_tables});
  Finalize();
  co_return DropOutcome::Done;
}

void DatabaseDrop::Finalize() {
  auto& server = GetCatalogStore();
  // Range-delete schema Definitions under db. Per-schema standalone-seq
  // counter wipes already ran inside each SchemaDrop::Finalize above.
  server.DropEntry(_id, catalog::ObjectType::Schema);
  server.DropEntry(_id, catalog::ObjectType::Subscription);
  server.DropDefinition(id::kInstance, catalog::ObjectType::Database, _id);
  server.DropDefinition(id::kInstance, catalog::ObjectType::Tombstone, _id);
}

AsyncResult DatabaseDrop::Execute() {
  SDB_ASSERT(_is_root);
  if (auto s = RemoveIndexStorage(_id); !s.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
    co_return DropOutcome::Retry;
  }
  co_await RunChildrenTasks(std::span{_schemas});
  Finalize();
  co_return DropOutcome::Done;
}

}  // namespace sdb::catalog
