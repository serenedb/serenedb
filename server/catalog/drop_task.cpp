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
#include "basics/lifecycle.h"
#include "catalog/catalog.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
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

bool DropTask::ShouldStop() noexcept {
  return lifecycle::IsStopping() ||
         BackgroundScheduler::instance().IsStopping();
}

AsyncResult DropTask::Schedule(std::shared_ptr<DropTask> task) noexcept {
  try {
    auto& scheduler = BackgroundScheduler::instance();
    while (!ShouldStop()) {
      co_await scheduler.Delay(task->_delay);
      if (ShouldStop()) {
        break;
      }
      // The delay completes on an io thread, which does socket IO only: hop
      // onto the background pool before touching a drop.
      co_await On(scheduler.executor());
      // Copy, not move: the loop still owns `task` for the next backoff.
      auto outcome = co_await DropTask::ExecuteTask(task);
      if (outcome != DropOutcome::Retry) {
        co_return outcome;
      }
      task->_delay = std::min(kMaxDelay, task->_delay * 2);
    }
    co_return DropOutcome::Abandoned;
  } catch (std::exception& e) {
    SDB_ERROR(GENERAL, "Unable to schedule ", task->GetName(), ": \"", e.what(),
              "\"");
    co_return DropOutcome::Done;
  }
}

void IndexDrop::Finalize() {
  GetCatalogStore().Write([&](auto& ctx) {
    ctx.store().DropIndex(_db_id, _id, _parent_id, _name);
    if (_is_root) {
      ctx.catalog().PrepareCommit(_id);
    }
  });
}

AsyncResult IndexDrop::Execute() {
  if (_inverted && _is_root) {
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
  // The async half of a drop runs long after its tombstone, so compaction has
  // to be able to run in between and carry the open drop.
  // A drop task holds no catalog mutex, so the rewrite has to take it itself.
  SDB_IF_FAILURE("compact_inside_drop") {
    GetCatalog().TryExcludingMutations([] { GetCatalogStore().CompactNow(); });
  }
  auto& server = GetCatalogStore();
  server.Write([&](auto& ctx) {
    // Counters live outside the definition tree, so no commit covers them.
    for (auto seq_id : _owned_sequences) {
      ctx.catalog().DropSequence(seq_id);
    }
    if (_is_root) {
      // The commit takes the table's definition and its indexes. Its owned
      // sequences hang off the schema, not the table, so they are named here.
      for (auto seq_id : _owned_sequences) {
        ctx.catalog().DropObject(_parent_id,
                                 duckdb::CatalogType::SEQUENCE_ENTRY, seq_id);
      }
      ctx.catalog().PrepareCommit(_id);
    } else {
      // No open drop of its own, so this level says what went with it.
      ctx.catalog().DropChildren(_id);
    }
    FinalizeStore(ctx);
  });
}

AsyncResult TableDrop::Execute() {
  if (_is_root && !_indexes.empty()) {
    if (auto s = RemoveIndexStorage(_db_id, _parent_id, _id); !s.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
      co_return DropOutcome::Retry;
    }
  }
  co_await RunChildrenTasks(std::span{_indexes});
  if (ShouldStop()) {
    co_return DropOutcome::Abandoned;
  }
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
  server.Write([&](auto& ctx) {
    // Counters live outside the definition tree, so no commit covers them. A
    // table's owned ones go with that table's own drop.
    for (auto seq_id : _sequences) {
      ctx.catalog().DropSequence(seq_id);
    }
    if (_is_root) {
      ctx.catalog().PrepareCommit(_id);
    } else {
      ctx.catalog().DropChildren(_id);
    }
  });
}

AsyncResult SchemaDrop::Execute() {
  if (_is_root) {
    if (auto s = RemoveIndexStorage(_parent_id, _id); !s.ok()) {
      SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
      co_return DropOutcome::Retry;
    }
  }
  co_await RunChildrenTasks(std::span{_tables});
  if (ShouldStop()) {
    co_return DropOutcome::Abandoned;
  }
  Finalize();
  co_return DropOutcome::Done;
}

void DatabaseDrop::Finalize() {
  auto& server = GetCatalogStore();
  // The commit takes the database's definition and everything filed under its
  // id -- its schemas, and the foreign servers that hang off the database
  // directly (PG shape). Each SchemaDrop::Finalize already erased its own
  // children, counters included.
  server.Write([&](auto& ctx) { ctx.catalog().PrepareCommit(_id); });
  // The record is gone, so the file is garbage whatever happens next: a crash
  // before the unlink leaves it for boot reclamation.
  const auto path = CatalogStore::DatabaseFilePath(_id);
  for (const auto& name : {path, path + ".wal"}) {
    std::error_code ec;
    std::filesystem::remove(name, ec);
    if (ec) {
      SDB_WARN(GENERAL, "could not remove '", name, "': ", ec.message());
    }
  }
}

AsyncResult DatabaseDrop::Execute() {
  SDB_ASSERT(_is_root);
  if (auto s = RemoveIndexStorage(_id); !s.ok()) {
    SDB_WARN(GENERAL, "Retrying ", GetContext(), ": ", s.message());
    co_return DropOutcome::Retry;
  }
  co_await RunChildrenTasks(std::span{_schemas});
  if (ShouldStop()) {
    co_return DropOutcome::Abandoned;
  }
  Finalize();
  co_return DropOutcome::Done;
}

}  // namespace sdb::catalog
