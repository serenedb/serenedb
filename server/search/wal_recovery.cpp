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

#include "search/wal_recovery.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/synchronization/mutex.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <atomic>
#include <chrono>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <duckdb/storage/table/index_entry.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "connector/inverted_store_index.h"
#include "search/store.h"

namespace sdb::search {
namespace {

using BoundIndexHandle =
  duckdb::IndexWriteHandle<connector::InvertedStoreIndex>;

std::optional<BoundIndexHandle> BoundIndexOf(
  catalog::InvertedIndexEntry& entry) {
  for (auto index : entry.info->info->GetIndexes().IndexEntries()) {
    if (index->GetBindState() == duckdb::IndexBindState::BOUND &&
        index->GetIndexOid() == entry.oid) {
      return index->GetWriteHandle<connector::InvertedStoreIndex>();
    }
  }
  return std::nullopt;
}

void SyncWal(duckdb::AttachedDatabase& db) {
  auto& storage = db.GetStorageManager();
  if (storage.InMemory()) {
    return;
  }
  auto& fs = db.GetDatabase().GetInternalFileSystem();
  if (auto wal =
        fs.OpenFile(storage.GetWALPath(),
                    duckdb::FileFlags::FILE_FLAGS_READ |
                      duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS)) {
    wal->Sync();
  }
}

}  // namespace

std::optional<MissingStorage> OpenSearchStorages(
  std::span<const duckdb::reference<catalog::SereneDBCatalog>> catalogs) {
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(
                                  irs::DuckDBEngine::Instance().instance()),
                                duckdb::TaskSchedulerType::ASYNC};
  absl::Mutex mutex;
  std::optional<MissingStorage> missing;
  const auto open = [&](const duckdb::CatalogEntry& entry,
                        std::shared_ptr<Store> storage) {
    if (storage->Opened()) {
      return;
    }
    executor.ScheduleTask(duckdb::make_uniq<duckdb::FunctionExecutorTask>(
      executor,
      [&mutex, &missing, &entry, storage = std::move(storage)] {
        if (storage->Open()) {
          return;
        }
        absl::MutexLock lock{&mutex};
        if (!missing || entry.oid < missing->entry.get().oid) {
          missing = MissingStorage{.entry = entry, .path = storage->Path()};
        }
      },
      "SearchOpenStorage"));
  };
  for (auto& catalog : catalogs) {
    catalog::ForEachSearchStorage(
      catalog.get(),
      [&](catalog::SearchTableEntry& table) { open(table, table.Storage()); },
      [&](catalog::InvertedIndexEntry& index) {
        open(index, index.Storage());
      });
  }
  executor.WorkOnTasks();
  return missing;
}

void RecoverSearchStorages(
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> attached) {
  const auto begin = std::chrono::steady_clock::now();
  std::vector<
    std::pair<std::optional<BoundIndexHandle>, std::shared_ptr<Store>>>
    recovering;
  std::vector<std::shared_ptr<Store>> tables;
  std::vector<duckdb::reference<duckdb::AttachedDatabase>> databases;
  const auto add_database = [&](duckdb::AttachedDatabase& db) {
    if (absl::c_none_of(databases, [&](const auto& synced) {
          return &synced.get() == &db;
        })) {
      databases.emplace_back(db);
    }
  };
  for (const auto& db : attached) {
    catalog::ForEachSearchStorage(
      db->GetCatalog().Cast<catalog::SereneDBCatalog>(),
      [&](catalog::SearchTableEntry& table) {
        add_database(*db);
        tables.push_back(table.Storage());
      },
      [&](catalog::InvertedIndexEntry& index) {
        if (!index.info) {
          return;
        }
        add_database(*db);
        recovering.emplace_back(BoundIndexOf(index), index.Storage());
      });
  }
  for (auto& db : databases) {
    SyncWal(db);
  }
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(
    irs::DuckDBEngine::Instance().instance())};
  std::atomic_size_t replayed{0};
  std::atomic_size_t replayed_tables{0};
  for (auto& [index, storage] : recovering) {
    executor.ScheduleTask(duckdb::make_uniq<duckdb::FunctionExecutorTask>(
      executor,
      [&replayed, index = std::move(index), storage]() mutable {
        if (index && (*index)->FinishReplay()) {
          replayed.fetch_add(1, std::memory_order_relaxed);
        }
        if (const auto status = storage->Refresh(); !status.ok()) {
          SDB_WARN(SEARCH, "refresh of index '", storage->GetId(),
                   "' after WAL replay failed: ", status.message());
        }
      },
      "SearchFinishReplay"));
  }
  for (auto& table : tables) {
    executor.ScheduleTask(duckdb::make_uniq<duckdb::FunctionExecutorTask>(
      executor,
      [&replayed_tables, table] {
        if (table->FinishReplay()) {
          replayed_tables.fetch_add(1, std::memory_order_relaxed);
        }
        if (const auto status = table->VacuumRefresh(); !status.ok()) {
          SDB_WARN(SEARCH, "refresh of search table '", table->GetId(),
                   "' after WAL replay failed: ", status.message());
        }
      },
      "SearchFinishReplay"));
  }
  executor.WorkOnTasks();
  for (const auto& db : attached) {
    auto& storage = db->GetStorageManager();
    if (storage.InMemory()) {
      continue;
    }
    const auto checkpoint =
      duckdb::DuckTransactionManager::Get(*db).SharedCheckpointLock();
    auto commit_lock = storage.GetCommitLock();
    storage.ReleaseRetainedWAL(commit_lock);
  }
  if (recovering.empty() && tables.empty()) {
    return;
  }
  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  const auto indexes = replayed.load(std::memory_order_relaxed);
  const auto searched = replayed_tables.load(std::memory_order_relaxed);
  std::vector<std::string> kinds;
  if (!recovering.empty()) {
    kinds.push_back(
      absl::StrCat(indexes, " of ", recovering.size(), " inverted index(es)"));
  }
  if (!tables.empty()) {
    kinds.push_back(
      absl::StrCat(searched, " of ", tables.size(), " search table(s)"));
  }
  const auto replayed_into = absl::StrJoin(kinds, " and ");
  if (indexes != 0 || searched != 0) {
    SDB_INFO(SEARCH, "search index recovery: replayed the WAL into ",
             replayed_into, " in ", absl::FormatDuration(duration));
  } else {
    SDB_DEBUG(SEARCH, "search index recovery: ", replayed_into, " in ",
              absl::FormatDuration(duration));
  }
}

void StartSearchStorageTasks() {
  for (const auto& db : catalog::SereneDBDatabases(duckdb::DatabaseManager::Get(
         irs::DuckDBEngine::Instance().instance()))) {
    catalog::ForEachSearchStorage(
      db->GetCatalog().Cast<catalog::SereneDBCatalog>(),
      [](catalog::SearchTableEntry& table) { table.Storage()->StartTasks(); },
      [](catalog::InvertedIndexEntry& index) {
        index.Storage()->StartTasks();
      });
  }
}

}  // namespace sdb::search
