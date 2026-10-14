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
#include <absl/functional/any_invocable.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <atomic>
#include <chrono>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
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
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::search {
namespace {

using BoundIndexHandle =
  duckdb::IndexWriteHandle<connector::InvertedStoreIndex>;

struct FinishReplayTask final : duckdb::BaseExecutorTask {
  FinishReplayTask(duckdb::TaskExecutor& executor,
                   absl::AnyInvocable<void()> finish)
    : BaseExecutorTask{executor}, finish{std::move(finish)} {}

  void ExecuteTask() final { finish(); }

  std::string TaskType() const final { return "SearchFinishReplay"; }

  absl::AnyInvocable<void()> finish;
};

std::vector<duckdb::reference<catalog::SereneDBCatalog>> SereneDBCatalogs() {
  std::vector<duckdb::reference<catalog::SereneDBCatalog>> catalogs;
  for (const auto& database :
       duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance())
         .GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() == catalog::SereneDBCatalog::kStorageType) {
      catalogs.emplace_back(catalog.Cast<catalog::SereneDBCatalog>());
    }
  }
  return catalogs;
}

std::vector<duckdb::reference<catalog::InvertedIndexEntry>>
InvertedIndexEntries() {
  std::vector<duckdb::reference<catalog::InvertedIndexEntry>> indexes;
  for (auto& catalog : SereneDBCatalogs()) {
    std::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> schemas;
    catalog.get().ScanSchemas([&](duckdb::SchemaCatalogEntry& schema) {
      schemas.emplace_back(schema);
    });
    for (auto& schema : schemas) {
      schema.get().Scan(
        duckdb::CatalogType::INDEX_ENTRY, [&](duckdb::CatalogEntry& entry) {
          if (connector::IsInvertedIndex(
                entry.Cast<duckdb::IndexCatalogEntry>())) {
            indexes.emplace_back(entry.Cast<catalog::InvertedIndexEntry>());
          }
        });
    }
  }
  return indexes;
}

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
  auto& fs = duckdb::FileSystem::GetFileSystem(db.GetDatabase());
  if (auto wal =
        fs.OpenFile(storage.GetWALPath(),
                    duckdb::FileFlags::FILE_FLAGS_READ |
                      duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS)) {
    wal->Sync();
  }
}

}  // namespace

void RecoverSearchStorages() {
  const auto begin = std::chrono::steady_clock::now();
  std::vector<std::pair<std::optional<BoundIndexHandle>,
                        std::shared_ptr<InvertedIndexStorage>>>
    recovering;
  std::vector<std::pair<duckdb::reference<duckdb::AttachedDatabase>,
                        std::shared_ptr<SearchTable>>>
    tables;
  std::vector<duckdb::reference<duckdb::AttachedDatabase>> databases;
  const auto add_database = [&](duckdb::AttachedDatabase& db) {
    if (absl::c_none_of(databases, [&](const auto& synced) {
          return &synced.get() == &db;
        })) {
      databases.emplace_back(db);
    }
  };
  for (auto& index : InvertedIndexEntries()) {
    const auto& storage = index.get().Storage();
    if (!storage || !index.get().info) {
      continue;
    }
    add_database(index.get().catalog.GetAttached());
    recovering.emplace_back(BoundIndexOf(index.get()), storage);
  }
  for (auto& catalog : SereneDBCatalogs()) {
    auto& db = catalog.get().GetAttached();
    for (auto& table : catalog::SearchTableStorages(catalog.get())) {
      add_database(db);
      tables.emplace_back(db, std::move(table));
    }
  }
  for (auto& db : databases) {
    SyncWal(db);
  }
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(
    irs::DuckDBEngine::Instance().instance())};
  std::atomic_size_t replayed{0};
  std::atomic_size_t replayed_tables{0};
  for (auto& [index, storage] : recovering) {
    executor.ScheduleTask(duckdb::make_uniq<FinishReplayTask>(
      executor, [&replayed, index = std::move(index), storage]() mutable {
        if (index && (*index)->FinishReplay()) {
          replayed.fetch_add(1, std::memory_order_relaxed);
        }
        if (const auto status = storage->Refresh(); !status.ok()) {
          SDB_WARN(SEARCH, "refresh of index '", storage->GetId(),
                   "' after WAL replay failed: ", status.message());
        }
      }));
  }
  for (auto& [db, table] : tables) {
    executor.ScheduleTask(duckdb::make_uniq<FinishReplayTask>(
      executor, [&replayed_tables, &db = db.get(), table] {
        if (table->FinishReplay(db)) {
          replayed_tables.fetch_add(1, std::memory_order_relaxed);
        }
        if (const auto status = table->VacuumRefresh(); !status.ok()) {
          SDB_WARN(SEARCH, "refresh of search table '", table->GetTableId(),
                   "' after WAL replay failed: ", status.message());
        }
      }));
  }
  executor.WorkOnTasks();
  for (auto& catalog : SereneDBCatalogs()) {
    auto& db = catalog.get().GetAttached();
    auto& storage = db.GetStorageManager();
    if (storage.InMemory()) {
      continue;
    }
    const auto checkpoint =
      duckdb::DuckTransactionManager::Get(db).SharedCheckpointLock();
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
  for (auto& index : InvertedIndexEntries()) {
    if (const auto& storage = index.get().Storage()) {
      storage->StartTasks();
    }
  }
  for (auto& catalog : SereneDBCatalogs()) {
    for (auto& table : catalog::SearchTableStorages(catalog.get())) {
      table->StartTasks();
    }
  }
}

}  // namespace sdb::search
