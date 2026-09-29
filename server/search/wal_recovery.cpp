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

#include <absl/cleanup/cleanup.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>

#include <chrono>
#include <duckdb/catalog/catalog_entry/duck_index_entry.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/storage/table/data_table_info.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "connector/inverted_store_index.h"
#include "search/inverted_index_storage.h"
#include "search/tick_domain.h"

namespace sdb::search {
namespace {

struct FinishReplayTask final : duckdb::BaseExecutorTask {
  FinishReplayTask(duckdb::TaskExecutor& executor,
                   connector::InvertedStoreIndex* index,
                   std::shared_ptr<InvertedIndexStorage> storage)
    : BaseExecutorTask{executor}, index{index}, storage{std::move(storage)} {}

  void ExecuteTask() final {
    if (index) {
      index->FinishReplay();
    }
    storage->Refresh();
  }

  std::string TaskType() const final { return "InvertedFinishReplay"; }

  connector::InvertedStoreIndex* index;
  std::shared_ptr<InvertedIndexStorage> storage;
};

std::vector<duckdb::reference<catalog::InvertedIndexEntry>>
InvertedIndexEntries() {
  std::vector<duckdb::reference<catalog::InvertedIndexEntry>> indexes;
  for (const auto& database :
       duckdb::DatabaseManager::Get(irs::DuckDBEngine::Instance().instance())
         .GetDatabases()) {
    auto& catalog = database->GetCatalog();
    if (catalog.GetCatalogType() != catalog::SereneDBCatalog::kStorageType) {
      continue;
    }
    std::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> schemas;
    catalog.Cast<catalog::SereneDBCatalog>().ScanSchemas(
      [&](duckdb::SchemaCatalogEntry& schema) {
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

connector::InvertedStoreIndex* BoundIndexOf(
  catalog::InvertedIndexEntry& entry) {
  for (auto& index : entry.info->info->GetIndexes().Indexes()) {
    if (index.IsBound() &&
        index.GetIndexType() == connector::InvertedStoreIndex::kTypeName &&
        index.Cast<connector::InvertedStoreIndex>().IndexId() == entry.oid) {
      return &index.Cast<connector::InvertedStoreIndex>();
    }
  }
  return nullptr;
}

}  // namespace

void InitInvertedIndexes() {
  const auto begin = std::chrono::steady_clock::now();
  std::vector<std::shared_ptr<InvertedIndexStorage>> storages;
  std::vector<std::pair<connector::InvertedStoreIndex*,
                        std::shared_ptr<InvertedIndexStorage>>>
    recovering;
  for (auto& index : InvertedIndexEntries()) {
    const auto& storage = index.get().Storage();
    if (!storage) {
      continue;
    }
    TickDomain::Instance().SeedAtLeast(storage->GetRecoveryTick());
    storages.emplace_back(storage);
    if (!index.get().info) {
      continue;
    }
    storage->StartRecovery();
    recovering.emplace_back(BoundIndexOf(index.get()), storage);
  }
  absl::Cleanup finish = [&] {
    for (auto& storage : storages) {
      storage->FinishCreation();
    }
  };
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(
    irs::DuckDBEngine::Instance().instance())};
  for (auto& [index, storage] : recovering) {
    executor.ScheduleTask(
      duckdb::make_uniq<FinishReplayTask>(executor, index, storage));
  }
  executor.WorkOnTasks();
  if (recovering.empty()) {
    return;
  }
  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "search index recovery: ", recovering.size(),
           " inverted index(es) in ", absl::FormatDuration(duration));
}

void StartInvertedIndexTasks() {
  for (auto& index : InvertedIndexEntries()) {
    if (const auto& storage = index.get().Storage()) {
      storage->StartTasks();
    }
  }
}

}  // namespace sdb::search
