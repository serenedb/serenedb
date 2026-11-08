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
#include <duckdb/common/fault_points.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "search/store.h"

namespace sdb::search {
namespace {

struct FinishReplayTask final : duckdb::BaseExecutorTask {
  FinishReplayTask(duckdb::TaskExecutor& executor,
                   absl::AnyInvocable<void()> finish)
    : BaseExecutorTask{executor}, finish{std::move(finish)} {}

  void ExecuteTask() final { finish(); }

  std::string TaskType() const final { return "SearchFinishReplay"; }

  absl::AnyInvocable<void()> finish;
};

}  // namespace

void RecoverSearchStorages(
  duckdb::ClientContext& context,
  std::span<const duckdb::shared_ptr<duckdb::AttachedDatabase>> attached) {
  const auto begin = std::chrono::steady_clock::now();
  std::vector<std::shared_ptr<Store>> tables;
  for (const auto& db : attached) {
    catalog::ForEachSearchStorage(
      db->GetCatalog().Cast<catalog::SereneDBCatalog>(),
      [&](catalog::SearchTableEntry& table) {
        tables.push_back(table.Storage());
      },
      [](catalog::InvertedIndexEntry&) {});
  }
  context.RunFunctionInTransaction([&] {
    for (auto& table : tables) {
      table->InvertReplayed(context);
    }
  });
  duckdb::TaskExecutor executor{duckdb::TaskScheduler::GetScheduler(
    irs::DuckDBEngine::Instance().instance())};
  std::atomic_size_t replayed_tables{0};
  for (auto& table : tables) {
    executor.ScheduleTask(
      duckdb::make_uniq<FinishReplayTask>(executor, [&replayed_tables, table] {
        if (table->FinishReplay()) {
          replayed_tables.fetch_add(1, std::memory_order_relaxed);
        }
        if (const auto status = table->CleanupUnsafe().res; !status.ok()) {
          SDB_WARN(SEARCH, "cleanup of table '", table->GetId(),
                   "' after log replay failed: ", status.message());
        }
      }));
  }
  executor.WorkOnTasks();
  if (tables.empty()) {
    return;
  }
  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(SEARCH, "recovery: replayed the log into ",
           replayed_tables.load(std::memory_order_relaxed), " of ",
           tables.size(), " table(s) in ", absl::FormatDuration(duration));
}

void CoverLog(uint64_t target) {
  std::vector<std::shared_ptr<Store>> tables;
  for (const auto& db : catalog::SereneDBDatabases(duckdb::DatabaseManager::Get(
         irs::DuckDBEngine::Instance().instance()))) {
    auto& catalog = db->GetCatalog().Cast<catalog::SereneDBCatalog>();
    if (!catalog.Durable()) {
      continue;
    }
    catalog::ForEachSearchStorage(
      catalog,
      [&](catalog::SearchTableEntry& table) {
        tables.push_back(table.Storage());
      },
      [](catalog::InvertedIndexEntry&) {});
  }
  SDB_PARK_ONCE_ON_FAILURE("pause_cover_after_collect");
  for (const auto& table : tables) {
    table->Cover(target);
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
