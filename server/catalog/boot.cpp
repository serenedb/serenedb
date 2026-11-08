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

#include "catalog/boot.h"

#include <absl/algorithm/container.h>
#include <absl/container/flat_hash_set.h>
#include <absl/flags/flag.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>

#include <duckdb/common/error_data.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <filesystem>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "backup/restore.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/log.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "pg/catalog/oids.h"
#include "search/store.h"
#include "search/wal_recovery.h"
#include "utils/file_utils.h"

ABSL_FLAG(std::string, missing_database, "refuse",
          "What boot does with a database whose directory is missing: "
          "'refuse' (default) stops the server, 'skip' leaves it unattached, "
          "'drop' removes it from the catalog.");

namespace sdb::catalog {
namespace {

constexpr std::string_view kEngineDir = "engine_v1";
constexpr std::string_view kLogDir = "log";

bool HoldsEntries(const std::filesystem::path& directory) {
  std::error_code ec;
  const bool empty = std::filesystem::is_empty(directory, ec);
  if (ec == std::errc::no_such_file_or_directory) {
    return false;
  }
  if (ec) {
    SDB_FATAL(STARTUP, "cannot list directory '", directory.string(),
              "': ", ec.message());
  }
  return !empty;
}

bool HasStorages(SereneDBCatalog& catalog) {
  bool found = false;
  ForEachSearchStorage(
    catalog, [&](SearchTableEntry&) { found = true; },
    [&](InvertedIndexEntry&) { found = true; });
  return found;
}

void ApplyMissingDatabasePolicy(duckdb::ClientContext& context,
                                const DataDirectory& layout,
                                ClusterCatalog& cluster,
                                std::string_view policy) {
  for (const auto& db :
       SereneDBDatabases(duckdb::DatabaseManager::Get(context))) {
    const auto directory = layout.DatabaseDir(db->oid);
    if (!HasStorages(db->GetCatalog().Cast<SereneDBCatalog>()) ||
        std::filesystem::exists(directory)) {
      continue;
    }
    const auto name = db->GetName();
    if (policy == "refuse") {
      SDB_FATAL(STARTUP, "database '", name.GetIdentifierName(), "' (oid ",
                db->oid, ") cannot be opened: '", directory.string(),
                "' does not exist. Pass --missing_database=skip to leave it "
                "unattached or --missing_database=drop to remove it from "
                "the catalog.");
    }
    if (policy == "skip") {
      SDB_WARN(STARTUP, "database '", name.GetIdentifierName(),
               "' is not attached: '", directory.string(), "' does not exist");
      context.RunFunctionInTransaction([&] {
        duckdb::DatabaseManager::Get(context).DetachDatabase(
          context, name, duckdb::OnEntryNotFound::RETURN_NULL);
      });
      continue;
    }
    SDB_WARN(STARTUP, "dropping database '", name.GetIdentifierName(),
             "' from the catalog: '", directory.string(), "' does not exist");
    context.RunFunctionInTransaction([&] {
      duckdb::DropInfo info;
      info.type = duckdb::CatalogType::DATABASE_ENTRY;
      info.SetName(name);
      cluster.DropDatabase(cluster.GetCatalogTransaction(context), info);
    });
  }
}

void RemoveUnowned(const std::filesystem::path& directory,
                   const absl::flat_hash_set<duckdb::idx_t>& owned) {
  for (const auto& [oid, path] : OidDirectories(directory)) {
    if (!owned.contains(oid)) {
      RemoveDirectory(path);
    }
  }
}

void OpenLive(search::Store& storage, const duckdb::CatalogEntry& entry) {
  if (!storage.Opened()) {
    storage.Open();
  }
  if (storage.Absent()) {
    SDB_FATAL(STARTUP, "the directory '", storage.Path().string(), "' of ",
              entry.name.GetIdentifierName(), " (oid ", entry.oid,
              ") is missing");
  }
}

absl::flat_hash_set<duckdb::idx_t> OpenStorages(SereneDBCatalog& catalog) {
  absl::flat_hash_set<duckdb::idx_t> oids;
  ForEachSearchStorage(
    catalog,
    [&](SearchTableEntry& table) {
      OpenLive(*table.Storage(), table);
      oids.insert(table.oid);
    },
    [&](InvertedIndexEntry& index) {
      OpenLive(*index.Storage(), index);
      oids.insert(index.oid);
    });
  return oids;
}

void RemoveUnownedDatabases(const DataDirectory& layout,
                            ClusterCatalog& cluster) {
  absl::flat_hash_set<duckdb::idx_t> databases;
  cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
    .ScanCommitted(
      [&](duckdb::CatalogEntry& entry) { databases.insert(entry.oid); });
  RemoveUnowned(layout.EngineDir(), databases);
}

}  // namespace

DataDirectory::DataDirectory(std::string directory_p)
  : directory{std::move(directory_p)} {}

std::filesystem::path DataDirectory::EngineDir() const {
  return std::filesystem::path{directory} / kEngineDir;
}

std::filesystem::path DataDirectory::LogDir() const {
  return EngineDir() / kLogDir;
}

std::vector<std::pair<duckdb::idx_t, std::filesystem::path>> OidDirectories(
  const std::filesystem::path& directory) {
  std::vector<std::pair<duckdb::idx_t, std::filesystem::path>> found;
  std::error_code ec;
  std::filesystem::directory_iterator entries{directory, ec};
  if (ec && ec != std::errc::no_such_file_or_directory) {
    SDB_FATAL(STARTUP, "cannot list directory '", directory.string(),
              "': ", ec.message());
  }
  for (const auto& entry : entries) {
    const bool is_directory = entry.is_directory(ec);
    if (ec) {
      SDB_FATAL(STARTUP, "cannot stat '", entry.path().string(),
                "': ", ec.message());
    }
    const auto name = entry.path().filename().string();
    duckdb::idx_t oid = 0;
    if (is_directory && absl::c_all_of(name, absl::ascii_isdigit) &&
        absl::SimpleAtoi(name, &oid)) {
      found.emplace_back(oid, entry.path());
    }
  }
  return found;
}

std::filesystem::path DataDirectory::DatabaseDir(duckdb::idx_t oid) const {
  return EngineDir() / absl::StrCat(oid);
}

std::filesystem::path DataDirectory::StorageDir(duckdb::idx_t database,
                                                duckdb::idx_t oid) const {
  return DatabaseDir(database) / absl::StrCat(oid);
}

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AccessMode access_mode,
            duckdb::AttachVisibility visibility) {
  duckdb::AttachOptions options{info.options, access_mode};
  options.db_type.assign(type);
  options.visibility = visibility;
  duckdb::DatabaseManager::Get(context).AttachDatabase(context, info, options);
}

duckdb::unique_ptr<duckdb::TransactionManager> CreateTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo>,
  duckdb::AttachedDatabase& db, duckdb::Catalog&) {
  return duckdb::make_uniq<TransactionManager>(db);
}

duckdb::InternalCatalog CreateInternalCatalog(duckdb::AttachedDatabase& db,
                                              const DataDirectory& layout) {
  duckdb::unique_ptr<duckdb::Catalog> catalog;
  if (db.IsSystem()) {
    catalog = duckdb::make_uniq<ClusterCatalog>(db, layout);
  } else {
    catalog = duckdb::make_uniq<SereneDBCatalog>(
      db, std::make_shared<DatabaseDirectory>(std::filesystem::path{}), false);
  }
  return {.catalog = std::move(catalog),
          .transaction_manager = duckdb::make_uniq<TransactionManager>(db)};
}

void InitCatalog(bool default_database) {
  auto& instance = irs::DuckDBEngine::Instance().instance();
  auto& cluster = ClusterOf(instance);
  const auto& layout = cluster.Layout();
  const auto missing_policy = absl::GetFlag(FLAGS_missing_database);
  if (missing_policy != "refuse" && missing_policy != "skip" &&
      missing_policy != "drop") {
    SDB_FATAL(STARTUP, "--missing_database must be refuse, skip or drop, not '",
              missing_policy, "'");
  }
  if (!layout.InMemory()) {
    if (std::filesystem::create_directories(layout.EngineDir())) {
      utils::file_utils::SyncDirectory(layout.EngineDir().parent_path());
    }
    backup::PrepareDataDirectory(layout);
    if (!HoldsEntries(layout.LogDir())) {
      const auto leftovers = OidDirectories(layout.EngineDir());
      if (absl::c_any_of(leftovers, [](const auto& entry) {
            return HoldsEntries(entry.second);
          })) {
        SDB_FATAL(STARTUP, "'", layout.LogDir().string(),
                  "' holds no catalog log, but '", layout.EngineDir().string(),
                  "' holds database directories: refusing to start without "
                  "the catalog that owns them");
      }
      for (const auto& [oid, path] : leftovers) {
        RemoveDirectory(path);
      }
    }
  }
  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  auto& transactions = Transactions::Get();
  if (!layout.InMemory()) {
    auto log = std::make_unique<Log>(
      duckdb::FileSystem::GetFileSystem(instance), layout.LogDir());
    const auto target = [&](duckdb::idx_t id) {
      return InstanceCatalog(context, id);
    };
    log->Replay(
      [&](Lsn lsn, std::span<uint8_t> catalog) {
        transactions.SetReplaying(lsn);
        context.RunFunctionInTransaction(
          [&] { ReplayCatalogPart(context, target, catalog); });
      },
      [&](const Log::Range& range, std::span<uint8_t> payload) {
        transactions.SetReplaying(range.end);
        context.RunFunctionInTransaction(
          [&] { ReplayRecord(context, target, range, payload); });
      });
    transactions.SetReplaying(0);
    transactions.Open(std::move(log));
  }
  context.RunFunctionInTransaction(
    [&] { cluster.Bootstrap(context, default_database); });
  auth::PublishRoles(cluster);
  if (layout.InMemory()) {
    return;
  }
  ApplyMissingDatabasePolicy(context, layout, cluster, missing_policy);
  RemoveUnownedDatabases(layout, cluster);
  const auto attached =
    SereneDBDatabases(duckdb::DatabaseManager::Get(instance));
  for (const auto& db : attached) {
    const auto live = OpenStorages(db->GetCatalog().Cast<SereneDBCatalog>());
    RemoveUnowned(layout.DatabaseDir(db->oid), live);
  }
  search::RecoverSearchStorages(context, attached);
  std::vector<duckdb::reference<ForeignServerCatalogEntry>> servers;
  for (const auto& db : attached) {
    db->GetCatalog()
      .Cast<SereneDBCatalog>()
      .Set(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
      .ScanCommitted([&](duckdb::CatalogEntry& entry) {
        servers.emplace_back(entry.Cast<ForeignServerCatalogEntry>());
      });
  }
  for (auto& server : servers) {
    try {
      context.RunFunctionInTransaction([&] { server.get().Attach(context); });
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "failed to re-attach foreign server '",
               server.get().name.GetIdentifierName(),
               "': ", duckdb::ErrorData{e}.RawMessage());
    }
  }
}

}  // namespace sdb::catalog
