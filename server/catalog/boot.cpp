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

#include <absl/container/flat_hash_set.h>
#include <absl/flags/flag.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <filesystem>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/log.hpp>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/foreign_server.h"
#include "search/wal_recovery.h"
#include "storage_engine/search_engine.h"

ABSL_FLAG(std::string, missing_database, "refuse",
          "What boot does with a database whose data file is missing: "
          "'refuse' (default) stops the server, 'skip' leaves it unattached, "
          "'drop' removes it from the catalog.");

namespace sdb::catalog {
namespace {

constexpr const char* kCatalogDir = "engine_catalog";
constexpr const char* kDatabaseDir = "engine_duckdb";

struct MissingDatabases {
  bool fresh_cluster = false;
  std::string policy;
  absl::flat_hash_set<duckdb::idx_t> missing;
  std::vector<duckdb::Identifier> skipped;
  std::vector<duckdb::Identifier> dropped;
};

MissingDatabases gMissingDatabases;

duckdb::unique_ptr<duckdb::Catalog> AttachCluster(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const std::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  RequestSereneDBStorageVersion(options);
  return duckdb::make_uniq<ClusterCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> MakeClusterTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<duckdb::DuckTransactionManager>(db);
}

const DataDirectory& Layout(duckdb::AttachedDatabase& db) {
  return static_cast<const DataDirectory&>(
    *db.GetStorageExtension()->storage_info);
}

void ApplyMissingDatabasePolicy(duckdb::AttachedDatabase& cluster,
                                const duckdb::Identifier& name,
                                duckdb::idx_t oid) {
  const auto file = Layout(cluster).DatabaseFile(oid);
  const auto& policy = gMissingDatabases.policy;
  if (policy == "refuse") {
    SDB_FATAL(STARTUP, "database '", name.GetIdentifierName(), "' (oid ", oid,
              ") cannot be opened: '", file,
              "' does not exist. Pass --missing_database=skip to leave it "
              "unattached or --missing_database=drop to remove it from "
              "the catalog.");
  }
  if (policy == "skip") {
    SDB_WARN(STARTUP, "database '", name.GetIdentifierName(),
             "' is not attached: '", file, "' does not exist");
    gMissingDatabases.skipped.push_back(name);
  } else {
    SDB_WARN(STARTUP, "dropping database '", name.GetIdentifierName(),
             "' from the catalog: '", file, "' does not exist");
    gMissingDatabases.dropped.push_back(name);
  }
}

}  // namespace

DataDirectory::DataDirectory(std::string directory_p)
  : directory{std::move(directory_p)} {}

void RequestSereneDBStorageVersion(duckdb::AttachOptions& options) {
  static_assert(
    duckdb::SERENEDB_VERSION_LOWER == duckdb::StorageVersion::SERENEDB_LATEST,
    "a file below SERENEDB_LATEST is raised on attach only in memory: "
    "checkpoint it before anything writes its WAL or search WAL, so "
    "neither log gets ahead of the file header");
  options.options["storage_version"] =
    duckdb::Value{duckdb::StorageVersionInfo::GetStorageVersionString(
      duckdb::StorageVersion::SERENEDB_LATEST)};
}

std::string DataDirectory::CatalogLogFile() const {
  return absl::StrCat(directory, "/", kCatalogDir, "/catalog.wal");
}

std::string DataDirectory::DatabaseDir() const {
  return absl::StrCat(directory, "/", kDatabaseDir);
}

std::string DataDirectory::DatabaseFile(duckdb::idx_t oid) const {
  return absl::StrCat(DatabaseDir(), "/", oid, ".db");
}

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AttachVisibility visibility,
            bool defer_storage_load) {
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.db_type = std::string{type};
  options.visibility = visibility;
  options.defer_storage_load = defer_storage_load;
  duckdb::DatabaseManager::Get(context).AttachDatabase(context, info, options);
}

duckdb::Catalog& AttachDatabaseCatalog(duckdb::ClientContext& context,
                                       const duckdb::Identifier& name,
                                       duckdb::idx_t oid) {
  auto& manager = duckdb::DatabaseManager::Get(context);
  if (auto attached = manager.GetDatabase(name)) {
    if (attached->oid == oid) {
      return attached->GetCatalog();
    }
    manager.DetachDatabase(context, name, duckdb::OnEntryNotFound::RETURN_NULL);
  }
  auto& cluster = ClusterOf(context);
  const auto file = Layout(cluster.GetAttached()).DatabaseFile(oid);
  duckdb::AttachInfo info;
  info.name = name;
  auto visibility = duckdb::AttachVisibility::SHOWN;
  std::error_code ec;
  if (!gMissingDatabases.fresh_cluster && !std::filesystem::exists(file, ec)) {
    gMissingDatabases.missing.insert(oid);
    info.path = IN_MEMORY_PATH;
    visibility = duckdb::AttachVisibility::HIDDEN;
  }
  Attach(context, info, SereneDBCatalog::kStorageType, visibility, true);
  return manager.GetDatabase(name)->GetCatalog();
}

const DataDirectory& ClusterLayout(duckdb::AttachedDatabase& cluster) {
  return Layout(cluster);
}

std::vector<std::filesystem::path> DatabaseArtifacts(
  duckdb::AttachedDatabase& cluster, duckdb::idx_t oid) {
  const auto file = Layout(cluster).DatabaseFile(oid);
  return {file, file + ".wal", file + ".wal.checkpoint", file + ".wal.recovery",
          search::GetSearchEngine().GetPersistedPath(oid)};
}

void RemoveDatabaseFiles(duckdb::AttachedDatabase& cluster, duckdb::idx_t oid) {
  std::error_code ec;
  for (const auto& path : DatabaseArtifacts(cluster, oid)) {
    std::filesystem::remove_all(path, ec);
  }
}

void RegisterClusterStorage(duckdb::DBConfig& config,
                            duckdb::shared_ptr<DataDirectory> layout) {
  auto extension = duckdb::make_shared_ptr<duckdb::StorageExtension>();
  extension->attach = AttachCluster;
  extension->create_transaction_manager = MakeClusterTransactionManager;
  extension->storage_info = std::move(layout);
  duckdb::StorageExtension::Register(config, ClusterCatalog::kStorageType,
                                     std::move(extension));
}

void InitCatalog(std::string_view directory) {
  const DataDirectory layout{std::string{directory}};
  std::filesystem::create_directories(
    absl::StrCat(directory, "/", kCatalogDir));
  std::filesystem::create_directories(layout.DatabaseDir());
  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  const auto log_path = layout.CatalogLogFile();
  std::error_code log_ec;
  gMissingDatabases.fresh_cluster = !std::filesystem::exists(log_path, log_ec);
  gMissingDatabases.policy = absl::GetFlag(FLAGS_missing_database);
  const auto& missing_policy = gMissingDatabases.policy;
  if (missing_policy != "refuse" && missing_policy != "skip" &&
      missing_policy != "drop") {
    SDB_FATAL(STARTUP, "--missing_database must be refuse, skip or drop, not '",
              missing_policy, "'");
  }
  duckdb::AttachInfo cluster_info;
  cluster_info.name = duckdb::Identifier{ClusterCatalog::kDatabaseName};
  cluster_info.path = IN_MEMORY_PATH;
  context.RunFunctionInTransaction([&] {
    Attach(context, cluster_info, ClusterCatalog::kStorageType,
           duckdb::AttachVisibility::HIDDEN, false);
  });
  auto& instance = irs::DuckDBEngine::Instance().instance();
  auto& cluster = ClusterOf(instance);
  auto catalog_log = duckdb::WriteAheadLog::Replay(
    context, cluster.GetAttached().GetStorageManager(), log_path);
  cluster.OpenCatalogLog(std::move(catalog_log),
                         gMissingDatabases.policy != "skip");
  context.RunFunctionInTransaction([&] { cluster.Bootstrap(context); });
  std::vector<std::pair<duckdb::Identifier, duckdb::idx_t>> databases;
  cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster.LoginTransaction(), [&](duckdb::CatalogEntry& entry) {
      databases.emplace_back(entry.name, entry.oid);
    });
  for (const auto& [name, oid] : databases) {
    context.RunFunctionInTransaction(
      [&] { AttachDatabaseCatalog(context, name, oid); });
    if (gMissingDatabases.missing.contains(oid)) {
      ApplyMissingDatabasePolicy(cluster.GetAttached(), name, oid);
    }
  }
  auto& manager = duckdb::DatabaseManager::Get(instance);
  for (const auto& db : manager.GetDatabases()) {
    if (db->GetCatalog().GetCatalogType() == SereneDBCatalog::kStorageType &&
        !FindDatabase(db->GetName().GetIdentifierName())) {
      manager.DetachDatabase(context, db->GetName(),
                             duckdb::OnEntryNotFound::RETURN_NULL);
    }
  }
  for (const auto& name : gMissingDatabases.skipped) {
    manager.DetachDatabase(context, name, duckdb::OnEntryNotFound::RETURN_NULL);
  }
  for (const auto& name : gMissingDatabases.dropped) {
    manager.DetachDatabase(context, name, duckdb::OnEntryNotFound::RETURN_NULL);
    context.RunFunctionInTransaction([&] {
      duckdb::DropInfo drop;
      drop.type = duckdb::CatalogType::DATABASE_ENTRY;
      drop.SetName(name);
      drop.if_not_found = duckdb::OnEntryNotFound::RETURN_NULL;
      cluster.DropDatabase(cluster.GetCatalogTransaction(context), drop);
    });
  }
  for (const auto& db : manager.GetDatabases()) {
    if (db->GetCatalog().GetCatalogType() == SereneDBCatalog::kStorageType) {
      db->GetStorageManager().FinishLoad(context);
    }
  }
  search::InitInvertedIndexes();
  cluster.ResolveArtifacts();
  std::vector<duckdb::reference<ForeignServerCatalogEntry>> servers;
  for (const auto& db : manager.GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (catalog.GetCatalogType() != SereneDBCatalog::kStorageType) {
      continue;
    }
    catalog.Cast<duckdb::DuckCatalog>()
      .GetCatalogSet(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
      .Scan([&](duckdb::CatalogEntry& entry) {
        servers.emplace_back(entry.Cast<ForeignServerCatalogEntry>());
      });
  }
  for (auto& server : servers) {
    try {
      context.RunFunctionInTransaction([&] { server.get().Attach(context); });
    } catch (const std::exception& e) {
      SDB_WARN(GENERAL, "failed to re-attach foreign server '",
               server.get().name.GetIdentifierName(), "': ", e.what());
    }
  }
}

}  // namespace sdb::catalog
