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
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/database_directory.h"
#include "catalog/entry/database.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"
#include "search/wal_recovery.h"

ABSL_FLAG(std::string, missing_database, "refuse",
          "What boot does with a database whose data file is missing: "
          "'refuse' (default) stops the server, 'skip' leaves it unattached, "
          "'drop' removes it from the catalog.");

namespace sdb::catalog {
namespace {

struct MissingDatabases {
  bool fresh_cluster = false;
  std::string policy;
  absl::flat_hash_set<duckdb::idx_t> missing;
  std::vector<duckdb::Identifier> skipped;
  std::vector<duckdb::Identifier> dropped;
};

MissingDatabases gMissingDatabases;

constexpr std::string_view kEngineDir = "engine_v1";
constexpr std::string_view kCatalogLog = "catalog.wal";

std::vector<std::pair<duckdb::idx_t, std::filesystem::path>> OidDirectories(
  const std::filesystem::path& directory) {
  std::vector<std::pair<duckdb::idx_t, std::filesystem::path>> found;
  std::error_code ec;
  for (const auto& entry : std::filesystem::directory_iterator{directory, ec}) {
    const auto name = entry.path().filename().string();
    duckdb::idx_t oid = 0;
    if (entry.is_directory(ec) && absl::c_all_of(name, absl::ascii_isdigit) &&
        absl::SimpleAtoi(name, &oid)) {
      found.emplace_back(oid, entry.path());
    }
  }
  return found;
}

void RemoveUnowned(const std::filesystem::path& directory,
                   const absl::flat_hash_set<duckdb::idx_t>& owned) {
  for (const auto& [oid, path] : OidDirectories(directory)) {
    if (owned.contains(oid)) {
      continue;
    }
    std::error_code ec;
    std::filesystem::remove_all(path, ec);
    if (ec) {
      SDB_WARN(STARTUP, "could not remove '", path.string(),
               "': ", ec.message());
    }
  }
}

template<typename Storage>
void RequirePresent(const Storage& storage, const duckdb::CatalogEntry& entry) {
  if (storage.Absent()) {
    SDB_FATAL(STARTUP, "the directory '", storage.Path().string(), "' of ",
              entry.name.GetIdentifierName(), " (oid ", entry.oid,
              ") is missing");
  }
}

absl::flat_hash_set<duckdb::idx_t> StorageOids(SereneDBCatalog& catalog) {
  std::vector<duckdb::reference<duckdb::SchemaCatalogEntry>> schemas;
  catalog.ScanSchemas(
    [&](duckdb::SchemaCatalogEntry& schema) { schemas.emplace_back(schema); });
  absl::flat_hash_set<duckdb::idx_t> oids;
  for (auto& schema : schemas) {
    schema.get().Scan(
      duckdb::CatalogType::TABLE_ENTRY, [&](duckdb::CatalogEntry& entry) {
        if (const auto* table = dynamic_cast<SearchTableEntry*>(&entry)) {
          RequirePresent(*table->Storage(), entry);
          oids.insert(entry.oid);
        }
      });
    schema.get().Scan(
      duckdb::CatalogType::INDEX_ENTRY, [&](duckdb::CatalogEntry& entry) {
        const auto* index = dynamic_cast<InvertedIndexEntry*>(&entry);
        if (index && index->Storage()) {
          RequirePresent(*index->Storage(), entry);
          oids.insert(entry.oid);
        }
      });
  }
  return oids;
}

void RemoveUnownedDatabases(const DataDirectory& layout,
                            ClusterCatalog& cluster) {
  absl::flat_hash_set<duckdb::idx_t> databases;
  cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster.LoginTransaction(),
          [&](duckdb::CatalogEntry& entry) { databases.insert(entry.oid); });
  RemoveUnowned(layout.EngineDir(), databases);
}

void RemoveUnownedStorages(const DataDirectory& layout,
                           duckdb::DatabaseManager& manager) {
  for (const auto& db : manager.GetDatabases()) {
    auto& catalog = db->GetCatalog();
    if (catalog.GetCatalogType() == SereneDBCatalog::kStorageType &&
        !catalog.InMemory()) {
      RemoveUnowned(layout.DatabaseDir(db->oid),
                    StorageOids(catalog.Cast<SereneDBCatalog>()));
    }
  }
}

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

std::filesystem::path DataDirectory::EngineDir() const {
  return std::filesystem::path{directory} / kEngineDir;
}

std::string DataDirectory::CatalogLogFile() const {
  return (EngineDir() / kCatalogLog).string();
}

std::filesystem::path DataDirectory::DatabaseDir(duckdb::idx_t oid) const {
  return EngineDir() / absl::StrCat(oid);
}

std::string DataDirectory::DatabaseFile(duckdb::idx_t oid) const {
  return (DatabaseDir(oid) / kDataFile).string();
}

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AttachVisibility visibility,
            bool defer_storage_load) {
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.db_type.assign(type);
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
  } else if (const auto entry =
               cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                 .GetEntry(cluster.GetCatalogTransaction(context), name)) {
    for (const auto& [key, value] :
         entry->Cast<DatabaseCatalogEntry>().Options()) {
      info.options.emplace(key, value);
    }
  }
  Attach(context, info, SereneDBCatalog::kStorageType, visibility, true);
  return manager.GetDatabase(name)->GetCatalog();
}

const DataDirectory& ClusterLayout(duckdb::AttachedDatabase& cluster) {
  return Layout(cluster);
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
  if (std::filesystem::create_directories(layout.EngineDir())) {
    SyncDirectory(layout.EngineDir().parent_path());
  }
  auto conn = irs::DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  const auto log_path = layout.CatalogLogFile();
  std::error_code log_ec;
  gMissingDatabases.fresh_cluster = !std::filesystem::exists(log_path, log_ec);
  if (gMissingDatabases.fresh_cluster &&
      absl::c_any_of(OidDirectories(layout.EngineDir()), [](const auto& dir) {
        std::error_code ec;
        return !std::filesystem::is_empty(dir.second, ec);
      })) {
    SDB_FATAL(STARTUP, "'", log_path, "' does not exist, but '",
              layout.EngineDir().string(),
              "' holds database directories: refusing to start without the "
              "catalog that owns them");
  }
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
  auto& manager = duckdb::DatabaseManager::Get(instance);
  for (const auto& db : manager.GetDatabases()) {
    if (db->GetCatalog().GetCatalogType() == SereneDBCatalog::kStorageType &&
        !ReadDatabase(db->GetName().GetIdentifierName(),
                      [](const DatabaseCatalogEntry&) {})) {
      manager.DetachDatabase(context, db->GetName(),
                             duckdb::OnEntryNotFound::RETURN_NULL);
    }
  }
  RemoveUnownedDatabases(layout, cluster);
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
  RemoveUnownedStorages(layout, manager);
  for (const auto& db : manager.GetDatabases()) {
    if (db->GetCatalog().GetCatalogType() == SereneDBCatalog::kStorageType) {
      db->GetStorageManager().FinishLoad(context);
    }
  }
  search::InitInvertedIndexes();
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
