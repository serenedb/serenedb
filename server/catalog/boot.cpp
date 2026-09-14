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

#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <filesystem>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/entry/foreign_server.h"
#include "storage_engine/search_engine.h"

namespace sdb::catalog {
namespace {

constexpr const char* kCatalogDir = "engine_catalog";
constexpr const char* kDatabaseDir = "engine_duckdb";

duckdb::unique_ptr<duckdb::Catalog> AttachCluster(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  return duckdb::make_uniq<ClusterCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> MakeClusterTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<duckdb::DuckTransactionManager>(db);
}

class ClusterStorageExtension final : public duckdb::StorageExtension {
 public:
  explicit ClusterStorageExtension(duckdb::shared_ptr<DataDirectory> layout) {
    attach = AttachCluster;
    create_transaction_manager = MakeClusterTransactionManager;
    storage_info = std::move(layout);
  }
};

const DataDirectory& Layout(duckdb::AttachedDatabase& db) {
  return static_cast<const DataDirectory&>(
    *db.GetStorageExtension()->storage_info);
}

}  // namespace

DataDirectory::DataDirectory(std::string directory_p)
  : directory{std::move(directory_p)} {}

std::string DataDirectory::ClusterFile() const {
  return absl::StrCat(directory, "/", kCatalogDir, "/catalog.db");
}

std::string DataDirectory::DatabaseDir() const {
  return absl::StrCat(directory, "/", kDatabaseDir);
}

std::string DataDirectory::DatabaseFile(duckdb::idx_t oid) const {
  return absl::StrCat(DatabaseDir(), "/", oid, ".db");
}

void Attach(duckdb::ClientContext& context, duckdb::AttachInfo& info,
            std::string_view type, duckdb::AttachVisibility visibility) {
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.db_type = std::string{type};
  options.visibility = visibility;
  duckdb::DatabaseManager::Get(context).AttachDatabase(context, info, options);
}

void RemoveDatabaseFiles(duckdb::AttachedDatabase& cluster, duckdb::idx_t oid) {
  const auto file = Layout(cluster).DatabaseFile(oid);
  std::error_code ec;
  std::filesystem::remove(file, ec);
  std::filesystem::remove(file + ".wal", ec);
  std::filesystem::remove_all(search::GetSearchEngine().GetPersistedPath(oid),
                              ec);
}

void RegisterClusterStorage(duckdb::DBConfig& config,
                            duckdb::shared_ptr<DataDirectory> layout) {
  duckdb::StorageExtension::Register(
    config, ClusterCatalog::kStorageType,
    duckdb::make_shared_ptr<ClusterStorageExtension>(std::move(layout)));
}

void InitCatalog(std::string_view directory) {
  const DataDirectory layout{std::string{directory}};
  std::filesystem::create_directories(
    absl::StrCat(directory, "/", kCatalogDir));
  std::filesystem::create_directories(layout.DatabaseDir());
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  duckdb::AttachInfo cluster_info;
  cluster_info.name = duckdb::Identifier{ClusterCatalog::kDatabaseName};
  cluster_info.path = layout.ClusterFile();
  context.RunFunctionInTransaction([&] {
    Attach(context, cluster_info, ClusterCatalog::kStorageType,
           duckdb::AttachVisibility::HIDDEN);
  });
  auto& instance = DuckDBEngine::Instance().instance();
  auto& cluster = ClusterOf(instance);
  std::vector<duckdb::Identifier> names;
  cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .Scan(cluster.LoginTransaction(),
          [&](duckdb::CatalogEntry& entry) { names.push_back(entry.name); });
  for (const auto& name : names) {
    duckdb::AttachInfo info;
    info.name = name;
    context.RunFunctionInTransaction([&] {
      Attach(context, info, SereneDBCatalog::kStorageType,
             duckdb::AttachVisibility::SHOWN);
    });
  }
  auto& manager = duckdb::DatabaseManager::Get(instance);
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
