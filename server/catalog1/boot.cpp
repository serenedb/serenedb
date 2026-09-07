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

#include "catalog1/boot.h"

#include <absl/strings/str_cat.h>

#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <filesystem>
#include <utility>

#include "basics/duckdb_engine.h"
#include "catalog1/catalog.h"
#include "catalog1/cluster.h"

namespace sdb::catalog {
namespace {

constexpr const char* kCatalogDir = "engine_catalog";
constexpr const char* kDatabaseDir = "engine_duckdb";
constexpr const char* kDefaultDatabase = "postgres";

duckdb::unique_ptr<duckdb::Catalog> AttachCluster(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::ClientContext& context, duckdb::AttachedDatabase& db,
  const duckdb::string& name, duckdb::AttachInfo& info,
  duckdb::AttachOptions& options) {
  return duckdb::make_uniq<ClusterCatalog>(db);
}

duckdb::unique_ptr<duckdb::TransactionManager> ClusterTransactionManager(
  duckdb::optional_ptr<duckdb::StorageExtensionInfo> storage_info,
  duckdb::AttachedDatabase& db, duckdb::Catalog& catalog) {
  return duckdb::make_uniq<duckdb::DuckTransactionManager>(db);
}

class ClusterStorageExtension final : public duckdb::StorageExtension {
 public:
  explicit ClusterStorageExtension(duckdb::shared_ptr<DataDirectory> layout) {
    attach = AttachCluster;
    create_transaction_manager = ClusterTransactionManager;
    storage_info = std::move(layout);
  }
};

void Attach(std::string_view name, std::string path, std::string_view type,
            duckdb::AttachVisibility visibility) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  duckdb::AttachInfo info;
  info.name = duckdb::Identifier{name};
  info.path = std::move(path);
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.db_type = std::string{type};
  options.visibility = visibility;
  context.RunFunctionInTransaction([&] {
    duckdb::DatabaseManager::Get(context).AttachDatabase(context, info,
                                                         options);
  });
}

}  // namespace

DataDirectory::DataDirectory(std::string directory_p)
  : directory{std::move(directory_p)} {}

std::string DataDirectory::ClusterFile() const {
  return absl::StrCat(directory, "/", kCatalogDir, "/catalog.db");
}

std::string DataDirectory::DatabaseFile(std::string_view name) const {
  return absl::StrCat(directory, "/", kDatabaseDir, "/", name, ".db");
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
  std::filesystem::create_directories(
    absl::StrCat(directory, "/", kDatabaseDir));
  Attach(ClusterCatalog::kDatabaseName, layout.ClusterFile(),
         ClusterCatalog::kStorageType, duckdb::AttachVisibility::HIDDEN);
  Attach(kDefaultDatabase, layout.DatabaseFile(kDefaultDatabase),
         SereneDBCatalog::kStorageType, duckdb::AttachVisibility::SHOWN);
}

}  // namespace sdb::catalog
