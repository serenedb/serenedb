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
#include <duckdb/main/config.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/storage/storage_extension.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
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
constexpr const char* kRootRole = "postgres";

std::string g_directory;

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
  ClusterStorageExtension() {
    attach = AttachCluster;
    create_transaction_manager = ClusterTransactionManager;
  }
};

void Attach(std::string_view name, std::string_view path,
            std::string_view type, duckdb::AttachVisibility visibility) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  duckdb::AttachInfo info;
  info.name = duckdb::Identifier{name};
  info.path = std::string{path};
  info.options.emplace("type", duckdb::Value{std::string{type}});
  duckdb::AttachOptions options{info.options, duckdb::AccessMode::READ_WRITE};
  options.db_type = std::string{type};
  options.visibility = visibility;
  conn->BeginTransaction();
  try {
    duckdb::DatabaseManager::Get(context).AttachDatabase(context, info,
                                                         options);
  } catch (...) {
    conn->Rollback();
    throw;
  }
  conn->Commit();
}

}  // namespace

void RegisterClusterStorage(duckdb::DBConfig& config) {
  auto ext = duckdb::make_shared_ptr<ClusterStorageExtension>();
  duckdb::StorageExtension::Register(config, ClusterCatalog::kStorageType,
                                     std::move(ext));
}

std::string ClusterFilePath(std::string_view directory) {
  return absl::StrCat(directory, "/", kCatalogDir, "/catalog.db");
}

std::string DatabaseFilePath(std::string_view name) {
  return absl::StrCat(g_directory, "/", kDatabaseDir, "/", name, ".db");
}

void AttachDatabase(std::string_view name) {
  auto& manager =
    duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
  if (manager.GetDatabase(duckdb::Identifier{name})) {
    return;
  }
  Attach(name, DatabaseFilePath(name), SereneDBCatalog::kStorageType,
         duckdb::AttachVisibility::SHOWN);
}

// Runs `mutate` against the cluster catalog in its own committed transaction.
// Writing a CatalogSet directly skips what a DDL statement's binder would have
// done, so the modification has to be declared or the transaction stays
// read-only and duckdb refuses the change.
template<typename Mutate>
void InClusterTransaction(Mutate&& mutate) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto& context = *conn->context;
  auto& cluster = ClusterOf(DuckDBEngine::Instance().instance());
  conn->BeginTransaction();
  try {
    mutate(cluster, cluster.GetCatalogTransaction(context));
  } catch (...) {
    conn->Rollback();
    throw;
  }
  conn->Commit();
}

// A database is two things: an attachment duckdb can route a query to, and a
// record in the cluster catalog. Connection setup resolves the name against
// the record, so an attachment without one is a database nobody can reach.
void RegisterDatabase(std::string_view name) {
  InClusterTransaction([&](ClusterCatalog& cluster,
                           duckdb::CatalogTransaction transaction) {
    CreateDatabaseInfo info;
    info.SetName(duckdb::Identifier{name});
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    cluster.CreateDatabase(transaction, info);
  });
}

void RegisterDatabaseIn(duckdb::ClientContext& context, std::string_view name) {
  auto& cluster = ClusterOf(DuckDBEngine::Instance().instance());
  CreateDatabaseInfo info;
  info.SetName(duckdb::Identifier{name});
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  cluster.CreateDatabase(cluster.GetCatalogTransaction(context), info);
}

// The root role. Login resolves the connecting user against the cluster's role
// set, so without this nobody can connect at all.
void RegisterRootRole(std::string_view name) {
  InClusterTransaction([&](ClusterCatalog& cluster,
                           duckdb::CatalogTransaction transaction) {
    CreateRoleInfo info;
    info.SetName(duckdb::Identifier{name});
    info.options = RoleOption::Superuser | RoleOption::Inherit |
                   RoleOption::CreateRole | RoleOption::CreateDb |
                   RoleOption::Login | RoleOption::Replication |
                   RoleOption::BypassRls;
    info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
    cluster.CreateRole(transaction, info);
  });
}

duckdb::optional_ptr<duckdb::AttachedDatabase> FindAttachedDatabase(
  duckdb::idx_t id) {
  auto& manager =
    duckdb::DatabaseManager::Get(DuckDBEngine::Instance().instance());
  for (auto& attached : manager.GetDatabases()) {
    if (attached->oid == id) {
      return attached.get();
    }
  }
  return nullptr;
}

void InitCatalog(std::string_view directory) {
  g_directory = std::string{directory};
  std::filesystem::create_directories(
    absl::StrCat(directory, "/", kCatalogDir));
  std::filesystem::create_directories(
    absl::StrCat(directory, "/", kDatabaseDir));

  Attach(ClusterCatalog::kDatabaseName, ClusterFilePath(directory),
         ClusterCatalog::kStorageType, duckdb::AttachVisibility::HIDDEN);

  RegisterRootRole(kRootRole);
  RegisterDatabase(kDefaultDatabase);
  AttachDatabase(kDefaultDatabase);
}

void ShutdownCatalog() {
  // The cluster catalog is an ordinary attached database, so the shutdown
  // checkpoint that runs before this already closed it along with every other
  // attachment. Detaching here would close it a second time and destroy the
  // AttachedDatabase out from under that teardown.
  g_directory.clear();
}

}  // namespace sdb::catalog
