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

#pragma once

#include <absl/base/thread_annotations.h>
#include <absl/container/flat_hash_map.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <atomic>
#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

#include "auth/role_closure.h"
#include "catalog/database_directory.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "catalog/entry/tablespace.h"

namespace sdb::catalog {

class ClusterCatalog final : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "sdb_cluster";
  static constexpr const char* kDatabaseName = "sdb_cluster";

  explicit ClusterCatalog(duckdb::AttachedDatabase& db)
    : duckdb::DuckCatalog{db, true} {}

  std::string GetCatalogType() final { return kStorageType; }
  duckdb::idx_t DefaultSchemaOid() const final;
  void Initialize(bool load_builtin) final;

  bool UsesCatalogLog() const final { return true; }
  duckdb::shared_ptr<duckdb::WriteAheadLog> CatalogLog() final {
    std::lock_guard guard{_log_mutex};
    return _catalog_log;
  }
  duckdb::Catalog& ReplayUseCatalog(duckdb::ClientContext& context,
                                    duckdb::idx_t catalog_oid) final;
  void OnCatalogLogPrepared() final;
  void OnCatalogLogDecided() final;
  void EndCatalogLogCommit() final;
  void OpenCatalogLog(duckdb::unique_ptr<duckdb::WriteAheadLog> log,
                      bool compactable);
  void MaybeCompactCatalogLog();

  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeRoleEntry(
    duckdb::CreateRoleInfo& info) final;
  bool IsPinnedEntry(const duckdb::CatalogEntry& entry) const final;
  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeDatabaseEntry(
    duckdb::CreateDatabaseInfo& info) final;
  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeTablespaceEntry(
    duckdb::CreateTablespaceInfo& info) final;

  void Bootstrap(duckdb::ClientContext& context);
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  duckdb::CatalogTransaction LoginTransaction() {
    return duckdb::CatalogTransaction{
      GetDatabase(), duckdb::TRANSACTION_ID_START - 1,
      duckdb::VisibilityBound::Through(
        duckdb::DuckTransactionManager::Get(GetAttached()).GetLastCommit())};
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateRole(
    duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) final;
  void DropRole(duckdb::CatalogTransaction transaction,
                duckdb::DropInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDatabase(
    duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info);
  void DropDatabase(duckdb::CatalogTransaction transaction,
                    duckdb::DropInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateTablespace(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateTablespaceInfo& info) final;
  void DropTablespace(duckdb::CatalogTransaction transaction,
                      duckdb::DropInfo& info) final;

  std::shared_ptr<TablespaceRoot> TablespaceRootOf(duckdb::idx_t oid);
  std::shared_ptr<DatabaseDirectory> MakeDatabaseDirectory(
    duckdb::idx_t oid, duckdb::idx_t tablespace);

  auth::RoleCache& Roles() noexcept { return _roles; }

 private:
  void CompactCatalogLog();
  bool HoldsPreparedBatch(duckdb::idx_t oid, duckdb::idx_t generation);
  bool HoldsTablespaceObjects(duckdb::idx_t tablespace);

  struct KnownRoot {
    std::weak_ptr<TablespaceRoot> root;
    std::weak_ptr<const std::string> path;
  };

  absl::Mutex _tablespace_creation_mutex;
  absl::Mutex _tablespaces_mutex;
  absl::flat_hash_map<duckdb::idx_t, KnownRoot> _tablespace_roots
    ABSL_GUARDED_BY(_tablespaces_mutex);
  std::mutex _log_mutex;
  duckdb::shared_ptr<duckdb::WriteAheadLog> _catalog_log;
  bool _compactable = false;
  std::atomic<duckdb::idx_t> _live_bytes{0};
  auth::RoleCache _roles;
};

ClusterCatalog& ClusterOf(duckdb::ClientContext& context);
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db);
ClusterCatalog& ClusterOf();

inline bool ReadDatabase(
  std::string_view name,
  absl::FunctionRef<void(const DatabaseCatalogEntry&)> read) {
  auto& cluster = ClusterOf();
  return cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .ReadEntry(cluster.LoginTransaction(), duckdb::Identifier{name},
               [&](duckdb::CatalogEntry& entry) {
                 read(entry.Cast<DatabaseCatalogEntry>());
               });
}

inline bool ReadRole(std::string_view name,
                     absl::FunctionRef<void(const RoleCatalogEntry&)> read) {
  auto& cluster = ClusterOf();
  return cluster.GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
    .ReadEntry(cluster.LoginTransaction(), duckdb::Identifier{name},
               [&](duckdb::CatalogEntry& entry) {
                 read(entry.Cast<RoleCatalogEntry>());
               });
}

}  // namespace sdb::catalog
