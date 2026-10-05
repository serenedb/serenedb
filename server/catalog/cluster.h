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
#include <mutex>
#include <string>
#include <string_view>
#include <thread>

#include "auth/role_closure.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"

namespace sdb::catalog {

class ClusterCatalog final : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "sdb_cluster";
  static constexpr const char* kDatabaseName = "sdb_cluster";

  explicit ClusterCatalog(duckdb::AttachedDatabase& db)
    : duckdb::DuckCatalog{db, true} {}
  ~ClusterCatalog() override;

  std::string GetCatalogType() final { return kStorageType; }
  duckdb::idx_t DefaultSchemaOid() const final;

  bool UsesCatalogLog() const final { return true; }
  duckdb::shared_ptr<duckdb::WriteAheadLog> CatalogLog() final {
    std::lock_guard guard{_log_mutex};
    return _catalog_log;
  }
  duckdb::Catalog& ReplayUseCatalog(duckdb::ClientContext& context,
                                    duckdb::idx_t catalog_oid) final;
  void OnCatalogLogPrepared() final;
  void OnCatalogLogDecided() final;
  void BeginCatalogLogCommit() final;
  void EndCatalogLogCommit() final;
  void RequestCatalogLogSync(duckdb::shared_ptr<duckdb::WriteAheadLog> log,
                             duckdb::idx_t offset) final;
  void OpenCatalogLog(duckdb::unique_ptr<duckdb::WriteAheadLog> log,
                      bool compactable);
  void MaybeCompactCatalogLog();

  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeRoleEntry(
    duckdb::CreateRoleInfo& info) final {
    return duckdb::make_uniq<RoleCatalogEntry>(*this, info);
  }
  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeDatabaseEntry(
    duckdb::CreateDatabaseInfo& info) final {
    return duckdb::make_uniq<DatabaseCatalogEntry>(*this, info);
  }

  void Bootstrap(duckdb::ClientContext& context);
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  duckdb::CatalogTransaction LoginTransaction() {
    return duckdb::CatalogTransaction{
      GetDatabase(), duckdb::TRANSACTION_ID_START - 1,
      duckdb::VisibilityBound::Through(
        duckdb::DuckTransactionManager::Get(GetAttached()).GetLastCommit())};
  }

  uint64_t CatalogGeneration() const {
    return _catalog_generation.load(std::memory_order_acquire);
  }
  std::shared_ptr<const auth::RoleGraph> CachedRoles(uint64_t generation) {
    std::lock_guard guard{_roles_mutex};
    return _roles_generation == generation ? _roles : nullptr;
  }
  void CacheRoles(uint64_t generation,
                  std::shared_ptr<const auth::RoleGraph> roles) {
    std::lock_guard guard{_roles_mutex};
    if (generation > _roles_generation) {
      _closures.clear();
    }
    if (generation >= _roles_generation) {
      _roles_generation = generation;
      _roles = std::move(roles);
    }
  }
  std::shared_ptr<const auth::RoleClosure> CachedClosure(uint64_t generation,
                                                         duckdb::idx_t role) {
    std::lock_guard guard{_roles_mutex};
    if (generation != _roles_generation) {
      return nullptr;
    }
    const auto it = _closures.find(role);
    return it == _closures.end() ? nullptr : it->second;
  }
  void CacheClosure(uint64_t generation, duckdb::idx_t role,
                    std::shared_ptr<const auth::RoleClosure> closure) {
    std::lock_guard guard{_roles_mutex};
    if (generation == _roles_generation) {
      _closures.try_emplace(role, std::move(closure));
    }
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateRole(
    duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) final;
  void DropRole(duckdb::CatalogTransaction transaction,
                duckdb::DropInfo& info) final;
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDatabase(
    duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info);
  void DropDatabase(duckdb::CatalogTransaction transaction,
                    duckdb::DropInfo& info) final;

 private:
  void CompactCatalogLog();
  bool HoldsPreparedBatch(duckdb::idx_t oid, duckdb::idx_t generation);
  void SyncCatalogLogLoop();
  bool SyncPending() const ABSL_EXCLUSIVE_LOCKS_REQUIRED(_sync_mutex) {
    return _sync_stop || _sync_log;
  }

  absl::Mutex _sync_mutex;
  duckdb::shared_ptr<duckdb::WriteAheadLog> _sync_log
    ABSL_GUARDED_BY(_sync_mutex);
  duckdb::idx_t _sync_offset ABSL_GUARDED_BY(_sync_mutex) = 0;
  bool _sync_stop ABSL_GUARDED_BY(_sync_mutex) = false;
  std::thread _sync_thread;
  std::mutex _log_mutex;
  duckdb::shared_ptr<duckdb::WriteAheadLog> _catalog_log;
  std::atomic_size_t _commits_in_flight{0};
  std::atomic_uint64_t _catalog_generation{1};
  std::atomic<duckdb::idx_t> _generation_version{0};
  std::mutex _roles_mutex;
  uint64_t _roles_generation = 0;
  std::shared_ptr<const auth::RoleGraph> _roles;
  irs::containers::FlatHashMap<duckdb::idx_t,
                               std::shared_ptr<const auth::RoleClosure>>
    _closures;
  bool _compactable = false;
  std::atomic<duckdb::idx_t> _live_bytes{0};
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
