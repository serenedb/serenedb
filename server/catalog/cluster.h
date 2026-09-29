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

#include <duckdb/catalog/catalog_set.hpp>
#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/duck_transaction_manager.hpp>
#include <filesystem>
#include <mutex>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "catalog/entry/database.h"
#include "catalog/entry/role.h"

namespace sdb::catalog {

class ClusterCatalog final : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "sdb_cluster";
  static constexpr const char* kDatabaseName = "sdb_cluster";

  explicit ClusterCatalog(duckdb::AttachedDatabase& db)
    : duckdb::DuckCatalog{db, true} {}

  std::string GetCatalogType() final { return kStorageType; }

  bool UsesCatalogLog() const final { return true; }
  duckdb::optional_ptr<duckdb::WriteAheadLog> CatalogLog() final {
    return _catalog_log.get();
  }
  duckdb::Catalog& ReplayUseCatalog(duckdb::ClientContext& context,
                                    duckdb::idx_t catalog_oid) final;
  void OnCatalogLogPrepared() final;
  void OnCatalogLogDecided() final;
  void OpenCatalogLog(duckdb::unique_ptr<duckdb::WriteAheadLog> log,
                      bool compactable);
  void MaybeCompactCatalogLog();

  void LogArtifact(duckdb::CatalogType type, duckdb::idx_t catalog_oid,
                   duckdb::idx_t oid,
                   const std::vector<std::filesystem::path>& paths, bool drop);
  void NoteDroppedArtifact(duckdb::CatalogType type, duckdb::idx_t catalog_oid,
                           duckdb::idx_t oid,
                           const std::vector<std::filesystem::path>& paths);
  void ReplayArtifact(duckdb::CatalogType type, duckdb::idx_t catalog_oid,
                      duckdb::idx_t oid,
                      duckdb::vector<std::string> paths) final;
  void ResolveArtifacts();

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
      duckdb::DuckTransactionManager::Get(GetAttached()).GetLastCommit() + 1};
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateRole(
    duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info);
  void DropRole(duckdb::CatalogTransaction transaction, duckdb::DropInfo& info);
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDatabase(
    duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info);
  void DropDatabase(duckdb::CatalogTransaction transaction,
                    duckdb::DropInfo& info);

 private:
  struct Artifact {
    duckdb::CatalogType type;
    duckdb::idx_t catalog_oid;
    duckdb::idx_t oid;
    duckdb::vector<std::string> paths;
    bool drop;
  };

  void CompactCatalogLog();
  bool IsLive(const Artifact& artifact);
  bool HoldsPreparedBatch(duckdb::idx_t oid, duckdb::idx_t generation);

  duckdb::unique_ptr<duckdb::WriteAheadLog> _catalog_log;
  bool _compactable = false;
  duckdb::idx_t _live_bytes = 0;
  std::mutex _artifacts_mutex;
  std::vector<Artifact> _artifacts;
  std::unordered_set<duckdb::idx_t> _replayed_drops;
};

ClusterCatalog& ClusterOf(duckdb::ClientContext& context);
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db);
ClusterCatalog& ClusterOf();

inline duckdb::optional_ptr<duckdb::CatalogEntry> FindDatabase(
  std::string_view name) {
  auto& cluster = ClusterOf();
  return cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
    .GetEntry(cluster.LoginTransaction(), duckdb::Identifier{name});
}

}  // namespace sdb::catalog
