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

#include <duckdb/catalog/catalog_transaction.hpp>
#include <duckdb/common/constants.hpp>
#include <duckdb/parser/parsed_data/create_replication_origin_info.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/base_catalog.h"
#include "catalog/boot.h"
#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "catalog/snapshot.h"
#include "catalog/transaction.h"

namespace sdb::catalog {

class ClusterCatalog final : public BaseCatalog {
 public:
  ClusterCatalog(duckdb::AttachedDatabase& db, DataDirectory layout)
    : BaseCatalog{db, false},
      _layout{std::move(layout)},
      _roles{true, _oids},
      _databases{true, _oids},
      _origins{true, _oids},
      _temporary_secrets{true, _oids},
      _persistent_secrets{true, _oids} {}

  void Initialize(bool load_builtin) final;

  duckdb::optional<duckdb::Identifier> GetDefaultSchema() const final {
    return duckdb::Identifier{irs::StaticStrings::kPgCatalogSchema};
  }

  std::string GetCatalogType() final { return "cluster"; }
  bool InMemory() final { return true; }
  std::string GetDBPath() final { return {}; }
  bool Durable() const final { return true; }
  duckdb::idx_t RecordId() const final { return 0; }

  const DataDirectory& Layout() const noexcept { return _layout; }

  EntrySet& Set(duckdb::CatalogType type) final;

  void Bootstrap(duckdb::ClientContext& context, bool default_database);

  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateRole(
    duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info) final;
  void DropRole(duckdb::CatalogTransaction transaction,
                duckdb::DropInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDatabase(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateDatabaseInfo& info) final;
  void DropDatabase(duckdb::CatalogTransaction transaction,
                    duckdb::DropInfo& info) final;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateReplicationOrigin(
    duckdb::CatalogTransaction transaction,
    duckdb::CreateReplicationOriginInfo& info);
  void DropReplicationOrigin(duckdb::CatalogTransaction transaction,
                             duckdb::DropInfo& info);

  void ReplayCreate(duckdb::ClientContext& context,
                    duckdb::unique_ptr<duckdb::CreateInfo> info) final;
  void ReplayDrop(duckdb::ClientContext& context,
                  duckdb::CatalogEntry& entry) final;

  void OnCommit() final;

  auth::RoleCache& Roles() noexcept { return _role_cache; }
  CatalogSnapshots& Snapshots() noexcept { return _snapshots; }
  EntrySet& Secrets(bool persistent) noexcept {
    return persistent ? _persistent_secrets : _temporary_secrets;
  }

 private:
  DataDirectory _layout;
  EntrySet _roles;
  EntrySet _databases;
  EntrySet _origins;
  EntrySet _temporary_secrets;
  EntrySet _persistent_secrets;
  auth::RoleCache _role_cache;
  CatalogSnapshots _snapshots{*this};
};

ClusterCatalog& ClusterOf(duckdb::ClientContext& context);
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db);
ClusterCatalog& ClusterOf();

inline bool ReadDatabase(
  std::string_view name,
  absl::FunctionRef<void(const DatabaseCatalogEntry&)> read) {
  auto& cluster = ClusterOf();
  const CommittedView view;
  const auto entry =
    cluster.Set(duckdb::CatalogType::DATABASE_ENTRY)
      .Get(view.Transaction(cluster.GetDatabase()), duckdb::Identifier{name});
  if (!entry) {
    return false;
  }
  read(entry->Cast<DatabaseCatalogEntry>());
  return true;
}

inline bool ReadRole(std::string_view name,
                     absl::FunctionRef<void(const RoleCatalogEntry&)> read) {
  auto& cluster = ClusterOf();
  const CommittedView view;
  const auto entry =
    cluster.Set(duckdb::CatalogType::ROLE_ENTRY)
      .Get(view.Transaction(cluster.GetDatabase()), duckdb::Identifier{name});
  if (!entry) {
    return false;
  }
  read(entry->Cast<RoleCatalogEntry>());
  return true;
}

}  // namespace sdb::catalog
