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
#include <duckdb/catalog/duck_catalog.hpp>
#include <functional>
#include <string>

#include "catalog1/entry/database.h"
#include "catalog1/entry/role.h"

namespace sdb::catalog {

// The cluster-wide catalog: the durable home of roles and of the database
// list. It is an ordinary attached database with real storage, because a
// storage-less non-system attachment cannot commit -- stock
// DuckTransactionManager dereferences GetStorageManager() unconditionally on
// the read-write commit path.
class ClusterCatalog final : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "serenedb_cluster";
  static constexpr const char* kDatabaseName = "__cluster__";

  explicit ClusterCatalog(duckdb::AttachedDatabase& db);

  std::string GetCatalogType() override { return kStorageType; }

  void Initialize(bool load_builtin) override;

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateRole(
    duckdb::CatalogTransaction transaction, CreateRoleInfo& info);
  bool DropRole(duckdb::CatalogTransaction transaction,
                const duckdb::Identifier& name, bool cascade);
  duckdb::optional_ptr<duckdb::CatalogEntry> LookupRole(
    duckdb::CatalogTransaction transaction, const duckdb::Identifier& name);
  void ScanRoles(duckdb::CatalogTransaction transaction,
                 const std::function<void(duckdb::CatalogEntry&)>& callback);

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDatabase(
    duckdb::CatalogTransaction transaction, CreateDatabaseInfo& info);
  bool DropDatabase(duckdb::CatalogTransaction transaction,
                    const duckdb::Identifier& name, bool cascade);
  duckdb::optional_ptr<duckdb::CatalogEntry> LookupDatabase(
    duckdb::CatalogTransaction transaction, const duckdb::Identifier& name);
  void ScanDatabases(
    duckdb::CatalogTransaction transaction,
    const std::function<void(duckdb::CatalogEntry&)>& callback);

 private:
  duckdb::CatalogSet _roles;
  duckdb::CatalogSet _databases;
};

ClusterCatalog& ClusterOf(duckdb::ClientContext& context);
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db);
ClusterCatalog& ClusterOf();


}  // namespace sdb::catalog
