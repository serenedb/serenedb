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
#include <string>

#include "catalog/entry/database.h"
#include "catalog/entry/role.h"

namespace sdb::catalog {

class ClusterCatalog final : public duckdb::DuckCatalog {
 public:
  static constexpr const char* kStorageType = "serenedb_cluster";
  static constexpr const char* kDatabaseName = "__cluster__";

  explicit ClusterCatalog(duckdb::AttachedDatabase& db);

  std::string GetCatalogType() override { return kStorageType; }

  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeRoleEntry(
    duckdb::CreateRoleInfo& info) override;
  duckdb::unique_ptr<duckdb::InCatalogEntry> MakeDatabaseEntry(
    duckdb::CreateDatabaseInfo& info) override;

  void FinalizeLoad(
    duckdb::optional_ptr<duckdb::ClientContext> context) override;
  void Alter(duckdb::CatalogTransaction transaction,
             duckdb::AlterInfo& info) override;

  duckdb::CatalogTransaction LoginTransaction() {
    return duckdb::CatalogTransaction{GetDatabase(),
                                      duckdb::TRANSACTION_ID_START - 1,
                                      duckdb::TRANSACTION_ID_START - 1};
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> CreateRole(
    duckdb::CatalogTransaction transaction, duckdb::CreateRoleInfo& info);
  void DropRole(duckdb::CatalogTransaction transaction, duckdb::DropInfo& info);
  duckdb::optional_ptr<duckdb::CatalogEntry> CreateDatabase(
    duckdb::CatalogTransaction transaction, duckdb::CreateDatabaseInfo& info);
  void DropDatabase(duckdb::CatalogTransaction transaction,
                    duckdb::DropInfo& info);
};

ClusterCatalog& ClusterOf(duckdb::ClientContext& context);
ClusterCatalog& ClusterOf(duckdb::DatabaseInstance& db);
ClusterCatalog& ClusterOf();

}  // namespace sdb::catalog
