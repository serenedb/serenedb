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

#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct DatabaseSettingRow {
  duckdb::idx_t database;
  const duckdb::DatabaseSetting& setting;
};

class PgDbRoleSetting final : public SystemTableScan<kPgDbRoleSettingSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSetSource{
      SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, {}},
    CatalogSetSource{
      SystemCatalog::Cluster, duckdb::CatalogType::DATABASE_ENTRY, {}}};

  static constexpr auto kSetting = Shape<kSql, const catalog::RoleCatalogEntry>(
    Col<"setrole">(&duckdb::CatalogEntry::oid),
    Col<"setconfig">(&catalog::RoleCatalogEntry::Config));

  static constexpr auto kDatabaseSetting =
    Shape<kSql, const DatabaseSettingRow>(
      Col<"setdatabase">([](const auto& row) { return row.database; }),
      Col<"setrole">([](const auto& row) { return row.setting.role; }),
      Col<"setconfig">(
        [](const auto& row) -> const auto& { return row.setting.config; }));

  void Row(const catalog::RoleCatalogEntry& role) {
    if (!role.Config().empty()) {
      Emit<kSetting>(role);
    }
  }

  void Row(const catalog::DatabaseCatalogEntry& database) {
    for (const auto& setting : database.Settings()) {
      Emit<kDatabaseSetting>({database.oid, setting});
    }
  }
};

}  // namespace

SystemTable gPgDbRoleSetting = SystemTableOf<PgDbRoleSetting>();

}  // namespace sdb::pg
