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

#include "catalog/entry/role.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kDbRoleSettingIndexes[] = {
  {kPgDbRoleSettingSql["setrole"], SystemLookup::Object},
};

class PgDbRoleSetting final : public SystemTableScan<kPgDbRoleSettingSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSetSource{SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY,
                     kDbRoleSettingIndexes}};

  static constexpr auto kSetting = Shape<kSql, const catalog::RoleCatalogEntry>(
    Col<"setrole">(&duckdb::CatalogEntry::oid),
    Col<"setconfig">(&catalog::RoleCatalogEntry::Config));

  void Row(const catalog::RoleCatalogEntry& role) {
    if (!role.Config().empty()) {
      Emit<kSetting>(role);
    }
  }
};

}  // namespace

SystemTable gPgDbRoleSetting = SystemTableOf<PgDbRoleSetting>();

}  // namespace sdb::pg
