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
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kAuthidIndexes[] = {
  {kPgAuthidSql["oid"], SystemLookup::Object},
  {kPgAuthidSql["rolname"], SystemLookup::Object},
};

template<catalog::RoleOption Option>
constexpr auto kRoleOption = [](const catalog::RoleCatalogEntry& role) {
  return HasOption(role.Options(), Option);
};

class PgAuthid final : public SystemTableScan<kPgAuthidSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, kAuthidIndexes}};

  static constexpr auto kRole = Shape<kSql, const catalog::RoleCatalogEntry>(
    Col<"oid">(&duckdb::CatalogEntry::oid),
    Col<"rolname">(&duckdb::CatalogEntry::name),
    Col<"rolsuper">(&catalog::RoleCatalogEntry::IsSuperuser),
    Col<"rolinherit">(kRoleOption<catalog::RoleOption::Inherit>),
    Col<"rolcreaterole">(kRoleOption<catalog::RoleOption::CreateRole>),
    Col<"rolcreatedb">(kRoleOption<catalog::RoleOption::CreateDb>),
    Col<"rolcanlogin">(&catalog::RoleCatalogEntry::CanLogin),
    Col<"rolreplication">(kRoleOption<catalog::RoleOption::Replication>),
    Col<"rolbypassrls">(kRoleOption<catalog::RoleOption::BypassRls>),
    Col<"rolconnlimit">(&catalog::RoleCatalogEntry::ConnLimit),
    Col<"rolpassword">([](const auto& role) -> std::optional<std::string_view> {
      if (role.Password().empty()) {
        return std::nullopt;
      }
      return role.Password();
    }),
    Col<"rolvaliduntil">(
      [](const auto& role) -> std::optional<duckdb::timestamp_tz_t> {
        if (!role.HasValidUntil()) {
          return std::nullopt;
        }
        return duckdb::timestamp_tz_t{role.ValidUntil()};
      }));

  void Row(const catalog::RoleCatalogEntry& role) { Emit<kRole>(role); }
};

}  // namespace

SystemTable gPgAuthid = SystemTableOf<PgAuthid>();

}  // namespace sdb::pg
