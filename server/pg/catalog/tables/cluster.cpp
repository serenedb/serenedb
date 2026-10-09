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

#include <duckdb/common/types/timestamp.hpp>
#include <duckdb/main/attached_database.hpp>

#include "catalog/entry/database.h"
#include "catalog/entry/role.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kDatabaseIndexes[] = {
  {kPgDatabaseSql["oid"], SystemLookup::Object},
  {kPgDatabaseSql["datname"], SystemLookup::Object},
};

constexpr SystemIndex kAuthidIndexes[] = {
  {kPgAuthidSql["oid"], SystemLookup::Object},
  {kPgAuthidSql["rolname"], SystemLookup::Object},
};

constexpr SystemIndex kDbRoleSettingIndexes[] = {
  {kPgDbRoleSettingSql["setrole"], SystemLookup::Object},
};

class PgDatabase final : public SystemTableScan<kPgDatabaseSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    CatalogSetSource{SystemCatalog::Cluster,
                     duckdb::CatalogType::DATABASE_ENTRY, kDatabaseIndexes}};

  static constexpr auto kDatabase =
    Shape<kSql, const catalog::DatabaseCatalogEntry>(
      Col<"oid">(&duckdb::CatalogEntry::oid),
      Col<"datname">(&duckdb::CatalogEntry::name),
      Col<"datdba">(
        [](const auto& database) { return database.permissions.owner; }),
      Col<"datacl">([](const auto& database) -> const auto& {
        return database.permissions.acl;
      }));

  void Row(const catalog::DatabaseCatalogEntry& database) {
    Emit<kDatabase>(database);
  }
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

struct AuthMember {
  const catalog::RoleCatalogEntry& role;
  const duckdb::Membership& edge;
  int64_t oid;
};

class PgAuthMembers final : public SystemTableScan<kPgAuthMembersSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, {}}};

  static constexpr auto kMember = Shape<kSql, const AuthMember>(
    Col<"oid">(&AuthMember::oid),
    Col<"roleid">([](const auto& row) { return row.edge.role; }),
    Col<"member">([](const auto& row) { return row.role.oid; }),
    Col<"grantor">([](const auto& row) { return row.edge.grantor; }),
    Col<"admin_option">([](const auto& row) { return row.edge.admin_option; }),
    Col<"inherit_option">(
      [](const auto& row) { return row.edge.inherit_option; }),
    Col<"set_option">([](const auto& row) { return row.edge.set_option; }));

  void Row(const catalog::RoleCatalogEntry& role) {
    for (const auto& edge : role.MemberOf()) {
      Emit<kMember>({role, edge, _oid++});
    }
  }

 private:
  int64_t _oid = 1;
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

char DefaultAclType(duckdb::CatalogType type) {
  using enum duckdb::CatalogType;
  if (type == SEQUENCE_ENTRY) {
    return 'S';
  }
  if (type == MACRO_ENTRY) {
    return 'f';
  }
  if (type == TYPE_ENTRY) {
    return 'T';
  }
  if (type == SCHEMA_ENTRY) {
    return 'n';
  }
  return 'r';
}

struct DefaultAcl {
  duckdb::idx_t oid;
  const duckdb::DefaultAcl& acl;
};

class PgDefaultAcl final : public SystemTableScan<kPgDefaultAclSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::DATABASE_ENTRY, {}}};

  static constexpr auto kDefaultAcl = Shape<kSql, const DefaultAcl>(
    Col<"oid">(&DefaultAcl::oid),
    Col<"defaclrole">([](const auto& row) { return row.acl.role; }),
    Col<"defaclnamespace">([](const auto& row) { return row.acl.scope; }),
    Col<"defaclobjtype">(
      [](const auto& row) { return DefaultAclType(row.acl.objtype); }),
    Col<"defaclacl">(
      [](const auto& row) -> const auto& { return row.acl.acl; }));

  void Row(const catalog::DatabaseCatalogEntry& database) {
    if (database.oid != Database().GetAttached().oid) {
      return;
    }
    VisitDefaultAcls(Context(), database,
                     [&](duckdb::idx_t oid, const duckdb::DefaultAcl& row) {
                       Emit<kDefaultAcl>({oid, row});
                     });
  }
};

}  // namespace

SystemTable gPgDatabase = SystemTableOf<PgDatabase>();

SystemTable gPgAuthid = SystemTableOf<PgAuthid>();

SystemTable gPgAuthMembers = SystemTableOf<PgAuthMembers>();

SystemTable gPgDbRoleSetting = SystemTableOf<PgDbRoleSetting>();

SystemTable gPgDefaultAcl = SystemTableOf<PgDefaultAcl>();

}  // namespace sdb::pg
