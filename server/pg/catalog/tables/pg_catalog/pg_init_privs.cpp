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

#include <iresearch/utils/static_strings.hpp>

#include "pg/catalog/engine/registry.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

struct InitPrivilege {
  duckdb::idx_t objoid;
  duckdb::idx_t classoid;
  duckdb::Permissions permissions;
};

SystemRows<InitPrivilege> LoadInitPrivileges(SystemScan&) {
  static const auto kPrivileges = [] {
    std::vector<InitPrivilege> privileges;
    VisitSystemTables(
      irs::StaticStrings::kPgCatalogSchema, [&](const SystemTable& table) {
        privileges.emplace_back(table.Sql().oid, kPgClassTable,
                                SystemPermissions(table.SuperuserOnly()));
      });
    VisitSystemViews(
      irs::StaticStrings::kPgCatalogSchema, [&](const StaticView& view) {
        privileges.emplace_back(view.oid, kPgClassTable, view.permissions);
      });
    for (const auto oid : {kPgCatalogSchema, kPgPublicSchema}) {
      privileges.emplace_back(oid, kPgNamespaceTable, SchemaPermissions());
    }
    return privileges;
  }();
  return std::span{kPrivileges};
}

class PgInitPrivs final : public SystemTableScan<kPgInitPrivsSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{
    ArraySource<InitPrivilege>{&LoadInitPrivileges, {}, {}}};

  static constexpr auto kPrivilege = Shape<kSql, const InitPrivilege>(
    Col<"objoid">(&InitPrivilege::objoid),
    Col<"classoid">(&InitPrivilege::classoid),
    Col<"privtype">([](const auto&) { return 'i'; }), Col<"initprivs">(kAcl));

  void Row(const InitPrivilege& privilege) { Emit<kPrivilege>(privilege); }
};

}  // namespace

SystemTable gPgInitPrivs = SystemTableOf<PgInitPrivs>();

}  // namespace sdb::pg
