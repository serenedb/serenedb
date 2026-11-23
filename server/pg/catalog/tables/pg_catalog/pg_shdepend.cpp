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

#include <absl/functional/function_ref.h>

#include <string>

#include "catalog/entry/role.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/acl.h"

namespace sdb::pg {
namespace {

constexpr SystemIndex kIndexes[] = {
  {kPgShdependSql["refobjid"], SystemLookup::Object},
};

struct SharedDependency {
  duckdb::idx_t dbid;
  duckdb::idx_t classid;
  duckdb::idx_t objid;
  int32_t objsubid;
  duckdb::idx_t refobjid;
  char deptype;
};

class PgShdepend final : public SystemTableScan<kPgShdependSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, kIndexes}};

  static constexpr auto kDependency = Shape<kSql, const SharedDependency>(
    Col<"dbid">(&SharedDependency::dbid),
    Col<"classid">(&SharedDependency::classid),
    Col<"objid">(&SharedDependency::objid),
    Col<"objsubid">(&SharedDependency::objsubid),
    Col<"refclassid">([](const auto&) { return kPgAuthidTable; }),
    Col<"refobjid">(&SharedDependency::refobjid),
    Col<"deptype">(&SharedDependency::deptype));

  void Row(catalog::RoleCatalogEntry& role) {
    if (role.oid == kRootUser) {
      return;
    }
    VisitRoleReferences(
      Context(), role.oid, &Role(),
      [&](const RoleReference& reference, absl::FunctionRef<std::string()>) {
        if (reference.classid != kInvalidOid) {
          Emit<kDependency>({reference.dbid, reference.classid, reference.objid,
                             reference.objsubid, role.oid, reference.deptype});
        }
      });
  }
};

}  // namespace

SystemTable gPgShdepend = SystemTableOf<PgShdepend>();

}  // namespace sdb::pg
