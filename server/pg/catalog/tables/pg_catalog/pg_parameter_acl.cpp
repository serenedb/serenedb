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

struct Parameter {
  duckdb::idx_t oid;
  const duckdb::NamedAcl& acl;
};

class PgParameterAcl final : public SystemTableScan<kPgParameterAclSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::ROLE_ENTRY, {}}};

  static constexpr auto kParameter = Shape<kSql, const Parameter>(
    Col<"oid">(&Parameter::oid),
    Col<"parname">([](const auto& row) -> const auto& { return row.acl.name; }),
    Col<"paracl">([](const auto& row) -> const auto& { return row.acl.acl; }));

  void Row(const catalog::RoleCatalogEntry& role) {
    if (role.oid != kRootUser) {
      return;
    }
    duckdb::idx_t oid = 0;
    for (const auto& row : role.permissions.parameters) {
      Emit<kParameter>({++oid, row});
    }
  }
};

}  // namespace

SystemTable gPgParameterAcl = SystemTableOf<PgParameterAcl>();

}  // namespace sdb::pg
