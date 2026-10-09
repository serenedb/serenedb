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

#include <duckdb/main/attached_database.hpp>

#include "catalog/entry/database.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/tables/tables.h"

namespace sdb::pg {
namespace {

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

SystemTable gPgDefaultAcl = SystemTableOf<PgDefaultAcl>();

}  // namespace sdb::pg
