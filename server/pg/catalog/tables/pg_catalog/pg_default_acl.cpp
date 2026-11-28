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
#include "pg/catalog/tables/tables.h"
#include "pg/catalog/utils/acl.h"

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

class PgDefaultAcl final : public SystemTableScan<kPgDefaultAclSql> {
 public:
  using SystemTableScan::SystemTableScan;

  static constexpr std::tuple kSources{CatalogSetSource{
    SystemCatalog::Cluster, duckdb::CatalogType::DATABASE_ENTRY, {}}};

  static constexpr auto kDefaultAcl = Shape<kSql, const duckdb::DefaultAcl>(
    Col<"oid">(&duckdb::DefaultAcl::oid),
    Col<"defaclrole">(&duckdb::DefaultAcl::role),
    Col<"defaclnamespace">(&duckdb::DefaultAcl::scope),
    Col<"defaclobjtype">(
      [](const auto& row) { return DefaultAclType(row.objtype); }),
    Col<"defaclacl">(&duckdb::DefaultAcl::acl));

  void Row(const catalog::DatabaseCatalogEntry& database) {
    if (database.oid != Database().GetAttached().oid) {
      return;
    }
    VisitDefaultAcls(Context(), database, [&](const duckdb::DefaultAcl& row) {
      Emit<kDefaultAcl>(row);
    });
  }
};

}  // namespace

SystemTable gPgDefaultAcl = SystemTableOf<PgDefaultAcl>();

}  // namespace sdb::pg
