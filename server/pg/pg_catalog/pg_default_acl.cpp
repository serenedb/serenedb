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

#include "pg/pg_catalog/pg_default_acl.h"

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>

#include "catalog/cluster.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

PgDefaultAcl::Defaclobjtype ObjType(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return PgDefaultAcl::Defaclobjtype::Sequence;
    case duckdb::CatalogType::MACRO_ENTRY:
    case duckdb::CatalogType::TABLE_MACRO_ENTRY:
      return PgDefaultAcl::Defaclobjtype::Function;
    case duckdb::CatalogType::TYPE_ENTRY:
      return PgDefaultAcl::Defaclobjtype::Type;
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return PgDefaultAcl::Defaclobjtype::Schema;
    default:
      return PgDefaultAcl::Defaclobjtype::Relation;
  }
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgDefaultAcl>::GetTableData() {
  std::vector<PgDefaultAcl> values;
  uint64_t oid = 1;
  auto& context = _context;
  std::vector<duckdb::idx_t> schemas;
  VisitSchemas(context, GetDatabase(), [&](duckdb::SchemaCatalogEntry& schema) {
    schemas.push_back(schema.oid);
  });
  auto& cluster = catalog::ClusterOf(context);
  auto database = cluster.GetCatalogSet(duckdb::CatalogType::DATABASE_ENTRY)
                    .GetEntry(cluster.GetCatalogTransaction(context),
                              GetDatabase().GetName());
  if (database) {
    for (const auto& entry : database->permissions.defaults) {
      if (entry.scope != kInvalidOid &&
          !std::ranges::contains(schemas, entry.scope)) {
        continue;
      }
      values.push_back(PgDefaultAcl{
        .oid = oid++,
        .defaclrole = entry.role,
        .defaclnamespace = entry.scope,
        .defaclobjtype = ObjType(entry.objtype),
        .defaclacl = {entry.acl},
      });
    }
  }

  auto result = CreateColumns<PgDefaultAcl>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], 0, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
