////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "pg/pg_catalog/pg_namespace.h"

#include <duckdb/catalog/catalog_entry/schema_catalog_entry.hpp>

#include "basics/assert.h"
#include "catalog1/lookup.h"
#include "pg/pg_types.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgNamespace::oid),
  GetIndex(&PgNamespace::nspname),
  GetIndex(&PgNamespace::nspowner),
  GetIndex(&PgNamespace::nspacl),
});

// The name and the ACL of every row are views into the entry the walk read
// them off, which the rows written after it still point at: an entry version
// stays in its set's chain for as long as a transaction can see it.
void RetrieveObjects(duckdb::ClientContext& context, duckdb::Catalog& database,
                     std::vector<PgNamespace>& values) {
  values.push_back({
    .oid = pg::kPgCatalogSchema,
    .nspname = "pg_catalog",
    .nspowner = pg::kRootUser,
  });
  values.push_back({
    .oid = pg::kPgInformationSchema,
    .nspname = "information_schema",
    .nspowner = pg::kRootUser,
  });
  database.ScanSchemas(context, [&](duckdb::SchemaCatalogEntry& schema) {
    values.push_back(PgNamespace{
      .oid = schema.oid,
      .nspname = schema.name.GetIdentifierName(),
      .nspowner = schema.permissions.owner,
      .nspacl = {catalog::AclView{schema.permissions.acl}},
    });
  });
}

}  // namespace

template<>
MaterializedData SystemTableSnapshot<PgNamespace>::GetTableData() {
  std::vector<PgNamespace> values;
  RetrieveObjects(_config.GetClientContext(), GetDatabase(), values);

  auto result = CreateColumns<PgNamespace>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
