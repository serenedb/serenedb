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

#include "auth/acl.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgNamespace::oid),
  GetIndex(&PgNamespace::nspname),
  GetIndex(&PgNamespace::nspowner),
  GetIndex(&PgNamespace::nspacl),
});

void RetrieveObjects(ObjectId database_id, std::vector<PgNamespace>& values,
                     const catalog::Snapshot& snapshot) {
  values.push_back({
    .oid = id::kPgCatalogSchema.id(),
    .nspname = "pg_catalog",
    .nspowner = id::kRootUser.id(),
  });
  values.push_back({
    .oid = id::kPgInformationSchema.id(),
    .nspname = "information_schema",
    .nspowner = id::kRootUser.id(),
  });
  for (const auto& schema : snapshot.GetSchemas(database_id)) {
    values.push_back(PgNamespace{
      .oid = schema->GetId().id(),
      .nspname = schema->GetName(),
      .nspowner = schema->GetOwner().id(),
      .nspacl = {schema->GetAcl()},
    });
  }
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgNamespace>::GetTableData() {
  std::vector<PgNamespace> values;
  auto snapshot = _config.EnsureCatalogSnapshot();
  RetrieveObjects(GetDatabaseId(), values, *snapshot);

  auto result = CreateColumns<PgNamespace>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row,
              *_config.EnsureCatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
