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

#include <deque>
#include <memory>

#include "auth/role_closure.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/schema.h"
#include "connector/duckdb_catalog_sets.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgNamespace::oid),
  GetIndex(&PgNamespace::nspname),
  GetIndex(&PgNamespace::nspowner),
  GetIndex(&PgNamespace::nspacl),
});

// A schema entry replaces its definition-and-permissions cell whole on an owner
// or ACL change, and the visitor is handed a version it does not own -- while
// the name and the ACL of every row here are views into one. The rows are
// written after the walk, so each version this projection read has to be kept
// until then; another session's GRANT ON SCHEMA otherwise drops the last holder
// mid-walk and the row renders freed memory (an ACL item whose privilege bits
// come out empty).
void RetrieveObjects(duckdb::ClientContext& context, ObjectId database_id,
                     std::vector<PgNamespace>& values,
                     std::deque<catalog::HeldSchema>& kept) {
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
  connector::VisitSchemas(
    &context, database_id,
    [&](const catalog::CreateSchemaInfo& schema,
        const catalog::Permissions& perm) {
      const auto& held = kept.emplace_back(catalog::HeldSchema{
        std::static_pointer_cast<const catalog::CreateSchemaInfo>(
          schema.CloneSchema()),
        perm});
      values.push_back(PgNamespace{
        .oid = held.first->GetId().id(),
        .nspname = held.first->GetName(),
        .nspowner = held.second.owner,
        .nspacl = {catalog::AclView{held.second.acl}},
      });
    });
}

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgNamespace>::GetTableData() {
  std::vector<PgNamespace> values;
  // A deque, not a vector: the rows hold spans into these, so their addresses
  // have to survive the next push.
  std::deque<catalog::HeldSchema> kept;
  RetrieveObjects(_config.GetClientContext(), GetDatabaseId(), values, kept);

  auto result = CreateColumns<PgNamespace>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
