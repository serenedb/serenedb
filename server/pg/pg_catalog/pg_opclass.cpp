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

#include "pg/pg_catalog/pg_opclass.h"

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_object_entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgOpclass>::GetTableData() {
  std::vector<PgOpclass> values;

  values.push_back({
    .oid = id::kPgOpclassIvf.id(),
    .opcmethod = id::kPgAmInverted.id(),
    .opcname = catalog::kIVFKind,
    .opcnamespace = id::kPgCatalogSchema.id(),
    .opcowner = id::kRootUser.id(),
    .opcfamily = 0,
    .opcintype = PgTypeOID::kFloat4Array,
    .opcdefault = false,
    .opckeytype = 0,
  });

  values.push_back({
    .oid = id::kPgOpclassIncluded.id(),
    .opcmethod = id::kPgAmInverted.id(),
    .opcname = catalog::kIncludedKind,
    .opcnamespace = id::kPgCatalogSchema.id(),
    .opcowner = id::kRootUser.id(),
    .opcfamily = 0,
    .opcintype = PgTypeOID::kAny,
    .opcdefault = false,
    .opckeytype = 0,
  });

  catalog::VisitDefinitions<catalog::SereneDBTokenizerEntry>(
    &_config.GetClientContext(), GetDatabaseId(),
    [&](const catalog::TokenizerRef& tokenizer,
        const catalog::Permissions& perm) {
      values.push_back({
        .oid = tokenizer->GetId().id(),
        .opcmethod = id::kPgAmInverted.id(),
        // A view into the entry's definition, which
        // outlives the walk: Name is a string_view.
        .opcname = tokenizer->GetName(),
        .opcnamespace = tokenizer->GetParentId().id(),
        .opcowner = perm.owner,
        .opcfamily = 0,
        .opcintype = PgTypeOID::kText,
        .opcdefault = false,
        .opckeytype = 0,
      });
    });

  static constexpr uint64_t kNullMask = 0;
  auto result = CreateColumns<PgOpclass>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
