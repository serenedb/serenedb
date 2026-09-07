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

#include "catalog1/entry/inverted_index.h"
#include "catalog1/entry/tokenizer.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/pg_types.h"

namespace sdb::pg {

template<>
MaterializedData SystemTableSnapshot<PgOpclass>::GetTableData() {
  std::vector<PgOpclass> values;

  values.push_back({
    .oid = pg::kPgOpclassIvf,
    .opcmethod = pg::kPgAmInverted,
    .opcname = catalog::kIVFKind,
    .opcnamespace = pg::kPgCatalogSchema,
    .opcowner = pg::kRootUser,
    .opcfamily = 0,
    .opcintype = PgTypeOID::kFloat4Array,
    .opcdefault = false,
    .opckeytype = 0,
  });

  values.push_back({
    .oid = pg::kPgOpclassIncluded,
    .opcmethod = pg::kPgAmInverted,
    .opcname = catalog::kIncludedKind,
    .opcnamespace = pg::kPgCatalogSchema,
    .opcowner = pg::kRootUser,
    .opcfamily = 0,
    .opcintype = PgTypeOID::kAny,
    .opcdefault = false,
    .opckeytype = 0,
  });

  VisitEntries<catalog::TokenizerCatalogEntry>(
    _context, GetDatabase(),
    [&](const catalog::TokenizerCatalogEntry& tokenizer) {
      values.push_back({
        .oid = tokenizer.oid,
        .opcmethod = pg::kPgAmInverted,
        // A view into the entry, which outlives the walk: Name is a
        // string_view.
        .opcname = tokenizer.name.GetIdentifierName(),
        .opcnamespace = tokenizer.ParentSchema().oid,
        .opcowner = tokenizer.permissions.owner,
        .opcfamily = 0,
        .opcintype = PgTypeOID::kText,
        .opcdefault = false,
        .opckeytype = 0,
      });
    });

  static constexpr uint64_t kNullMask = 0;
  auto result = CreateColumns<PgOpclass>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
