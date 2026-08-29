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

#include "pg/pg_catalog/pg_rewrite.h"

#include <vector>

#include "catalog/ddl/catalog.h"
#include "catalog/entry/duckdb_view_entry.h"
#include "catalog/read/duckdb_catalog_sets.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgRewrite::ev_qual),
  GetIndex(&PgRewrite::ev_action),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgRewrite>::GetTableData() {
  std::vector<PgRewrite> values;
  catalog::Visit<catalog::SereneDBViewEntry>(
    &_config.GetClientContext(), GetDatabaseId(),
    [&](const duckdb::ViewCatalogEntry& view) {
      values.push_back(PgRewrite{
        Oid{view.oid},
        Name{"_RETURN"},
        Oid{view.oid},
        PgRewrite::EvType::Select,
        PgRewrite::EvEnabled::Origin,
        true,
        {},
        {},
      });
    });

  auto result = CreateColumns<PgRewrite>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, Roles());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
