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

#include "pg/pg_catalog/pg_attrdef.h"

#include <deque>
#include <string>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_table_entry.h"

namespace sdb::pg {

template<>
catalog::MaterializedData SystemTableSnapshot<PgAttrdef>::GetTableData() {
  std::vector<PgAttrdef> values;
  // The rendered expressions the rows point at. A deque, because a row holds a
  // view into one and a vector would move them as it grows.
  std::deque<std::string> adbin_storage;
  connector::VisitTableEntries(
    _config.GetClientContext(), GetDatabaseId(),
    [&](const catalog::CreateSchemaInfo&,
        const connector::SereneDBTableEntry& table) {
      for (const auto& col : table.GetColumns().Logical()) {
        // A generation expression lives here too, as postgres records it.
        if (!col.HasDefaultValue() && !col.Generated()) {
          continue;
        }
        adbin_storage.push_back(col.Generated()
                                  ? col.GeneratedExpression().ToString()
                                  : col.DefaultValue().ToString());
        values.push_back(PgAttrdef{
          Oid{col.CatalogOid()},
          Oid{catalog::IdOf(table).id()},
          static_cast<int16_t>(col.Logical().index + 1),
          adbin_storage.back(),
        });
      }
    });

  auto result = CreateColumns<PgAttrdef>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], /*null_mask=*/0, row,
              *sdb::auth::RolesOf(&_config.GetClientContext()));
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
