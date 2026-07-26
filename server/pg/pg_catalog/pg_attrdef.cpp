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

#include <vector>

#include "catalog/catalog.h"
#include "catalog/column_expr.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgAttrdef::adbin),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgAttrdef>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();

  std::vector<PgAttrdef> values;
  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& table :
         catalog->GetTables(GetDatabaseId(), schema->GetName())) {
      for (const auto& col : table->Columns()) {
        if (col.GetId() == catalog::Column::kGeneratedPKId) {
          continue;
        }
        if (!(col.expr && col.expr->HasExpr())) {
          continue;
        }
        auto pos = table->ColumnPosById(col.GetId());
        if (pos == table->Columns().size()) {
          continue;
        }
        values.push_back(PgAttrdef{
          AttrdefOid(col.GetId().id()),
          Oid{table->GetId().id()},
          static_cast<int16_t>(pos + 1),
          {},
        });
      }
    }
  }

  auto result = CreateColumns<PgAttrdef>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
