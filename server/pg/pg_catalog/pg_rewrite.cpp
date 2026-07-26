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

#include "catalog/catalog.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgRewrite::ev_qual),
  GetIndex(&PgRewrite::ev_action),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgRewrite>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();

  std::vector<PgRewrite> values;
  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& view :
         catalog->GetViews(GetDatabaseId(), schema->GetName())) {
      values.push_back(PgRewrite{
        ViewRuleOid(view->GetId().id()),
        Name{"_RETURN"},
        Oid{view->GetId().id()},
        PgRewrite::EvType::Select,
        PgRewrite::EvEnabled::Origin,
        true,
        {},
        {},
      });
    }
  }

  auto result = CreateColumns<PgRewrite>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *catalog);
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
