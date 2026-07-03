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

#include "pg/pg_catalog/pg_ts_dict.h"

#include "basics/assert.h"
#include "catalog/catalog.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgTsDict::dictinitoption),
});

}  // namespace

template<>
catalog::MaterializedData SystemTableSnapshot<PgTsDict>::GetTableData() {
  auto catalog = _config.CatalogSnapshot();

  std::vector<PgTsDict> values;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& tokenizer :
         catalog->GetTokenizers(GetDatabaseId(), schema->GetName())) {
      values.push_back({
        .oid = tokenizer->GetId().id(),
        .dictname = tokenizer->GetName(),
        .dictnamespace = tokenizer->GetParentId().id(),
        .dictowner = tokenizer->GetOwner().id(),
        .dicttemplate = 0,
      });
    }
  }

  auto result = CreateColumns<PgTsDict>(values.size());
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, *_config.CatalogSnapshot());
  }
  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
