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

#include "catalog/catalog.h"
#include "pg/pg_catalog/fwd.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNulls({
  GetIndex(&PgTsDict::dictinitoption),
});

}  // namespace

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgTsDict>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto catalog = _config.EnsureCatalogSnapshot();

  std::vector<PgTsDict> values;

  for (const auto& schema : catalog->GetSchemas(GetDatabaseId())) {
    for (const auto& tokenizer :
         catalog->GetTokenizers(GetDatabaseId(), schema->GetName())) {
      auto owner = tokenizer->GetOwnerId();
      if (!owner) {
        owner = id::kRootUser;
      }
      values.push_back({
        .oid = tokenizer->GetId().id(),
        .dictname = tokenizer->GetName(),
        .dictnamespace = tokenizer->GetSchemaId().id(),
        .dictowner = owner.id(),
        .dicttemplate = 0,
      });
    }
  }

  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<PgTsDict>);
  boost::pfr::for_each_field(
    PgTsDict{}, [&]<typename Field>(const Field& field) {
      auto column = CreateColumn<Field>(values.size(), &pool);
      result.push_back(std::move(column));
    });

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
