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

#include "app/app_server.h"
#include "catalog/catalog.h"

namespace sdb::pg {
namespace {

constexpr auto kPgCatalog = PgNamespace{
  .oid = 11,
  .nspname = "pg_catalog",
};

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgNamespace::oid),
  GetIndex(&PgNamespace::nspname),
});

void RetrieveObjects(ObjectId database_id,
                     const catalog::LogicalCatalog& catalog,
                     std::vector<PgNamespace>& values) {
  auto schemas = catalog.GetSnapshot()->GetSchemas(database_id);

  values.emplace_back(kPgCatalog);
  for (const auto& object : schemas) {
    PgNamespace row{
      .oid = object->GetId().id(),
      .nspname = object->GetName(),
    };

    // TODO(codeworse): fill other fields
    values.emplace_back(std::move(row));
  }
}

}  // namespace

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgNamespace>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();

  std::vector<PgNamespace> values;
  RetrieveObjects(GetDatabaseId(), catalog, values);

  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<PgNamespace>);
  boost::pfr::for_each_field(
    PgNamespace{}, [&]<typename Field>(const Field& field) {
      auto column = CreateColumn<Field>(values.size(), &pool);
      result.push_back(std::move(column));
    });
  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }
  return result;
}
}  // namespace sdb::pg
