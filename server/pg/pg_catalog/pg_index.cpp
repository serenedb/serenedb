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

#include "pg/pg_catalog/pg_index.h"

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/local_catalog.h"
#include "catalog/object.h"
#include "catalog/schema.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"
#include "rest_server/serened.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgIndex::indexrelid),
  GetIndex(&PgIndex::indrelid),
});

}  // namespace

void RetrieveObjects(ObjectId database_id,
                     const catalog::LogicalCatalog& catalog,
                     std::vector<PgIndex>& values) {
  auto snapshot = catalog.GetSnapshot();

  auto insert_object =
    [&](const std::shared_ptr<catalog::SchemaObject>& object) {
      if (object->GetType() != catalog::ObjectType::Index) {
        return;
      }

      auto& index = basics::downCast<catalog::Index>(*object);

      PgIndex row{
        .indexrelid = index.GetId().id(),
        .indrelid = index.GetRelationId().id(),
      };

      // TODO(codeworse): fill other fields
      values.push_back(std::move(row));
    };

  for (const auto& schema : snapshot->GetSchemas(database_id)) {
    for (const auto& relation :
         snapshot->GetRelations(database_id, schema->GetName())) {
      insert_object(relation);
    }
  }
}

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgIndex>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<PgIndex>);
  std::vector<PgIndex> values;
  RetrieveObjects(GetDatabaseId(), catalog, values);

  boost::pfr::for_each_field(
    PgIndex{}, [&]<typename Field>(const Field& field) {
      auto column = CreateColumn<Field>(values.size(), &pool);
      result.push_back(std::move(column));
    });

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
