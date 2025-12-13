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

#include "pg/pg_catalog/pg_class.h"

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/local_catalog.h"
#include "catalog/object.h"
#include "catalog/schema.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"
#include "rest_server/serened.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgClass::oid),
  GetIndex(&PgClass::relname),
  GetIndex(&PgClass::relnamespace),
  GetIndex(&PgClass::reltablespace),
  GetIndex(&PgClass::relkind),
  GetIndex(&PgClass::reloptions),
});

constexpr Oid kPgCatalogNamespaceOid = 11;

}  // namespace

void RetrieveObjects(ObjectId database_id,
                     const catalog::LogicalCatalog& catalog,
                     std::vector<PgClass>& values) {
  auto insert_object =
    [&](const std::shared_ptr<catalog::SchemaObject>& object) {
      PgClass::Relkind relkind;
      switch (object->GetType()) {
        case catalog::ObjectType::Table:
          relkind = PgClass::Relkind::OrdinaryTable;
          break;
        case catalog::ObjectType::View:
          relkind = PgClass::Relkind::View;
          break;
        default:
          SDB_THROW(ERROR_INTERNAL, "Unsupported object type for pg_class: {}",
                    static_cast<uint8_t>(object->GetType()));
      };

      PgClass row{
        .oid = object->GetId().id(),
        .relname = object->GetName(),
        .relnamespace = object->GetSchemaId().id(),
        .reltablespace = 0,
        .relkind = relkind,
      };

      // TODO(codeworse): fill other fields
      values.push_back(std::move(row));
    };

  for (const auto& view : catalog.GetSnapshot()->GetRelations(
         database_id, StaticStrings::kPublic)) {
    insert_object(view);
  }
}

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgClass>::GetTableData(
  velox::memory::MemoryPool& pool) {
  auto& catalog =
    SerenedServer::Instance().getFeature<catalog::CatalogFeature>().Global();
  std::vector<velox::VectorPtr> result;
  result.reserve(boost::pfr::tuple_size_v<PgClass>);
  std::vector<PgClass> values;
  std::vector<uint64_t> database_ids;
  RetrieveObjects(GetDatabaseId(), catalog, values);

  {  // get system tables
    VisitSystemTables([&](const catalog::VirtualTable& table) {
      PgClass row{
        .oid = table.Id().id(),
        .relname = table.Name(),
        .relnamespace = kPgCatalogNamespaceOid,
        .reltablespace = 0,
        .relkind = PgClass::Relkind::OrdinaryTable,
      };
      // TODO(codeworse): fill other fields
      values.push_back(std::move(row));
    });
  }

  boost::pfr::for_each_field(
    PgClass{}, [&]<typename Field>(const Field& field) {
      auto column = CreateColumn<Field>(values.size(), &pool);
      result.push_back(std::move(column));
    });

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
