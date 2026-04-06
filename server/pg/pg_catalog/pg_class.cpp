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
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/local_catalog.h"
#include "catalog/view.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"
#include "rest_server/serened.h"

namespace sdb::pg {
namespace {

constexpr uint64_t kNullMask = MaskFromNonNulls({
  GetIndex(&PgClass::oid),
  GetIndex(&PgClass::relname),
  GetIndex(&PgClass::relnamespace),
  GetIndex(&PgClass::reltype),
  GetIndex(&PgClass::reloftype),
  GetIndex(&PgClass::relowner),
  GetIndex(&PgClass::relam),
  GetIndex(&PgClass::relfilenode),
  GetIndex(&PgClass::reltablespace),
  GetIndex(&PgClass::relpages),
  GetIndex(&PgClass::reltuples),
  GetIndex(&PgClass::relallvisible),
  GetIndex(&PgClass::relallfrozen),
  GetIndex(&PgClass::reltoastrelid),
  GetIndex(&PgClass::relhasindex),
  GetIndex(&PgClass::relisshared),
  GetIndex(&PgClass::relpersistence),
  GetIndex(&PgClass::relkind),
  GetIndex(&PgClass::relnatts),
  GetIndex(&PgClass::relchecks),
  GetIndex(&PgClass::relhasrules),
  GetIndex(&PgClass::relhastriggers),
  GetIndex(&PgClass::relhassubclass),
  GetIndex(&PgClass::relrowsecurity),
  GetIndex(&PgClass::relforcerowsecurity),
  GetIndex(&PgClass::relispopulated),
  GetIndex(&PgClass::relreplident),
  GetIndex(&PgClass::relispartition),
  GetIndex(&PgClass::relrewrite),
  GetIndex(&PgClass::relfrozenxid),
  GetIndex(&PgClass::relminmxid),
  GetIndex(&PgClass::reloptions),
});

}  // namespace

PgClass MakeBaseRow(ObjectId schema_id, const catalog::SchemaObject& object) {
  auto owner = object.GetOwnerId();
  if (!owner) {
    owner = id::kRootUser;
  }
  return {
    .oid = object.GetId().id(),
    .relname = object.GetName(),
    .relnamespace = schema_id.id(),
    .reltype = 0,
    .reloftype = 0,
    .relowner = owner.id(),
    .relam = 0,
    .relfilenode = 0,
    .reltablespace = 0,
    .relpages = 0,
    .reltuples = -1,
    .relallvisible = 0,
    .relallfrozen = 0,
    .reltoastrelid = 0,
    .relhasindex = false,
    .relisshared = false,
    .relpersistence = PgClass::Relpersistence::Permanent,
    .relkind = PgClass::Relkind::OrdinaryTable,
    .relnatts = 0,
    .relchecks = 0,
    .relhasrules = false,
    .relhastriggers = false,
    .relhassubclass = false,
    .relrowsecurity = false,
    .relforcerowsecurity = false,
    .relispopulated = true,
    .relreplident = PgClass::Relreplident::Default,
    .relispartition = false,
    .relrewrite = 0,
    .relfrozenxid = 0,
    .relminmxid = 0,
  };
}

void RetrieveObjects(ObjectId database_id, std::vector<PgClass>& values,
                     const catalog::Snapshot& catalog) {
  for (const auto& schema : catalog.GetSchemas(database_id)) {
    const auto schema_id = schema->GetId();

    for (const auto& table :
         catalog.GetTables(database_id, schema->GetName())) {
      auto row = MakeBaseRow(schema_id, *table);
      row.relkind = PgClass::Relkind::OrdinaryTable;
      row.relnatts = static_cast<int16_t>(table->Columns().size());
      row.relchecks = static_cast<int16_t>(table->CheckConstraints().size());
      row.relhasindex = catalog.HasIndexes(table->GetId());
      auto shard = catalog.GetTableShard(table->GetId());
      SDB_ASSERT(shard);
      row.reltuples = static_cast<float>(shard->GetTableStats().num_rows);
      values.push_back(std::move(row));
    }

    for (const auto& view : catalog.GetViews(database_id, schema->GetName())) {
      auto row = MakeBaseRow(schema_id, *view);
      row.relkind = PgClass::Relkind::View;
      values.push_back(std::move(row));
    }

    for (const auto& index :
         catalog.GetIndexes(database_id, schema->GetName())) {
      auto row = MakeBaseRow(schema_id, *index);
      row.relkind = PgClass::Relkind::Index;
      row.relnatts = static_cast<int16_t>(index->GetColumnIds().size());
      values.push_back(std::move(row));
    }
  }
}

template<>
std::vector<velox::VectorPtr> SystemTableSnapshot<PgClass>::GetTableData(
  velox::memory::MemoryPool& pool) {
  std::vector<PgClass> values;
  auto catalog = _config.EnsureCatalogSnapshot();
  RetrieveObjects(GetDatabaseId(), values, *catalog);

  {
    VisitSystemTables([&](const catalog::VirtualTable& table, Oid schema_oid) {
      auto row_type = table.RowType();
      int16_t natts = row_type ? static_cast<int16_t>(row_type->size()) : 0;
      PgClass row{
        .oid = table.Id().id(),
        .relname = table.Name(),
        .relnamespace = schema_oid,
        .reltype = 0,
        .reloftype = 0,
        .relowner = id::kRootUser.id(),
        .relam = 0,
        .relfilenode = 0,
        .reltablespace = 0,
        .relpages = 0,
        .reltuples = -1,
        .relallvisible = 0,
        .relallfrozen = 0,
        .reltoastrelid = 0,
        .relhasindex = false,
        .relisshared = false,
        .relpersistence = PgClass::Relpersistence::Permanent,
        .relkind = PgClass::Relkind::OrdinaryTable,
        .relnatts = natts,
        .relchecks = 0,
        .relhasrules = false,
        .relhastriggers = false,
        .relhassubclass = false,
        .relrowsecurity = false,
        .relforcerowsecurity = false,
        .relispopulated = true,
        .relreplident = PgClass::Relreplident::Default,
        .relispartition = false,
        .relrewrite = 0,
        .relfrozenxid = 0,
        .relminmxid = 0,
      };
      values.push_back(std::move(row));
    });
  }

  {
    VisitSystemViews([&](const catalog::PgSqlView& view, Oid schema_oid) {
      PgClass row{
        .oid = view.GetId().id(),
        .relname = view.GetName(),
        .relnamespace = schema_oid,
        .reltype = 0,
        .reloftype = 0,
        .relowner = id::kRootUser.id(),
        .relam = 0,
        .relfilenode = 0,
        .reltablespace = 0,
        .relpages = 0,
        .reltuples = -1,
        .relallvisible = 0,
        .relallfrozen = 0,
        .reltoastrelid = 0,
        .relhasindex = false,
        .relisshared = false,
        .relpersistence = PgClass::Relpersistence::Permanent,
        .relkind = PgClass::Relkind::View,
        .relnatts = 0,
        .relchecks = 0,
        .relhasrules = false,
        .relhastriggers = false,
        .relhassubclass = false,
        .relrowsecurity = false,
        .relforcerowsecurity = false,
        .relispopulated = true,
        .relreplident = PgClass::Relreplident::Default,
        .relispartition = false,
        .relrewrite = 0,
        .relfrozenxid = 0,
        .relminmxid = 0,
      };
      values.push_back(std::move(row));
    });
  }

  auto result = CreateColumns<PgClass>(values, &pool);

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row, &pool);
  }

  return result;
}

}  // namespace sdb::pg
