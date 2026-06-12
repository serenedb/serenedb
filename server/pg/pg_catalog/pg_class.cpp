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

#include <absl/strings/str_cat.h>

#include <duckdb/main/connection.hpp>
#include <duckdb/main/database.hpp>

#include "catalog/store/store.h"

#include <deque>
#include <string>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/catalog.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/local_catalog.h"
#include "catalog/sequence.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/system_catalog.h"

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

PgClass MakeBaseRow(ObjectId schema_id, const catalog::Object& object) {
  return {
    .oid = object.GetId().id(),
    .relname = object.GetName(),
    .relnamespace = schema_id.id(),
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
                     std::deque<std::string>& pk_index_names,
                     const catalog::Snapshot& catalog,
                     duckdb::ClientContext& context) {
  // Store-table row counts ride a scratch connection: the native plane has
  // no per-table counter yet. TODO(M2): maintain a commit-time counter.
  std::optional<duckdb::Connection> store_conn;
  auto count_store_rows = [&](const catalog::Table& table,
                              std::string_view db_name,
                              std::string_view schema_name) -> float {
    if (table.GetEngine() != catalog::TableEngine::Transactional ||
        table.Tombstoned()) {
      return 0.0F;
    }
    if (!store_conn) {
      store_conn.emplace(*context.db);
    }
    auto store_name =
      catalog::StoreTableName(db_name, schema_name, table.GetName());
    auto result = store_conn->Query(
      absl::StrCat("SELECT count(*) FROM \"", catalog::kStoreDatabaseName,
                   "\".main.\"", store_name, "\""));
    if (result->HasError()) {
      return 0.0F;
    }
    auto chunk = result->Fetch();
    if (!chunk || chunk->size() == 0) {
      return 0.0F;
    }
    return static_cast<float>(
      chunk->GetValue(0, 0).GetValue<int64_t>());
  };
  auto database = catalog.GetDatabase(database_id);
  SDB_ASSERT(database);
  for (const auto& schema : catalog.GetSchemas(database_id)) {
    const auto schema_id = schema->GetId();

    for (const auto& table :
         catalog.GetTables(database_id, schema->GetName())) {
      auto row = MakeBaseRow(schema_id, *table);
      row.relkind = PgClass::Relkind::OrdinaryTable;
      row.relnatts = static_cast<int16_t>(table->Columns().size());
      row.relchecks = static_cast<int16_t>(table->CheckConstraints().size());
      row.relhasindex = catalog.HasIndexes(table->GetId());
      row.reltuples =
        count_store_rows(*table, database->GetName(), schema->GetName());
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

    for (const auto& sequence :
         catalog.GetSequences(database_id, schema->GetName())) {
      auto row = MakeBaseRow(schema_id, *sequence);
      row.relkind = PgClass::Relkind::Sequence;
      values.push_back(std::move(row));
    }

    for (const auto& type : catalog.GetTypes(database_id, schema->GetName())) {
      const auto& info = type->GetInfo();
      if (info.type.id() != duckdb::LogicalTypeId::STRUCT) {
        continue;
      }
      auto row = MakeBaseRow(schema_id, *type);
      row.relkind = PgClass::Relkind::CompositeType;
      row.relnatts = static_cast<int16_t>(
        duckdb::StructType::GetChildTypes(info.type).size());
      values.push_back(std::move(row));
    }
  }

  // Synthetic pg_class entries for primary key indexes (PG semantics).
  // Name is '<table>_pkey', OID is PkIndexOid(table_oid). Kept in separate
  // storage to survive the value push_backs above.
  for (const auto& schema : catalog.GetSchemas(database_id)) {
    const auto schema_id = schema->GetId();
    for (const auto& table :
         catalog.GetTables(database_id, schema->GetName())) {
      if (table->PKColumns().empty()) {
        continue;
      }
      pk_index_names.push_back(std::string{table->GetName()} + "_pkey");
      PgClass row{
        .oid = PkIndexOid(table->GetId().id()),
        .relname = pk_index_names.back(),
        .relnamespace = schema_id.id(),
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
        .relkind = PgClass::Relkind::Index,
        .relnatts = static_cast<int16_t>(table->PKColumns().size()),
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
    }
  }
}

template<>
catalog::MaterializedData SystemTableSnapshot<PgClass>::GetTableData() {
  std::vector<PgClass> values;
  std::deque<std::string> pk_index_names;
  auto catalog = _config.EnsureCatalogSnapshot();
  RetrieveObjects(GetDatabaseId(), values, pk_index_names, *catalog,
                  _config.GetClientContext());

  {
    VisitSystemTables([&](const catalog::VirtualTable& table, Oid schema_oid) {
      auto row_type = table.RowType();
      int16_t natts = row_type.id() == duckdb::LogicalTypeId::STRUCT
                        ? static_cast<int16_t>(
                            duckdb::StructType::GetChildTypes(row_type).size())
                        : 0;
      PgClass row{
        .oid = table.Id().id(),
        .relname = table.GetName(),
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

  auto result = CreateColumns<PgClass>(values.size());

  for (size_t row = 0; row < values.size(); ++row) {
    WriteData(result, values[row], kNullMask, row);
  }

  return {std::move(result), values.size()};
}

}  // namespace sdb::pg
