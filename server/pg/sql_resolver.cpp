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

#include "pg/sql_resolver.h"

#include "app/app_server.h"
#include "basics/down_cast.h"
#include "catalog/catalog.h"
#include "catalog/native_functions.h"
#include "catalog/sql_function_impl.h"
#include "catalog/sql_query_view.h"
#include "catalog/virtual_table.h"
#include "pg/system_catalog.h"
#include "rest_server/serened.h"

namespace sdb::pg {

namespace {

void ResolveEntity(ObjectId database, std::span<const std::string> search_path,
                   Objects& objects, Disallowed& disallowed,
                   const Objects& query, std::string_view entity_name);

void ResolveObject(ObjectId database, std::span<const std::string> search_path,
                   Objects& objects, Disallowed& disallowed,
                   const Objects::ObjectName& name, Objects::ObjectData& data) {
  if (data.object) {
    return;
  }
  if (const auto* table = pg::GetTable(name.relation)) {
    data.object = table->CreateSnapshot(database);
    return;
  }
  if (const auto func = pg::GetFunction(name.relation)) {
    data.object = std::move(func);
    return;
  }

  if (const auto view = pg::GetView(name.relation)) {
    data.object = std::move(view);
  } else {
    auto& catalogs =
      SerenedServer::Instance().getFeature<catalog::CatalogFeature>();
    auto& catalog = catalogs.Global();
    auto snapshot = catalog.GetSnapshot();

    std::string_view key = name.relation;

    auto resolve_object = [&](std::string_view schema) {
      if (auto object = snapshot->GetRelation(database, schema, key)) {
        data.object = object;
      } else {
        data.object = snapshot->GetFunction(database, schema, key);
      }
    };

    if (!name.schema.empty()) {
      resolve_object(name.schema);
    } else {
      for (const auto& schema : search_path) {
        resolve_object(schema);
        if (data.object) {
          break;
        }
      }
    }
    if (!data.object) {
      SDB_THROW(ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"",
                name.FullName(), "\" does not exist");
    }
  }

  if (data.object->GetType() == catalog::ObjectType::View) {
    bool changed = disallowed.emplace(name).second;
    SDB_ASSERT(changed);
    auto state = basics::downCast<SqlQueryView>(*data.object).GetState();
    ResolveQueryView(database, search_path, objects, disallowed,
                     state->objects);
    changed = disallowed.erase(name) != 0;
    SDB_ASSERT(changed);
  } else if (data.object->GetType() == catalog::ObjectType::Function) {
    auto& func = basics::downCast<catalog::Function>(*data.object);
    if (func.Options().language == catalog::FunctionLanguage::SQL) {
      bool changed = disallowed.emplace(name).second;
      SDB_ASSERT(changed);
      ResolveFunction(database, search_path, objects, disallowed,
                      func.SqlFunction().GetObjects());
      changed = disallowed.erase(name) != 0;
      SDB_ASSERT(changed);
    }
  } else if (data.object->GetType() == catalog::ObjectType::Table) {
    auto table = basics::downCast<catalog::Table>(*data.object);
    for (const auto& column : table.Columns()) {
      if (const auto& default_value = column.default_value) {
        const auto& default_value_objects = default_value->GetObjects();
        ResolveEntity(database, search_path, objects, disallowed,
                      default_value_objects, "default_value");
      }
    }
  }
}

void ResolveEntity(ObjectId database, std::span<const std::string> search_path,
                   Objects& objects, Disallowed& disallowed,
                   const Objects& query, std::string_view entity_name) {
  for (const auto& [name, old_data] : query.getObjects()) {
    if (disallowed.contains(name)) {
      SDB_THROW(ERROR_BAD_PARAMETER, entity_name,
                " doesn't support recursive references");
    }
    auto& new_data = objects.ensureData(name.schema, name.relation);
    new_data = old_data;
    ResolveObject(database, search_path, objects, disallowed, name, new_data);
  }
}

}  // namespace

void ResolveQueryView(ObjectId database,
                      std::span<const std::string> search_path,
                      Objects& objects, Disallowed& disallowed,
                      const Objects& query) {
  ResolveEntity(database, search_path, objects, disallowed, query, "view");
}

void ResolveFunction(ObjectId database,
                     std::span<const std::string> search_path, Objects& objects,
                     Disallowed& disallowed, const Objects& query) {
  ResolveEntity(database, search_path, objects, disallowed, query, "function");
}

void Resolve(ObjectId database, Objects& objects, const Config& config) {
  SDB_ASSERT(!ServerState::instance()->IsDBServer());
  Disallowed disallowed;
  auto query = std::move(objects.getObjects());
  auto search_path = config.Get<VariableType::PgSearchPath>("search_path");

  for (auto& [name, old_data] : query) {
    auto& new_data = objects.ensureData(name.schema, name.relation);
    new_data = std::move(old_data);
    ResolveObject(database, search_path, objects, disallowed, name, new_data);
  }
}

}  // namespace sdb::pg
