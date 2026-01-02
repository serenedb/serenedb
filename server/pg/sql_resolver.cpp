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

void ResolveObjects(ObjectId database, std::span<const std::string> search_path,
                    Objects& objects, Disallowed& disallowed,
                    const Objects& query);

namespace {

void ResolveInformationSchema(ObjectId database, std::string_view relation,
                              Objects::ObjectData& data) {
  if (const auto* table =
        GetSystemTable(StaticStrings::kInformationSchema, relation)) {
    data.object = table->CreateSnapshot(database);
  }
  // TODO(codeworse): add views and functions from information_schema
}

enum class ObjectType : uint8_t {
  Function = 0,
  Relation = 1  // view, table
};

void ResolveObject(ObjectId database, ObjectType type,
                   std::span<const std::string> search_path, Objects& objects,
                   Disallowed& disallowed, const Objects::ObjectName& name,
                   Objects::ObjectData& data) {
  if (data.object) {
    return;
  }

  if (type == ObjectType::Relation) {
    // system tables
    if (const auto* table = pg::GetTable(name.relation)) {
      data.object = table->CreateSnapshot(database);
      return;
    }
  } else if (type == ObjectType::Function) {
    if (const auto func = pg::GetFunction(name.relation)) {
      data.object = std::move(func);
      return;
    }
  }

  if (name.schema == StaticStrings::kInformationSchema) {
    // information_schema must be explicitly defined
    // (except the case it is in the search path)
    ResolveInformationSchema(database, name.relation, data);
    if (data.object) {
      return;
    }
  }

  auto resolve_subquery = [&] {
    bool changed = disallowed.emplace(name).second;
    SDB_ASSERT(changed);
    auto state = basics::downCast<SqlQueryView>(*data.object).GetState();
    ResolveQueryView(database, search_path, objects, disallowed,
                     state->objects);
    changed = disallowed.erase(name) != 0;
    SDB_ASSERT(changed);
  };

  if (type == ObjectType::Relation) {
    if (const auto view = pg::GetView(name.relation)) {
      data.object = std::move(view);
      resolve_subquery();
      return;
    }
  }

  auto resolve_object = [&](std::string_view schema) {
    SDB_ASSERT(!data.object);
    if (schema == StaticStrings::kInformationSchema) {
      // In case information_schema is in the search path
      ResolveInformationSchema(database, name.relation, data);
      return;
    }

    auto& instance = SerenedServer::Instance();
    auto& catalog = instance.getFeature<catalog::CatalogFeature>().Global();
    auto snapshot = catalog.GetSnapshot();
    data.object = [&] -> std::shared_ptr<catalog::SchemaObject> {
      switch (type) {
        case ObjectType::Relation:
          return snapshot->GetRelation(database, schema, name.relation);
        case ObjectType::Function:
          return snapshot->GetFunction(database, schema, name.relation);
      }
    }();
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
    const std::string object_name = [&] {
      switch (type) {
        case ObjectType::Relation:
          return "relation";
        case ObjectType::Function:
          return "function";
      }
    }();
    SDB_THROW(ERROR_SERVER_DATA_SOURCE_NOT_FOUND, object_name, " \"",
              name.FullName(), "\" does not exist");
  }

  if (type == ObjectType::Relation) {
    if (data.object->GetType() == catalog::ObjectType::Table) {
      auto table = basics::downCast<catalog::Table>(*data.object);
      for (const auto& column : table.Columns()) {
        if (const auto& default_value = column.expr) {
          const auto& default_value_objects = default_value->GetObjects();
          ResolveObjects(database, search_path, objects, disallowed,
                         default_value_objects);
        }
      }
    } else if (data.object->GetType() == catalog::ObjectType::View) {
      resolve_subquery();
    }
  } else if (type == ObjectType::Function) {
    SDB_ASSERT(data.object->GetType() == catalog::ObjectType::Function);
    auto& func = basics::downCast<catalog::Function>(*data.object);
    if (func.Options().language == catalog::FunctionLanguage::SQL) {
      bool changed = disallowed.emplace(name).second;
      SDB_ASSERT(changed);
      ResolveSqlFunction(database, search_path, objects, disallowed,
                         func.SqlFunction().GetObjects());
      changed = disallowed.erase(name) != 0;
      SDB_ASSERT(changed);
    }
  }
}

}  // namespace

void ResolveRelations(ObjectId database,
                      std::span<const std::string> search_path,
                      Objects& objects, Disallowed& disallowed,
                      const Objects& query) {
  for (auto& [name, old_data] : objects.getRelations()) {
    auto& new_data = objects.ensureRelation(name.schema, name.relation);
    new_data = std::move(old_data);
    ResolveObject(database, ObjectType::Relation, search_path, objects,
                  disallowed, name, new_data);
  }
}

void ResolveFunctions(ObjectId database,
                      std::span<const std::string> search_path,
                      Objects& objects, Disallowed& disallowed,
                      const Objects& query) {
  auto functions = std::move(objects.getFunctions());
  for (auto& [name, old_data] : functions) {
    auto& new_data = objects.ensureFunction(name.schema, name.relation);
    new_data = std::move(old_data);
    ResolveObject(database, ObjectType::Function, search_path, objects,
                  disallowed, name, new_data);
  }
}

void ResolveObjects(ObjectId database, std::span<const std::string> search_path,
                    Objects& objects, Disallowed& disallowed,
                    const Objects& query) {
  ResolveRelations(database, search_path, objects, disallowed, query);
  ResolveFunctions(database, search_path, objects, disallowed, query);
}

void Resolve(ObjectId database, Objects& objects, const Config& config) {
  SDB_ASSERT(!ServerState::instance()->IsDBServer());
  Disallowed dummy;
  auto search_path = config.Get<VariableType::PgSearchPath>("search_path");
  ResolveObjects(database, search_path, objects, dummy, objects);
}

void ResolveQueryView(ObjectId database,
                      std::span<const std::string> search_path,
                      Objects& objects, Disallowed& disallowed,
                      const Objects& query) {
  for (const auto& [name, old_data] : query.getRelations()) {
    if (disallowed.contains(name)) {
      SDB_THROW(ERROR_BAD_PARAMETER,
                "view doesn't support recursive references");
    }
    auto& new_data = objects.ensureRelation(name.schema, name.relation);
    new_data = old_data;
    ResolveObject(database, ObjectType::Relation, search_path, objects,
                  disallowed, name, new_data);
  }
  for (const auto& [name, old_data] : query.getFunctions()) {
    auto& new_data = objects.ensureFunction(name.schema, name.relation);
    new_data = old_data;
    ResolveObject(database, ObjectType::Function, search_path, objects,
                  disallowed, name, new_data);
  }
}

void ResolveSqlFunction(ObjectId database,
                        std::span<const std::string> search_path,
                        Objects& objects, Disallowed& disallowed,
                        const Objects& query) {
  for (const auto& [name, old_data] : query.getRelations()) {
    auto& new_data = objects.ensureRelation(name.schema, name.relation);
    new_data = old_data;
    ResolveObject(database, ObjectType::Relation, search_path, objects,
                  disallowed, name, new_data);
  }
  for (const auto& [name, old_data] : query.getFunctions()) {
    if (disallowed.contains(name)) {
      SDB_THROW(ERROR_BAD_PARAMETER,
                "function doesn't support recursive references");
    }
    auto& new_data = objects.ensureFunction(name.schema, name.relation);
    new_data = old_data;
    ResolveObject(database, ObjectType::Function, search_path, objects,
                  disallowed, name, new_data);
  }
}

}  // namespace sdb::pg
