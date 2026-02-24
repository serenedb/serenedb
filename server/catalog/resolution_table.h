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

#pragma once

#include <optional>
#include <string_view>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/errors.h"
#include "basics/system-compiler.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/view.h"

namespace sdb::catalog {

enum class ResolveType {
  Database = 0,
  Schema,
  Function,
  Relation,
};
/// Name-based resolution for the in-memory catalog.
///
/// _base:      database_name -> schema_name -> schema_id
/// _relations: schema_id     -> relation_name -> object_id  (tables, indexes,
/// views) _functions: schema_id     -> function_name -> object_id
class ResolutionTable {
 public:
  template<ResolveType Type>
  std::optional<ObjectId> ResolveObject(ObjectId parent_id,
                                        std::string_view object_name) const {
    if constexpr (Type == ResolveType::Database) {
      auto it = _databases.find(object_name);
      return it == _databases.end() ? std::nullopt : std::optional{it->second};
    } else {
      auto resolve =
        [](const auto& lookup_map, ObjectId parent_id,
           std::string_view object_name) -> std::optional<ObjectId> {
        auto object_it = lookup_map.find(parent_id);
        if (object_it == lookup_map.end()) {
          return std::nullopt;
        }
        auto object_id_it = object_it->second.find(object_name);
        return object_id_it == object_it->second.end()
                 ? std::nullopt
                 : std::optional{object_id_it->second};
      };
      if constexpr (Type == ResolveType::Function) {
        return resolve(_functions, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Schema) {
        return resolve(_schemas, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Relation) {
        return resolve(_relations, parent_id, object_name);
      } else {
        SDB_UNREACHABLE();
      }
    }
  }

  template<ResolveType Type>
  Result AddObject(ObjectId parent_id, std::string_view object_name,
                   ObjectId object_id, bool replace) {
    if constexpr (Type == ResolveType::Database) {
      if (!replace) {
        auto [_, inserted] = _databases.try_emplace(object_name, object_id);
        if (!inserted) {
          return {ERROR_SERVER_DUPLICATE_NAME};
        }
      } else {
        _databases.insert_or_assign(object_name, object_id);
      }
      auto [_, inserted] = _schemas.try_emplace(object_id);
      SDB_ASSERT(inserted);
      return {};
    } else {
      auto insert = [replace](auto& insert_map, ObjectId parent_id,
                              std::string_view object_name,
                              ObjectId object_id) {
        auto it = insert_map.find(parent_id);
        SDB_ASSERT(it != insert_map.end());
        if (!replace) {
          auto [_, inserted] = it->second.try_emplace(object_name, object_id);
          return inserted;
        }
        it->second.insert_or_assign(object_name, object_id);
        return true;
      };
      if constexpr (Type == ResolveType::Function) {
        return insert(_functions, parent_id, object_name, object_id)
                 ? Result{}
                 : Result{ERROR_SERVER_DUPLICATE_NAME};
      } else if constexpr (Type == ResolveType::Schema) {
        auto inserted = insert(_schemas, parent_id, object_name, object_id);
        if (inserted) {
          auto [_, insert_rel] = _relations.try_emplace(object_id);
          auto [_, insert_function] = _functions.try_emplace(object_id);
          SDB_ASSERT(insert_rel && insert_function);
          return {};
        }
        return {ERROR_SERVER_DUPLICATE_NAME};
      } else if constexpr (Type == ResolveType::Relation) {
        return insert(_relations, parent_id, object_name, object_id)
                 ? Result{}
                 : Result{ERROR_SERVER_DUPLICATE_NAME};
      } else {
        SDB_UNREACHABLE();
      }
    }
  }

  template<ResolveType Type>
  std::optional<ObjectId> RemoveObject(ObjectId parent_id,
                                       std::string_view object_name) {
    if constexpr (Type == ResolveType::Database) {
      auto object = _databases.extract(object_name);
      if (object.empty()) {
        return std::nullopt;
      }
      auto id = object.mapped();
      SDB_ASSERT(id.isSet());
      auto schemas = _schemas.extract(id);
      SDB_ASSERT(!schemas.empty());
      for (auto& schema : schemas.mapped()) {
        RemoveObject<Type>(id, schema.first);
      }
      return {id};
    } else {
      auto remove = [](
                      auto& remove_map, ObjectId parent_id,
                      std::string_view object_name) -> std::optional<ObjectId> {
        auto it = remove_map.find(parent_id);
        SDB_ASSERT(it != remove_map.end());
        auto object = it->second.extract(object_name);
        if (object.empty()) {
          return std::nullopt;
        }
        auto id = object.mapped();
        SDB_ASSERT(id.isSet());
        return {id};
      };
      if constexpr (Type == ResolveType::Function) {
        return remove(_functions, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Schema) {
        auto result = remove(_schemas, parent_id, object_name);
        if (result) {
          _relations.erase(*result);
          _functions.erase(*result);
        }
        return result;
      } else if constexpr (Type == ResolveType::Relation) {
        return remove(_relations, parent_id, object_name);
      } else {
        SDB_UNREACHABLE();
      }
    }
  }

  auto GetDatabaseIds() const { return _databases | std::views::values; }
  auto GetSchemaIds(ObjectId db_id) const {
    return _schemas.at(db_id) | std::views::values;
  }

  auto GetRelationIds(ObjectId db_id, ObjectId schema_id) const {
    return _relations.at(schema_id) | std::views::values;
  }

  auto GetFunctionIds(ObjectId db_id, ObjectId schema_id) const {
    return _functions.at(schema_id) | std::views::values;
  }

 private:
  template<typename T>
  using MapByName = containers::FlatHashMap<std::string_view, T>;
  template<typename T>
  using MapById = containers::FlatHashMap<ObjectId, T>;

  // database_name -> database_id
  MapByName<ObjectId> _databases;
  // database_id -> (schema_name -> schema_id)
  MapById<MapByName<ObjectId>> _schemas;
  // schema_id -> (relation_name -> object_id)
  MapById<MapByName<ObjectId>> _relations;
  // schema_id -> (function_name -> object_id)
  MapById<MapByName<ObjectId>> _functions;
};

}  // namespace sdb::catalog
