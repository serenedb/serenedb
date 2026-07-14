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

#include <memory>
#include <optional>
#include <ranges>
#include <string_view>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/system-compiler.h"
#include "catalog/identifiers/object_id.h"

namespace sdb::catalog {

enum class ResolveType {
  Database = 0,
  Role,
  Schema,
  Function,
  Relation,
  Tokenizer,
  ForeignServer,
  UserMapping,
  Type,
};

class ResolutionTable {
 public:
  ResolutionTable() {
    _roles = std::make_shared<MapByName<ObjectId>>();
    _databases = std::make_shared<MapByName<ObjectId>>();
    _schemas = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
    _relations = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
    _functions = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
    _tokenizers = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
    _foreign_servers = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
    _user_mappings = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
    _types = std::make_shared<MapById<MapByNamePtr<ObjectId>>>();
  }
  template<ResolveType Type>
  std::optional<ObjectId> ResolveObject(ObjectId parent_id,
                                        std::string_view object_name) const {
    if constexpr (Type == ResolveType::Database) {
      auto it = _databases->find(object_name);
      return it == _databases->end() ? std::nullopt : std::optional{it->second};
    } else if constexpr (Type == ResolveType::Role) {
      auto it = _roles->find(object_name);
      return it == _roles->end() ? std::nullopt : std::optional{it->second};
    } else {
      auto resolve =
        [](const MapByIdPtr<MapByNamePtr<ObjectId>>& lookup_map,
           ObjectId parent_id,
           std::string_view object_name) -> std::optional<ObjectId> {
        auto object_it = lookup_map->find(parent_id);
        if (object_it == lookup_map->end()) {
          return std::nullopt;
        }
        auto object_id_it = object_it->second->find(object_name);
        return object_id_it == object_it->second->end()
                 ? std::nullopt
                 : std::optional{object_id_it->second};
      };
      if constexpr (Type == ResolveType::Function) {
        return resolve(_functions, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Schema) {
        return resolve(_schemas, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Relation) {
        return resolve(_relations, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Tokenizer) {
        return resolve(_tokenizers, parent_id, object_name);
      } else if constexpr (Type == ResolveType::ForeignServer) {
        return resolve(_foreign_servers, parent_id, object_name);
      } else if constexpr (Type == ResolveType::UserMapping) {
        return resolve(_user_mappings, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Type) {
        return resolve(_types, parent_id, object_name);
      } else {
        SDB_UNREACHABLE();
      }
    }
  }

  template<ResolveType Type>
  bool AddObject(ObjectId parent_id, std::string_view object_name,
                 ObjectId object_id, bool replace) {
    if constexpr (Type == ResolveType::Database) {
      auto& databases = CloneData(_databases);
      if (!replace) {
        auto [_, inserted] = databases.try_emplace(object_name, object_id);
        if (!inserted) {
          return false;
        }
        auto [_, schemas] = CloneData(_schemas).try_emplace(
          object_id, std::make_shared<MapByName<ObjectId>>());
        SDB_ASSERT(schemas);
      } else {
        InsertOrRebind(databases, object_name, object_id);
      }
      return true;
    } else if constexpr (Type == ResolveType::Role) {
      auto& roles = CloneData(_roles);
      if (!replace) {
        auto [_, inserted] = roles.try_emplace(object_name, object_id);
        if (!inserted) {
          return false;
        }
      } else {
        InsertOrRebind(roles, object_name, object_id);
      }
      return true;
    } else {
      auto insert = [replace](MapByIdPtr<MapByNamePtr<ObjectId>>& insert_map,
                              ObjectId parent_id, std::string_view object_name,
                              ObjectId object_id) {
        auto& outer = CloneData(insert_map);
        auto it = outer.find(parent_id);
        SDB_ASSERT(it != outer.end());
        SDB_ASSERT(it->second);
        auto& inner = CloneData(it->second);
        if (!replace) {
          auto [_, inserted] = inner.try_emplace(object_name, object_id);
          return inserted;
        }
        InsertOrRebind(inner, object_name, object_id);
        return true;
      };
      if constexpr (Type == ResolveType::Function) {
        return insert(_functions, parent_id, object_name, object_id);
      } else if constexpr (Type == ResolveType::Schema) {
        auto inserted = insert(_schemas, parent_id, object_name, object_id);
        if (!inserted) {
          return false;
        }
        if (!replace) {
          auto [_, insert_relation] =
            CloneData(_relations)
              .try_emplace(object_id, std::make_shared<MapByName<ObjectId>>());
          auto [_, insert_function] =
            CloneData(_functions)
              .try_emplace(object_id, std::make_shared<MapByName<ObjectId>>());
          auto [_, insert_tokenizer] =
            CloneData(_tokenizers)
              .try_emplace(object_id, std::make_shared<MapByName<ObjectId>>());
          auto [_, insert_foreign_server] =
            CloneData(_foreign_servers)
              .try_emplace(object_id, std::make_shared<MapByName<ObjectId>>());
          auto [_, insert_user_mapping] =
            CloneData(_user_mappings)
              .try_emplace(object_id, std::make_shared<MapByName<ObjectId>>());
          auto [_, insert_type] = CloneData(_types).try_emplace(
            object_id, std::make_shared<MapByName<ObjectId>>());
          SDB_ASSERT(insert_relation);
          SDB_ASSERT(insert_function);
          SDB_ASSERT(insert_tokenizer);
          SDB_ASSERT(insert_foreign_server);
          SDB_ASSERT(insert_user_mapping);
          SDB_ASSERT(insert_type);
        }
        return true;
      } else if constexpr (Type == ResolveType::Relation) {
        return insert(_relations, parent_id, object_name, object_id);
      } else if constexpr (Type == ResolveType::Tokenizer) {
        return insert(_tokenizers, parent_id, object_name, object_id);
      } else if constexpr (Type == ResolveType::ForeignServer) {
        return insert(_foreign_servers, parent_id, object_name, object_id);
      } else if constexpr (Type == ResolveType::UserMapping) {
        return insert(_user_mappings, parent_id, object_name, object_id);
      } else if constexpr (Type == ResolveType::Type) {
        return insert(_types, parent_id, object_name, object_id);
      } else {
        SDB_UNREACHABLE();
      }
    }
  }

  template<ResolveType Type>
  std::optional<ObjectId> RemoveObject(ObjectId parent_id,
                                       std::string_view object_name) {
    if constexpr (Type == ResolveType::Database) {
      auto object = CloneData(_databases).extract(object_name);
      if (object.empty()) {
        return std::nullopt;
      }
      auto id = object.mapped();
      SDB_ASSERT(id.isSet());
      auto node = CloneData(_schemas).extract(id);
      SDB_ASSERT(!node.empty());
      for (auto [_, id] : *node.mapped()) {
        CloneData(_relations).erase(id);
        CloneData(_functions).erase(id);
        CloneData(_tokenizers).erase(id);
        CloneData(_foreign_servers).erase(id);
        CloneData(_user_mappings).erase(id);
        CloneData(_types).erase(id);
      }
      return {id};
    } else if constexpr (Type == ResolveType::Role) {
      auto object = CloneData(_roles).extract(object_name);
      if (object.empty()) {
        return std::nullopt;
      }
      auto id = object.mapped();
      SDB_ASSERT(id.isSet());
      return {id};
    } else {
      auto remove =
        [](MapByIdPtr<MapByNamePtr<ObjectId>>& remove_map, ObjectId parent_id,
           std::string_view object_name) -> std::optional<ObjectId> {
        auto& outer = CloneData(remove_map);
        auto it = outer.find(parent_id);
        SDB_ASSERT(it != outer.end());
        auto object = CloneData(it->second).extract(object_name);
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
          CloneData(_relations).erase(*result);
          CloneData(_functions).erase(*result);
          CloneData(_tokenizers).erase(*result);
          CloneData(_foreign_servers).erase(*result);
          CloneData(_user_mappings).erase(*result);
          CloneData(_types).erase(*result);
        }
        return result;
      } else if constexpr (Type == ResolveType::Relation) {
        return remove(_relations, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Tokenizer) {
        return remove(_tokenizers, parent_id, object_name);
      } else if constexpr (Type == ResolveType::ForeignServer) {
        return remove(_foreign_servers, parent_id, object_name);
      } else if constexpr (Type == ResolveType::UserMapping) {
        return remove(_user_mappings, parent_id, object_name);
      } else if constexpr (Type == ResolveType::Type) {
        return remove(_types, parent_id, object_name);
      } else {
        SDB_UNREACHABLE();
      }
    }
  }

  auto GetDatabaseIds() const { return *_databases | std::views::values; }

  auto GetRoleIds() const { return *_roles | std::views::values; }

  auto GetRelationIds(ObjectId schema_id) const {
    auto it = _relations->find(schema_id);
    SDB_ASSERT(it != _relations->end());
    return *it->second | std::views::values;
  }

  auto GetTokenizerIds(ObjectId schema_id) const {
    auto it = _tokenizers->find(schema_id);
    SDB_ASSERT(it != _tokenizers->end());
    return *it->second | std::views::values;
  }

  auto GetForeignServerIds(ObjectId schema_id) const {
    auto it = _foreign_servers->find(schema_id);
    SDB_ASSERT(it != _foreign_servers->end());
    return *it->second | std::views::values;
  }

  auto GetUserMappingIds(ObjectId schema_id) const {
    auto it = _user_mappings->find(schema_id);
    SDB_ASSERT(it != _user_mappings->end());
    return *it->second | std::views::values;
  }

  auto GetTypeIds(ObjectId schema_id) const {
    auto it = _types->find(schema_id);
    SDB_ASSERT(it != _types->end());
    return *it->second | std::views::values;
  }

 private:
  template<typename T>
  using MapByName = containers::FlatHashMap<std::string_view, T>;
  template<typename T>
  using MapByNamePtr = std::shared_ptr<const MapByName<T>>;
  template<typename T>
  using MapById = containers::FlatHashMap<ObjectId, T>;
  template<typename T>
  using MapByIdPtr = std::shared_ptr<const MapById<T>>;

  template<typename T>
  [[nodiscard]] static T& CloneData(std::shared_ptr<const T>& ptr) {
    auto clone = std::make_shared<T>(*ptr);
    ptr = clone;
    return *clone;
  }

  static void InsertOrRebind(MapByName<ObjectId>& map, std::string_view name,
                             ObjectId id) {
    auto [it, inserted] = map.insert_or_assign(name, id);
    if (!inserted) {
      const_cast<std::string_view&>(it->first) = name;
    }
  }

  // role_name -> role_id
  MapByNamePtr<ObjectId> _roles;
  // database_name -> database_id
  MapByNamePtr<ObjectId> _databases;
  // database_id -> (schema_name -> schema_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _schemas;
  // schema_id -> (relation_name -> object_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _relations;
  // schema_id -> (function_name -> object_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _functions;
  // schema_id -> (tokenizer_name -> object_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _tokenizers;
  // schema_id -> (foreign_server_name -> object_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _foreign_servers;
  // schema_id -> (user_mapping_name -> object_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _user_mappings;
  // schema_id -> (type_name -> object_id)
  MapByIdPtr<MapByNamePtr<ObjectId>> _types;
};

}  // namespace sdb::catalog
