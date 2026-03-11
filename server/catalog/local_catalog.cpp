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

#include "catalog/local_catalog.h"

#include <absl/cleanup/cleanup.h>
#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iterator>
#include <magic_enum/magic_enum.hpp>
#include <memory>
#include <ranges>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "app/app_server.h"
#include "auth/role_utils.h"
#include "basics/application-exit.h"
#include "basics/assert.h"
#include "basics/buffer.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/debugging.h"
#include "basics/down_cast.h"
#include "basics/error_code.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "basics/recursive_locker.h"
#include "basics/result.h"
#include "basics/result_or.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/object_dependency.h"
#include "catalog/resolution_table.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "catalog/view.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "pg/pg_catalog/fwd.h"
#include "pg/sql_resolver.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/index_shard.h"
#include "storage_engine/search_engine.h"
#include "storage_engine/table_shard.h"
#include "utils/exec_context.h"
#include "utils/operation_options.h"
#include "vpack/builder.h"
#include "vpack/slice.h"
#include "yaclib/async/future.hpp"
#include "yaclib/async/when_all.hpp"

#ifdef SDB_CLUSTER
#include "aql/query_registry_feature.h"
#include "replication/replication_feature.h"
#include "search/search_engine.h"
#include "transaction/cluster_utils.h"
#endif

namespace sdb::catalog {

class SnapshotImpl;

namespace {

Result Apply(
  auto& snapshot, auto&& f,
  std::function<void(const std::shared_ptr<SnapshotImpl>&)> rollback = {}) {
  auto self = std::atomic_load_explicit(&snapshot, std::memory_order_acquire);
  std::shared_ptr<SnapshotImpl> clone = self->Clone();
  self.reset();
  if (auto r = f(clone); !r.ok()) {
    if (rollback) {
      rollback(clone);
    }
    return r;
  }
  std::shared_ptr<const SnapshotImpl> clone_const = std::move(clone);
  std::atomic_store_explicit(&snapshot, std::move(clone_const),
                             std::memory_order_release);
  return {};
}

}  // namespace

class SnapshotImpl : public Snapshot {
 public:
  std::shared_ptr<SnapshotImpl> Clone() const {
    // TODO(gnusi): COW
    auto result = std::make_shared<SnapshotImpl>();
    result->_resolution_table = _resolution_table;
    result->_objects = _objects;
    result->_object_dependencies.reserve(_object_dependencies.size());
    for (auto& [id, dep] : _object_dependencies) {
      result->_object_dependencies.emplace(id, dep->Clone());
    }
    return result;
  }

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(ObjectId db_id) {
    auto db_deps = GetDependency<DatabaseDependency>(db_id);
    auto drop_task = std::make_shared<DatabaseDrop>(db_id);
    drop_task->schemas = db_deps->schemas |
                         std::views::transform([&](ObjectId id) {
                           return CreateSchemaDrop(db_id, id, false);
                         }) |
                         std::ranges::to<std::vector>();
    return drop_task;
  }

  std::shared_ptr<SchemaDrop> CreateSchemaDrop(ObjectId db_id,
                                               ObjectId schema_id,
                                               bool is_root) {
    auto schema_deps = GetDependency<SchemaDependency>(schema_id);
    auto drop_task = std::make_shared<SchemaDrop>(db_id, schema_id, is_root);
    drop_task->tables =
      schema_deps->tables | std::views::transform([&](ObjectId id) {
        auto table = GetObject<Table>(id);
        SDB_ASSERT(table);
        return CreateTableDrop(db_id, schema_id, std::move(table), false);
      }) |
      std::ranges::to<std::vector>();

    return drop_task;
  }

  std::shared_ptr<TableDrop> CreateTableDrop(
    ObjectId db_id, ObjectId schema_id, const std::shared_ptr<Table>& table,
    bool is_root) {
    auto table_deps = GetDependency<TableDependency>(table->GetId());
    auto drop_task = std::make_shared<TableDrop>(
      schema_id, table->GetId(), table->GetTableType(), is_root);
    drop_task->indexes =
      table_deps->indexes | std::views::transform([&](ObjectId id) {
        return CreateIndexDrop(db_id, schema_id, table->GetId(), id, is_root);
      }) |
      std::ranges::to<std::vector>();
    drop_task->shard_id = table_deps->shard_id;
    auto shard = GetObject<TableShard>(table_deps->shard_id);
    SDB_ASSERT(shard);
    drop_task->table_size =
      table->Columns().size() * shard->GetTableStats().num_rows;
    return drop_task;
  }

  std::shared_ptr<IndexDrop> CreateIndexDrop(ObjectId db_id, ObjectId schema_id,
                                             ObjectId table_id,
                                             ObjectId index_id, bool is_root) {
    auto index_deps = GetDependency<IndexDependency>(index_id);
    auto index = GetObject<Index>(index_id);
    auto drop_task = std::make_shared<IndexDrop>(
      db_id, schema_id, table_id, index_id, index->GetIndexType(), is_root);
    drop_task->shard_id = index_deps->shard_id;
    return drop_task;
  }

  template<typename T>
  Result RegisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool replace) {
    if constexpr (std::is_same_v<T, Database>) {
      auto r = AddToResolution<ResolveType::Database>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<DatabaseDependency>(parent_id,
                                                     std::move(object));
    } else if constexpr (std::is_same_v<T, Role>) {
      auto r = AddToResolution<ResolveType::Role>(parent_id, object->GetId(),
                                                  object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, Schema>) {
      auto r = AddToResolution<ResolveType::Schema>(parent_id, object->GetId(),
                                                    object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<SchemaDependency>(parent_id,
                                                   std::move(object));
    } else if constexpr (std::is_same_v<T, View>) {
      auto r = AddToResolution<ResolveType::Relation>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, Function>) {
      auto r = AddToResolution<ResolveType::Function>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition(parent_id, object);
    } else if constexpr (std::is_same_v<T, Table>) {
      auto r = AddToResolution<ResolveType::Relation>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<TableDependency>(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, Index>) {
      auto r = AddToResolution<ResolveType::Relation>(
        object->GetSchemaId(), object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<IndexDependency>(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, TableShard>) {
      return AddObjectDefinition(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, IndexShard>) {
      return AddObjectDefinition(parent_id, std::move(object));
    } else {
      static_assert(false);
    }
  }

  template<typename T>
  void UnregisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool maybe_not_found = false) noexcept {
    if constexpr (std::is_same_v<T, Database>) {
      RemoveFromResolution<ResolveType::Database>(parent_id, object->GetName(),
                                                  maybe_not_found);
    } else if constexpr (std::is_same_v<T, Role>) {
      RemoveFromResolution<ResolveType::Role>(parent_id, object->GetName(),
                                              maybe_not_found);
    } else if constexpr (std::is_same_v<T, Schema>) {
      RemoveFromResolution<ResolveType::Schema>(parent_id, object->GetName(),
                                                maybe_not_found);
    } else if constexpr (std::is_same_v<T, View>) {
      RemoveFromResolution<ResolveType::Relation>(parent_id, object->GetName(),
                                                  maybe_not_found);
    } else if constexpr (std::is_same_v<T, Function>) {
      RemoveFromResolution<ResolveType::Function>(parent_id, object->GetName(),
                                                  maybe_not_found);
    } else if constexpr (std::is_same_v<T, Table>) {
      RemoveFromResolution<ResolveType::Relation>(parent_id, object->GetName(),
                                                  maybe_not_found);
    } else if constexpr (std::is_same_v<T, Index>) {
      RemoveFromResolution<ResolveType::Relation>(
        object->GetSchemaId(), object->GetName(), maybe_not_found);
      parent_id = object->GetRelationId();
    } else if constexpr (std::is_same_v<T, TableShard>) {
    } else if constexpr (std::is_same_v<T, IndexShard>) {
    } else {
      static_assert(false);
    }
    SDB_ASSERT(parent_id.isSet());
    RemoveObjectDefinition(parent_id, object->GetId(), true, maybe_not_found);
  }

  template<ResolveType Type>
  Result AddToResolution(ObjectId parent_id, ObjectId id, std::string_view name,
                         bool replace) {
    return _resolution_table.AddObject<Type>(parent_id, name, id, replace);
  }

  template<ResolveType Type>
  void RemoveFromResolution(ObjectId parent_id, std::string_view name,
                            bool maybe_not_found = false) noexcept {
    auto res = _resolution_table.RemoveObject<Type>(parent_id, name);
    if (!maybe_not_found) {
      SDB_ASSERT(res);
    }
  }

  template<typename DependencyType = void>
  Result AddObjectDefinition(ObjectId parent_id,
                             std::shared_ptr<Object> object) {
    if constexpr (!std::is_void_v<DependencyType>) {
      auto [_, inserted] = _object_dependencies.try_emplace(
        object->GetId(), std::make_shared<DependencyType>());
      SDB_ASSERT(inserted);
    }
    SDB_ASSERT(object->GetId().isSet());
    switch (object->GetType()) {
      case ObjectType::Database:
      case ObjectType::Role:
        break;
      case ObjectType::Schema: {
        auto db_deps = GetDependency<DatabaseDependency>(parent_id);
        db_deps->schemas.insert(object->GetId());
      } break;
      case ObjectType::Table: {
        auto schema_deps = GetDependency<SchemaDependency>(parent_id);
        schema_deps->tables.insert(object->GetId());
      } break;
      case ObjectType::Function: {
        auto schema_deps = GetDependency<SchemaDependency>(parent_id);
        schema_deps->functions.insert(object->GetId());
      } break;
      case ObjectType::View: {
        auto schema_deps = GetDependency<SchemaDependency>(parent_id);
        schema_deps->views.insert(object->GetId());
      } break;
      case ObjectType::Index: {
        auto table_deps = GetDependency<TableDependency>(parent_id);
        table_deps->indexes.insert(object->GetId());
      } break;
      case ObjectType::TableShard: {
        auto table_deps = GetDependency<TableDependency>(parent_id);
        table_deps->shard_id = object->GetId();
      } break;
      case ObjectType::IndexShard: {
        auto index_deps = GetDependency<IndexDependency>(parent_id);
        index_deps->shard_id = object->GetId();
      } break;
      default:
        SDB_UNREACHABLE();
    }
    auto [_, inserted] = _objects.insert(std::move(object));
    SDB_ASSERT(inserted);
    return {};
  }

  std::vector<std::shared_ptr<Role>> GetRoles() const final {
    return _resolution_table.GetRoleIds() |
           std::views::transform([&](ObjectId role_id) {
             auto it = _objects.find(role_id);
             SDB_ASSERT(it != _objects.end());
             return basics::downCast<Role>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Database>> GetDatabases() const final {
    return _resolution_table.GetDatabaseIds() |
           std::views::transform([&](ObjectId db_id) {
             auto it = _objects.find(db_id);
             SDB_ASSERT(it != _objects.end());
             return basics::downCast<Database>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Schema>> GetSchemas(ObjectId db_id) const final {
    auto db_deps = GetDependency<DatabaseDependency>(db_id);
    return db_deps->schemas | std::views::transform([&](ObjectId schema_id) {
             auto it = _objects.find(schema_id);
             SDB_ASSERT(it != _objects.end());
             return basics::downCast<Schema>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<SchemaObject>> GetRelations(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        return _resolution_table.GetRelationIds(db_id, schema_id) |
               std::views::transform(
                 [&](ObjectId relation_id) -> std::shared_ptr<SchemaObject> {
                   return GetObject<SchemaObject>(relation_id);
                 }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<SchemaObject>>{});
  }

  std::vector<std::shared_ptr<Function>> GetFunctions(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        auto schema_deps = GetDependency<SchemaDependency>(schema_id);
        return schema_deps->functions |
               std::views::transform([&](ObjectId function_id) {
                 auto it = _objects.find(function_id);
                 SDB_ASSERT(it != _objects.end());
                 return basics::downCast<Function>(*it);
               }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<Function>>{});
  }

  std::shared_ptr<Database> GetDatabase(std::string_view database) const final {
    return _resolution_table
      .ResolveObject<ResolveType::Database>(id::kInstance, database)
      .transform([&](ObjectId db_id) {
        auto it = _objects.find(db_id);
        SDB_ASSERT(it != _objects.end());
        return basics::downCast<Database>(*it);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Schema> GetSchema(ObjectId db_id,
                                    std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        auto it = _objects.find(schema_id);
        SDB_ASSERT(it != _objects.end());
        return basics::downCast<Schema>(*it);
      })
      .value_or(nullptr);
  }

  bool CheckSchemaEmptyDependency(ObjectId schema_id) {
    auto it = _object_dependencies.find(schema_id);
    SDB_ASSERT(it != _object_dependencies.end());
    auto& deps = basics::downCast<SchemaDependency>(*it->second);
    return deps.Empty();
  }

  std::shared_ptr<SchemaObject> GetRelation(
    ObjectId db_id, std::string_view schema,
    std::string_view relation) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .and_then([&](ObjectId schema_id) {
        return _resolution_table.ResolveObject<ResolveType::Relation>(schema_id,
                                                                      relation);
      })
      .transform([&](ObjectId relation_id) {
        auto it = _objects.find(relation_id);
        SDB_ASSERT(it != _objects.end());
        return basics::downCast<SchemaObject>(*it);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Function> GetFunction(ObjectId db_id, std::string_view schema,
                                        std::string_view function) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .and_then([&](ObjectId schema_id) {
        return _resolution_table.ResolveObject<ResolveType::Function>(schema_id,
                                                                      function);
      })
      .transform([&](ObjectId function_id) {
        auto it = _objects.find(function_id);
        SDB_ASSERT(it != _objects.end());
        return basics::downCast<Function>(*it);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Table> GetTable(ObjectId db_id, std::string_view schema,
                                  std::string_view table) const final {
    auto rel = GetRelation(db_id, schema, table);
    if (!rel) {
      return nullptr;
    }
    return basics::downCast<Table>(rel);
  }

  std::shared_ptr<Object> GetObject(ObjectId id) const final {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
      return nullptr;
    }
    return *it;
  }

  std::shared_ptr<TableShard> GetTableShard(ObjectId table_id) const final {
    auto table_deps = GetDependency<TableDependency>(table_id);
    if (!table_deps->shard_id.isSet()) {
      return nullptr;
    }
    return GetObject<TableShard>(table_deps->shard_id);
  }

  std::shared_ptr<IndexShard> GetIndexShard(ObjectId index_id) const final {
    auto index_deps = GetDependency<IndexDependency>(index_id);
    if (!index_deps->shard_id.isSet()) {
      return nullptr;
    }
    return GetObject<IndexShard>(index_deps->shard_id);
  }

  std::vector<std::shared_ptr<IndexShard>> GetIndexShardsByTable(
    ObjectId id) const final {
    auto table_dep = GetDependency<TableDependency>(id);
    return table_dep->indexes | std::views::transform([&](auto index_id) {
             return GetIndexShard(index_id);
           }) |
           std::ranges::to<std::vector>();
  }

  template<ResolveType Type>
  std::optional<ObjectId> GetObjectId(ObjectId parent_id,
                                      std::string_view name) const {
    return _resolution_table.ResolveObject<Type>(parent_id, name);
  }

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
      return {};
    }
    return std::dynamic_pointer_cast<T>(*it);
  }

  std::shared_ptr<Role> GetRole(std::string_view name) const final {
    auto id =
      _resolution_table.ResolveObject<ResolveType::Role>(id::kInstance, name);
    if (!id) {
      return {};
    }
    auto role = GetObject<Role>(*id);
    SDB_ASSERT(role);
    return role;
  }

  std::shared_ptr<Database> GetDatabase(ObjectId database) const final {
    auto obj = GetObject(database);
    if (!obj) {
      return nullptr;
    }
    return basics::downCast<Database>(obj);
  }

  template<ResolveType Type>
  Result ReplaceObject(ObjectId parent_id, std::string_view old_name,
                       std::shared_ptr<Object> new_object) {
    if (old_name != new_object->GetName()) {
      auto removed = _resolution_table.RemoveObject<Type>(parent_id, old_name);
      SDB_ASSERT(removed);
      auto r = _resolution_table.AddObject<Type>(
        parent_id, new_object->GetName(), new_object->GetId(), false);
      if (!r.ok()) {
        return r;
      }
    } else {
      // Name unchanged, but must refresh the string_view to point to new
      // object's _name
      auto r = _resolution_table.AddObject<Type>(
        parent_id, new_object->GetName(), new_object->GetId(), true);
      SDB_ASSERT(r.ok());
    }

    auto it = _objects.find(new_object->GetId());
    SDB_ASSERT(it != _objects.end());
    SDB_ASSERT((*it)->GetId() == new_object->GetId());
    const_cast<std::shared_ptr<Object>&>(*it) = std::move(new_object);
    return {};
  }

 private:
  template<typename T>
  std::shared_ptr<T> GetDependency(ObjectId id) const {
    auto it = _object_dependencies.find(id);
    SDB_ASSERT(it != _object_dependencies.end());
    auto deps = it->second;
    SDB_ASSERT(deps);
    return basics::downCast<T>(deps);
  }

  void RemoveObjectDefinition(ObjectId parent_id, ObjectId id,
                              bool root = false,
                              bool maybe_not_found = false) noexcept {
    auto node = _objects.extract(id);
    if (maybe_not_found && node.empty()) {
      return;
    }
    SDB_ASSERT(!node.empty());
    std::shared_ptr<Object> obj = node.value();
    SDB_ASSERT(obj);
    auto drop_childs = [&](const auto& deps) {
      for (auto child_id : deps) {
        RemoveObjectDefinition(id, child_id);
      }
    };
    // Drop from parent deps
    if (root) {
      switch (obj->GetType()) {
        case ObjectType::Database:
        case ObjectType::Role:
          break;
        case ObjectType::Schema: {
          auto db_deps = GetDependency<DatabaseDependency>(parent_id);
          SDB_ASSERT(db_deps);
          db_deps->schemas.erase(id);
        } break;
        case ObjectType::Index: {
          auto table_deps = GetDependency<TableDependency>(parent_id);
          SDB_ASSERT(table_deps);
          table_deps->indexes.erase(id);
        } break;
        case ObjectType::Function: {
          auto schema_deps = GetDependency<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->functions.erase(id);
        } break;
        case ObjectType::Table: {
          auto schema_deps = GetDependency<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->tables.erase(id);
        } break;
        case ObjectType::View: {
          auto schema_deps = GetDependency<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->views.erase(id);
        } break;
        default:
          SDB_UNREACHABLE();
      }
    }
    // Drop childs
    switch (obj->GetType()) {
      case ObjectType::Database: {
        auto db_deps = GetDependency<DatabaseDependency>(id);
        drop_childs(db_deps->schemas);
      } break;
      case ObjectType::Role:
        break;
      case ObjectType::Schema: {
        auto schema_deps = GetDependency<SchemaDependency>(id);
        drop_childs(schema_deps->functions);
        drop_childs(schema_deps->views);
        drop_childs(schema_deps->tables);
      } break;
      case ObjectType::Table: {
        auto table_deps = GetDependency<TableDependency>(id);
        if (table_deps->shard_id.isSet()) {
          RemoveObjectDefinition(id, table_deps->shard_id);
          table_deps->shard_id = ObjectId::none();
        }
        auto index_ids = table_deps->indexes;
        if (root) {
          for (auto index_id : index_ids) {
            // While `DROP TABLE` statement indexes were not deleted from
            // resolution table, because they were nested in schema scope.
            // Therefore, we have to explicitly erase them here.
            auto index = GetObject<Index>(index_id);
            UnregisterObject(index, id, false);
          }
        }
      } break;
      case ObjectType::Index: {
        auto index_deps = GetDependency<IndexDependency>(id);
        if (index_deps->shard_id.isSet()) {
          RemoveObjectDefinition(id, index_deps->shard_id);
          index_deps->shard_id = ObjectId::none();
        }
      } break;
      case ObjectType::Function:
      case ObjectType::View:
        break;
      case ObjectType::TableShard:
      case ObjectType::IndexShard:
        SDB_ASSERT(!root);
        break;
      default:
        SDB_UNREACHABLE();
    }
    _object_dependencies.erase(id);
  }

  template<typename W>
  Result ResolveRole(this auto&& self, std::string_view role, W&& writer) {
    auto role_it = self._roles.find(role);
    if (role_it == self._roles.end()) {
      return {ERROR_USER_NOT_FOUND, "Role not found: ", role};
    }

    return writer(role_it);
  }

  template<typename T>
  using ObjectSetByName =
    containers::FlatHashSet<std::shared_ptr<T>, ObjectByName, ObjectByName>;
  template<typename T>
  using ObjectSetById =
    containers::FlatHashSet<std::shared_ptr<T>, ObjectById, ObjectById>;
  template<typename K, typename V>
  using ObjectMapByName =
    containers::NodeHashMap<std::shared_ptr<K>, V, ObjectByName, ObjectByName>;
  template<typename K, typename V>
  using ObjectMapById =
    containers::FlatHashMap<std::shared_ptr<K>, V, ObjectById, ObjectById>;

  struct SchemaObjects {
    bool empty() const { return relations.empty() && functions.empty(); }

    bool VisitObjects(auto&& writer) {
      for (auto& object : relations) {
        if (!writer(object)) {
          return false;
        }
      }
      for (auto& object : functions) {
        if (!writer(object)) {
          return false;
        }
      }
      return true;
    }

    ObjectSetByName<SchemaObject> relations;
    ObjectSetByName<SchemaObject> functions;
  };

  ResolutionTable _resolution_table;
  containers::FlatHashMap<ObjectId, std::shared_ptr<ObjectDependencyBase>>
    _object_dependencies;
  ObjectSetById<Object> _objects;
};

LocalCatalog::LocalCatalog(bool skip_background_errors)
  : _snapshot(std::make_shared<SnapshotImpl>()),
    _engine{&GetServerEngine()},
    _skip_background_errors{skip_background_errors} {}

Result LocalCatalog::RegisterRole(std::shared_ptr<Role> role) {
  SDB_INFO("xxxxx", Logger::FIXME, "Register role ", role->GetName());
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(role), id::kInstance, false);
  });
}

Result LocalCatalog::RegisterDatabase(std::shared_ptr<Database> database) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(database), id::kInstance, false);
  });
}

Result LocalCatalog::RegisterSchema(ObjectId database_id,
                                    std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(schema), database_id, false);
  });
}

Result LocalCatalog::RegisterView(ObjectId schema_id,
                                  std::shared_ptr<View> view) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(view), schema_id, false);
  });
}

Result LocalCatalog::RegisterTable(ObjectId database_id, ObjectId schema_id,
                                   CreateTableOptions options) {
  auto table = std::make_shared<Table>(std::move(options), database_id);

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(table, schema_id, false);
  });
}

Result LocalCatalog::RegisterFunction(ObjectId database_id, ObjectId schema_id,
                                      std::shared_ptr<Function> function) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(function), schema_id, false);
  });
}

Result LocalCatalog::CreateDatabase(std::shared_ptr<Database> database) {
  const auto owner_id = database->GetOwnerId();
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(database, id::kInstance, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      {
        vpack::Builder builder;
        database->WriteInternal(builder);
        auto r = _engine->CreateDefinition(
          id::kInstance, RocksDBEntryType::Database, database_id,
          [&](bool) { return builder.slice(); });

        if (!r.ok()) {
          return r;
        }
      }

      auto schema = std::make_shared<Schema>(
        database_id, SchemaOptions{
                       .owner_id = owner_id,
                       .name = std::string{StaticStrings::kPublic},
                     });
      r = clone->RegisterObject(schema, database_id, false);
      SDB_ASSERT(r.ok());
      vpack::Builder builder;
      schema->WriteInternal(builder);

      return _engine->CreateDefinition(database_id, RocksDBEntryType::Schema,
                                       schema->GetId(),
                                       [&](bool) { return builder.slice(); });
    },
    [&](auto clone) {
      clone->UnregisterObject(database, id::kInstance, true);
    });
}

Result LocalCatalog::CreateSchema(ObjectId database_id,
                                  std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot,
    [&](auto& clone) {
      if (auto r = clone->RegisterObject(schema, database_id, false); !r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      vpack::Builder builder;
      schema->WriteInternal(builder);

      return _engine->CreateDefinition(database_id, RocksDBEntryType::Schema,
                                       schema->GetId(),
                                       [&](bool) { return builder.slice(); });
    },
    [&](auto clone) { clone->UnregisterObject(schema, database_id, true); });
}

Result LocalCatalog::CreateRole(std::shared_ptr<Role> role) {
  SDB_INFO("xxxxx", Logger::FIXME, "Creating role ", role->GetName());
  absl::MutexLock lock{&_mutex};
  auto r = Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(role, id::kInstance, false);
      if (!r.ok()) {
        return r;
      }
      vpack::Builder b;
      b.openObject();
      role->WriteInternal(b);
      b.close();
      return _engine->CreateDefinition(id::kInstance, RocksDBEntryType::Role,
                                       role->GetId(),
                                       [&](bool) { return b.slice(); });
    },
    [&](auto& clone) { clone->UnregisterObject(role, id::kInstance, true); });

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  return {};
}

ResultOr<std::shared_ptr<Index>> LocalCatalog::RegisterIndex(
  ObjectId database_id, ObjectId schema_id, ObjectId id, ObjectId relation_id,
  IndexBaseOptions options) {
  auto index =
    MakeIndex(database_id, schema_id, id, relation_id, std::move(options));
  if (!index) {
    return std::unexpected<Result>(std::in_place, std::move(index).error());
  }

  absl::MutexLock lock{&_mutex};

  auto r = Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(*index, relation_id, false);
  });
  if (!r.ok()) {
    return std::unexpected<Result>(std::in_place, r.errorNumber(),
                                   r.errorMessage());
  }
  return *index;
}

Result LocalCatalog::RegisterIndexShard(std::shared_ptr<IndexShard> shard) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(shard, shard->GetIndexId(), false);
  });
}

Result LocalCatalog::RegisterTableShard(std::shared_ptr<TableShard> shard) {
  absl::MutexLock lock{&_mutex};

  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(shard, shard->GetTableId(), false);
  });
}

Result LocalCatalog::CreateIndex(
  ObjectId database_id, std::string_view relation_schema,
  std::string_view relation_name, const std::vector<std::string>& column_names,
  IndexBaseOptions options, IndexShardOptions& shard_options,
  CreateIndexOperationOptions operation_options) {
  if (column_names.empty()) {
    return Result{ERROR_BAD_PARAMETER, "Cannot create index without columns"};
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, relation_schema);
  if (!schema_id) {
    return {ERROR_SERVER_ILLEGAL_NAME, "Cannot resolve schema \"",
            relation_schema, "\""};
  }

  auto relation =
    _snapshot->GetRelation(database_id, relation_schema, relation_name);
  if (!relation) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation_name,
            "\" does not exist"};
  }
  if (relation->GetType() != catalog::ObjectType::Table) {
    return Result{ERROR_NOT_IMPLEMENTED, "Only table indexes are supported"};
  }

  auto& table = basics::downCast<Table>(*relation);
  auto& columns = table.Columns();
  auto find_column = [&](std::string_view name) {
    auto it = absl::c_find_if(
      columns, [&](const catalog::Column& c) { return c.name == name; });
    return it != columns.end() ? &*it : nullptr;
  };

  std::vector<const catalog::Column*> index_columns;
  index_columns.reserve(column_names.size());
  options.column_ids.reserve(column_names.size());
  for (const auto& name : column_names) {
    const auto* column = find_column(name);
    if (!column) {
      return Result{ERROR_BAD_PARAMETER, "column \"", name,
                    "\" does not exist"};
    }
    options.column_ids.push_back(column->id);
    index_columns.push_back(column);
  }

  auto validation_res = ValidateIndexOptions(options, index_columns);
  if (validation_res.fail()) {
    return validation_res;
  }

  auto index = MakeIndex(database_id, *schema_id, ObjectId{0}, table.GetId(),
                         std::move(options));
  if (!index) {
    return std::move(index).error();
  }

  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(*index, (*index)->GetRelationId(), false);
      if (!r.ok()) {
        return r;
      }
      auto shard = (*index)->CreateIndexShard(true, ObjectId{0}, shard_options);
      if (!shard) {
        return std::move(shard).error();
      }
      r = clone->RegisterObject(*shard, (*index)->GetId(), false);
      SDB_ASSERT(r.ok());

      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      if (operation_options.create_with_tombstone) {
        r =
          _engine->WriteTombstone((*index)->GetRelationId(), (*index)->GetId());
        if (!r.ok()) {
          return r;
        }
        (*index)->SetTombstoned(true);
      }
      {  // Write index definition
        vpack::Builder b;
        (*index)->WriteInternal(b);
        r = _engine->CreateDefinition(
          (*index)->GetRelationId(), RocksDBEntryType::Index, (*index)->GetId(),
          [&](bool) { return b.slice(); });
        if (!r.ok()) {
          return r;
        }
      }
      {  // Write index shard definition
        vpack::Builder b;
        (*shard)->WriteInternal(b);

        r = _engine->CreateDefinition(
          (*index)->GetId(), RocksDBEntryType::IndexShard, (*shard)->GetId(),
          [&](bool) { return b.slice(); });
        if (!r.ok()) {
          return r;
        }
      }
      return Result{};
    },
    [&](auto& clone) {
      clone->UnregisterObject(*index, (*index)->GetRelationId(), true);
    });
}

Result LocalCatalog::CreateView(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<View> view, bool replace) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result(ERROR_SERVER_ILLEGAL_NAME);
  }
  if (replace) {
    // Check replaced object have the same type
    Result r =
      _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, view->GetName())
        .transform([&](ObjectId existed_id) {
          auto existed_object = _snapshot->GetObject<SchemaObject>(existed_id);
          return existed_object->GetType() == ObjectType::View
                   ? Result{}
                   : Result{ERROR_SERVER_ILLEGAL_NAME, "\"", view->GetName(),
                            "\" is not a view"};
        })
        .value_or(Result{});
    if (!r.ok()) {
      return r;
    }
  }

  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(view, *schema_id, replace);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      vpack::Builder builder;
      builder.openObject();
      view->WriteProperties(builder);
      builder.close();

      return _engine->CreateDefinition(
        *schema_id, RocksDBEntryType::View, view->GetId(),
        [&](bool internal) { return builder.slice(); });
    },
    [&](auto clone) { clone->UnregisterObject(view, *schema_id, true); });
}

Result LocalCatalog::CreateFunction(ObjectId database_id,
                                    std::string_view schema,
                                    std::shared_ptr<Function> function,
                                    bool replace) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(function, *schema_id, replace);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      vpack::Builder builder;
      builder.openObject();
      function->WriteInternal(builder);
      builder.close();
      return _engine->CreateDefinition(*schema_id, RocksDBEntryType::Function,
                                       function->GetId(),
                                       [&](bool) { return builder.slice(); });
    },
    [&](auto clone) { clone->UnregisterObject(function, *schema_id, true); });
}

Result LocalCatalog::CreateTable(
  ObjectId database_id, std::string_view schema, CreateTableOptions options,
  CreateTableOperationOptions operation_options) {
  for (auto pk_id : options.pkColumns) {
    auto col = absl::c_find_if(options.columns,
                               [&](const auto& c) { return c.id == pk_id; });
    SDB_ASSERT(col != options.columns.end());
    // PK must be default sortable or we can not guarantee table scan order
    if (col->type->providesCustomComparison()) {
      return {
        ERROR_BAD_PARAMETER, "Column ", col->name,
        " has type with custom comparison and can not be part of primary key"};
    }
    // this is current limitation of our pirmary key builder. And might be
    // lifted some day.
    if (!col->type->isPrimitiveType()) {
      return {ERROR_BAD_PARAMETER, "Column ", col->name,
              " has non primitive type and can not be part of primary key"};
    }
  }
  auto table = std::make_shared<Table>(std::move(options), database_id);
  if (operation_options.create_with_tombstone) {
    table->SetTombstoned(true);
  }
  auto shard = std::make_shared<TableShard>(table->GetId(), TableStats{});

  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(
    _snapshot,
    [&](auto& clone) -> Result {
      auto r = clone->RegisterObject(table, *schema_id, false);
      if (!r.ok()) {
        return r;
      }

      r = clone->RegisterObject(shard, table->GetId(), false);
      SDB_ASSERT(r.ok());
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      if (operation_options.create_with_tombstone) {
        r = _engine->WriteTombstone(*schema_id, table->GetId());
        if (!r.ok()) {
          return r;
        }
      }

      vpack::Builder b;
      b.openObject();
      table->WriteInternal(b);
      b.close();
      r = _engine->CreateDefinition(*schema_id, RocksDBEntryType::Table,
                                    table->GetId(),
                                    [&](bool) { return b.slice(); });
      if (!r.ok()) {
        return r;
      }

      b.clear();
      shard->WriteInternal(b);

      r = _engine->CreateDefinition(
        shard->GetTableId(), RocksDBEntryType::TableShard, shard->GetId(),
        [&](bool) -> vpack::Slice { return b.slice(); });
      return r;
    },
    [&](auto clone) { clone->UnregisterObject(table, *schema_id, true); });
}

Result LocalCatalog::RenameView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto view = basics::downCast<View>(_snapshot->GetObject(*object_id));
  if (!view) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  if (view->GetName() == new_name) {
    return {};
  }

  std::shared_ptr<View> new_view;
  auto r = view->Rename(new_view, new_name);
  if (!r.ok()) {
    return r;
  }

  return Apply(
    _snapshot,
    [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
      if (auto r = clone->ReplaceObject<ResolveType::Relation>(*schema_id, name,
                                                               new_view);
          !r.ok()) {
        return r;
      }

      vpack::Builder builder;
      builder.openObject();
      new_view->WriteProperties(builder);
      builder.close();

      return _engine->CreateDefinition(
        *schema_id, RocksDBEntryType::View, new_view->GetId(),
        [&](bool internal) { return builder.slice(); });
    },
    [&](const std::shared_ptr<SnapshotImpl>& clone) {
      auto obj = clone->GetObject<View>(new_view->GetId());
      if (obj->GetName() == new_view->GetName()) {
        auto r = clone->ReplaceObject<ResolveType::Relation>(*schema_id,
                                                             new_name, view);
        SDB_ASSERT(r.ok());
      }
    });
}

Result LocalCatalog::RenameTable(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto old_table = basics::downCast<Table>(_snapshot->GetObject(*object_id));
  if (!old_table) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  if (old_table->GetName() == new_name) {
    return {};
  }

  NewOptions options{
    .name = new_name,
    .schema = old_table->GetSchema(),
    .number_of_shards = old_table->numberOfShards(),
    .replication_factor = old_table->replicationFactor(),
    .write_concern = old_table->writeConcern(),
    .wait_for_sync = old_table->waitForSync(),
  };

  auto new_table = std::make_shared<Table>(*old_table, std::move(options));

  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot,
    [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
      auto r = clone->ReplaceObject<ResolveType::Relation>(*schema_id, name,
                                                           new_table);
      if (!r.ok()) {
        return r;
      }

      vpack::Builder b;
      b.openObject();
      new_table->WriteInternal(b);
      b.close();
      return _engine->CreateDefinition(*schema_id, RocksDBEntryType::Table,
                                       new_table->GetId(),
                                       [&](bool) { return b.slice(); });
    },
    [&](const std::shared_ptr<SnapshotImpl>& clone) {
      auto obj = clone->GetObject<Table>(new_table->GetId());
      if (obj->GetName() == new_table->GetName()) {
        auto r = clone->ReplaceObject<ResolveType::Relation>(
          *schema_id, new_name, old_table);
        SDB_ASSERT(r.ok());
      }
    });
}

Result LocalCatalog::ChangeRole(std::string_view name,
                                ChangeCallback<Role> new_role) {
  absl::MutexLock lock{&_mutex};
  auto role = _snapshot->GetRole(name);
  if (!role) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  std::shared_ptr<Role> new_role_ptr;
  auto r = new_role(*role, new_role_ptr);
  if (!r.ok()) {
    return r;
  }
  r = Apply(
    _snapshot,
    [&](std::shared_ptr<SnapshotImpl>& clone) {
      auto r = clone->ReplaceObject<ResolveType::Role>(id::kInstance, name,
                                                       new_role_ptr);
      if (!r.ok()) {
        return r;
      }
      vpack::Builder b;
      b.openObject();
      new_role_ptr->WriteInternal(b);
      b.close();
      return _engine->CreateDefinition(id::kInstance, RocksDBEntryType::Role,
                                       new_role_ptr->GetId(),
                                       [&](bool) { return b.slice(); });
    },
    [&](const std::shared_ptr<SnapshotImpl>& clone) {
      auto obj = clone->GetObject<Role>(new_role_ptr->GetId());
      if (obj->GetName() == new_role_ptr->GetName()) {
        auto r = clone->ReplaceObject<ResolveType::Relation>(
          id::kInstance, new_role_ptr->GetName(), role);
        SDB_ASSERT(r.ok());
      }
    });

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  return {};
}

Result LocalCatalog::ChangeView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                ChangeCallback<View> new_view) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto view = basics::downCast<View>(_snapshot->GetObject(*object_id));
  if (!view) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  std::shared_ptr<View> updated;
  auto r = new_view(*view, updated);
  if (!r.ok()) {
    return r;
  }
  if (!updated) {
    return {};
  }
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
    auto r =
      clone->ReplaceObject<ResolveType::Relation>(*schema_id, name, updated);
    if (!r.ok()) {
      return r;
    }

    vpack::Builder builder;
    builder.openObject();
    updated->WriteProperties(builder);
    builder.close();

    return _engine->CreateDefinition(
      *schema_id, RocksDBEntryType::View, updated->GetId(),
      [&](bool internal) { return builder.slice(); });
  });
}

Result LocalCatalog::ChangeTable(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 ChangeCallback<Table> new_table) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto table = basics::downCast<Table>(_snapshot->GetObject(*object_id));
  if (!table) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  std::shared_ptr<Table> updated;
  auto r = new_table(*table, updated);
  if (!r.ok()) {
    return r;
  }
  if (!updated) {
    return {};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
    auto r =
      clone->ReplaceObject<ResolveType::Relation>(*schema_id, name, updated);
    if (!r.ok()) {
      return r;
    }

    return basics::SafeCall([&] {
      vpack::Builder b;
      updated->WriteInternal(b);
      return _engine->CreateDefinition(*schema_id, RocksDBEntryType::Table,
                                       updated->GetId(),
                                       [&](bool) { return b.slice(); });
    });
  });
}

Result LocalCatalog::DropRole(std::string_view role) {
  absl::MutexLock lock{&_mutex};
  auto role_ptr = _snapshot->GetRole(role);
  if (!role_ptr) {
    return {ERROR_SERVER_ILLEGAL_NAME};
  }
  auto r = Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    clone->UnregisterObject(role_ptr, id::kInstance);
    return _engine->DropDefinition(id::kInstance, RocksDBEntryType::Role,
                                   role_ptr->GetId());
  });

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  return {};
}

Result LocalCatalog::DropDatabase(std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    auto db_id = clone->GetObjectId<ResolveType::Database>(id::kInstance, name);
    if (!db_id) {
      return Result{ERROR_SERVER_DATABASE_NOT_FOUND, "database \"", name,
                    "\" does not exist"};
    }
    auto task = clone->CreateDatabaseDrop(*db_id);

    if (auto r = GetServerEngine().WriteTombstone(id::kInstance, *db_id);
        !r.ok()) {
      return r;
    }
    clone->UnregisterObject(clone->GetObject<Database>(*db_id), id::kInstance);
    // Check that SereneDB won't open this database after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::DropSchema(ObjectId db_id, std::string_view name,
                                bool cascade) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    auto schema_id = clone->GetObjectId<ResolveType::Schema>(db_id, name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME, "schema \"", name,
                    "\" does not exist"};
    }

    if (!cascade && !clone->CheckSchemaEmptyDependency(*schema_id)) {
      return Result{ERROR_BAD_PARAMETER, "cannot drop schema ", name,
                    " because other objects depend on it"};
    }

    auto task = clone->CreateSchemaDrop(db_id, *schema_id, true);

    if (auto r = _engine->WriteTombstone(db_id, *schema_id); !r.ok()) {
      return r;
    }
    clone->UnregisterObject(clone->GetObject<Schema>(*schema_id), db_id);
    // Check that SereneDB won't open this schema after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::DropTable(ObjectId db_id, std::string_view schema_name,
                               std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    auto schema_id =
      clone->GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto table_id = clone->GetObjectId<ResolveType::Relation>(*schema_id, name);
    if (!table_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto table = clone->GetObject<Table>(*table_id);
    SDB_ASSERT(table);
    auto task = clone->CreateTableDrop(db_id, *schema_id, table, true);
    if (auto r = _engine->WriteTombstone(*schema_id, *table_id); !r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(table), *schema_id);
    // Check that SereneDB won't open this table after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::RemoveTombstone(ObjectId object_id) {
  absl::MutexLock lock{&_mutex};

  auto object = _snapshot->GetObject(object_id);
  if (!object) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  ObjectId tombstone_parent;
  if (object->GetType() == ObjectType::Index) {
    auto& index = basics::downCast<Index>(*object);
    tombstone_parent = index.GetRelationId();
  } else {
    auto& schema_obj = basics::downCast<SchemaObject>(*object);
    tombstone_parent = schema_obj.GetSchemaId();
  }

  auto r = _engine->DropDefinition(tombstone_parent,
                                   RocksDBEntryType::Tombstone, object_id);

  // Unlike most catalog operations that clone the snapshot, here we modify the
  // object in-place because the tombstone flag is simple in-memory state.
  auto& schema_obj = basics::downCast<SchemaObject>(*object);
  schema_obj.SetTombstoned(false);

  return r;
}

Result LocalCatalog::DropIndex(ObjectId db_id, std::string_view schema_name,
                               std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto schema_id =
      clone->GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto index_id = clone->GetObjectId<ResolveType::Relation>(*schema_id, name);
    if (!index_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto index = clone->GetObject<Index>(*index_id);
    SDB_ASSERT(index);
    if (auto r = _engine->WriteTombstone(index->GetRelationId(), *index_id);
        !r.ok()) {
      return r;
    }
    // Check that SereneDB won't open this index after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }

    auto task = clone->CreateIndexDrop(db_id, *schema_id,
                                       index->GetRelationId(), *index_id, true);
    clone->UnregisterObject(index, *schema_id);
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::DropView(ObjectId db_id, std::string_view schema_name,
                              std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    auto schema_id =
      clone->GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto view_id = clone->GetObjectId<ResolveType::Relation>(*schema_id, name);
    if (!view_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto view = clone->GetObject<View>(*view_id);
    SDB_ASSERT(view);
    auto r = _engine->DropDefinition(*schema_id, RocksDBEntryType::View,
                                     view->GetId());
    if (!r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(view), *schema_id);
    return Result{};
  });
}

Result LocalCatalog::DropFunction(ObjectId db_id, std::string_view schema_name,
                                  std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    auto schema_id =
      clone->GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto func_id = clone->GetObjectId<ResolveType::Function>(*schema_id, name);
    if (!func_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto func = clone->GetObject<Function>(*func_id);
    SDB_ASSERT(func);
    auto r =
      _engine->DropDefinition(*schema_id, RocksDBEntryType::Function, *func_id);
    if (!r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(func), *schema_id);
    return Result{};
  });
}

std::shared_ptr<const Snapshot> LocalCatalog::GetSnapshot() const noexcept {
  return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
}

}  // namespace sdb::catalog
