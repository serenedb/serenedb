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
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "search/inverted_index_shard.h"
#include "storage_engine/engine_feature.h"
#include "storage_engine/index_shard.h"
#include "storage_engine/search_engine.h"
#include "utils/exec_context.h"
#include "utils/operation_options.h"
#include "utils/query_cache.h"
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
namespace {

auto MakePropertiesWriter() {
  return [b = vpack::Builder{}](const DatabaseObject& object,
                                bool internal) mutable {
    b.clear();
    b.openObject();
    if (internal) {
      object.WriteInternal(b);
    } else {
      object.WriteProperties(b);
    }
    b.close();

    return b.slice();
  };
}

Result RegisterDatabaseImpl(ObjectId id, std::string_view name,
                            bool start_applier) {
#ifdef SDB_CLUSTER
  if (ServerState::instance()->IsAgent()) {
    return {};
  }

  auto& server = SerenedServer::Instance();
  server.getFeature<QueryRegistryFeature>().AddDatabase(id);
  server.getFeature<ReplicationFeature>().AddReplicationClients(id);
  auto* applier = AddReplicationApplier(id, name);

  if (!start_applier) {
    return {};
  }

  return basics::SafeCall([&] {
    if (auto replication = server.TryGetFeature<ReplicationFeature>();
        replication) {
      replication->startApplier(name, *applier);
    }
  });
#else
  return {};
#endif
}

Result Apply(auto& snapshot, auto&& f) {
  auto clone =
    std::atomic_load_explicit(&snapshot, std::memory_order_relaxed)->Clone();
  if (auto r = f(clone); !r.ok()) {
    return r;
  }
  std::atomic_store_explicit(&snapshot, std::move(clone),
                             std::memory_order_release);
  return {};
}

}  // namespace

class SnapshotImpl : public Snapshot {
 public:
  using ObjectWithDependencies =
    std::tuple<std::shared_ptr<Object>, std::shared_ptr<ObjectDependencyBase>>;
  std::shared_ptr<SnapshotImpl> Clone() const {
    // TODO(gnusi): COW
    auto result = std::make_shared<SnapshotImpl>();
    result->_roles = _roles;
    result->_resolution_table = _resolution_table;
    result->_objects = _objects;
    result->_object_dependencies = _object_dependencies;
    return result;
  }

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(ObjectId db_id) {
    auto deps = _object_dependencies.find(db_id);
    SDB_ASSERT(deps != _object_dependencies.end());
    auto& db_deps = basics::downCast<ObjectDependency>(*deps->second);
    auto drop_task = std::make_shared<DatabaseDrop>();
    drop_task->schemas = db_deps.objects |
                         std::views::transform([&](ObjectId id) {
                           return CreateSchemaDrop(db_id, id, false);
                         }) |
                         std::ranges::to<std::vector>();
    drop_task->parent_id = ObjectId{0};
    drop_task->id = db_id;
    drop_task->is_root = true;
    return drop_task;
  }

  std::shared_ptr<SchemaDrop> CreateSchemaDrop(ObjectId db_id,
                                               ObjectId schema_id,
                                               bool is_root) {
    auto deps = _object_dependencies.find(schema_id);
    SDB_ASSERT(deps != _object_dependencies.end());
    auto& schema_deps = basics::downCast<SchemaDependency>(*deps->second);
    auto drop_task = std::make_shared<SchemaDrop>();
    drop_task->tables = schema_deps.tables |
                        std::views::transform([&](ObjectId id) {
                          return CreateTableDrop(db_id, schema_id, id, false);
                        }) |
                        std::ranges::to<std::vector>();

    auto functions =
      schema_deps.functions | std::views::transform([&](ObjectId id) {
        return CreateDefinitionDrop(schema_id, id, RocksDBEntryType::Function,
                                    false);
      });
    auto views = schema_deps.views | std::views::transform([&](ObjectId id) {
                   return CreateDefinitionDrop(schema_id, id,
                                               RocksDBEntryType::View, false);
                 });
    drop_task->definitions.append_range(functions);
    drop_task->definitions.append_range(views);

    drop_task->parent_id = db_id;
    drop_task->id = schema_id;
    drop_task->is_root = is_root;
    return drop_task;
  }

  std::shared_ptr<TableDrop> CreateTableDrop(ObjectId db_id, ObjectId schema_id,
                                             ObjectId table_id, bool is_root) {
    auto deps = _object_dependencies.find(table_id);
    SDB_ASSERT(deps != _object_dependencies.end());
    auto& table_deps = basics::downCast<TableDependency>(*deps->second);
    auto drop_task = std::make_shared<TableDrop>();
    drop_task->parent_id = schema_id;
    drop_task->id = table_id;
    drop_task->is_root = is_root;
    drop_task->indexes =
      table_deps.indexes | std::views::transform([&](ObjectId id) {
        return CreateIndexDrop(db_id, schema_id, table_id, id, is_root);
      }) |
      std::ranges::to<std::vector>();
    drop_task->shard_id = table_deps.shard_id;
    return drop_task;
  }

  std::shared_ptr<IndexDrop> CreateIndexDrop(ObjectId db_id, ObjectId schema_id,
                                             ObjectId table_id,
                                             ObjectId index_id, bool is_root) {
    auto drop_task = std::make_shared<IndexDrop>();
    auto deps = _object_dependencies.find(index_id);
    SDB_ASSERT(deps != _object_dependencies.end());
    auto& index_deps = basics::downCast<IndexDependency>(*deps->second);
    auto obj = _objects.find(index_id);
    SDB_ASSERT(obj != _objects.end());
    auto& index = basics::downCast<Index>(**obj);
    drop_task->parent_id = table_id;
    drop_task->id = index_id;
    drop_task->is_root = is_root;
    drop_task->type = index.GetIndexType();
    drop_task->shard_id = index_deps.shard_id;
    drop_task->schema_id = schema_id;
    drop_task->db_id = db_id;
    return drop_task;
  }

  std::shared_ptr<DefinitionDrop> CreateDefinitionDrop(ObjectId parent_id,
                                                       ObjectId definition_id,
                                                       RocksDBEntryType type,
                                                       bool is_root) {
    auto drop_task = std::make_shared<DefinitionDrop>(
      DropTask{.parent_id = parent_id, .id = definition_id, .is_root = is_root},
      type);
    return drop_task;
  }

  std::vector<std::shared_ptr<Role>> GetRoles() const final {
    return {_roles.begin(), _roles.end()};
  }

  std::vector<std::shared_ptr<Database>> GetDatabases() const final {
    return _resolution_table.GetDatabaseIds() |
           std::views::transform([&](ObjectId db_id) {
             auto it = _objects.find(db_id);
             SDB_ASSERT(it != _objects.end());
             return std::dynamic_pointer_cast<Database>(*it);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Schema>> GetSchemas(ObjectId db_id) const final {
    return _resolution_table.GetSchemaIds(db_id) |
           std::views::transform([&](ObjectId schema_id) {
             auto it = _objects.find(schema_id);
             SDB_ASSERT(it != _objects.end());
             return std::dynamic_pointer_cast<Schema>(*it);
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
                   auto it = _objects.find(relation_id);
                   SDB_ASSERT(it != _objects.end());
                   return std::dynamic_pointer_cast<SchemaObject>(*it);
                 }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<SchemaObject>>());
  }

  std::vector<std::shared_ptr<Function>> GetFunctions(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        return _resolution_table.GetFunctionIds(db_id, schema_id) |
               std::views::transform([&](ObjectId function_id) {
                 auto it = _objects.find(function_id);
                 SDB_ASSERT(it != _objects.end());
                 return std::dynamic_pointer_cast<Function>(*it);
               }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<Function>>());
  }

  std::shared_ptr<Database> GetDatabase(std::string_view database) const final {
    return _resolution_table
      .ResolveObject<ResolveType::Database>(ObjectId{0}, database)
      .transform([&](ObjectId db_id) {
        auto it = _objects.find(db_id);
        SDB_ASSERT(it != _objects.end());
        return std::dynamic_pointer_cast<Database>(*it);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Schema> GetSchema(ObjectId db_id,
                                    std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        auto it = _objects.find(schema_id);
        SDB_ASSERT(it != _objects.end());
        return std::dynamic_pointer_cast<Schema>(*it);
      })
      .value_or(nullptr);
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
        return std::dynamic_pointer_cast<SchemaObject>(*it);
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
        return std::dynamic_pointer_cast<Function>(*it);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Table> GetTable(ObjectId db_id, std::string_view schema,
                                  std::string_view table) const final {
    auto rel = GetRelation(db_id, schema, table);
    if (!rel) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Table>(rel);
  }

  std::shared_ptr<Object> GetObject(ObjectId id) const final {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
      return nullptr;
    }
    return *it;
  }

  std::shared_ptr<TableShard> GetTableShard(ObjectId id) const final {
    auto obj = GetObject(id);
    if (!obj) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<TableShard>(obj);
  }

  std::shared_ptr<IndexShard> GetIndexShard(ObjectId id) const final {
    auto obj = GetObject(id);
    if (!obj) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<IndexShard>(obj);
  }

  std::vector<std::shared_ptr<IndexShard>> GetIndexShardsByTable(
    ObjectId id) const final {
    auto dep_it = _object_dependencies.find(id);
    SDB_ASSERT(dep_it != _object_dependencies.end());
    SDB_ASSERT(dep_it->second);
    auto& table_dep = basics::downCast<TableDependency>(*dep_it->second);
    return table_dep.indexes | std::views::transform([&](const auto& index_id) {
             auto deps = _object_dependencies.find(index_id);
             SDB_ASSERT(deps != _object_dependencies.end());
             auto& index_deps =
               basics::downCast<IndexDependency>(*deps->second);
             return index_deps.shard_id;
           }) |
           std::views::transform(
             [&](ObjectId shard_id) { return GetIndexShard(shard_id); }) |
           std::ranges::to<std::vector>();
  }

  template<ResolveType Type>
  std::optional<ObjectId> GetObjectId(ObjectId parent_id,
                                      std::string_view name) {
    return _resolution_table.ResolveObject<Type>(parent_id, name);
  }

  template<ResolveType Type>
  std::optional<ObjectId> UnregisterObject(ObjectId parent_id,
                                           std::string_view name) {
    return _resolution_table.RemoveObject<Type>(parent_id, name);
  }

  template<ResolveType Type, typename DependencyType = void>
  Result RegisterObject(std::shared_ptr<Object> object, ObjectId parent_id,
                        bool replace) {
    auto r = _resolution_table.AddObject<Type>(parent_id, object->GetName(),
                                               object->GetId(), replace);
    if (!r.ok()) {
      return r;
    }
    if constexpr (!std::is_same_v<DependencyType, void>) {
      auto [_, inserted] = _object_dependencies.try_emplace(
        object->GetId(), std::make_shared<DependencyType>());
      SDB_ASSERT(inserted);
    }
    auto [_, inserted] = _objects.insert(object);
    SDB_ASSERT(inserted);
    return {};
  }

  template<typename W>
  Result RegisterRole(std::shared_ptr<Role> role, W&& writer) {
    const auto [it, is_new] = _roles.emplace(role);

    if (!is_new) {
      return {ERROR_USER_DUPLICATE, "Role already exists: ", role->GetName()};
    };

    auto [_, inserted] = _objects.insert(role);
    SDB_ASSERT(inserted);
    return Result{ERROR_OK};
  }

  template<typename W>
  Result DropRole(std::string_view name, W&& writer) {
    return ResolveRole(name, [&](auto it) -> Result {
      if (auto r = writer(*it); !r.ok()) {
        return r;
      }

      _roles.erase(it);
      return {};
    });
  }

  // TODO(gnusi): unify wiht ReplaceObject
  template<typename F, typename W>
  Result ReplaceRole(std::string_view name, F&& factory, W&& writer) {
    return ResolveRole(name, [&](auto object_it) -> Result {
      std::shared_ptr<Role> new_object;
      if (auto r = factory(**object_it, new_object); !r.ok()) {
        return r;
      }

      if (!new_object) {
        return {};  // Nothing to change
      }

      auto object = *object_it;
      auto [new_object_it, is_new] = _roles.emplace(new_object);

      if (!is_new && object_it != new_object_it) {
        return {ERROR_USER_DUPLICATE,
                "Role already exists: ", new_object->GetName()};
      }

      absl::Cleanup cleanup = [&] {
        if (is_new) {
          // Rollback only if we created a new object
          _roles.erase(new_object_it);
        }
      };

      if (auto r = writer(new_object); !r.ok()) {
        return r;
      }

      if (is_new) {
        _roles.erase(object);  // role_it might be invalidated
      } else {
        SDB_ASSERT(new_object->GetName() == (*object_it)->GetName());
        const_cast<std::shared_ptr<Role>&>(*object_it) = new_object;
      }

      auto it = _objects.find(new_object->GetId());
      SDB_ASSERT(it != _objects.end());
      const_cast<std::shared_ptr<Object>&>(*it) = std::move(new_object);

      std::move(cleanup).Cancel();
      return {};
    });
  }

  std::shared_ptr<Role> GetRole(std::string_view name) const final {
    const auto it = _roles.find(name);
    if (it == _roles.end()) {
      return {};
    }
    return *it;
  }

  std::shared_ptr<Database> GetDatabase(ObjectId database) const final {
    auto obj = GetObject(database);
    if (!obj) {
      return nullptr;
    }
    return std::dynamic_pointer_cast<Database>(obj);
  }

  void AddIndexShard(std::shared_ptr<IndexShard> shard) {
    auto shard_id = shard->GetId();
    auto index_id = shard->GetIndexId();
    _objects.insert(std::move(shard));

    auto dep_it = _object_dependencies.find(index_id);
    SDB_ASSERT(dep_it != _object_dependencies.end());
    auto& index_dep = basics::downCast<IndexDependency>(*dep_it->second);
    index_dep.shard_id = shard_id;
  }

  void AddTableShard(std::shared_ptr<TableShard> shard) {
    auto shard_id = shard->GetId();
    auto table_id = shard->GetTableId();
    _objects.insert(std::move(shard));

    auto dep_it = _object_dependencies.find(table_id);
    SDB_ASSERT(dep_it != _object_dependencies.end());
    auto& table_dep = basics::downCast<TableDependency>(*dep_it->second);
    table_dep.shard_id = shard_id;
  }

  std::shared_ptr<IndexShard> DropIndexShard(ObjectId shard_id) {
    auto res = _objects.extract(shard_id);
    SDB_ASSERT(!res.empty());
    auto shard = std::dynamic_pointer_cast<IndexShard>(std::move(res.value()));

    auto index_id = shard->GetIndexId();
    auto dep_it = _object_dependencies.find(index_id);
    SDB_ASSERT(dep_it != _object_dependencies.end());
    auto& index_dep = basics::downCast<IndexDependency>(*dep_it->second);
    index_dep.shard_id = ObjectId::none();
    return shard;
  }

  std::vector<std::shared_ptr<Index>> GetIndexesByTable(ObjectId table_id) {
    auto dep_it = _object_dependencies.find(table_id);
    if (dep_it == _object_dependencies.end()) {
      return {};
    }
    auto& table_dep = basics::downCast<TableDependency>(*dep_it->second);
    std::vector<std::shared_ptr<Index>> result;
    for (auto index_id : table_dep.indexes) {
      auto obj = GetObject(index_id);
      if (obj) {
        result.push_back(std::dynamic_pointer_cast<Index>(obj));
      }
    }
    return result;
  }

  template<ResolveType Type>
  Result ReplaceObject(ObjectId parent_id, std::string_view old_name,
                       std::shared_ptr<Object> new_object) {
    if (old_name != new_object->GetName()) {
      auto removed = _resolution_table.RemoveObject<Type>(parent_id, old_name);
      SDB_ASSERT(removed.has_value());
      auto r = _resolution_table.AddObject<Type>(
        parent_id, new_object->GetName(), new_object->GetId(), false);
      if (!r.ok()) {
        return r;
      }
    }

    auto it = _objects.find(new_object->GetId());
    SDB_ASSERT(it != _objects.end());
    const_cast<std::shared_ptr<Object>&>(*it) = std::move(new_object);
    return {};
  }

 private:
  template<typename W>
  Result ResolveRole(this auto&& self, std::string_view role, W&& writer) {
    auto role_it = self._roles.find(role);
    if (role_it == self._roles.end()) {
      return {ERROR_USER_NOT_FOUND, "Role not found: ", role};
    }

    return writer(role_it);
  }

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto it = _objects.find(id);
    if (it == _objects.end()) {
      return {};
    }
    return std::dynamic_pointer_cast<T>(*it);
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

  ObjectSetByName<Role> _roles;
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
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterRole(std::move(role),
                                 [](auto&) { return Result{}; });
}

Result LocalCatalog::RegisterDatabase(std::shared_ptr<Database> database) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject<ResolveType::Database, ObjectDependency>(
    std::move(database), ObjectId{0}, false);
}

Result LocalCatalog::RegisterSchema(ObjectId database_id,
                                    std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject<ResolveType::Schema, ObjectDependency>(
    std::move(schema), database_id, false);
}

Result LocalCatalog::RegisterView(ObjectId database_id, ObjectId schema_id,
                                  std::shared_ptr<View> view) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject<ResolveType::Relation, ObjectDependency>(
    std::move(view), schema_id, false);
}

Result LocalCatalog::RegisterTable(ObjectId database_id, ObjectId schema_id,
                                   CreateTableOptions options) {
  auto table = std::make_shared<Table>(std::move(options), database_id);

  absl::MutexLock lock{&_mutex};
  auto r = _snapshot->RegisterObject<ResolveType::Relation, TableDependency>(
    table, schema_id, false);
  {
    std::shared_ptr<TableShard> physical;
    auto r = _engine->createTableShard(*table, false, physical);
    if (!r.ok()) {
      return r;
    }

    _snapshot->AddTableShard(std::move(physical));

    return {};
  };
}

Result LocalCatalog::RegisterFunction(ObjectId database_id, ObjectId schema_id,
                                      std::shared_ptr<Function> function) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject<ResolveType::Function>(std::move(function),
                                                          schema_id, false);
}

Result LocalCatalog::CreateDatabase(std::shared_ptr<Database> database) {
  const auto owner_id = database->GetOwnerId();
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto r =
      clone->template RegisterObject<ResolveType::Database, ObjectDependency>(
        database, ObjectId{0}, false);
    if (!r.ok()) {
      return r;
    }
    r = RegisterDatabaseImpl(database->GetId(), database->GetName(), true);
    if (!r.ok()) {
      return r;
    }
    {
      vpack::Builder builder;
      database->WriteInternal(builder);
      auto r = _engine->createDatabase(database->GetId(), builder.slice());

      if (!r.ok()) {
        return r;
      }
    }

    auto schema = std::make_shared<Schema>(
      database_id, SchemaOptions{
                     .owner_id = owner_id,
                     .name = std::string{StaticStrings::kPublic},
                   });
    return clone
      ->template RegisterObject<ResolveType::Schema, ObjectDependency>(
        schema, database_id, false);
    vpack::Builder builder;
    schema->WriteInternal(builder);

    return _engine->CreateSchema(
      schema->GetDatabaseId(), schema->GetId(),
      [&](bool internal) { return builder.slice(); });
  });
}

Result LocalCatalog::CreateSchema(ObjectId database_id,
                                  std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone
      ->template RegisterObject<ResolveType::Schema, ObjectDependency>(
        schema, database_id, false);
    vpack::Builder builder;
    schema->WriteInternal(builder);

    return _engine->CreateSchema(
      schema->GetDatabaseId(), schema->GetId(),
      [&](bool internal) { return builder.slice(); });
  });
}

Result LocalCatalog::CreateRole(std::shared_ptr<Role> role) {
  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->RegisterRole(std::move(role), [&](auto& object) -> Result {
        return _engine->CreateRole(basics::downCast<Role>(*object));
      });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  return {};
}

Result LocalCatalog::RegisterIndex(ObjectId database_id, ObjectId schema_id,
                                   IndexBaseOptions options) {
  auto index = MakeIndex(std::move(options));
  if (!index) {
    return std::move(index).error();
  }

  absl::MutexLock lock{&_mutex};

  return _snapshot->RegisterObject<ResolveType::Relation, IndexDependency>(
    *index, schema_id, false);

  return (*index)
    ->CreateIndexShard(false, std::move(args))
    .transform([&](auto&& index_shard) {
      auto r = _engine->StoreIndexShard(*index_shard);
      if (!r.ok()) {
        return r;
      }
      _snapshot->AddIndexShard(std::move(index_shard));
      return Result{};
    })
    .error_or(Result{});
}

Result LocalCatalog::CreateIndex(ObjectId database_id, std::string_view schema,
                                 std::string_view relation_name,
                                 const std::vector<std::string>& column_names,
                                 IndexBaseOptions options, vpack::Slice args) {
  if (column_names.empty()) {
    return Result{ERROR_BAD_PARAMETER, "Cannot create index without columns"};
  }
  absl::MutexLock lock{&_mutex};

  auto relation = _snapshot->GetRelation(database_id, schema, relation_name);
  if (!relation) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation_name,
            "\" does not exist"};
  }
  if (relation->GetType() != catalog::ObjectType::Table) {
    return Result{ERROR_NOT_IMPLEMENTED, "Only table indexes are supported"};
  }
  auto& table = basics::downCast<Table>(*relation);
  options.id = catalog::NextId();
  options.relation_id = relation->GetId();
  options.database_id = relation->GetDatabaseId();
  auto& columns = table.Columns();
  auto find_column = [&](std::string_view name) {
    auto it = absl::c_find_if(
      columns, [&](const catalog::Column& c) { return c.name == name; });
    return it != columns.end() ? &*it : nullptr;
  };

  for (const auto& name : column_names) {
    const auto* column = find_column(name);
    if (!column) {
      return Result{ERROR_BAD_PARAMETER, "column \"", name,
                    "\" does not exist"};
    }
    options.column_ids.push_back(column->id);
  }

  auto index = MakeIndex(std::move(options));
  if (!index) {
    return std::move(index).error();
  }

  return Apply(_snapshot, [&](auto& clone) {
    auto r =
      clone->template RegisterObject<ResolveType::Relation, IndexDependency>(
        *index, relation->GetSchemaId(), false);
    if (!r.ok()) {
      return r;
    }
    r = (*index)
          ->CreateIndexShard(true, std::move(args))
          .transform([&](auto&& index_shard) {
            auto r = _engine->StoreIndexShard(*index_shard);
            if (!r.ok()) {
              return r;
            }
            clone->AddIndexShard(std::move(index_shard));
            return Result{};
          })
          .error_or(Result{});
    if (!r.ok()) {
      return r;
    }
    return _engine->CreateIndex(**index);
  });
}

Result LocalCatalog::CreateView(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<View> view, bool replace) {
  auto writer = MakePropertiesWriter();

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
    if (!schema_id) {
      return Result(ERROR_SERVER_ILLEGAL_NAME);
    }

    auto r = clone->template RegisterObject<ResolveType::Relation>(
      view, *schema_id, false);
    if (!r.ok()) {
      return r;
    }

    return _engine->CreateView(
      view->GetDatabaseId(), view->GetSchemaId(), view->GetId(),
      [&](bool internal) { return writer(*view, internal); });
  });
}

Result LocalCatalog::CreateFunction(ObjectId database_id,
                                    std::string_view schema,
                                    std::shared_ptr<Function> function,
                                    bool replace) {
  auto writer = MakePropertiesWriter();

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto r = clone->template RegisterObject<ResolveType::Function>(
      function, *schema_id, false);
    return _engine->CreateFunction(
      function->GetDatabaseId(), function->GetSchemaId(), function->GetId(),
      [&](bool internal) { return writer(*function, internal); });
  });
}

Result LocalCatalog::CreateTable(
  ObjectId database_id, std::string_view schema, CreateTableOptions options,
  CreateTableOperationOptions operation_options) {
  auto table = std::make_shared<Table>(std::move(options), database_id);

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) -> Result {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }

    auto r =
      clone->template RegisterObject<ResolveType::Relation, TableDependency>(
        table, *schema_id, false);
    if (!r.ok()) {
      return r;
    }

    std::shared_ptr<TableShard> physical;
    r = _engine->createTableShard(*table, true, physical);
    if (!r.ok()) {
      return r;
    }

    clone->AddTableShard(physical);
    _engine->createTable(*table, *physical);

    return {};
  });
}

Result LocalCatalog::RenameView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) {
  auto writer = MakePropertiesWriter();

  auto r = [&] -> Result {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) -> Result {
      auto schema_id =
        clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
      if (!schema_id) {
        return Result{ERROR_SERVER_ILLEGAL_NAME};
      }

      auto object_id =
        clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
      if (!object_id) {
        return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
      }

      auto view = std::dynamic_pointer_cast<View>(clone->GetObject(*object_id));
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

      r = clone->template ReplaceObject<ResolveType::Relation>(*schema_id, name,
                                                               new_view);
      if (!r.ok()) {
        return r;
      }

      return _engine->ChangeView(
        new_view->GetDatabaseId(), new_view->GetSchemaId(), new_view->GetId(),
        [&](bool internal) { return writer(*new_view, internal); });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  aql::QueryCache::instance()->invalidate(database_id);
  return {};
}

Result LocalCatalog::RenameTable(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  ObjectId table_id;

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) -> Result {
      auto schema_id =
        clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
      if (!schema_id) {
        return Result{ERROR_SERVER_ILLEGAL_NAME};
      }

      auto object_id =
        clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
      if (!object_id) {
        return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
      }

      auto old_table =
        std::dynamic_pointer_cast<Table>(clone->GetObject(*object_id));
      if (!old_table) {
        return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
      }

      if (old_table->GetName() == new_name) {
        return {};
      }

      table_id = old_table->GetId();

      NewOptions options{
        .name = new_name,
        .schema = old_table->GetSchema(),
        .number_of_shards = old_table->numberOfShards(),
        .replication_factor = old_table->replicationFactor(),
        .write_concern = old_table->writeConcern(),
        .wait_for_sync = old_table->waitForSync(),
      };

      auto new_table = std::make_shared<Table>(*old_table, std::move(options));

      auto r = clone->template ReplaceObject<ResolveType::Relation>(
        *schema_id, name, new_table);
      if (!r.ok()) {
        return r;
      }

      auto shard = clone->GetTableShard(table_id);
      SDB_ENSURE(shard, ERROR_INTERNAL);
      return _engine->RenameTable(*new_table, *shard, name);
    });
  }();

  if (!r.ok()) {
    return r;
  }

  if (table_id.isSet()) {
    aql::QueryCache::instance()->invalidate(database_id, table_id);
  }
  return {};
}

Result LocalCatalog::ChangeRole(std::string_view name,
                                ChangeCallback<Role> new_role) {
  vpack::Builder b;

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->ReplaceRole(name, new_role, [&](auto& object) -> Result {
        return _engine->ChangeRole(object->GetId(), [&](bool) {
          if (b.isEmpty()) {
            b.openObject();
            object->WriteInternal(b);
            b.close();
          }
          return b.slice();
        });
      });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  aql::QueryCache::instance()->invalidate();
  return {};
}

Result LocalCatalog::ChangeView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                ChangeCallback<View> new_view) {
  auto writer = MakePropertiesWriter();

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) -> Result {
      auto schema_id =
        clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
      if (!schema_id) {
        return Result{ERROR_SERVER_ILLEGAL_NAME};
      }

      auto object_id =
        clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
      if (!object_id) {
        return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
      }

      auto view = std::dynamic_pointer_cast<View>(clone->GetObject(*object_id));
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

      r = clone->template ReplaceObject<ResolveType::Relation>(*schema_id, name,
                                                               updated);
      if (!r.ok()) {
        return r;
      }

      return _engine->ChangeView(
        updated->GetDatabaseId(), updated->GetSchemaId(), updated->GetId(),
        [&](bool internal) { return writer(*updated, internal); });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  aql::QueryCache::instance()->invalidate(database_id);
  return {};
}

Result LocalCatalog::ChangeTable(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 ChangeCallback<Table> new_table) {
  ObjectId table_id;

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) -> Result {
      auto schema_id =
        clone->template GetObjectId<ResolveType::Schema>(database_id, schema);
      if (!schema_id) {
        return Result{ERROR_SERVER_ILLEGAL_NAME};
      }

      auto object_id =
        clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
      if (!object_id) {
        return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
      }

      auto table =
        std::dynamic_pointer_cast<Table>(clone->GetObject(*object_id));
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

      table_id = updated->GetId();

      r = clone->template ReplaceObject<ResolveType::Relation>(*schema_id, name,
                                                               updated);
      if (!r.ok()) {
        return r;
      }

      auto shard = clone->GetTableShard(table_id);
      SDB_ENSURE(shard, ERROR_INTERNAL);

      return basics::SafeCall([&] { _engine->ChangeTable(*updated, *shard); });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  aql::QueryCache::instance()->invalidate(database_id, table_id);
  return {};
}

Result LocalCatalog::DropRole(std::string_view role) {
  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->DropRole(role, [&](auto& object) -> Result {
        return _engine->DropRole(*object);
      });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  aql::QueryCache::instance()->invalidate();
  return {};
}

Result LocalCatalog::DropDatabase(std::string_view name,
                                  AsyncResult* async_result) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto db_id =
      clone->template GetObjectId<ResolveType::Database>(ObjectId{0}, name);
    if (!db_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto task = clone->CreateDatabaseDrop(*db_id);
    auto res = QueueDropTask(std::move(task));
    auto id = clone->template UnregisterObject<ResolveType::Database>(
      ObjectId{0}, name);
    SDB_ASSERT(id && *id == *db_id);
    if (async_result) {
      *async_result = std::move(res);
    }
    return Result{};
  });
}

Result LocalCatalog::DropSchema(ObjectId db_id, std::string_view name,
                                bool cascade, AsyncResult* async_result) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(db_id, name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto task = clone->CreateSchemaDrop(db_id, *schema_id, true);
    auto res = QueueDropTask(std::move(task));
    auto id =
      clone->template UnregisterObject<ResolveType::Schema>(db_id, name);
    SDB_ASSERT(id && *id == *schema_id);
    if (async_result) {
      *async_result = std::move(res);
    }
    return Result{};
  });
}

Result LocalCatalog::DropTable(ObjectId db_id, std::string_view schema_name,
                               std::string_view name,
                               AsyncResult* async_result) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto table_id =
      clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
    if (!table_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto task = clone->CreateTableDrop(db_id, *schema_id, *table_id, true);
    auto res = QueueDropTask(std::move(task));

    auto id =
      clone->template UnregisterObject<ResolveType::Relation>(*schema_id, name);
    SDB_ASSERT(id && *id == *table_id);

    if (async_result) {
      *async_result = std::move(res);
    }
    return Result{};
  });
}

Result LocalCatalog::DropIndex(ObjectId db_id, std::string_view schema_name,
                               std::string_view name,
                               AsyncResult* async_result) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto index_id =
      clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
    if (!index_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto obj = clone->GetObject(*index_id);
    SDB_ASSERT(obj);
    auto index = std::dynamic_pointer_cast<catalog::Index>(obj);
    SDB_ASSERT(index);
    auto task = clone->CreateIndexDrop(db_id, *schema_id,
                                       index->GetRelationId(), *index_id, true);
    auto res = QueueDropTask(std::move(task));

    auto id =
      clone->template UnregisterObject<ResolveType::Relation>(*schema_id, name);
    SDB_ASSERT(id && *id == *index_id);

    if (async_result) {
      *async_result = std::move(res);
    }
    return Result{};
  });
}

Result LocalCatalog::DropView(ObjectId db_id, std::string_view schema_name,
                              std::string_view name,
                              AsyncResult* async_result) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto view_id =
      clone->template GetObjectId<ResolveType::Relation>(*schema_id, name);
    if (!view_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto obj = clone->GetObject(*view_id);
    SDB_ASSERT(obj);
    auto view = std::dynamic_pointer_cast<catalog::View>(obj);
    SDB_ASSERT(view);
    auto task = clone->CreateDefinitionDrop(*schema_id, *view_id,
                                            RocksDBEntryType::View, true);
    auto res = QueueDropTask(std::move(task));

    auto id =
      clone->template UnregisterObject<ResolveType::Relation>(*schema_id, name);
    SDB_ASSERT(id && *id == *view_id);

    if (async_result) {
      *async_result = std::move(res);
    }
    return Result{};
  });
}

Result LocalCatalog::DropFunction(ObjectId db_id, std::string_view schema_name,
                                  std::string_view name,
                                  AsyncResult* async_result) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto schema_id =
      clone->template GetObjectId<ResolveType::Schema>(db_id, schema_name);
    if (!schema_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto func_id =
      clone->template GetObjectId<ResolveType::Function>(*schema_id, name);
    if (!func_id) {
      return Result{ERROR_SERVER_ILLEGAL_NAME};
    }
    auto obj = clone->GetObject(*func_id);
    SDB_ASSERT(obj);
    auto func = std::dynamic_pointer_cast<catalog::Function>(obj);
    SDB_ASSERT(func);
    auto task = clone->CreateDefinitionDrop(*schema_id, *func_id,
                                            RocksDBEntryType::Function, true);
    auto res = QueueDropTask(std::move(task));

    auto id =
      clone->template UnregisterObject<ResolveType::Relation>(*schema_id, name);
    SDB_ASSERT(id && *id == *func_id);

    if (async_result) {
      *async_result = std::move(res);
    }
    return Result{};
  });
}

std::shared_ptr<Snapshot> LocalCatalog::GetSnapshot() const noexcept {
  return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
}

}  // namespace sdb::catalog
