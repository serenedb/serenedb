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
#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/error_code.h"
#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/logger/logger.h"
#include "basics/misc.hpp"
#include "basics/recursive_locker.h"
#include "basics/result.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "catalog/view.h"
#include "general_server/scheduler.h"
#include "general_server/scheduler_feature.h"
#include "general_server/state.h"
#include "rest_server/serened.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "storage_engine/engine_feature.h"
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
#include "search/search_feature.h"
#include "transaction/cluster_utils.h"
#endif

namespace sdb::catalog {
namespace {

constexpr uint32_t kInitialDelay = 125;
constexpr uint32_t kMaxDelay = kInitialDelay << 7;

template<typename T>
AsyncResult QueueTask(std::shared_ptr<T> task) {
  auto* scheduler = GetScheduler();
  if (!scheduler) {
    return yaclib::MakeFuture<Result>();
  }

  try {
    return scheduler
      ->queueWithFuture(RequestLane::InternalLow, [task] { return (*task)(); })
      .ThenInline([task = std::move(task)](Result&& r) mutable -> AsyncResult {
        if (r.errorNumber() == ERROR_LOCKED) {
          auto* scheduler = GetScheduler();
          if (!scheduler) {
            return yaclib::MakeFuture<Result>();
          }

          task->delay = std::min(kMaxDelay, task->delay << 1);

          return scheduler
            ->delay(T::kName, std::chrono::microseconds{task->delay})
            .ThenInline([task] { return QueueTask(std::move(task)); });
        }

        if (!r.ok()) {
          SDB_FATAL("xxxxx", Logger::THREADS, "Failed to execute ",
                    task->GetContext(), ", error: ", r.errorMessage());
        }

        return yaclib::MakeFuture<Result>();
      });
  } catch (...) {
    SDB_FATAL("xxxxx", Logger::THREADS, "Unable to schedule ", T::kName,
              ", shutting down");
  }
}

TableTombstone MakeTableTombstone(const TableShard& physical) {
  TableTombstone result{
    .table = physical.GetMeta().id,
    .number_documents = physical.approxNumberDocuments(),
  };

#ifdef SDB_CLUSTER
  auto indexes = physical.getAllIndexes();
  result.indexes.reserve(indexes.size());
  for (const auto& index : indexes) {
    result.indexes.emplace_back(IndexTombstone{
      .id = ObjectId{index->objectId()},
      .type = index->type(),
      .number_documents = result.number_documents,
      .unique = index->unique(),
    });
  }
#endif

  return result;
}

struct TableDrop {
  static constexpr std::string_view kName = "table drop";

  TableTombstone tombstone;
  std::shared_ptr<TableShard> physical;
  std::shared_ptr<LocalCatalog> catalog;
  uint32_t delay = kInitialDelay;  // delay in microseconds

  std::string GetContext() const {
    return absl::StrCat("table ", tombstone.table);
  }

  Result operator()() {
    SDB_DEBUG("xxxxx", Logger::THREADS,
              "Start dropping table: ", tombstone.table);

    auto& server = SerenedServer::Instance();
    if (server.isStopping()) {
      return {};
    }

#ifdef SDB_CLUSTER
    auto r = basics::SafeCall([&] {
      transaction::cluster::AbortTransactions(ExecContext::superuser(),
                                              tombstone.table);
    });

    if (!r.ok()) {
      SDB_WARN("xxxxx", Logger::THREADS,
               "Failed to abort ongoing transactions: ", r.errorMessage());
    }
#endif

    if (physical.use_count() > 2) {
      SDB_DEBUG("xxxxx", Logger::THREADS,
                "Reschedule dropping table: ", tombstone.table);
      return {ERROR_LOCKED};
    }

    const bool fatal = !catalog->GetSkipBackgroundErrors();

    auto& engine = GetServerEngine();
    for (auto& index : tombstone.indexes) {
      if (auto r = engine.DropIndex(index); !r.ok()) {
        SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping index ", index.id,
                 ", error: ", r.errorMessage());
        if (fatal) {
          return {ERROR_INTERNAL};
        }
      }
    }

    if (auto r = engine.DropTable(tombstone); !r.ok()) {
      SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping table ",
               tombstone.table, ", error: ", r.errorMessage());
      if (fatal) {
        return {ERROR_INTERNAL};
      }
    }

    if (physical) {
      physical->drop();
    }

    catalog->DropTableShard(tombstone.table);

    SDB_DEBUG("xxxxx", Logger::THREADS,
              "Finish dropping table: ", tombstone.table);

    return {};
  }
};

struct ScopeDrop : std::enable_shared_from_this<ScopeDrop> {
  static constexpr std::string_view kName = "scope drop";

  ObjectId database;
  ObjectId schema;
  std::vector<TableDrop> tables;
  std::shared_ptr<LocalCatalog> catalog;
  uint32_t delay = kInitialDelay;  // delay in microseconds

  std::string GetContext() const {
    if (schema.isSet()) {
      return absl::StrCat("schema ", database, ".", schema);
    } else {
      return absl::StrCat("database ", database);
    }
  }

  Result Finish() {
    auto r = [&]() {
      if (schema.isSet()) {
        SDB_ASSERT(database.isSet());
        return GetServerEngine().DropSchema(database, schema);
      } else {
#ifdef SDB_CLUSTER
        search::CleanupDatabase(database);
#endif
        return GetServerEngine().dropDatabase(database);
      }
    }();
    if (!r.ok()) {
      SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping ", GetContext(),
               ", error: ", r.errorMessage());
      if (!catalog->GetSkipBackgroundErrors()) {
        return {ERROR_INTERNAL};
      }
    }

    return {};
  }

  Result DropTables(auto& tasks) const {
    if (tasks.empty()) {
      return {};
    }

    yaclib::Wait(tasks.begin(), tasks.size());
    const bool fatal = !catalog->GetSkipBackgroundErrors();
    bool error = false;
    for (auto& task : tasks) {
      if (auto r = std::move(task).Touch().Ok(); !r.ok()) {
        SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping table in ",
                 GetContext(), ", error: ", r.errorMessage());
        error = true;
      }

      if (error && fatal) {
        return {ERROR_INTERNAL};
      }
    }
    return {};
  }

  Result operator()() {
    SDB_DEBUG("xxxxx", Logger::THREADS, "Start dropping ", GetContext());

    if (SerenedServer::Instance().isStopping()) {
      return {};
    }

    // TODO(gnusi): this can be overwhelming
    auto tasks = tables | std::views::transform([&](auto& table) {
                   return QueueTask(
                     std::shared_ptr<TableDrop>{shared_from_this(), &table});
                 }) |
                 std::ranges::to<std::vector>();

    auto r = DropTables(tasks);
    if (!r.ok()) {
      return r;
    }

    r = Finish();
    if (!r.ok()) {
      return r;
    }

    SDB_DEBUG("xxxxx", Logger::THREADS, "Finish dropping ", GetContext());
    return {};
  }
};

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
  std::shared_ptr<SnapshotImpl> Clone() const {
    // TODO(gnusi): COW
    auto result = std::make_shared<SnapshotImpl>();
    result->_roles = _roles;
    result->_databases = _databases;
    result->_databases_by_name = _databases_by_name;
    result->_objects_by_id = _objects_by_id;
    result->_table_shards = _table_shards;
    return result;
  }

  std::shared_ptr<Table> GetTable(ObjectId database_id, std::string_view schema,
                                  std::string_view name) const final {
    return GetObject<Table>(database_id, schema, name);
  }

  std::vector<std::shared_ptr<TableShard>> GetTableShards() const final {
    return {_table_shards.begin(), _table_shards.end()};
  }

  std::shared_ptr<TableShard> GetTableShard(ObjectId id) const final {
    auto it = _table_shards.find(id);
    return it == _table_shards.end() ? nullptr : *it;
  }

  std::shared_ptr<Object> GetObject(ObjectId id) const final {
    auto it = _objects_by_id.find(id);
    return it == _objects_by_id.end() ? nullptr : *it;
  }

  void AddTableShard(std::shared_ptr<TableShard> physical) {
    const auto is_new = _table_shards.emplace(std::move(physical)).second;
    SDB_ENSURE(is_new, ERROR_INTERNAL);
  }

  void DropTableShard(ObjectId id) { _table_shards.erase(id); }

  std::vector<std::shared_ptr<Role>> GetRoles() const final {
    return {_roles.begin(), _roles.end()};
  }

  std::vector<std::shared_ptr<Database>> GetDatabases() const noexcept final {
    return _databases | std::views::keys | std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Schema>> GetSchemas(
    ObjectId database) const noexcept final {
    std::vector<std::shared_ptr<Schema>> schemas;
    std::ignore = ResolveDatabase(database, [&](auto database_it) -> Result {
      schemas.assign_range(database_it->second | std::views::keys);
      return {};
    });
    return schemas;
  }

  template<typename W>
  Result RegisterRole(std::shared_ptr<Role> role, W&& writer) {
    const auto [it, is_new] = _roles.emplace(role);

    if (!is_new) {
      return {ERROR_USER_DUPLICATE, "Role already exists: ", role->GetName()};
    };

    return RegisterObjectId(
      std::move(role), [&] { _roles.erase(it); }, std::move(writer));
  }

  template<typename W>
  Result RegisterDatabase(std::shared_ptr<Database> database, W&& writer) {
    auto [it1, is_new1] = _databases_by_name.emplace(database);

    if (!is_new1) [[unlikely]] {
      return {ERROR_SERVER_DUPLICATE_NAME,
              "Database already exists: ", database->GetName()};
    };

    absl::Cleanup cleanup1 = [&] { _databases_by_name.erase(it1); };

    auto [it2, is_new2] = _databases.emplace(
      std::piecewise_construct, std::forward_as_tuple(std::move(database)),
      std::forward_as_tuple());

    if (!is_new2) [[unlikely]] {
      return {ERROR_SERVER_DUPLICATE_NAME,
              "Database with the same id already exists: ", database->GetId()};
    };

    absl::Cleanup cleanup2 = [&] { _databases.erase(it2); };

    if (auto r = writer(it2->first); !r.ok()) {
      return r;
    }

    std::move(cleanup1).Cancel();
    std::move(cleanup2).Cancel();
    return {};
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

  template<typename W, typename C>
  Result DropDatabase(std::string_view database, W&& writer, C&& callback) {
    return ResolveDatabase(database, [&](auto it) -> Result {
      if (auto r = writer(it->first); !r.ok()) {
        return r;
      }

      for (auto& [schema, schema_objects] : it->second) {
        schema_objects.VisitObjects([&](auto& object) {
          auto it = _objects_by_id.find(object->GetId());
          SDB_ASSERT(it != _objects_by_id.end());
          callback(*it);
          _objects_by_id.erase(it);
          return true;
        });
        _objects_by_id.erase(schema->GetId());
      }
      _objects_by_id.erase(it->first->GetId());
      _databases.erase(it);
      _databases_by_name.erase(database);
      return {};
    });
  }

  template<typename W>
  Result RegisterSchema(ObjectId database, std::shared_ptr<Schema> schema,
                        W&& writer) {
    return ResolveDatabase(database, [&](auto database_it) -> Result {
      const auto [schema_it, is_new] = database_it->second.emplace(
        std::piecewise_construct, std::forward_as_tuple(schema),
        std::forward_as_tuple());

      if (!is_new) {
        return {ERROR_SERVER_DUPLICATE_NAME, "schema \"", schema->GetName(),
                "\" already exists"};
      }

      return RegisterObjectId(
        std::move(schema), [&] { database_it->second.erase(schema_it); },
        std::forward<W>(writer));
    });
  }

  template<typename W>
  Result RegisterObject(ObjectId database, std::string_view schema,
                        std::shared_ptr<SchemaObject> object, bool replace,
                        W&& writer) {
    return ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        const auto type = object->GetType();
        auto& objects = type == ObjectType::Function
                          ? schema_it->second.functions
                          : schema_it->second.relations;
        const auto [object_it, is_new] = objects.emplace(std::move(object));

        if (!is_new) {
          if (!replace || (*object_it)->GetType() != type) {
            return {ERROR_SERVER_DUPLICATE_NAME,
                    "Object already exists: ", (*object_it)->GetName()};
          }

          const bool found = _objects_by_id.erase((*object_it)->GetId());
          SDB_ASSERT(found);
          const_cast<std::shared_ptr<SchemaObject>&>(*object_it) =
            std::move(object);
        }

        // TODO(gnusi): remove after schema management is done
        (*object_it)->SetSchemaId(schema_it->first->GetId());

        return RegisterObjectId(
          *object_it, [&] { objects.erase(object_it); },
          std::forward<W>(writer));
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

      auto it = _objects_by_id.find(new_object->GetId());
      SDB_ASSERT(it != _objects_by_id.end());
      const_cast<std::shared_ptr<Object>&>(*it) = std::move(new_object);

      std::move(cleanup).Cancel();
      return {};
    });
  }

  template<typename T, typename F, typename W>
  Result ReplaceObject(ObjectId database, std::string_view schema,
                       std::string_view name, F&& factory, W&& writer) {
    return ResolveObject<T>(
      database, schema, name,
      [&](auto database_it, auto schema_it, auto object_it) -> Result {
        std::shared_ptr<T> new_object;
        if (auto r = factory(basics::downCast<T>(**object_it), new_object);
            !r.ok()) {
          return r;
        }

        if (!new_object) {
          return {};  // Nothing to change
        }

        SDB_ASSERT(new_object->GetId() == (*object_it)->GetId());
        new_object->SetSchemaId((*object_it)->GetSchemaId());

        auto object = *object_it;
        auto& objects = std::is_same_v<T, Function>
                          ? schema_it->second.functions
                          : schema_it->second.relations;
        auto [new_object_it, is_new] = objects.emplace(new_object);

        if (!is_new && object_it != new_object_it) {
          return {ERROR_SERVER_DUPLICATE_NAME,
                  "Object already exists: ", new_object->GetName()};
        }

        absl::Cleanup cleanup = [&] {
          if (is_new) {
            // Rollback only if we created a new object
            objects.erase(new_object_it);
          }
        };

        if (auto r = writer(database_it->first, schema_it->first, new_object);
            !r.ok()) {
          return r;
        }

        if (is_new) {
          objects.erase(object);  // object_it might be invalidated
        } else {
          SDB_ASSERT(new_object->GetName() == (*object_it)->GetName());
          const_cast<std::shared_ptr<SchemaObject>&>(*object_it) = new_object;
        }

        auto it = _objects_by_id.find(new_object->GetId());
        SDB_ASSERT(it != _objects_by_id.end());
        const_cast<std::shared_ptr<Object>&>(*it) = std::move(new_object);

        std::move(cleanup).Cancel();
        return {};
      });
  }

  template<typename W, typename C>
  Result DropSchema(ObjectId database, std::string_view schema, bool cascade,
                    W&& writer, C&& callback) {
    return ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        if (!cascade && !schema_it->second.empty()) {
          return {ERROR_BAD_PARAMETER, "cannot drop schema ", schema,
                  " because other objects depend on it"};
        }

        if (auto r = writer(database_it->first, schema_it->first); !r.ok()) {
          return r;
        }

        schema_it->second.VisitObjects([&](auto& object) {
          auto it = _objects_by_id.find(object->GetId());
          SDB_ASSERT(it != _objects_by_id.end());
          callback(*it);
          _objects_by_id.erase(it);
          return true;
        });
        _objects_by_id.erase(schema_it->first->GetId());
        database_it->second.erase(schema_it);
        return {};
      });
  }

  template<typename T, typename W>
  Result DropObject(ObjectId database, std::string_view schema,
                    std::string_view name, W&& writer) {
    return ResolveObject<T>(
      database, schema, name,
      [&](auto database_it, auto schema_it, auto object_it) -> Result {
        if (auto r = writer(database_it->first, schema_it->first, *object_it);
            !r.ok()) {
          return r;
        }

        _objects_by_id.erase(*object_it);

        auto& objects = std::is_same_v<T, Function>
                          ? schema_it->second.functions
                          : schema_it->second.relations;
        objects.erase(object_it);
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

  std::shared_ptr<Database> GetDatabase(std::string_view database) const final {
    const auto it = _databases_by_name.find(database);
    if (it == _databases_by_name.end()) {
      return {};
    }
    return *it;
  }

  std::shared_ptr<Database> GetDatabase(ObjectId database) const {
    const auto it = _databases.find(database);
    if (it == _databases.end()) {
      return {};
    }
    return it->first;
  }

  std::shared_ptr<Schema> GetSchema(ObjectId database,
                                    std::string_view schema) const final {
    std::shared_ptr<Schema> object;
    auto r =
      ResolveSchema(database, schema, [&](auto, auto schema_it) -> Result {
        object = schema_it->first;
        return {};
      });
    return object;
  }

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId database, std::string_view schema,
                               std::string_view name) const {
    std::shared_ptr<T> object;
    std::ignore = ResolveObject<T>(
      database, schema, name, [&](auto, auto, auto object_it) -> Result {
        object = std::static_pointer_cast<T>(*object_it);
        return {};
      });
    return object;
  }

  std::shared_ptr<SchemaObject> GetRelation(ObjectId database,
                                            std::string_view schema,
                                            std::string_view name) const final {
    std::shared_ptr<SchemaObject> object;
    std::ignore = ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        auto& objects = schema_it->second.relations;
        if (const auto it = objects.find(name); it != objects.end()) {
          object = *it;
        }
        return {};
      });
    return object;
  }

  std::shared_ptr<Function> GetFunction(ObjectId database,
                                        std::string_view schema,
                                        std::string_view name) const final {
    std::shared_ptr<Function> object;
    std::ignore = ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        auto& objects = schema_it->second.functions;
        if (const auto it = objects.find(name); it != objects.end()) {
          object = std::static_pointer_cast<Function>(*it);
        }
        return {};
      });
    return object;
  }

  template<typename W>
  auto VisitObjects(ObjectId database, std::string_view schema, W&& writer) {
    return ResolveSchema(database, schema,
                         [&](auto database_it, auto schema_it) -> Result {
                           schema_it->second.VisitObjects(
                             [&](auto& object) { return writer(object); });
                           return {};
                         });
  }

  template<typename T>
  void GetObjects(ObjectId database, std::string_view schema,
                  std::vector<std::shared_ptr<T>>& objects) {
    std::ignore = VisitObjects(database, schema, [&](auto& object) {
      if (object->GetType() == GetObjectType<T>()) {
        objects.emplace_back(std::static_pointer_cast<T>(object));
      }
      return true;
    });
  }

  std::vector<std::shared_ptr<SchemaObject>> GetRelations(
    ObjectId database, std::string_view schema) const final {
    std::vector<std::shared_ptr<SchemaObject>> objects;
    std::ignore = ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        auto& relations = schema_it->second.relations;
        objects.assign_range(relations);
        return {};
      });
    return objects;
  }

  std::vector<std::shared_ptr<Function>> GetFunctions(
    ObjectId database, std::string_view schema) const final {
    std::vector<std::shared_ptr<Function>> objects;
    std::ignore = ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        auto& functions = schema_it->second.functions;
        objects.assign_range(functions | std::views::transform([](auto& func) {
                               return basics::downCast<Function>(func);
                             }));
        return {};
      });
    return objects;
  }

  template<typename W, typename D>
  Result ResolveDatabase(this auto&& self, D key, W&& writer) {
    auto error = [&] {
      return Result{ERROR_SERVER_DATABASE_NOT_FOUND,
                    "Database not found: ", key};
    };

    auto impl = [&](ObjectId database) -> Result {
      auto it = self._databases.find(database);
      if (it == self._databases.end()) {
        return error();
      }
      return writer(it);
    };

    if constexpr (std::is_same_v<D, ObjectId>) {
      return impl(key);
    } else if constexpr (std::is_same_v<D, std::string_view>) {
      auto database = self._databases_by_name.find(key);
      if (database == self._databases_by_name.end()) {
        return error();
      }
      return impl((*database)->GetId());
    } else {
      static_assert(false);
    }
  }

  template<typename T, typename W>
  Result ResolveObject(this auto&& self, ObjectId database,
                       std::string_view schema, std::string_view name,
                       W&& writer) {
    return self.ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        auto& objects = std::is_same_v<T, Function>
                          ? schema_it->second.functions
                          : schema_it->second.relations;
        const auto object_it = objects.find(name);
        if (object_it == objects.end() ||
            GetObjectType<T>() != (*object_it)->GetType()) {
          return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND,
                  magic_enum::enum_name(GetObjectType<T>()),
                  " not found: ", name};
        }

        return writer(database_it, schema_it, object_it);
      });
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

  template<typename W>
  Result ResolveSchema(this auto&& self, ObjectId database,
                       std::string_view schema, W&& writer) {
    return self.ResolveDatabase(database, [&](auto database_it) -> Result {
      auto& schemas = database_it->second;
      auto schema_it = schemas.find(schema);
      if (schema_it == schemas.end()) {
        return {ERROR_SERVER_DATABASE_NOT_FOUND,  // TODO: schema not found
                "schema \"", schema, "\" does not exist"};
      }

      return writer(database_it, schema_it);
    });
  }

  Result RegisterObjectId(std::shared_ptr<Object> object, auto&& f1,
                          auto&& f2) {
    absl::Cleanup cleanup1 = std::move(f1);
    const auto [it, is_new] = _objects_by_id.emplace(std::move(object));
    if (!is_new) {
      return {ERROR_SERVER_DUPLICATE_IDENTIFIER,
              "Object already exists: ", (*it)->GetId()};
    }
    absl::Cleanup cleanup2 = [&] { _objects_by_id.erase(it); };
    if (auto r = f2(*it); !r.ok()) {
      return r;
    }
    std::move(cleanup2).Cancel();
    std::move(cleanup1).Cancel();
    return {};
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
  ObjectMapById<Database, ObjectMapByName<Schema, SchemaObjects>> _databases;
  ObjectSetByName<Database> _databases_by_name;
  ObjectSetById<Object> _objects_by_id;
  ObjectSetById<TableShard> _table_shards;
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
  return _snapshot->RegisterDatabase(std::move(database), [&](auto& object) {
    return RegisterDatabaseImpl(object->GetId(), object->GetName(), false);
  });
}

Result LocalCatalog::RegisterSchema(ObjectId database_id,
                                    std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterSchema(database_id, std::move(schema),
                                   [&](auto&) { return Result{}; });
}

Result LocalCatalog::RegisterView(ObjectId database_id, std::string_view schema,
                                  std::shared_ptr<View> view) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject(database_id, schema, std::move(view), false,
                                   [&](auto& object) { return Result{}; });
}

Result LocalCatalog::RegisterTable(ObjectId database_id,
                                   std::string_view schema,
                                   CreateTableOptions options) {
  auto table = std::make_shared<Table>(std::move(options), database_id);

  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject(
    database_id, schema, std::move(table), false, [&](auto& object) -> Result {
      auto& table = basics::downCast<Table>(*object);

      std::shared_ptr<TableShard> physical;
      auto r = _engine->createTableShard(table, false, physical);
      if (!r.ok()) {
        return r;
      }

      // TODO(gnusi): this might throw, but indexes will become a separate
      // objects soon anyway
      physical->prepareIndexes(table, options.indexes);
      _snapshot->AddTableShard(std::move(physical));

      return {};
    });
}

Result LocalCatalog::RegisterFunction(ObjectId database_id,
                                      std::string_view schema,
                                      std::shared_ptr<Function> function) {
  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject(database_id, schema, std::move(function),
                                   false,
                                   [&](auto& object) { return Result{}; });
}

Result LocalCatalog::CreateDatabase(std::shared_ptr<Database> database) {
  const auto owner_id = database->GetOwnerId();
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    auto r =
      clone->RegisterDatabase(std::move(database), [&](auto& object) -> Result {
        auto r = RegisterDatabaseImpl(object->GetId(), object->GetName(), true);
        if (!r.ok()) {
          return r;
        }

        vpack::Builder builder;
        basics::downCast<Database>(*object).WriteInternal(builder);

        return _engine->createDatabase(object->GetId(), builder.slice());
      });

    if (!r.ok()) {
      return r;
    }

    return clone->RegisterSchema(
      database_id,
      std::make_shared<Schema>(database_id,
                               SchemaOptions{
                                 .owner_id = owner_id,
                                 .name = std::string{StaticStrings::kPublic},
                               }),
      [&](auto& object) {
        auto& schema = basics::downCast<Schema>(*object);

        vpack::Builder builder;
        schema.WriteInternal(builder);

        return _engine->CreateSchema(
          schema.GetDatabaseId(), object->GetId(),
          [&](bool internal) { return builder.slice(); });
      });
  });
}

Result LocalCatalog::CreateSchema(ObjectId database_id,
                                  std::shared_ptr<Schema> schema) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterSchema(
      database_id, std::move(schema), [&](auto& object) {
        auto& schema = basics::downCast<Schema>(*object);

        vpack::Builder builder;
        schema.WriteInternal(builder);

        return _engine->CreateSchema(
          schema.GetDatabaseId(), object->GetId(),
          [&](bool internal) { return builder.slice(); });
      });
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

Result LocalCatalog::RegisterIndex(ObjectId database_id,
                                   std::string_view schema,
                                   IndexFactory factory) {
  auto index = factory(nullptr);
  if (!index) {
    return std::move(index).error();
  }

  absl::MutexLock lock{&_mutex};
  return _snapshot->RegisterObject(database_id, schema, std::move(*index),
                                   false,
                                   [&](auto& object) -> Result { return {}; });
}

Result LocalCatalog::CreateIndex(ObjectId database_id, std::string_view schema,
                                 std::string_view relation_name,
                                 IndexFactory index_factory) {
  absl::MutexLock lock{&_mutex};

  auto relation = _snapshot->GetRelation(database_id, schema, relation_name);
  if (!relation) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation_name,
            "\" does not exist"};
  }

  auto index = index_factory(relation.get());
  if (!index) {
    return std::move(index).error();
  }

  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(database_id, schema, std::move(*index), false,
                                 [&](auto& object) -> Result {
                                   auto& index =
                                     basics::downCast<Index>(*object);
                                   return _engine->CreateIndex(index);
                                 });
  });
}

Result LocalCatalog::DropIndex(ObjectId database_id, std::string_view schema,
                               std::string_view name) {
  IndexTombstone tombstone;

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->template DropObject<Index>(
      database_id, schema, name,
      [&](auto& database, auto& schema, auto& object) -> Result {
        auto& index = basics::downCast<Index>(*object);

        tombstone.id = index.GetId();
        return _engine->MarkDeleted(index, tombstone);
      });
  });
}

Result LocalCatalog::CreateView(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<View> view, bool replace) {
  auto writer = MakePropertiesWriter();

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(
      database_id, schema, std::move(view), replace, [&](auto& object) {
        auto& view = basics::downCast<View>(*object);
        return _engine->CreateView(
          view.GetDatabaseId(), view.GetSchemaId(), view.GetId(),
          [&](bool internal) { return writer(view, internal); });
      });
  });
}

Result LocalCatalog::CreateFunction(ObjectId database_id,
                                    std::string_view schema,
                                    std::shared_ptr<Function> function,
                                    bool replace) {
  auto writer = MakePropertiesWriter();

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(
      database_id, schema, std::move(function), replace, [&](auto& object) {
        auto& function = basics::downCast<Function>(*object);
        return _engine->CreateFunction(
          function.GetDatabaseId(), function.GetSchemaId(), function.GetId(),
          [&](bool internal) { return writer(function, internal); });
      });
  });
}

Result LocalCatalog::CreateTable(
  ObjectId database_id, std::string_view schema, CreateTableOptions options,
  CreateTableOperationOptions operation_options) {
  auto table = std::make_shared<Table>(std::move(options), database_id);

  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(
      database_id, schema, std::move(table), false,
      [&](auto& object) -> Result {
        auto& table = basics::downCast<Table>(*object);

        std::shared_ptr<TableShard> physical;
        auto r = _engine->createTableShard(table, true, physical);
        if (!r.ok()) {
          return r;
        }

        physical->prepareIndexes(table, options.indexes);
        clone->AddTableShard(physical);
        _engine->createTable(table, *physical);

        return {};
      });
  });
}

Result LocalCatalog::RenameView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) {
  auto writer = MakePropertiesWriter();

  auto r = [&] -> Result {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) -> Result {
      return clone->template ReplaceObject<View>(
        database_id, schema, name,
        [&](const auto& old_object, auto& new_view) -> Result {
          if (old_object.GetName() == new_name) {
            return {};  // Nothing to change
          }

          return old_object.Rename(new_view, new_name);
        },
        [&](auto& database, auto& schema, auto& object) -> Result {
          return _engine->ChangeView(
            object->GetDatabaseId(), object->GetSchemaId(), object->GetId(),
            [&](bool internal) { return writer(*object, internal); });
        });

      return {};
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
  auto writer = MakePropertiesWriter();
  ObjectId table_id;

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) -> Result {
      return clone->template ReplaceObject<Table>(
        database_id, schema, name,
        [&](const auto& old_object, auto& new_table) -> Result {
          if (old_object.GetName() == new_name) {
            return {};  // Nothing to change
          }

          table_id = old_object.GetId();
          auto& old_table = basics::downCast<Table>(old_object);

          NewOptions options{
            .name = new_name,
            .schema = old_table.GetSchema(),
            .number_of_shards = old_table.numberOfShards(),
            .replication_factor = old_table.replicationFactor(),
            .write_concern = old_table.writeConcern(),
            .wait_for_sync = old_table.waitForSync(),
          };

          new_table = std::make_shared<Table>(old_table, std::move(options));
          return {};
        },
        [&](auto& database, auto& schema, auto& object) -> Result {
          auto& table = basics::downCast<Table>(*object);
          auto shard = clone->GetTableShard(table.GetId());
          SDB_ENSURE(shard, ERROR_INTERNAL);
          return _engine->RenameTable(table, *shard, name);
        });
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
    return Apply(_snapshot, [&](auto& clone) {
      return clone->template ReplaceObject<View>(
        database_id, schema, name, new_view,
        [&](auto& database, auto& schema, auto& object) -> Result {
          return _engine->ChangeView(
            object->GetDatabaseId(), object->GetSchemaId(), object->GetId(),
            [&](bool internal) { return writer(*object, internal); });
        });
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
  auto writer = MakePropertiesWriter();

  ObjectId table_id;

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->template ReplaceObject<Table>(
        database_id, schema, name, new_table,
        [&](auto& database, auto& schema, auto& object) -> Result {
          auto& table = basics::downCast<Table>(*object);

          table_id = table.GetId();
          auto shard = clone->GetTableShard(table_id);
          SDB_ENSURE(shard, ERROR_INTERNAL);

          return basics::SafeCall([&] { _engine->ChangeTable(table, *shard); });
        });
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
  ObjectId database_id;
  auto self = shared_from_this();
  auto task = std::make_shared<ScopeDrop>();

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->DropDatabase(
        name,
        [&](auto& object) {
          database_id = object->GetId();
          return _engine->MarkDeleted(basics::downCast<Database>(*object));
        },
        [&](auto& obj) {
          auto shard = clone->GetTableShard(obj->GetId());
          if (!shard) {
            return;
          }
          task->tables.emplace_back(MakeTableTombstone(*shard), shard, self);
        });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  irs::Finally cleanup = [database_id] noexcept {
    aql::QueryCache::instance()->invalidate(database_id);
  };

#ifdef SDB_CLUSTER
  if (auto registry = QueryRegistryFeature::registry(); registry) {
    registry->destroy(name);
  }

  if (auto* cursors = GetCursors(database_id); cursors) {
    cursors->garbageCollect(true);
  }
#endif

  for (auto& shard : task->tables) {
    shard.physical->setDeleted();
    // TODO(mbkkt) probably index estimators should be cleared in unload
    shard.physical->freeMemory();
  }
  task->database = database_id;
  task->catalog = std::move(self);

  if (auto f = QueueTask(std::move(task)); async_result) {
    *async_result = std::move(f);
  } else {
    std::move(f).Detach();
  }

  return {};
}

Result LocalCatalog::DropSchema(ObjectId database_id, std::string_view schema,
                                bool cascade, AsyncResult* async_result) {
  ObjectId schema_id;
  auto self = shared_from_this();
  auto task = std::make_shared<ScopeDrop>();

  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->DropSchema(
        database_id, schema, cascade,
        [&](auto&, auto& schema) {
          schema_id = schema->GetId();

          return _engine->MarkDeleted(*schema);
        },
        [&](auto& obj) {
          if (obj->GetType() == ObjectType::Table) {
            auto shard = clone->GetTableShard(obj->GetId());
            if (!shard) {
              return;
            }
            task->tables.emplace_back(MakeTableTombstone(*shard), shard, self);
          }
        });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  irs::Finally cleanup = [database_id] noexcept {
    aql::QueryCache::instance()->invalidate(database_id);
  };

  for (auto& shard : task->tables) {
    shard.physical->setDeleted();
    // TODO(mbkkt) probably index estimators should be cleared in unload
    shard.physical->freeMemory();
  }
  task->database = database_id;
  task->schema = schema_id;
  task->catalog = std::move(self);

  if (auto f = QueueTask(std::move(task)); async_result) {
    *async_result = std::move(f);
  } else {
    std::move(f).Detach();
  }

  return {};
}

Result LocalCatalog::DropView(ObjectId database_id, std::string_view schema,
                              std::string_view name) {
  auto r = [&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->template DropObject<View>(
        database_id, schema, name,
        [&](auto& database, auto&, auto& object) -> Result {
          return _engine->DropView(database->GetId(), object->GetSchemaId(),
                                   object->GetId(), object->GetName());
        });
    });
  }();

  if (!r.ok()) {
    return r;
  }

  aql::QueryCache::instance()->invalidate(database_id);
  return {};
}

Result LocalCatalog::DropFunction(ObjectId database_id, std::string_view schema,
                                  std::string_view name) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->template DropObject<Function>(
      database_id, schema, name, [&](auto& database, auto&, auto& object) {
        return _engine->DropFunction(database->GetId(), object->GetSchemaId(),
                                     object->GetId(), object->GetName());
      });
  });
}

Result LocalCatalog::DropTable(ObjectId database_id, std::string_view schema,
                               std::string_view name,
                               AsyncResult* async_result) {
  std::shared_ptr<TableShard> shard;
  auto task = std::make_shared<TableDrop>();

  auto r = basics::SafeCall([&] {
    absl::MutexLock lock{&_mutex};
    return Apply(_snapshot, [&](auto& clone) {
      return clone->template DropObject<Table>(
        database_id, schema, name,
        [&](auto& database, auto&, auto& object) -> Result {
          shard = clone->GetTableShard(object->GetId());
          SDB_ENSURE(shard, ERROR_INTERNAL);

          task->tombstone = MakeTableTombstone(*shard);

          return _engine->MarkDeleted(basics::downCast<Table>(*object), *shard,
                                      task->tombstone);
        });
    });
  });

  if (!r.ok()) {
    return r;
  }

  shard->setDeleted();
  _engine->prepareDropTable(shard->GetMeta().id);

  irs::Finally cleanup = [database_id, id = shard->GetMeta().id] noexcept {
    aql::QueryCache::instance()->invalidate(database_id, id);
  };

  task->catalog = shared_from_this();
  task->physical = std::move(shard);

  if (auto f = QueueTask(std::move(task)); async_result) {
    *async_result = std::move(f);
  } else {
    std::move(f).Detach();
  }

  return {};
}

void LocalCatalog::DropTableShard(ObjectId id) {
  absl::MutexLock lock{&_mutex};
  std::ignore = Apply(_snapshot, [&](auto& clone) {
    clone->DropTableShard(id);
    return Result{};
  });
}

void LocalCatalog::RegisterTableDrop(TableTombstone tombstone) {
  auto task = std::make_shared<TableDrop>();
  task->tombstone = std::move(tombstone);
  task->catalog = shared_from_this();

  QueueTask(std::move(task)).Detach();
}

void LocalCatalog::RegisterScopeDrop(ObjectId database_id, ObjectId schema_id) {
  auto task = std::make_shared<ScopeDrop>();
  task->database = database_id;
  task->schema = schema_id;
  task->catalog = shared_from_this();

  QueueTask(std::move(task)).Detach();
}

std::shared_ptr<Snapshot> LocalCatalog::GetSnapshot() const noexcept {
  return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
}

}  // namespace sdb::catalog
