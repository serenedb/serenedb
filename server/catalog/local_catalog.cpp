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
#include <absl/synchronization/mutex.h>

#include <algorithm>
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
#include "basics/write_locker.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
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
#include "storage_engine/engine_selector_feature.h"
#include "storage_engine/storage_engine.h"
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
          SDB_FATAL("xxxxx", Logger::THREADS, "Failed to execute ", T::kName,
                    ", error: ", r.errorMessage());
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
    .collection = physical.GetMeta().id,
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
  static constexpr std::string_view kName = "collection drop";

  TableTombstone tombstone;
  std::shared_ptr<TableShard> physical;
  std::shared_ptr<LocalCatalog> catalog;
  uint32_t delay = kInitialDelay;  // delay in microseconds

  Result operator()() {
    SDB_DEBUG("xxxxx", Logger::THREADS,
              "Start dropping collection: ", tombstone.collection);

    auto& server = SerenedServer::Instance();
    if (server.isStopping()) {
      return {};
    }

    auto& engine = server.getFeature<EngineSelectorFeature>().engine();

#ifdef SDB_CLUSTER
    auto r = basics::SafeCall([&] {
      transaction::cluster::AbortTransactions(ExecContext::superuser(),
                                              tombstone.collection);
    });

    if (!r.ok()) {
      SDB_WARN("xxxxx", Logger::THREADS,
               "Failed to abort ongoing transactions: ", r.errorMessage());
    }
#endif

    if (physical.use_count() > 2) {
      SDB_DEBUG("xxxxx", Logger::THREADS,
                "Reschedule dropping collection: ", tombstone.collection);
      return {ERROR_LOCKED};
    }

    const bool fatal = !catalog->GetSkipBackgroundErrors();

    for (auto& index : tombstone.indexes) {
      if (auto r = engine.dropIndex(index); !r.ok()) {
        SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping index ", index.id,
                 ", error: ", r.errorMessage());
        if (fatal) {
          return {ERROR_INTERNAL};
        }
      }
    }

    if (auto r = engine.dropCollection(tombstone); !r.ok()) {
      SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping collection ",
               tombstone.collection, ", error: ", r.errorMessage());
      if (fatal) {
        return {ERROR_INTERNAL};
      }
    }

    if (physical) {
      physical->drop();
    }

    catalog->DropTableShard(tombstone.collection);

    SDB_DEBUG("xxxxx", Logger::THREADS,
              "Finish dropping collection: ", tombstone.collection);

    return {};
  }
};

struct DatabaseDrop : std::enable_shared_from_this<DatabaseDrop> {
  static constexpr std::string_view kName = "database drop";

  ObjectId database;
  std::vector<TableDrop> collections;
  std::shared_ptr<LocalCatalog> catalog;
  uint32_t delay = kInitialDelay;  // delay in microseconds

  Result DropTables(auto& tasks) const {
    if (tasks.empty()) {
      return {};
    }

    return yaclib::WhenAll<yaclib::FailPolicy::None, yaclib::OrderPolicy::Fifo>(
             tasks.begin(), tasks.size())
      .ThenInline(
        [this](std::vector<yaclib::Result<Result>>&& results) -> Result {
          const bool fatal = !catalog->GetSkipBackgroundErrors();
          bool error = false;
          for (auto& result : results) {
            if (auto r = std::move(result).Ok(); !r.ok()) {
              SDB_WARN("xxxxx", Logger::THREADS,
                       "Failed dropping collection in database ", database,
                       ", error: ", r.errorMessage());
              error = true;
            }

            if (error && fatal) {
              return {ERROR_INTERNAL};
            }
          }
          return {};
        })
      .Get()
      .Ok();
  }

  Result Finish() const {
#ifdef SDB_CLUSTER
    search::CleanupDatabase(database);
#endif

    if (auto r = SerenedServer::Instance()
                   .getFeature<EngineSelectorFeature>()
                   .engine()
                   .dropDatabase(database);
        !r.ok()) {
      SDB_WARN("xxxxx", Logger::THREADS, "Failed dropping database ", database,
               ", error: ", r.errorMessage());
      if (const bool fatal = !catalog->GetSkipBackgroundErrors(); fatal) {
        return {ERROR_INTERNAL};
      }
    }

    SDB_DEBUG("xxxxx", Logger::THREADS, "Finish dropping database: ", database);
    return {};
  }

  Result operator()() {
    SDB_DEBUG("xxxxx", Logger::THREADS, "Start dropping database: ", database);

    if (SerenedServer::Instance().isStopping()) {
      return {};
    }

    // TODO(gnusi): this can be overwhelming
    auto tasks = collections | std::views::transform([&](auto& collection) {
                   return QueueTask(std::shared_ptr<TableDrop>{
                     shared_from_this(), &collection});
                 }) |
                 std::ranges::to<std::vector>();

    auto r = DropTables(tasks);
    if (!r.ok()) {
      return r;
    }
    return Finish();
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

}  // namespace

class Snapshot {
 public:
  auto& GetDatabases() const noexcept { return _databases; }

  template<typename W>
  Result RegisterRole(std::shared_ptr<catalog::Role> role, W&& writer) {
    const auto [it, is_new] = _roles.emplace(role);

    if (!is_new) {
      return {ERROR_USER_DUPLICATE, "Role already exists: ", role->GetName()};
    };

    return RegisterObjectId(
      std::move(role), [&] { _roles.erase(it); }, std::move(writer));
  }

  template<typename W>
  Result RegisterDatabase(std::shared_ptr<Database> database, W&& writer) {
    const auto [it, is_new] = _databases.emplace(
      std::piecewise_construct, std::forward_as_tuple(database),
      std::forward_as_tuple());

    if (!is_new) {
      return {ERROR_SERVER_DUPLICATE_NAME,
              "Database already exists: ", database->GetName()};
    };

    return RegisterObjectId(
      std::move(database), [&] { _databases.erase(it); },
      std::forward<W>(writer));
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

  template<typename W>
  Result DropDatabase(std::string_view database, W&& writer) {
    return ResolveDatabase(database, [&](auto it) -> Result {
      if (auto r = writer(it->first); !r.ok()) {
        return r;
      }

      for (auto& [schema, schema_objects] : it->second) {
        for (auto& obj : schema_objects) {
          _objects_by_id.erase(obj->GetId());
        }
        _objects_by_id.erase(schema->GetId());
      }

      _objects_by_id.erase(it->first->GetId());
      _databases.erase(it);
      return {};
    });
  }

  template<typename W, typename D>
  Result RegisterSchema(D database, std::shared_ptr<Schema> schema,
                        W&& writer) {
    return ResolveDatabase(database, [&](auto database_it) -> Result {
      const auto [schema_it, is_new] = database_it->second.emplace(
        std::piecewise_construct, std::forward_as_tuple(schema),
        std::forward_as_tuple());

      if (!is_new) {
        return {ERROR_SERVER_DUPLICATE_NAME,
                "Schema already exists: ", schema->GetName()};
      }

      return RegisterObjectId(
        std::move(schema), [&] { database_it->second.erase(schema_it); },
        std::forward<W>(writer));
    });
  }

  template<typename W, typename D>
  Result RegisterObject(D database, std::string_view schema,
                        std::shared_ptr<SchemaObject> object, W&& writer) {
    return ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        const auto [object_it, is_new] = schema_it->second.emplace(object);

        if (!is_new) {
          return {ERROR_SERVER_DUPLICATE_NAME,
                  "Object already exists: ", object->GetName()};
        }

        return RegisterObjectId(
          std::move(object), [&] { schema_it->second.erase(object_it); },
          std::forward<W>(writer));
      });
  }

  // TODO(gnusi): unify wiht ReplaceObject
  template<typename F, typename W>
  Result ReplaceRole(std::string_view name, F&& factory, W&& writer) {
    return ResolveRole(name, [&](auto object_it) -> Result {
      std::shared_ptr<catalog::Role> new_object;
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
        const_cast<std::shared_ptr<catalog::Role>&>(*object_it) = new_object;
      }

      auto it = _objects_by_id.find(new_object->GetId());
      SDB_ASSERT(it != _objects_by_id.end());
      const_cast<std::shared_ptr<Object>&>(*it) = std::move(new_object);

      std::move(cleanup).Cancel();
      return {};
    });
  }

  template<typename T, typename F, typename W, typename D>
  Result ReplaceObject(D database, std::string_view schema,
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

        auto object = *object_it;
        auto [new_object_it, is_new] = schema_it->second.emplace(new_object);

        if (!is_new && object_it != new_object_it) {
          return {ERROR_SERVER_DUPLICATE_NAME,
                  "Object already exists: ", new_object->GetName()};
        }

        absl::Cleanup cleanup = [&] {
          if (is_new) {
            // Rollback only if we created a new object
            schema_it->second.erase(new_object_it);
          }
        };

        if (auto r = writer(database_it->first, schema_it->first, new_object);
            !r.ok()) {
          return r;
        }

        if (is_new) {
          schema_it->second.erase(object);  // object_it might be invalidated
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

  template<typename W, typename D>
  Result DropSchema(D database, std::string_view schema, W&& writer) {
    return ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        if (auto r = writer(database_it->first, schema_it->first); !r.ok()) {
          return r;
        }

        for (auto& obj : schema_it->second) {
          _objects_by_id.erase(obj->GetId());
        }
        _objects_by_id.erase(schema_it->first->GetId());
        database_it->second.erase(schema_it);
        return {};
      });
  }

  template<typename T, typename W, typename D>
  Result DropObject(D database, std::string_view schema, std::string_view name,
                    W&& writer) {
    return ResolveObject<T>(
      database, schema, name,
      [&](auto database_it, auto schema_it, auto object_it) -> Result {
        if (auto r = writer(database_it->first, schema_it->first, *object_it);
            !r.ok()) {
          return r;
        }

        _objects_by_id.erase(*object_it);
        schema_it->second.erase(object_it);
        return {};
      });
  }

  std::shared_ptr<catalog::Role> GetRole(std::string_view name) const {
    const auto it = _roles.find(name);
    if (it == _roles.end()) {
      return {};
    }
    return *it;
  }

  std::shared_ptr<Database> GetDatabase(std::string_view database) const {
    const auto it = _databases.find(database);
    if (it == _databases.end()) {
      return {};
    }
    return it->first;
  }

  template<typename D>
  std::shared_ptr<Schema> GetSchema(D database, std::string_view schema) const {
    std::shared_ptr<Schema> object;
    auto r =
      ResolveSchema(database, schema, [&](auto, auto schema_it) -> Result {
        object = schema_it->first;
        return {};
      });
    return object;
  }

  template<typename T, typename D>
  std::shared_ptr<T> GetObject(D database, std::string_view schema,
                               std::string_view name) const {
    std::shared_ptr<T> object;
    std::ignore = ResolveObject<T>(
      database, schema, name, [&](auto, auto, auto object_it) -> Result {
        object = std::static_pointer_cast<T>(*object_it);
        return {};
      });
    return object;
  }

  std::shared_ptr<Object> GetObject(ObjectId id) const {
    const auto it = _objects_by_id.find(id);
    if (it == _objects_by_id.end()) {
      return {};
    }
    return *it;
  }

  std::vector<std::shared_ptr<catalog::Role>> GetRoles() const {
    return {_roles.begin(), _roles.end()};
  }

  template<typename W, typename D>
  auto VisitObjects(D database, std::string_view schema, W&& writer) {
    return ResolveSchema(database, schema,
                         [&](auto database_it, auto schema_it) -> Result {
                           for (auto& object : schema_it->second) {
                             if (!writer(object)) {
                               break;
                             }
                           }
                           return {};
                         });
  }

  template<typename T, typename D>
  void GetObjects(D database, std::string_view schema,
                  std::vector<std::shared_ptr<T>>& objects) {
    std::ignore = VisitObjects(database, schema, [&](auto& object) {
      if (object->GetType() == GetObjectType<T>()) {
        objects.emplace_back(std::static_pointer_cast<T>(object));
      }
      return true;
    });
  }

  template<typename W, typename D>
  Result ResolveDatabase(this auto&& self, D key, W&& writer) {
    auto error = [&] {
      return Result{ERROR_SERVER_DATABASE_NOT_FOUND,
                    "Database not found: ", key};
    };

    auto impl = [&](std::string_view database) -> Result {
      auto it = self._databases.find(database);
      if (it == self._databases.end()) {
        return error();
      }
      return writer(it);
    };

    if constexpr (std::is_same_v<D, ObjectId>) {
      auto database = self.GetObject(key);
      if (!database || database->GetType() != ObjectType::Database) {
        return error();
      }
      return impl(database->GetName());
    } else if constexpr (std::is_same_v<D, std::string_view>) {
      return impl(key);
    } else {
      static_assert(false);
    }
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

  template<typename W, typename D>
  Result ResolveSchema(this auto&& self, D database, std::string_view schema,
                       W&& writer) {
    return self.ResolveDatabase(database, [&](auto database_it) -> Result {
      auto impl = [&](auto& objects) -> Result {
        auto schema_it = objects.find(schema);
        if (schema_it == objects.end()) {
          return {ERROR_SERVER_DATABASE_NOT_FOUND,  // schema not found
                  "Schema not found: ", schema};
        }

        return writer(database_it, schema_it);
      };

      return impl(database_it->second);
    });
  }

  template<typename T, typename W, typename D>
  Result ResolveObject(this auto&& self, D database, std::string_view schema,
                       std::string_view name, W&& writer) {
    return self.ResolveSchema(
      database, schema, [&](auto database_it, auto schema_it) -> Result {
        const auto object_it = schema_it->second.find(name);
        if (object_it == schema_it->second.end() ||
            GetObjectType<T>() != (*object_it)->GetType()) {
          return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND,
                  magic_enum::enum_name(GetObjectType<T>()),
                  " not found: ", name};
        }

        return writer(database_it, schema_it, object_it);
      });
  }

  Result RegisterObjectId(std::shared_ptr<Object> object, auto&& f1,
                          auto&& f2) {
    absl::Cleanup cleanup1 = std::move(f1);
    const auto [it, is_new] = _objects_by_id.emplace(object);
    if (!is_new) {
      return {ERROR_SERVER_DUPLICATE_IDENTIFIER,
              "Object already exists: ", object->GetId()};
    }
    absl::Cleanup cleanup2 = [&] { _objects_by_id.erase(it); };
    if (auto r = f2(object); !r.ok()) {
      return r;
    }
    std::move(cleanup2).Cancel();
    std::move(cleanup1).Cancel();
    return {};
  }

  using SchemaObjects = containers::FlatHashSet<std::shared_ptr<SchemaObject>,
                                                ObjectByName, ObjectByName>;
  using Schemas =
    containers::FlatHashMap<std::shared_ptr<Schema>, SchemaObjects,
                            ObjectByName, ObjectByName>;
  using Databases = containers::FlatHashMap<std::shared_ptr<Database>, Schemas,
                                            ObjectByName, ObjectByName>;
  using Roles = containers::FlatHashSet<std::shared_ptr<catalog::Role>,
                                        ObjectByName, ObjectByName>;

  Roles _roles;
  Databases _databases;
  containers::FlatHashSet<std::shared_ptr<Object>, ObjectById, ObjectById>
    _objects_by_id;
};

LocalCatalog::LocalCatalog(StorageEngine& engine, bool skip_background_errors)
  : _snapshot(std::make_shared<Snapshot>()),
    _engine{&engine},
    _skip_background_errors{skip_background_errors} {}

Result LocalCatalog::RegisterRole(std::shared_ptr<catalog::Role> role) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterRole(std::move(role),
                                 [](auto&) { return Result{}; });
}

Result LocalCatalog::RegisterDatabase(std::shared_ptr<Database> database) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterDatabase(std::move(database), [&](auto& object) {
    return RegisterDatabaseImpl(object->GetId(), object->GetName(), false);
  });
}

Result LocalCatalog::RegisterSchema(ObjectId database_id,
                                    std::shared_ptr<catalog::Schema> schema) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterSchema(database_id, std::move(schema),
                                   [&](auto&) { return Result{}; });
}

Result LocalCatalog::RegisterView(ObjectId database_id, std::string_view schema,
                                  std::shared_ptr<catalog::View> view) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterObject(database_id, schema, std::move(view),
                                   [&](auto& object) { return Result{}; });
}

Result LocalCatalog::RegisterTable(ObjectId database_id,
                                   std::string_view schema,
                                   CreateTableOptions options) {
  auto collection =
    std::make_shared<catalog::Table>(std::move(options), database_id);

  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterObject(
    database_id, schema, std::move(collection), [&](auto& object) -> Result {
      auto& collection = basics::downCast<catalog::Table>(*object);

      std::shared_ptr<TableShard> physical;
      auto r = _engine->createTableShard(collection, false, physical);
      if (!r.ok()) {
        return r;
      }

      // TODO(gnusi): this might throw, but indexes will become a separate
      // objects soon anyway
      physical->prepareIndexes(collection, options.indexes);

      auto [it, is_new] =
        _collections.try_emplace(collection.GetId(), std::move(physical));
      SDB_ASSERT(is_new);

      return {};
    });
}

Result LocalCatalog::RegisterFunction(
  ObjectId database_id, std::string_view schema,
  std::shared_ptr<catalog::Function> function) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterObject(database_id, schema, std::move(function),
                                   [&](auto& object) { return Result{}; });
}

Result LocalCatalog::CreateDatabase(std::shared_ptr<Database> database) {
  const auto owner_id = database->GetOwnerId();
  const auto database_id = database->GetId();

  // TODO(gnusi): make it atomic

  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  auto r = _snapshot->RegisterDatabase(
    std::move(database), [&](auto& object) -> Result {
      auto r = RegisterDatabaseImpl(object->GetId(), object->GetName(), true);
      if (!r.ok()) {
        return r;
      }

      vpack::Builder builder;
      basics::downCast<catalog::Database>(*object).WriteInternal(builder);

      return _engine->createDatabase(object->GetId(), builder.slice());
    });

  if (!r.ok()) {
    return r;
  }

  return CreateSchema(
    database_id, std::make_shared<Schema>(
                   database_id, SchemaOptions{
                                  .owner_id = owner_id,
                                  .name = std::string{StaticStrings::kPublic},
                                }));
}

Result LocalCatalog::CreateSchema(ObjectId database_id,
                                  std::shared_ptr<catalog::Schema> schema) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterSchema(
    database_id, std::move(schema), [&](auto& object) {
      auto& schema = basics::downCast<catalog::Schema>(*object);

      vpack::Builder builder;
      schema.WriteInternal(builder);

      return _engine->createSchema(
        schema.GetDatabaseId(), object->GetId(),
        [&](bool internal) { return builder.slice(); });
    });
}

Result LocalCatalog::CreateRole(std::shared_ptr<catalog::Role> role) {
  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->RegisterRole(
      std::move(role), [&](auto& object) -> Result {
        return _engine->createRole(basics::downCast<catalog::Role>(*object));
      });
  }();

  if (!r.ok()) {
    return r;
  }

  auth::IncGlobalVersion();
  return {};
}

Result LocalCatalog::CreateView(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<catalog::View> view) {
  auto writer = MakePropertiesWriter();

  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterObject(
    database_id, schema, std::move(view), [&](auto& object) {
      auto& view = basics::downCast<catalog::View>(*object);
      return _engine->createView(
        view.GetDatabaseId(), view.GetSchemaId(), view.GetId(),
        [&](bool internal) { return writer(view, internal); });
    });
}

Result LocalCatalog::CreateFunction(
  ObjectId database_id, std::string_view schema,
  std::shared_ptr<catalog::Function> function) {
  auto writer = MakePropertiesWriter();

  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterObject(
    database_id, schema, std::move(function), [&](auto& object) {
      auto& function = basics::downCast<catalog::Function>(*object);
      return _engine->createFunction(
        function.GetDatabaseId(), function.GetSchemaId(), function.GetId(),
        [&](bool internal) { return writer(function, internal); });
    });
}

Result LocalCatalog::CreateTable(
  ObjectId database_id, std::string_view schema, CreateTableOptions options,
  CreateTableOperationOptions operation_options) {
  auto collection =
    std::make_shared<catalog::Table>(std::move(options), database_id);

  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->RegisterObject(
    database_id, schema, std::move(collection), [&](auto& object) -> Result {
      auto& collection = basics::downCast<catalog::Table>(*object);

      std::shared_ptr<TableShard> physical;
      auto r = _engine->createTableShard(collection, true, physical);
      if (!r.ok()) {
        return r;
      }

      // TODO(gnusi): this might throw, but indexes will become a separate
      // objects soon anyway
      physical->prepareIndexes(collection, options.indexes);

      auto [it, is_new] =
        _collections.try_emplace(collection.GetId(), std::move(physical));
      SDB_ASSERT(is_new);

      try {
        _engine->createTable(collection, *it->second);
      } catch (...) {
        _collections.erase(it);
        throw;
      }

      return {};
    });
}

Result LocalCatalog::RenameView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) {
  auto writer = MakePropertiesWriter();

  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->ReplaceObject<catalog::View>(
      database_id, schema, name,
      [&](const auto& old_object, auto& new_view) -> Result {
        if (old_object.GetName() == new_name) {
          return {};  // Nothing to change
        }

        return old_object.Rename(new_view, new_name);
      },
      [&](auto& database, auto& schema, auto& object) -> Result {
        return _engine->changeView(
          object->GetDatabaseId(), object->GetSchemaId(), object->GetId(),
          [&](bool internal) { return writer(*object, internal); });
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
  ObjectId collection_id;

  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->ReplaceObject<catalog::Table>(
      database_id, schema, name,
      [&](const auto& old_object, auto& new_collection) -> Result {
        if (old_object.GetName() == new_name) {
          return {};  // Nothing to change
        }

        collection_id = old_object.GetId();
        auto& old_collection = basics::downCast<catalog::Table>(old_object);

        NewOptions options{
          .name = new_name,
          .schema = old_collection.GetSchema(),
          .number_of_shards = old_collection.numberOfShards(),
          .replication_factor = old_collection.replicationFactor(),
          .write_concern = old_collection.writeConcern(),
          .wait_for_sync = old_collection.waitForSync(),
        };

        new_collection =
          std::make_shared<catalog::Table>(old_collection, std::move(options));
        return {};
      },
      [&](auto& database, auto& schema, auto& object) -> Result {
        auto& collection = basics::downCast<catalog::Table>(*object);
        auto it = _collections.find(collection.GetId());
        SDB_ENSURE(it != _collections.end(), ERROR_INTERNAL);
        return _engine->renameCollection(collection, *it->second, name);
      });
  }();

  if (!r.ok()) {
    return r;
  }

  if (collection_id.isSet()) {
    aql::QueryCache::instance()->invalidate(database_id, collection_id);
  }
  return {};
}

Result LocalCatalog::ChangeRole(std::string_view name,
                                ChangeCallback<catalog::Role> new_role) {
  auto writer = MakePropertiesWriter();

  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->ReplaceRole(name, new_role, [&](auto& object) -> Result {
      return _engine->changeRole(
        object->GetDatabaseId(), object->GetId(),
        [&](bool internal) { return writer(*object, internal); });
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
                                ChangeCallback<catalog::View> new_view) {
  auto writer = MakePropertiesWriter();

  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->ReplaceObject<catalog::View>(
      database_id, schema, name, new_view,
      [&](auto& database, auto& schema, auto& object) -> Result {
        return _engine->changeView(
          object->GetDatabaseId(), object->GetSchemaId(), object->GetId(),
          [&](bool internal) { return writer(*object, internal); });
      });
  }();

  if (!r.ok()) {
    return r;
  }

  aql::QueryCache::instance()->invalidate(database_id);
  return {};
}

Result LocalCatalog::ChangeTable(
  ObjectId database_id, std::string_view schema, std::string_view name,
  ChangeCallback<catalog::Table> new_collection) {
  auto writer = MakePropertiesWriter();

  ObjectId collection_id;

  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->ReplaceObject<catalog::Table>(
      database_id, schema, name, new_collection,
      [&](auto& database, auto& schema, auto& object) -> Result {
        auto& collection = basics::downCast<catalog::Table>(*object);

        collection_id = collection.GetId();
        auto it = _collections.find(collection_id);
        SDB_ENSURE(it != _collections.end(), ERROR_INTERNAL);

        return basics::SafeCall(
          [&] { _engine->changeCollection(collection, *it->second); });
      });
  }();

  if (!r.ok()) {
    return r;
  }

  aql::QueryCache::instance()->invalidate(database_id, collection_id);
  return {};
}

Result LocalCatalog::DropRole(std::string_view role) {
  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->DropRole(
      role, [&](auto& object) -> Result { return _engine->dropRole(*object); });
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
  std::vector<
    std::pair<std::shared_ptr<catalog::Table>, std::shared_ptr<TableShard>>>
    collections;

  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->DropDatabase(name, [&](auto& object) {
      database_id = object->GetId();

      // TODO(gnusi): all schemas
      auto r = GetTables(database_id, StaticStrings::kPublic, collections);
      if (!r.ok()) {
        return r;
      }

      return _engine->MarkDeleted(basics::downCast<catalog::Database>(*object));
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

  auto self = shared_from_this();
  auto task = std::make_shared<DatabaseDrop>();
  task->collections.reserve(collections.size());
  for (auto& [_, physical] : collections) {
    physical->setDeleted();
    // TODO(mbkkt) probably index estimators should be cleared in unload
    physical->freeMemory();

    task->collections.emplace_back(MakeTableTombstone(*physical),
                                   std::move(physical), self);
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

Result LocalCatalog::DropSchema(ObjectId database_id, std::string_view schema) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->DropSchema(database_id, schema, [&](auto&, auto& schema) {
    return _engine->dropSchema(database_id, schema->GetId(), schema->GetName());
  });
}

Result LocalCatalog::DropView(ObjectId database_id, std::string_view schema,
                              std::string_view name) {
  auto r = [&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->DropObject<catalog::View>(
      database_id, schema, name,
      [&](auto& database, auto&, auto& object) -> Result {
        return _engine->dropView(database->GetId(), object->GetSchemaId(),
                                 object->GetId(), object->GetName());
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
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  return _snapshot->DropObject<catalog::Function>(
    database_id, schema, name, [&](auto& database, auto&, auto& object) {
      return _engine->dropFunction(database->GetId(), object->GetSchemaId(),
                                   object->GetId(), object->GetName());
    });
}

Result LocalCatalog::DropTable(ObjectId database_id, std::string_view schema,
                               std::string_view name,
                               AsyncResult* async_result) {
  std::shared_ptr<TableShard> physical;
  auto task = std::make_shared<TableDrop>();

  auto r = basics::SafeCall([&] {
    RECURSIVE_WRITE_LOCKER(_mutex, _owner);
    return _snapshot->DropObject<catalog::Table>(
      database_id, schema, name,
      [&](auto& database, auto&, auto& object) -> Result {
        auto it = _collections.find(object->GetId());
        SDB_ENSURE(it != _collections.end(), ERROR_INTERNAL,
                   "Physical collection not found");
        physical = it->second;
        physical->setDeleted();

        task->tombstone = MakeTableTombstone(*physical);

        return _engine->MarkDeleted(basics::downCast<catalog::Table>(*object),
                                    *physical, task->tombstone);
      });
  });

  if (!r.ok()) {
    return r;
  }

  _engine->prepareDropTable(physical->GetMeta().id);

  irs::Finally cleanup = [database_id, id = physical->GetMeta().id] noexcept {
    aql::QueryCache::instance()->invalidate(database_id, id);
  };

  task->catalog = shared_from_this();
  task->physical = std::move(physical);

  if (auto f = QueueTask(std::move(task)); async_result) {
    *async_result = std::move(f);
  } else {
    std::move(f).Detach();
  }

  return {};
}

std::shared_ptr<catalog::Role> LocalCatalog::GetRole(
  std::string_view name) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetRole(name);
}

std::shared_ptr<catalog::View> LocalCatalog::GetView(
  ObjectId database_id, std::string_view schema, std::string_view name) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetObject<catalog::View>(database_id, schema, name);
}

std::shared_ptr<catalog::Function> LocalCatalog::GetFunction(
  ObjectId database_id, std::string_view schema, std::string_view name) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetObject<catalog::Function>(database_id, schema, name);
}

std::shared_ptr<catalog::Table> LocalCatalog::GetTable(
  ObjectId database_id, std::string_view schema, std::string_view name) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetObject<catalog::Table>(database_id, schema, name);
}

std::shared_ptr<Schema> LocalCatalog::GetSchema(ObjectId database_id,
                                                std::string_view schema) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetSchema(database_id, schema);
}

std::shared_ptr<Database> LocalCatalog::GetDatabase(
  std::string_view name) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetDatabase(name);
}

Result LocalCatalog::GetRoles(
  std::vector<std::shared_ptr<catalog::Role>>& roles) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  roles = _snapshot->GetRoles();
  return {};
}

Result LocalCatalog::GetViews(
  ObjectId database_id, std::string_view schema,
  std::vector<std::shared_ptr<catalog::View>>& views) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  _snapshot->GetObjects(database_id, schema, views);
  return {};
}

Result LocalCatalog::GetFunctions(
  ObjectId database_id, std::string_view schema,
  std::vector<std::shared_ptr<catalog::Function>>& functions) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  _snapshot->GetObjects(database_id, schema, functions);
  return {};
}

Result LocalCatalog::GetTables(
  ObjectId database_id, std::string_view schema,
  std::vector<std::pair<std::shared_ptr<catalog::Table>,
                        std::shared_ptr<TableShard>>>& collections) const {
  SDB_ASSERT(collections.empty());

  RECURSIVE_READ_LOCKER(_mutex, _owner);
  collections.reserve(_collections.size());
  std::ignore = _snapshot->VisitObjects(database_id, schema, [&](auto& object) {
    if (object->GetType() == GetObjectType<catalog::Table>()) {
      auto it = _collections.find(object->GetId());
      SDB_ENSURE(it != _collections.end(), ERROR_INTERNAL);

      collections.emplace_back(std::static_pointer_cast<catalog::Table>(object),
                               it->second);
    }
    return true;
  });

  return {};
}

std::vector<std::shared_ptr<Database>> LocalCatalog::GetDatabases() const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetDatabases() | std::views::keys |
         std::ranges::to<std::vector>();
}

Result LocalCatalog::GetSchemas(
  ObjectId database_id, std::vector<std::shared_ptr<Schema>>& schemas) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->ResolveDatabase(
    database_id, [&](auto database_it) -> Result {
      schemas.assign_range(database_it->second | std::views::keys);
      return {};
    });
}

std::shared_ptr<Object> LocalCatalog::GetObject(ObjectId id) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _snapshot->GetObject(id);
}

std::shared_ptr<TableShard> LocalCatalog::GetTableShard(ObjectId id) const {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  auto it = _collections.find(id);
  return it == _collections.end() ? nullptr : it->second;
}

void LocalCatalog::DropTableShard(ObjectId id) {
  RECURSIVE_WRITE_LOCKER(_mutex, _owner);
  _collections.erase(id);
}

void LocalCatalog::RegisterTableDrop(TableTombstone tombstone) {
  auto task = std::make_shared<TableDrop>();
  task->tombstone = std::move(tombstone);
  task->catalog = shared_from_this();

  QueueTask(std::move(task)).Detach();
}

void LocalCatalog::RegisterDatabaseDrop(ObjectId database_id) {
  auto task = std::make_shared<DatabaseDrop>();
  task->database = database_id;
  task->catalog = shared_from_this();

  QueueTask(std::move(task)).Detach();
}

std::vector<std::shared_ptr<TableShard>> LocalCatalog::GetTableShards() {
  RECURSIVE_READ_LOCKER(_mutex, _owner);
  return _collections | std::views::values | std::ranges::to<std::vector>();
}

}  // namespace sdb::catalog
