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
#include <vpack/builder.h>
#include <vpack/iterator.h>
#include <vpack/serializer.h>
#include <vpack/slice.h>

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
#include <yaclib/async/future.hpp>
#include <yaclib/async/when_all.hpp>

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
#include "basics/static_strings.h"
#include "basics/system-compiler.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/object.h"
#include "catalog/object_dependency.h"
#include "catalog/resolution_table.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/secondary_index.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_entry_cache.h"
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
#include "storage_engine/secondary_index_shard.h"
#include "storage_engine/table_shard.h"
#include "utils/exec_context.h"

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
    result->_object_dependencies = _object_dependencies;
    // New snapshot starts with empty DuckDB cache (lazily populated)
    return result;
  }

  connector::DuckDBEntryCache& GetDuckDBEntryCache() const final {
    return _duckdb_cache;
  }

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(
    const std::shared_ptr<Database>& db) {
    auto db_deps = GetDependency<DatabaseDependency>(db->GetId());
    auto schemas_drop = db_deps->schemas |
                        std::views::transform([&](ObjectId id) {
                          auto schema = GetObject<Schema>(id);
                          SDB_ASSERT(schema);
                          return CreateSchemaDrop(db->GetId(), schema, false);
                        }) |
                        std::ranges::to<std::vector>();
    auto drop_task =
      std::make_shared<DatabaseDrop>(db, std::move(schemas_drop));
    return drop_task;
  }

  std::shared_ptr<SchemaDrop> CreateSchemaDrop(
    ObjectId db_id, const std::shared_ptr<Schema>& schema, bool is_root) {
    auto schema_deps = GetDependency<SchemaDependency>(schema->GetId());
    auto tables_drop =
      schema_deps->tables | std::views::transform([&](ObjectId id) {
        auto table = GetObject<Table>(id);
        SDB_ASSERT(table);
        return CreateTableDrop(db_id, schema->GetId(), table, false);
      }) |
      std::ranges::to<std::vector>();

    auto drop_task = std::make_shared<SchemaDrop>(
      schema, std::move(tables_drop), db_id, is_root);
    return drop_task;
  }

  std::shared_ptr<TableDrop> CreateTableDrop(
    ObjectId db_id, ObjectId schema_id, const std::shared_ptr<Table>& table,
    bool is_root) {
    auto table_deps = GetDependency<TableDependency>(table->GetId());
    auto shard = GetObject<TableShard>(table_deps->shard_id);
    auto indexes = table_deps->indexes |
                   std::views::transform([&](ObjectId id) {
                     auto index = GetObject<Index>(id);
                     SDB_ASSERT(index);
                     return CreateIndexDrop(db_id, schema_id, table->GetId(),
                                            index, is_root);
                   }) |
                   std::ranges::to<std::vector>();

    return std::make_shared<TableDrop>(table, shard, std::move(indexes),
                                       schema_id, is_root);
  }

  std::shared_ptr<IndexDrop> CreateIndexDrop(
    ObjectId db_id, ObjectId schema_id, ObjectId table_id,
    const std::shared_ptr<Index>& index, bool is_root) {
    auto index_deps = GetDependency<IndexDependency>(index->GetId());
    auto shard = GetObject<IndexShard>(index_deps->shard_id);
    auto shard_drop = std::make_shared<IndexShardDrop>(shard);
    return std::make_shared<IndexDrop>(index, std::move(shard_drop), db_id,
                                       schema_id, table_id, is_root);
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
    } else if constexpr (std::is_same_v<T, PgSqlView>) {
      auto r = AddToResolution<ResolveType::Relation>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<ViewDependency>(parent_id, std::move(object));
    } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
      auto r = AddToResolution<ResolveType::Function>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition(parent_id, object);
    } else if constexpr (std::is_same_v<T, Tokenizer>) {
      auto r = AddToResolution<ResolveType::Tokenizer>(
        parent_id, object->GetId(), object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition<TokenizerDependency>(parent_id, object);
    } else if constexpr (std::is_same_v<T, PgSqlType>) {
      auto r = AddToResolution<ResolveType::Type>(parent_id, object->GetId(),
                                                  object->GetName(), replace);
      if (!r.ok()) {
        return r;
      }
      return AddObjectDefinition(parent_id, std::move(object));
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
    } else if constexpr (std::is_same_v<T, PgSqlView>) {
      RemoveFromResolution<ResolveType::Relation>(parent_id, object->GetName(),
                                                  maybe_not_found);
    } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
      RemoveFromResolution<ResolveType::Function>(parent_id, object->GetName(),
                                                  maybe_not_found);
    } else if constexpr (std::is_same_v<T, Tokenizer>) {
      RemoveFromResolution<ResolveType::Tokenizer>(parent_id, object->GetName(),
                                                   maybe_not_found);
    } else if constexpr (std::is_same_v<T, PgSqlType>) {
      RemoveFromResolution<ResolveType::Type>(parent_id, object->GetName(),
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
      bool inserted =
        _object_dependencies.AddDependency<DependencyType>(object->GetId());
      SDB_ASSERT(inserted);
    }
    SDB_ASSERT(object->GetId().isSet());
    switch (object->GetType()) {
      case ObjectType::Database:
      case ObjectType::Role:
        break;
      case ObjectType::Schema: {
        auto db_deps = GetDependencyForWrite<DatabaseDependency>(parent_id);
        db_deps->schemas.insert(object->GetId());
      } break;
      case ObjectType::Table: {
        auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
        schema_deps->tables.insert(object->GetId());
      } break;
      case ObjectType::PgSqlFunction: {
        auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
        schema_deps->functions.insert(object->GetId());
      } break;
      case ObjectType::Tokenizer: {
        auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
        schema_deps->tokenizers.insert(object->GetId());
      } break;
      case ObjectType::PgSqlView: {
        auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
        schema_deps->views.insert(object->GetId());
      } break;
      case ObjectType::PgSqlType: {
        auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
        schema_deps->types.insert(object->GetId());
      } break;
      case ObjectType::SecondaryIndex:
      case ObjectType::InvertedIndex: {
        auto relation_deps =
          GetDependencyForWrite<RelationDependency>(parent_id);
        relation_deps->indexes.insert(object->GetId());
        const auto& index = basics::downCast<Index>(*object);
        for (auto tokenizer_id : index.GetTokenizers()) {
          auto dep = GetDependencyForWrite<TokenizerDependency>(tokenizer_id);
          SDB_ASSERT(dep);
          dep->indexes.insert(object->GetId());
        }
      } break;
      case ObjectType::TableShard: {
        auto table_deps = GetDependencyForWrite<TableDependency>(parent_id);
        table_deps->shard_id = object->GetId();
      } break;
      case ObjectType::SecondaryIndexShard:
      case ObjectType::InvertedIndexShard: {
        auto index_deps = GetDependencyForWrite<IndexDependency>(parent_id);
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
        return _resolution_table.GetRelationIds(schema_id) |
               std::views::transform(
                 [&](ObjectId relation_id) -> std::shared_ptr<SchemaObject> {
                   return GetObject<SchemaObject>(relation_id);
                 }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<SchemaObject>>{});
  }

  std::vector<std::shared_ptr<Table>> GetTables(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
        return schema_deps->tables |
               std::views::transform([&](ObjectId table_id) {
                 auto it = _objects.find(table_id);
                 SDB_ASSERT(it != _objects.end());
                 return basics::downCast<Table>(*it);
               }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<Table>>{});
  }

  std::vector<std::shared_ptr<PgSqlView>> GetViews(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
        return schema_deps->views |
               std::views::transform([&](ObjectId view_id) {
                 auto it = _objects.find(view_id);
                 SDB_ASSERT(it != _objects.end());
                 return basics::downCast<PgSqlView>(*it);
               }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<PgSqlView>>{});
  }

  std::vector<std::shared_ptr<PgSqlFunction>> GetFunctions(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
        return schema_deps->functions |
               std::views::transform([&](ObjectId function_id) {
                 auto it = _objects.find(function_id);
                 SDB_ASSERT(it != _objects.end());
                 return basics::downCast<PgSqlFunction>(*it);
               }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<PgSqlFunction>>{});
  }

  std::vector<std::shared_ptr<Index>> GetIndexes(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        std::vector<std::shared_ptr<Index>> result;
        const auto& schema_deps = GetDependency<SchemaDependency>(schema_id);
        for (const auto table_id : schema_deps->tables) {
          const auto& table_deps = GetDependency<TableDependency>(table_id);
          for (const auto index_id : table_deps->indexes) {
            auto it = _objects.find(index_id);
            SDB_ASSERT(it != _objects.end());
            result.push_back(basics::downCast<Index>(*it));
          }
        }
        for (const auto view_id : schema_deps->views) {
          const auto& view_deps = GetDependency<ViewDependency>(view_id);
          for (const auto index_id : view_deps->indexes) {
            auto it = _objects.find(index_id);
            SDB_ASSERT(it != _objects.end());
            result.push_back(basics::downCast<Index>(*it));
          }
        }
        return result;
      })
      .value_or(std::vector<std::shared_ptr<Index>>{});
  }

  void VisitRelations(
    ObjectId db_id, std::string_view schema,
    absl::FunctionRef<void(const SchemaObject&)> visitor) const final {
    auto schema_id =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!schema_id) {
      return;
    }
    for (const auto relation_id :
         _resolution_table.GetRelationIds(*schema_id)) {
      auto it = _objects.find(relation_id);
      SDB_ASSERT(it != _objects.end());
      visitor(basics::downCast<SchemaObject>(**it));
    }
  }

  void VisitViews(
    ObjectId db_id, std::string_view schema,
    absl::FunctionRef<void(const PgSqlView&)> visitor) const final {
    auto schema_id =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!schema_id) {
      return;
    }
    const auto& schema_deps = GetDependency<SchemaDependency>(*schema_id);
    for (const auto view_id : schema_deps->views) {
      auto it = _objects.find(view_id);
      SDB_ASSERT(it != _objects.end());
      visitor(basics::downCast<PgSqlView>(**it));
    }
  }

  void VisitFunctions(
    ObjectId db_id, std::string_view schema,
    absl::FunctionRef<void(const PgSqlFunction&)> visitor) const final {
    auto schema_id =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!schema_id) {
      return;
    }
    const auto& schema_deps = GetDependency<SchemaDependency>(*schema_id);
    for (const auto function_id : schema_deps->functions) {
      auto it = _objects.find(function_id);
      SDB_ASSERT(it != _objects.end());
      visitor(basics::downCast<PgSqlFunction>(**it));
    }
  }

  void VisitIndexes(ObjectId db_id, std::string_view schema,
                    absl::FunctionRef<void(const Index&)> visitor) const final {
    auto schema_id =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!schema_id) {
      return;
    }
    const auto& schema_deps = GetDependency<SchemaDependency>(*schema_id);
    for (const auto table_id : schema_deps->tables) {
      const auto& table_deps = GetDependency<TableDependency>(table_id);
      for (const auto index_id : table_deps->indexes) {
        auto it = _objects.find(index_id);
        SDB_ASSERT(it != _objects.end());
        visitor(basics::downCast<Index>(**it));
      }
    }
    for (const auto view_id : schema_deps->views) {
      const auto& view_deps = GetDependency<ViewDependency>(view_id);
      for (const auto index_id : view_deps->indexes) {
        auto it = _objects.find(index_id);
        SDB_ASSERT(it != _objects.end());
        visitor(basics::downCast<Index>(**it));
      }
    }
  }

  std::vector<std::shared_ptr<Tokenizer>> GetTokenizers(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        return _resolution_table.GetTokenizerIds(schema_id) |
               std::views::transform(
                 [&](ObjectId tokenizer_id) -> std::shared_ptr<Tokenizer> {
                   return GetObject<Tokenizer>(tokenizer_id);
                 }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<Tokenizer>>{});
  }

  std::vector<std::shared_ptr<PgSqlType>> GetTypes(
    ObjectId db_id, std::string_view schema) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .transform([&](ObjectId schema_id) {
        return _resolution_table.GetTypeIds(schema_id) |
               std::views::transform(
                 [&](ObjectId type_id) -> std::shared_ptr<PgSqlType> {
                   return GetObject<PgSqlType>(type_id);
                 }) |
               std::ranges::to<std::vector>();
      })
      .value_or(std::vector<std::shared_ptr<PgSqlType>>{});
  }

  std::shared_ptr<PgSqlType> GetType(ObjectId db_id, std::string_view schema,
                                     std::string_view name) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .and_then([&](ObjectId schema_id) {
        return _resolution_table.ResolveObject<ResolveType::Type>(schema_id,
                                                                  name);
      })
      .transform(
        [&](ObjectId type_id) { return GetObject<PgSqlType>(type_id); })
      .value_or(nullptr);
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

  bool CheckSchemaEmptyDependency(ObjectId schema_id) const {
    return GetDependency<SchemaDependency>(schema_id)->Empty();
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

  std::shared_ptr<PgSqlFunction> GetFunction(
    ObjectId db_id, std::string_view schema,
    std::string_view function) const final {
    return _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema)
      .and_then([&](ObjectId schema_id) {
        return _resolution_table.ResolveObject<ResolveType::Function>(schema_id,
                                                                      function);
      })
      .transform([&](ObjectId function_id) {
        return GetObject<PgSqlFunction>(function_id);
      })
      .value_or(nullptr);
  }

  std::shared_ptr<Tokenizer> GetTokenizer(ObjectId db_id,
                                          std::string_view schema,
                                          std::string_view name) const final {
    auto schema_id =
      _resolution_table.ResolveObject<ResolveType::Schema>(db_id, schema);
    if (!schema_id) {
      return nullptr;
    }
    auto id =
      _resolution_table.ResolveObject<ResolveType::Tokenizer>(*schema_id, name);
    if (!id) {
      return nullptr;
    }
    return GetObject<Tokenizer>(*id);
  }

  std::shared_ptr<Table> GetTable(ObjectId db_id, std::string_view schema,
                                  std::string_view table) const final {
    auto rel = GetRelation(db_id, schema, table);
    if (!rel || rel->GetType() != ObjectType::Table) {
      return nullptr;
    }
    return basics::downCast<Table>(rel);
  }

  bool HasIndexes(ObjectId relation_id) const final {
    return !GetDependency<RelationDependency>(relation_id)->indexes.empty();
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

  std::vector<std::shared_ptr<IndexShard>> GetIndexShardsByRelation(
    ObjectId relation_id) const final {
    auto relation_deps = GetDependency<RelationDependency>(relation_id);
    return relation_deps->indexes | std::views::transform([&](auto index_id) {
             return GetIndexShard(index_id);
           }) |
           std::ranges::to<std::vector>();
  }

  std::vector<std::shared_ptr<Index>> GetIndexesByRelation(
    ObjectId relation_id) const final {
    auto relation_deps = GetDependency<RelationDependency>(relation_id);
    return relation_deps->indexes | std::views::transform([&](auto index_id) {
             return GetObject<Index>(index_id);
           }) |
           std::ranges::to<std::vector>();
  }

  template<ResolveType Type>
  std::optional<ObjectId> GetObjectId(ObjectId parent_id,
                                      std::string_view name) const {
    return _resolution_table.ResolveObject<Type>(parent_id, name);
  }

  std::vector<std::shared_ptr<Index>> GetIndexesByTokenizer(
    ObjectId tokenizer_id) const {
    auto deps = GetDependency<TokenizerDependency>(tokenizer_id);
    SDB_ASSERT(deps);
    std::vector<std::shared_ptr<Index>> result;
    result.reserve(deps->indexes.size());
    for (auto id : deps->indexes) {
      result.push_back(GetObject<Index>(id));
    }
    return result;
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

  template<typename T>
  std::shared_ptr<const T> GetDependency(ObjectId id) const {
    return basics::downCast<const T>(_object_dependencies.GetDependency(id));
  }

  template<typename T>
  std::shared_ptr<T> GetDependencyForWrite(ObjectId id) {
    auto dep =
      basics::downCast<T>(_object_dependencies.GetDependency(id)->Clone());

    auto inserted = _object_dependencies.AddDependency<T>(id, dep);
    SDB_ASSERT(!inserted);
    return dep;
  }

 private:
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
          auto db_deps = GetDependencyForWrite<DatabaseDependency>(parent_id);
          SDB_ASSERT(db_deps);
          db_deps->schemas.erase(id);
        } break;
        case ObjectType::SecondaryIndex:
        case ObjectType::InvertedIndex: {
          auto relation_deps =
            GetDependencyForWrite<RelationDependency>(parent_id);
          relation_deps->indexes.erase(id);
          const auto& index = basics::downCast<Index>(*obj);
          for (auto tokenizer_id : index.GetTokenizers()) {
            auto dep = GetDependencyForWrite<TokenizerDependency>(tokenizer_id);
            SDB_ASSERT(dep);
            dep->indexes.erase(obj->GetId());
          }
        } break;
        case ObjectType::PgSqlFunction: {
          auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->functions.erase(id);
        } break;
        case ObjectType::Tokenizer: {
          auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->tokenizers.erase(id);
        } break;
        case ObjectType::Table: {
          auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->tables.erase(id);
        } break;
        case ObjectType::PgSqlView: {
          auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->views.erase(id);
        } break;
        case ObjectType::PgSqlType: {
          auto schema_deps = GetDependencyForWrite<SchemaDependency>(parent_id);
          SDB_ASSERT(schema_deps);
          schema_deps->types.erase(id);
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
        drop_childs(schema_deps->types);
        drop_childs(schema_deps->functions);
        drop_childs(schema_deps->views);
        drop_childs(schema_deps->tables);
      } break;
      case ObjectType::Table:
      case ObjectType::PgSqlView: {
        auto relation_deps = GetDependency<RelationDependency>(id);
        if (obj->GetType() == ObjectType::Table) {
          const auto& table_deps =
            basics::downCast<TableDependency>(*relation_deps);
          if (table_deps.shard_id.isSet()) {
            RemoveObjectDefinition(id, table_deps.shard_id);
          }
        }
        // TODO(codeworse): Avoid copy, maybe erase_if?
        auto index_ids = relation_deps->indexes;
        for (auto index_id : index_ids) {
          if (root) {
            // Indexes are not erased from the resolution table during DROP --
            // they're nested in schema scope. Erase explicitly.
            auto index = GetObject<Index>(index_id);
            UnregisterObject(index, id, false);
          } else {
            RemoveObjectDefinition(id, index_id);
          }
        }
      } break;
      case ObjectType::SecondaryIndex:
      case ObjectType::InvertedIndex: {
        auto index_deps = GetDependency<IndexDependency>(id);
        if (index_deps->shard_id.isSet()) {
          RemoveObjectDefinition(id, index_deps->shard_id);
        }

      } break;
      case ObjectType::PgSqlFunction:
      case ObjectType::PgSqlType:
      case ObjectType::Tokenizer:
        break;
      case ObjectType::TableShard:
      case ObjectType::SecondaryIndexShard:
      case ObjectType::InvertedIndexShard:
        SDB_ASSERT(!root);
        break;
      default:
        SDB_UNREACHABLE();
    }
    _object_dependencies.RemoveDependency(id);
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
  ObjectDependencies _object_dependencies;
  ObjectSetById<Object> _objects;
  mutable connector::DuckDBEntryCache _duckdb_cache;
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
                                  std::shared_ptr<PgSqlView> view) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(view), schema_id, false);
  });
}

Result LocalCatalog::RegisterTable(ObjectId database_id, ObjectId schema_id,
                                   std::shared_ptr<Table> table) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(table, schema_id, false);
  });
}

Result LocalCatalog::RegisterFunction(ObjectId database_id, ObjectId schema_id,
                                      std::shared_ptr<PgSqlFunction> function) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(function), schema_id, false);
  });
}

Result LocalCatalog::RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                                       std::shared_ptr<Tokenizer> tokenizer) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(tokenizer), schema_id, false);
  });
}

Result LocalCatalog::RegisterType(ObjectId database_id, ObjectId schema_id,
                                  std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(std::move(type), schema_id, false);
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
          id::kInstance, ObjectType::Database, database_id,
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
      return _engine->CreateDefinition(database_id, ObjectType::Schema,
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
      return _engine->CreateDefinition(database_id, ObjectType::Schema,
                                       schema->GetId(),
                                       [&](bool) { return builder.slice(); });
    },
    [&](auto clone) { clone->UnregisterObject(schema, database_id, true); });
}

Result LocalCatalog::CreateRole(std::shared_ptr<Role> role) {
  SDB_INFO("xxxxx", Logger::FIXME, "Creating role: ", role->GetName());
  absl::MutexLock lock{&_mutex};
  auto r = Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(role, id::kInstance, false);
      if (!r.ok()) {
        return r;
      }
      vpack::Builder b;
      role->WriteInternal(b);
      return _engine->CreateDefinition(id::kInstance, ObjectType::Role,
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

Result LocalCatalog::RegisterIndex(ObjectId database_id, ObjectId schema_id,
                                   std::shared_ptr<Index> index) {
  absl::MutexLock lock{&_mutex};
  return Apply(_snapshot, [&](auto& clone) {
    return clone->RegisterObject(index, index->GetRelationId(), false);
  });
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

Result LocalCatalog::CreateIndexImpl(
  std::string_view relation_schema, std::shared_ptr<Index> index,
  IndexShardOptions& shard_options,
  CreateIndexOperationOptions operation_options) {
  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(index, index->GetRelationId(), false);
      if (!r.ok()) {
        return r;
      }
      auto shard = index->CreateIndexShard(true, ObjectId{0}, shard_options);
      if (!shard) {
        return std::move(shard).error();
      }
      r = clone->RegisterObject(*shard, index->GetId(), false);
      SDB_ASSERT(r.ok());

      if (operation_options.create_with_tombstone) {
        r = _engine->WriteTombstone(index->GetRelationId(), index->GetId());
        if (!r.ok()) {
          return r;
        }
        index->SetTombstoned(true);
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }
      auto shard_type = IndexShardType(index->GetType());
      {  // Write index definition
        vpack::Builder b;
        index->WriteInternal(b);
        r = _engine->CreateDefinition(index->GetRelationId(), index->GetType(),
                                      index->GetId(),
                                      [&](bool) { return b.slice(); });
        if (!r.ok()) {
          return r;
        }
      }
      {  // Write index shard definition
        vpack::Builder b;
        (*shard)->WriteInternal(b);

        r = _engine->CreateDefinition(index->GetId(), shard_type,
                                      (*shard)->GetId(),
                                      [&](bool) { return b.slice(); });
        if (!r.ok()) {
          return r;
        }
      }
      return Result{};
    },
    [&](auto& clone) {
      clone->UnregisterObject(index, index->GetRelationId(), true);
    });
}

namespace {

struct ResolvedIndexRelation {
  ObjectId relation_id;
  std::vector<Column> columns;
};

ResultOr<ResolvedIndexRelation> ResolveIndexRelation(
  const std::shared_ptr<SchemaObject>& relation) {
  if (relation->GetType() == ObjectType::Table) {
    auto& table = basics::downCast<Table>(*relation);
    return ResolvedIndexRelation{
      .relation_id = table.GetId(),
      .columns = table.Columns(),
    };
  } else if (relation->GetType() == ObjectType::PgSqlView) {
    auto& view = basics::downCast<PgSqlView>(*relation);
    const auto& view_info = view.GetInfo();
    auto columns = std::views::iota(size_t{0}, view_info.names.size()) |
                   std::views::transform([&](size_t i) {
                     return Column{
                       .id = static_cast<Column::Id>(i),
                       .type = view_info.types[i],
                       .name = view_info.names[i],
                     };
                   }) |
                   std::ranges::to<std::vector>();
    return ResolvedIndexRelation{
      .relation_id = view.GetId(),
      .columns = std::move(columns),
    };
  }
  return std::unexpected<Result>{std::in_place, ERROR_NOT_IMPLEMENTED,
                                 "Only table or view indexes are supported"};
}

}  // namespace

Result LocalCatalog::CreateSecondaryIndex(
  ObjectId database_id, std::string_view schema, std::string_view relation,
  std::string name, std::vector<CreateIndexColumn>&& columns, bool unique,
  CreateIndexOperationOptions operation_options) {
  if (columns.empty()) {
    return Result{ERROR_BAD_PARAMETER, "Cannot create index without columns"};
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return {ERROR_SERVER_ILLEGAL_NAME, "Cannot resolve schema \"", schema,
            "\""};
  }
  auto rel = _snapshot->GetRelation(database_id, schema, relation);
  if (!rel) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation,
            "\" does not exist"};
  }
  auto resolved = ResolveIndexRelation(rel);
  if (!resolved) {
    return std::move(resolved).error();
  }
  for (auto& c : columns) {
    auto it = absl::c_find_if(
      resolved->columns, [&](const Column& col) { return col.name == c.name; });
    if (it == resolved->columns.end()) {
      return Result{ERROR_BAD_PARAMETER, "column \"", c.name,
                    "\" does not exist"};
    }
    c.catalog_column = &*it;
  }
  auto index = catalog::CreateSecondaryIndex(
    database_id, *schema_id, ObjectId{0}, resolved->relation_id,
    std::move(name), std::move(columns), unique);
  if (!index) {
    return std::move(index).error();
  }
  SecondaryIndexShardOptions shard_options;
  shard_options.base.unique = unique;
  return CreateIndexImpl(schema, *index, shard_options, operation_options);
}

Result LocalCatalog::CreateInvertedIndex(
  ObjectId database_id, std::string_view schema, std::string_view relation,
  std::string name, std::vector<CreateIndexColumn>&& columns,
  IndexShardOptions& shard_options,
  CreateIndexOperationOptions operation_options,
  std::optional<ScorerOptions> wand_scorer) {
  if (columns.empty()) {
    return Result{ERROR_BAD_PARAMETER, "Cannot create index without columns"};
  }
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return {ERROR_SERVER_ILLEGAL_NAME, "Cannot resolve schema \"", schema,
            "\""};
  }
  auto rel = _snapshot->GetRelation(database_id, schema, relation);
  if (!rel) {
    return {ERROR_SERVER_DATA_SOURCE_NOT_FOUND, "relation \"", relation,
            "\" does not exist"};
  }
  auto resolved = ResolveIndexRelation(rel);
  if (!resolved) {
    return std::move(resolved).error();
  }
  for (auto& c : columns) {
    auto it = absl::c_find_if(
      resolved->columns, [&](const Column& col) { return col.name == c.name; });
    if (it == resolved->columns.end()) {
      return Result{ERROR_BAD_PARAMETER, "column \"", c.name,
                    "\" does not exist"};
    }
    c.catalog_column = &*it;
  }
  auto index = catalog::CreateInvertedIndex(
    database_id, schema, *schema_id, ObjectId{0}, resolved->relation_id,
    std::move(name), std::move(columns), _snapshot, std::move(wand_scorer));
  if (!index) {
    return std::move(index).error();
  }
  return CreateIndexImpl(schema, *index, shard_options, operation_options);
}

Result LocalCatalog::CreateView(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<PgSqlView> view, bool replace) {
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
          return existed_object->GetType() == ObjectType::PgSqlView
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
      view->WriteInternal(builder);
      return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlView,
                                       view->GetId(),
                                       [&](bool) { return builder.slice(); });
    },
    [&](auto clone) { clone->UnregisterObject(view, *schema_id, true); });
}

Result LocalCatalog::CreateFunction(ObjectId database_id,
                                    std::string_view schema,
                                    std::shared_ptr<PgSqlFunction> function,
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
      function->WriteInternal(builder);
      return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlFunction,
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
    // // PK must be default sortable or we can not guarantee table scan order
    // if (col->type->providesCustomComparison()) {
    //   return {
    //     ERROR_BAD_PARAMETER, "Column ", col->name,
    //     " has type with custom comparison and can not be part of primary
    //     key"};
    // }
    // // this is current limitation of our pirmary key builder. And might be
    // // lifted some day.
    // if (!col->type->isPrimitiveType()) {
    //   return {ERROR_BAD_PARAMETER, "Column ", col->name,
    //           " has non primitive type and can not be part of primary key"};
    // }
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
      if (operation_options.create_with_tombstone) {
        r = _engine->WriteTombstone(*schema_id, table->GetId());
        if (!r.ok()) {
          return r;
        }
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      vpack::Builder b;
      table->WriteInternal(b);
      r =
        _engine->CreateDefinition(*schema_id, ObjectType::Table, table->GetId(),
                                  [&](bool) { return b.slice(); });
      if (!r.ok()) {
        return r;
      }

      vpack::Builder shard_b;
      shard->WriteInternal(shard_b);
      r = _engine->CreateDefinition(
        shard->GetTableId(), ObjectType::TableShard, shard->GetId(),
        [&](bool) -> vpack::Slice { return shard_b.slice(); });
      return r;
    },
    [&](auto clone) { clone->UnregisterObject(table, *schema_id, true); });
}

Result LocalCatalog::CreateTokenizer(ObjectId database_id,
                                     std::string_view schema,
                                     std::shared_ptr<Tokenizer> dict) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(
    _snapshot,
    [&](std::shared_ptr<SnapshotImpl>& clone) {
      auto r = clone->RegisterObject(dict, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      vpack::Builder b;
      dict->WriteInternal(b);
      return _engine->CreateDefinition(*schema_id, ObjectType::Tokenizer,
                                       dict->GetId(),
                                       [&](bool) { return b.slice(); });
    },
    [&](auto& clone) {
      return clone->UnregisterObject(dict, *schema_id, true);
    });
}

Result LocalCatalog::CreateType(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<PgSqlType> type) {
  absl::MutexLock lock{&_mutex};
  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(
    _snapshot,
    [&](auto& clone) {
      auto r = clone->RegisterObject(type, *schema_id, false);
      if (!r.ok()) {
        return r;
      }
      SDB_IF_FAILURE("unable_to_create") { return Result{ERROR_INTERNAL}; }

      vpack::Builder builder;
      type->WriteInternal(builder);
      return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlType,
                                       type->GetId(),
                                       [&](bool) { return builder.slice(); });
    },
    [&](auto clone) { clone->UnregisterObject(type, *schema_id, true); });
}

template<typename T>
Result LocalCatalog::RenameObjectImpl(ObjectId database_id,
                                      std::string_view schema,
                                      std::string_view name,
                                      std::string_view new_name) {
  constexpr auto kResolveType = []() {
    if constexpr (std::is_same_v<T, PgSqlFunction>) {
      return ResolveType::Function;
    } else {
      return ResolveType::Relation;
    }
  }();

  auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object_id = _snapshot->GetObjectId<kResolveType>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto obj = _snapshot->GetObject(*object_id);
  if (!obj) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  auto obj_type = obj->GetType();
  auto old_obj = std::dynamic_pointer_cast<T>(std::move(obj));
  if (!old_obj) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                  pg::ToPgObjectTypeName(obj_type)};
  }

  if (old_obj->GetName() == new_name) {
    return Result{ERROR_SERVER_DUPLICATE_NAME};
  }

  auto cloned = old_obj->Clone();
  if (!cloned) {
    return Result{ERROR_INTERNAL, "Failed to clone object"};
  }
  auto new_obj = basics::downCast<T>(std::move(cloned));
  SDB_ASSERT(new_obj);
  new_obj->SetName(new_name);

  absl::MutexLock lock{&_mutex};
  return Apply(
    _snapshot,
    [&](std::shared_ptr<SnapshotImpl>& clone) -> Result {
      auto r = clone->ReplaceObject<kResolveType>(*schema_id, name, new_obj);
      if (!r.ok()) {
        return r;
      }

      vpack::Builder b;
      new_obj->WriteInternal(b);

      ObjectId parent_id;
      if constexpr (std::is_same_v<T, Index>) {
        parent_id = old_obj->GetRelationId();
      } else {
        parent_id = *schema_id;
      }

      return _engine->CreateDefinition(parent_id, new_obj->GetType(),
                                       new_obj->GetId(),
                                       [&](bool) { return b.slice(); });
    },
    [&](const std::shared_ptr<SnapshotImpl>& clone) {
      auto current = clone->GetObject<T>(new_obj->GetId());
      if (current->GetName() == new_obj->GetName()) {
        auto r =
          clone->ReplaceObject<kResolveType>(*schema_id, new_name, old_obj);
        SDB_ASSERT(r.ok());
      }
    });
}

Result LocalCatalog::RenameView(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) {
  return RenameObjectImpl<PgSqlView>(database_id, schema, name, new_name);
}

Result LocalCatalog::RenameTable(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  return RenameObjectImpl<Table>(database_id, schema, name, new_name);
}

Result LocalCatalog::RenameIndex(ObjectId database_id, std::string_view schema,
                                 std::string_view name,
                                 std::string_view new_name) {
  return RenameObjectImpl<Index>(database_id, schema, name, new_name);
}

Result LocalCatalog::RenameRelation(ObjectId database_id,
                                    std::string_view schema,
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

  auto obj = _snapshot->GetObject(*object_id);
  if (!obj) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  switch (obj->GetType()) {
    case ObjectType::Table:
      return RenameTable(database_id, schema, name, new_name);
    case ObjectType::PgSqlView:
      return RenameView(database_id, schema, name, new_name);
    case ObjectType::SecondaryIndex:
    case ObjectType::InvertedIndex:
      return RenameIndex(database_id, schema, name, new_name);
    default:
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(obj->GetType())};
  }
}

Result LocalCatalog::RenameFunction(ObjectId database_id,
                                    std::string_view schema,
                                    std::string_view name,
                                    std::string_view new_name) {
  return RenameObjectImpl<PgSqlFunction>(database_id, schema, name, new_name);
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
      new_role_ptr->WriteInternal(b);
      return _engine->CreateDefinition(id::kInstance, ObjectType::Role,
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
                                ChangeCallback<PgSqlView> new_view) {
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

  auto view = basics::downCast<PgSqlView>(_snapshot->GetObject(*object_id));
  if (!view) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }

  std::shared_ptr<PgSqlView> updated;
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
    updated->WriteInternal(builder);
    return _engine->CreateDefinition(*schema_id, ObjectType::PgSqlView,
                                     updated->GetId(),
                                     [&](bool) { return builder.slice(); });
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

  auto obj = _snapshot->GetObject(*object_id);
  if (!obj) {
    return Result{ERROR_SERVER_DATA_SOURCE_NOT_FOUND};
  }
  if (obj->GetType() != ObjectType::Table) {
    return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                  pg::ToPgObjectTypeName(obj->GetType())};
  }

  auto table = basics::downCast<Table>(std::move(obj));
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
      return _engine->CreateDefinition(*schema_id, ObjectType::Table,
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
    return _engine->DropDefinition(id::kInstance, ObjectType::Role,
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
    SDB_ASSERT(clone);
    auto database = clone->GetDatabase(name);
    if (!database) {
      return Result{ERROR_SERVER_DATABASE_NOT_FOUND, "database \"", name,
                    "\" does not exist"};
    }
    auto task = clone->CreateDatabaseDrop(database);
    if (auto r =
          GetServerEngine().WriteTombstone(id::kInstance, database->GetId());
        !r.ok()) {
      return r;
    }
    clone->UnregisterObject(clone->GetObject<Database>(database->GetId()),
                            id::kInstance);
    // Check that SereneDB won't open this database after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::DropSchema(std::string_view database,
                                std::string_view name, bool cascade) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, name);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  if (!cascade && !_snapshot->CheckSchemaEmptyDependency(*schema_id)) {
    return Result{ERROR_BAD_PARAMETER};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto schema = clone->GetObject<Schema>(*schema_id);
    SDB_ASSERT(schema);
    auto task = clone->CreateSchemaDrop(*database_id, schema, true);
    if (auto r = _engine->WriteTombstone(*database_id, *schema_id); !r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(schema), *database_id);
    // Check that SereneDB won't open this schema after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::DropTable(std::string_view database,
                               std::string_view schema, std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto table_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!table_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*table_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::Table) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto table = basics::downCast<Table>(std::move(object));
    auto task = clone->CreateTableDrop(*database_id, *schema_id, table, true);
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

Result LocalCatalog::RemoveTombstone(ObjectId database_id,
                                     std::string_view schema,
                                     std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto object_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!object_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  auto object = _snapshot->GetObject(*object_id);
  if (!object) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  ObjectId tombstone_parent;
  if (IsIndex(object->GetType())) {
    auto& index = basics::downCast<Index>(*object);
    tombstone_parent = index.GetRelationId();
  } else {
    tombstone_parent = *schema_id;
  }

  auto r = _engine->DropDefinition(tombstone_parent, ObjectType::Tombstone,
                                   *object_id);

  // Unlike most catalog operations that clone the snapshot, here we modify the
  // object in-place because the tombstone flag is simple in-memory state.
  auto& schema_obj = basics::downCast<SchemaObject>(*object);
  schema_obj.SetTombstoned(false);

  return r;
}

Result LocalCatalog::DropIndex(std::string_view database,
                               std::string_view schema, std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto index_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!index_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto obj = clone->GetObject(*index_id);
    SDB_ASSERT(obj);
    if (!IsIndex(obj->GetType())) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(obj->GetType())};
    }
    auto index = basics::downCast<Index>(std::move(obj));
    if (auto r = _engine->WriteTombstone(index->GetRelationId(), *index_id);
        !r.ok()) {
      return r;
    }

    // Check that SereneDB won't open this index after reboot
    SDB_IF_FAILURE("crash_on_drop") { return Result{}; }

    auto task = clone->CreateIndexDrop(*database_id, *schema_id,
                                       index->GetRelationId(), index, true);
    clone->UnregisterObject(index, *schema_id);
    DropTask::Schedule(std::move(task)).Detach();
    return Result{};
  });
}

Result LocalCatalog::DropView(std::string_view database,
                              std::string_view schema, std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto view_id =
    _snapshot->GetObjectId<ResolveType::Relation>(*schema_id, name);
  if (!view_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*view_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::PgSqlView) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto view = basics::downCast<PgSqlView>(std::move(object));
    auto r =
      _engine->DropDefinition(*schema_id, ObjectType::PgSqlView, view->GetId());
    if (!r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(view), *schema_id);
    return Result{};
  });
}

Result LocalCatalog::DropType(std::string_view database,
                              std::string_view schema, std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto type_id =
    _snapshot->GetObjectId<ResolveType::Type>(*schema_id, name);
  if (!type_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto object = clone->GetObject(*type_id);
    SDB_ASSERT(object);
    if (object->GetType() != ObjectType::PgSqlType) {
      return Result{ERROR_SERVER_OBJECT_TYPE_MISMATCH,
                    pg::ToPgObjectTypeName(object->GetType())};
    }
    auto type = basics::downCast<PgSqlType>(std::move(object));
    auto r =
      _engine->DropDefinition(*schema_id, ObjectType::PgSqlType, type->GetId());
    if (!r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(type), *schema_id);
    return Result{};
  });
}

Result LocalCatalog::DropFunction(std::string_view database,
                                  std::string_view schema,
                                  std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto function_id =
    _snapshot->GetObjectId<ResolveType::Function>(*schema_id, name);
  if (!function_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto function = clone->GetObject<PgSqlFunction>(*function_id);
    SDB_ASSERT(function);
    auto r = _engine->DropDefinition(*schema_id, ObjectType::PgSqlFunction,
                                     *function_id);
    if (!r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(function), *schema_id);
    return Result{};
  });
}

Result LocalCatalog::DropTokenizer(std::string_view database,
                                   std::string_view schema,
                                   std::string_view name) {
  absl::MutexLock lock{&_mutex};

  const auto database_id =
    _snapshot->GetObjectId<ResolveType::Database>(id::kInstance, database);
  if (!database_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto schema_id =
    _snapshot->GetObjectId<ResolveType::Schema>(*database_id, schema);
  if (!schema_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }
  const auto tokenizer_id =
    _snapshot->GetObjectId<ResolveType::Tokenizer>(*schema_id, name);
  if (!tokenizer_id) {
    return Result{ERROR_SERVER_ILLEGAL_NAME};
  }

  const auto deps = _snapshot->GetIndexesByTokenizer(*tokenizer_id);
  if (!deps.empty()) {
    static constexpr size_t kReportIndexes = 5;
    std::vector<std::string_view> confliciting_indexes;
    for (auto index : deps) {
      SDB_ASSERT(index);
      confliciting_indexes.push_back(index->GetName());
      if (confliciting_indexes.size() == kReportIndexes) {
        break;
      }
    }
    if (confliciting_indexes.size() < deps.size()) {
      confliciting_indexes.push_back("...");
    }
    return Result{ERROR_INTERNAL,
                  "Can not drop text dictionary used in the indexes ",
                  absl::StrJoin(confliciting_indexes, ", ")};
  }

  return Apply(_snapshot, [&](std::shared_ptr<SnapshotImpl>& clone) {
    SDB_ASSERT(clone);
    auto tokenizer = clone->GetObject<Tokenizer>(*tokenizer_id);
    SDB_ASSERT(tokenizer);
    auto r =
      _engine->DropDefinition(*schema_id, ObjectType::Tokenizer, *tokenizer_id);
    if (!r.ok()) {
      return r;
    }
    clone->UnregisterObject(std::move(tokenizer), *schema_id);
    return Result{};
  });
}

std::shared_ptr<const Snapshot> LocalCatalog::GetCatalogSnapshot()
  const noexcept {
  return std::atomic_load_explicit(&_snapshot, std::memory_order_acquire);
}

}  // namespace sdb::catalog
