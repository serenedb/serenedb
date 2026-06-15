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

#include <absl/functional/function_ref.h>
#include <absl/synchronization/mutex.h>

#include <expected>
#include <functional>
#include <memory>
#include <shared_mutex>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/containers/node_hash_map.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/result_or.h"
#include "catalog/column_expr.h"
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
#include "catalog/sequence.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "connector/duckdb_entry_cache.h"
#include "storage_engine/index_shard.h"

namespace sdb::catalog {

template<typename T>
using ChangeCallback = absl::FunctionRef<Result(const T&, std::shared_ptr<T>&)>;

class SecondaryIndex;
class InvertedIndex;

enum class EdgeAction : uint8_t { Add, Delete };

struct CreateTableOperationOptions {
  bool create_with_tombstone = false;
};

struct CreateIndexOperationOptions {
  bool create_with_tombstone = false;
};

template<typename T>
constexpr ObjectType GetObjectType() noexcept {
  if constexpr (std::is_same_v<T, PgSqlView>) {
    return ObjectType::PgSqlView;
  } else if constexpr (std::is_same_v<T, Database>) {
    return ObjectType::Database;
  } else if constexpr (std::is_same_v<T, Schema>) {
    return ObjectType::Schema;
  } else if constexpr (std::is_same_v<T, Role>) {
    return ObjectType::Role;
  } else if constexpr (std::is_same_v<T, PgSqlFunction>) {
    return ObjectType::PgSqlFunction;
  } else if constexpr (std::is_same_v<T, PgSqlType>) {
    return ObjectType::PgSqlType;
  } else if constexpr (std::is_same_v<T, Table>) {
    return ObjectType::Table;
  } else if constexpr (std::is_same_v<T, SecondaryIndex>) {
    return ObjectType::SecondaryIndex;
  } else if constexpr (std::is_same_v<T, InvertedIndex>) {
    return ObjectType::InvertedIndex;
  } else if constexpr (std::is_same_v<T, Tokenizer>) {
    return ObjectType::Tokenizer;
  } else if constexpr (std::is_same_v<T, Sequence>) {
    return ObjectType::Sequence;
  } else {
    static_assert(false);
  }
}

struct Snapshot {
  Snapshot();
  ~Snapshot();

  std::shared_ptr<Snapshot> Clone() const;
  void EndLoad() noexcept;

  connector::DuckDBEntryCache& GetDuckDBEntryCache() const;
  std::vector<std::shared_ptr<Role>> GetRoles() const;
  std::vector<std::shared_ptr<Database>> GetDatabases() const;
  std::vector<std::shared_ptr<Schema>> GetSchemas(ObjectId database) const;
  std::vector<std::shared_ptr<Object>> GetRelations(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<Table>> GetTables(ObjectId database,
                                                std::string_view schema) const;
  std::vector<std::shared_ptr<PgSqlView>> GetViews(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<PgSqlFunction>> GetFunctions(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<Index>> GetIndexes(ObjectId database,
                                                 std::string_view schema) const;
  std::vector<std::shared_ptr<Sequence>> GetSequences(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<Tokenizer>> GetTokenizers(
    ObjectId database, std::string_view schema) const;
  std::vector<std::shared_ptr<PgSqlType>> GetTypes(
    ObjectId database, std::string_view schema) const;

  // Allocation-free iteration over schema objects. Use these when the caller
  // can process each item inline and only needs to buffer the misses.
  void VisitRelations(ObjectId database, std::string_view schema,
                      absl::FunctionRef<void(const Object&)> visitor) const;
  void VisitViews(ObjectId database, std::string_view schema,
                  absl::FunctionRef<void(const PgSqlView&)> visitor) const;
  void VisitFunctions(
    ObjectId database, std::string_view schema,
    absl::FunctionRef<void(const PgSqlFunction&)> visitor) const;
  void VisitIndexes(ObjectId database, std::string_view schema,
                    absl::FunctionRef<void(const Index&)> visitor) const;

  std::shared_ptr<Role> GetRole(std::string_view name) const;
  std::shared_ptr<Database> GetDatabase(std::string_view database) const;
  std::shared_ptr<Database> GetDatabase(ObjectId database) const;
  std::shared_ptr<Schema> GetSchema(ObjectId database,
                                    std::string_view schema) const;
  std::shared_ptr<Object> GetRelation(ObjectId database,
                                      std::string_view schema,
                                      std::string_view name) const;
  std::shared_ptr<PgSqlFunction> GetFunction(ObjectId database,
                                             std::string_view schema,
                                             std::string_view name) const;
  std::shared_ptr<Tokenizer> GetTokenizer(ObjectId database,
                                          std::string_view schema,
                                          std::string_view name) const;
  std::shared_ptr<PgSqlType> GetType(ObjectId database, std::string_view schema,
                                     std::string_view name) const;
  std::shared_ptr<Table> GetTable(ObjectId database_id, std::string_view schema,
                                  std::string_view name) const;
  std::shared_ptr<Sequence> GetSequence(ObjectId database, ObjectId schema_id,
                                        std::string_view name) const;

  bool HasIndexes(ObjectId relation_id) const;
  std::shared_ptr<Object> GetObject(ObjectId id) const;

  ObjectId GetDatabaseId(const Object& obj) const {
    if (obj.GetType() == ObjectType::Database) {
      return obj.GetId();
    }
    for (auto cur = obj.GetParentId(); cur.isSet();) {
      auto parent = GetObject(cur);
      if (!parent) {
        return {};
      }
      if (parent->GetType() == ObjectType::Database) {
        return cur;
      }
      cur = parent->GetParentId();
    }
    return {};
  }

  std::shared_ptr<TableShard> GetTableShard(ObjectId id) const;
  std::vector<std::shared_ptr<IndexShard>> GetIndexShardsByRelation(
    ObjectId relation_id) const;
  std::vector<std::shared_ptr<Index>> GetIndexesByRelation(
    ObjectId relation_id) const;
  std::shared_ptr<IndexShard> GetIndexShard(ObjectId index_id) const;

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto obj = GetObject(id);
    if (!obj) {
      return nullptr;
    }
    if constexpr (std::is_same_v<T, Object>) {
      return obj;
    } else if constexpr (std::is_same_v<T, TableShard>) {
      if (obj->GetType() != ObjectType::TableShard) {
        return nullptr;
      }
    } else if constexpr (std::is_same_v<T, Index>) {
      if (!IsIndex(obj->GetType())) {
        return nullptr;
      }
    } else if constexpr (std::is_same_v<T, IndexShard>) {
      if (!IsIndexShard(obj->GetType())) {
        return nullptr;
      }
    } else {
      if (obj->GetType() != GetObjectType<T>()) {
        return nullptr;
      }
    }
    return basics::downCast<T>(obj);
  }

  std::shared_ptr<DatabaseDrop> CreateDatabaseDrop(
    const std::shared_ptr<Database>& db, duckdb::shared_ptr<void> keep_alive);
  std::shared_ptr<SchemaDrop> CreateSchemaDrop(
    ObjectId db_id, const std::shared_ptr<Schema>& schema, bool is_root);
  std::shared_ptr<TableDrop> CreateTableDrop(
    ObjectId db_id, ObjectId schema_id, const std::shared_ptr<Table>& table,
    bool is_root);
  std::shared_ptr<IndexDrop> CreateIndexDrop(
    ObjectId db_id, ObjectId schema_id, ObjectId table_id,
    const std::shared_ptr<Index>& index, bool is_root);

  // Store-table name of `table_id` ("db.schema.table"), or nullopt when
  // the id is unset (self-referencing FK) or not resolvable.
  std::optional<std::string> ComposeStoreTableName(ObjectId table_id) const;

  // Cross-tree fixups for DROP seed. Composition cleanup is async.
  DropPlan ComputeDropPlan(ObjectId seed) const;
  // Plan for ALTER TABLE DROP COLUMN: rewrite the owning table without the
  // column and cascade-drop every index covering it (PG column->index cascade).
  DropPlan ComputeColumnDropPlan(ObjectId table_id, ObjectId col_id) const;
  void CommitDropPlan(CatalogStore::WriteContext& ctx,
                      const DropPlan& plan) const;
  // Apply cross-tree mutations in-memory; schedule IndexDrop tasks for
  // cascade-dropped indexes (column->index cascade).
  void ApplyDropPlan(ObjectId db_id, DropPlan& plan);

  bool CheckSchemaEmptyDependency(ObjectId schema_id) const;

  const auto& Objects() const noexcept { return _objects; }

  void AddDependencies(ObjectId parent_id, const Object& obj);
  void ModifyTableDependencies(ObjectId schema_id, const Table& table,
                               EdgeAction action);
  void ModifyViewDependencies(ObjectId schema_id, const PgSqlView& view,
                              RefKinds kinds, EdgeAction action);
  void ModifyFunctionDependencies(ObjectId schema_id, const PgSqlFunction& func,
                                  EdgeAction action);
  void ModifyInvertedIndexDependencies(const InvertedIndex& index,
                                       ObjectId index_id, EdgeAction action);

  template<typename T>
  Result RegisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool replace);

  template<typename T>
  void UnregisterObject(std::shared_ptr<T> object, ObjectId parent_id,
                        bool maybe_not_found = false) noexcept;

  template<typename DependencyType = void>
  Result AddObjectDefinition(ObjectId parent_id,
                             std::shared_ptr<Object> object);

  template<ResolveType Type>
  Result AddToResolution(ObjectId parent_id, ObjectId id, std::string_view name,
                         bool replace);

  template<ResolveType Type>
  void RemoveFromResolution(ObjectId parent_id, std::string_view name,
                            bool maybe_not_found = false) noexcept;

  template<ResolveType Type>
  std::optional<ObjectId> GetObjectId(ObjectId parent_id,
                                      std::string_view name) const;

  template<ResolveType Type>
  Result ReplaceObject(ObjectId parent_id, std::string_view old_name,
                       std::shared_ptr<Object> new_object);

  template<typename Dep, typename Member, typename Edge>
  void ModifyDependency(ObjectId target, Member Dep::* mem, const Edge& edge,
                        EdgeAction action);

  template<ResolveType Kind, ObjectType... Allowed>
  ObjectId Resolve(ObjectId db_id, ObjectId default_schema_id,
                   std::string_view catalog, std::string_view schema,
                   std::string_view name) const;

  template<typename T>
  std::shared_ptr<const T> GetDependency(ObjectId id) const;

  template<typename T>
  std::shared_ptr<T> GetDependencyForWrite(ObjectId id);

 private:
  void RemoveObjectDefinition(ObjectId parent_id, ObjectId id,
                              bool root = false,
                              bool maybe_not_found = false) noexcept;

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

    ObjectSetByName<Object> relations;
    ObjectSetByName<Object> functions;
  };

  ResolutionTable _resolution_table;
  ObjectDependencies _deps;
  ObjectSetById<Object> _objects;
  mutable connector::DuckDBEntryCache _duckdb_cache;
  bool _in_load = true;
};

template<typename V>
void VisitTableShards(const Snapshot& snapshot, ObjectId database_id,
                      std::string_view schema, V&& v) {
  for (auto& rel : snapshot.GetRelations(database_id, schema)) {
    if (rel->GetType() != ObjectType::Table) {
      continue;
    }

    auto table = basics::downCast<Table>(rel);
    auto shard = snapshot.GetTableShard(table->GetId());
    if (!shard) {
      continue;
    }
    // SDB_ENSURE(shard, ERROR_INTERNAL);
    v(shard);
  }
}

using IndexFactory =
  absl::FunctionRef<ResultOr<std::shared_ptr<Index>>(const Object*)>;

class Catalog final {
 public:
  explicit Catalog();

  Result RegisterRole(std::shared_ptr<Role> role);
  Result RegisterDatabase(std::shared_ptr<Database> database);
  Result RegisterSchema(ObjectId database_id, std::shared_ptr<Schema> schema);
  Result RegisterView(ObjectId schema_id, std::shared_ptr<PgSqlView> view);
  Result RegisterSequence(ObjectId database_id, ObjectId schema_id,
                          std::shared_ptr<Sequence> sequence);
  Result RegisterFunction(ObjectId database_id, ObjectId schema_id,
                          std::shared_ptr<PgSqlFunction> function);
  Result RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                           std::shared_ptr<Tokenizer> tokenizer);
  Result RegisterType(ObjectId database_id, ObjectId schema_id,
                      std::shared_ptr<PgSqlType> type);
  Result RegisterTable(ObjectId database_id, ObjectId schema_id,
                       std::shared_ptr<Table> table);
  Result RegisterTableShard(std::shared_ptr<TableShard> shard);
  Result RegisterIndex(ObjectId database_id, ObjectId schema_id,
                       std::shared_ptr<Index> index);
  Result RegisterIndexShard(std::shared_ptr<IndexShard> shard);

  Result CreateDatabase(std::shared_ptr<Database> database);
  Result CreateRole(std::shared_ptr<Role> role);
  Result CreateView(ObjectId database_id, std::string_view schema,
                    std::shared_ptr<PgSqlView> view, bool replace);
  Result CreateSequence(ObjectId database_id, std::string_view schema,
                        std::shared_ptr<Sequence> sequence, bool if_not_exists);
  Result CreateSchema(ObjectId database_id, std::shared_ptr<Schema> schema);
  Result CreateFunction(ObjectId database_id, std::string_view schema,
                        std::shared_ptr<PgSqlFunction> function, bool replace);
  Result CreateTable(ObjectId database_id, std::string_view schema,
                     CreateTableOptions table,
                     CreateTableOperationOptions operation_options);
  Result CreateSecondaryIndex(ObjectId database_id, std::string_view schema,
                              std::string_view relation, std::string name,
                              std::vector<CreateIndexColumn>&& columns,
                              bool unique,
                              CreateIndexOperationOptions operation_options);
  Result CreateInvertedIndex(duckdb::ClientContext& context,
                             ObjectId database_id, std::string_view schema,
                             std::string_view relation, std::string name,
                             std::vector<CreateIndexColumn>&& columns,
                             InvertedIndexOptions options,
                             CreateIndexOperationOptions operation_options);
  Result CreateTokenizer(ObjectId database_id, std::string_view schema,
                         std::shared_ptr<Tokenizer> dict);
  Result CreateType(ObjectId database_id, std::string_view schema,
                    std::shared_ptr<PgSqlType> type);

  Result RenameView(ObjectId database_id, std::string_view schema,
                    std::string_view name, std::string_view new_name);
  Result RenameTable(ObjectId database_id, std::string_view schema,
                     std::string_view name, std::string_view new_name);
  Result RenameIndex(ObjectId database_id, std::string_view schema,
                     std::string_view name, std::string_view new_name);
  Result RenameRelation(ObjectId database_id, std::string_view schema,
                        std::string_view name, std::string_view new_name);
  Result RenameFunction(ObjectId database_id, std::string_view schema,
                        std::string_view name, std::string_view new_name);

  Result ChangeView(ObjectId database_id, std::string_view schema,
                    std::string_view name, ChangeCallback<PgSqlView> callback);
  Result ChangeTable(ObjectId database_id, std::string_view schema,
                     std::string_view name, ChangeCallback<Table> callback);
  Result ChangeRole(std::string_view name, ChangeCallback<Role> callback);

  Result DropDatabase(std::string_view name,
                      duckdb::shared_ptr<void> keep_alive);
  Result DropRole(std::string_view role);
  Result DropSchema(std::string_view database, std::string_view name,
                    bool cascade);
  Result DropView(std::string_view database, std::string_view schema,
                  std::string_view name, bool cascade);
  Result DropSequence(std::string_view database, std::string_view schema,
                      std::string_view name, bool if_exists, bool cascade);
  Result DropType(std::string_view database, std::string_view schema,
                  std::string_view name, bool cascade);
  Result DropFunction(std::string_view database, std::string_view schema,
                      std::string_view name, bool cascade);
  Result DropTokenizer(std::string_view database, std::string_view schema,
                       std::string_view name, bool cascade);
  Result DropTable(std::string_view database, std::string_view schema,
                   std::string_view name, bool cascade);
  Result DropIndex(std::string_view database, std::string_view schema,
                   std::string_view name, bool cascade);
  Result DropTableColumn(ObjectId database_id, std::string_view schema,
                         std::string_view table, std::string_view column,
                         bool if_exists);
  Result ChangeColumnType(ObjectId database_id, std::string_view schema,
                          std::string_view table, std::string_view column,
                          duckdb::LogicalType new_type, std::string using_sql);

  Result RemoveTombstone(ObjectId database_id, std::string_view schema,
                         std::string_view name);

  Result FinalizeLoad();

  std::shared_ptr<const Snapshot> GetCatalogSnapshot() const noexcept;

 private:
  Result CreateIndexImpl(std::string_view schema, std::shared_ptr<Index> index,
                         CreateIndexOperationOptions operation_options);

  template<typename T>
  Result RenameObjectImpl(ObjectId database_id, std::string_view schema,
                          std::string_view name, std::string_view new_name);

  template<typename T>
  Result RenameObjectImpl(ObjectId schema_id, std::string_view database_name,
                          std::string_view schema_name, std::string_view name,
                          std::string_view new_name, std::shared_ptr<T> object);

  mutable absl::Mutex _mutex;
  mutable std::shared_mutex _snapshot_mutex;
  std::shared_ptr<const Snapshot> _snapshot;
  CatalogStore* _engine;
};

// Builds the single in-process catalog, loads boot state, bootstraps the
// default role, and attaches the databases. Throws on failure.
void InitCatalog();
void ShutdownCatalog();

ResultOr<std::shared_ptr<Database>> GetDatabase(ObjectId database_id);
ResultOr<std::shared_ptr<Database>> GetDatabase(std::string_view name);
Catalog& GetCatalog();

}  // namespace sdb::catalog
