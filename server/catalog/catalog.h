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
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/containers/flat_hash_set.h"
#include "basics/down_cast.h"
#include "basics/errors.h"
#include "basics/result_or.h"
#include "catalog/database.h"
#include "catalog/drop_task.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/types.h"
#include "catalog/user_type.h"
#include "catalog/view.h"
#include "rocksdb_engine_catalog/rocksdb_engine_catalog.h"
#include "rocksdb_engine_catalog/rocksdb_types.h"
#include "storage_engine/index_shard.h"

namespace sdb::connector {

class DuckDBEntryCache;
}

namespace sdb::catalog {

template<typename T>
using ChangeCallback = absl::FunctionRef<Result(const T&, std::shared_ptr<T>&)>;

class SecondaryIndex;
class InvertedIndex;

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
  virtual ~Snapshot() = default;

  virtual connector::DuckDBEntryCache& GetDuckDBEntryCache() const = 0;
  virtual std::vector<std::shared_ptr<Role>> GetRoles() const = 0;
  virtual std::vector<std::shared_ptr<Database>> GetDatabases() const = 0;
  virtual std::vector<std::shared_ptr<Schema>> GetSchemas(
    ObjectId database) const = 0;
  virtual std::vector<std::shared_ptr<Object>> GetRelations(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<Table>> GetTables(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<PgSqlView>> GetViews(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<PgSqlFunction>> GetFunctions(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<Index>> GetIndexes(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<Sequence>> GetSequences(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<Tokenizer>> GetTokenizers(
    ObjectId database, std::string_view schema) const = 0;
  virtual std::vector<std::shared_ptr<PgSqlType>> GetTypes(
    ObjectId database, std::string_view schema) const = 0;

  // Allocation-free iteration over schema objects. Use these when the caller
  // can process each item inline and only needs to buffer the misses.
  virtual void VisitRelations(
    ObjectId database, std::string_view schema,
    absl::FunctionRef<void(const Object&)> visitor) const = 0;
  virtual void VisitViews(
    ObjectId database, std::string_view schema,
    absl::FunctionRef<void(const PgSqlView&)> visitor) const = 0;
  virtual void VisitFunctions(
    ObjectId database, std::string_view schema,
    absl::FunctionRef<void(const PgSqlFunction&)> visitor) const = 0;
  virtual void VisitIndexes(
    ObjectId database, std::string_view schema,
    absl::FunctionRef<void(const Index&)> visitor) const = 0;

  virtual std::shared_ptr<Role> GetRole(std::string_view name) const = 0;
  virtual std::shared_ptr<Database> GetDatabase(
    std::string_view database) const = 0;
  virtual std::shared_ptr<Database> GetDatabase(ObjectId database) const = 0;
  virtual std::shared_ptr<Schema> GetSchema(ObjectId database,
                                            std::string_view schema) const = 0;
  virtual std::shared_ptr<Object> GetRelation(ObjectId database,
                                              std::string_view schema,
                                              std::string_view name) const = 0;
  virtual std::shared_ptr<PgSqlFunction> GetFunction(
    ObjectId database, std::string_view schema,
    std::string_view name) const = 0;
  virtual std::shared_ptr<Tokenizer> GetTokenizer(
    ObjectId database, std::string_view schema,
    std::string_view name) const = 0;
  virtual std::shared_ptr<PgSqlType> GetType(ObjectId database,
                                             std::string_view schema,
                                             std::string_view name) const = 0;
  virtual std::shared_ptr<Table> GetTable(ObjectId database_id,
                                          std::string_view schema,
                                          std::string_view name) const = 0;
  virtual std::shared_ptr<Sequence> GetSequence(
    ObjectId database, ObjectId schema_id, std::string_view name) const = 0;

  virtual bool HasIndexes(ObjectId relation_id) const = 0;
  virtual std::shared_ptr<Object> GetObject(ObjectId id) const = 0;

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

  virtual std::shared_ptr<TableShard> GetTableShard(ObjectId id) const = 0;
  virtual std::vector<std::shared_ptr<IndexShard>> GetIndexShardsByRelation(
    ObjectId relation_id) const = 0;
  virtual std::vector<std::shared_ptr<Index>> GetIndexesByRelation(
    ObjectId relation_id) const = 0;
  virtual std::shared_ptr<IndexShard> GetIndexShard(
    ObjectId index_id) const = 0;

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto obj = GetObject(id);
    if (!obj) {
      return nullptr;
    }
    if constexpr (std::is_same_v<T, Index>) {
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

struct LogicalCatalog {
  virtual ~LogicalCatalog() = default;

  virtual Result RegisterRole(std::shared_ptr<catalog::Role> role) = 0;
  virtual Result RegisterDatabase(std::shared_ptr<Database> database) = 0;
  virtual Result RegisterSchema(ObjectId database,
                                std::shared_ptr<catalog::Schema> schema) = 0;
  virtual Result RegisterView(ObjectId schema_id,
                              std::shared_ptr<catalog::PgSqlView> view) = 0;
  virtual Result RegisterSequence(
    ObjectId database_id, ObjectId schema_id,
    std::shared_ptr<catalog::Sequence> sequence) = 0;
  virtual Result RegisterTable(ObjectId database_id, ObjectId schema_id,
                               std::shared_ptr<Table> table) = 0;
  virtual Result RegisterTableShard(std::shared_ptr<TableShard> shard) = 0;
  virtual Result RegisterFunction(
    ObjectId database_id, ObjectId schema_id,
    std::shared_ptr<catalog::PgSqlFunction> function) = 0;
  virtual Result RegisterTokenizer(
    ObjectId database_id, ObjectId schema_id,
    std::shared_ptr<catalog::Tokenizer> tokenizer) = 0;
  virtual Result RegisterType(ObjectId database_id, ObjectId schema_id,
                              std::shared_ptr<catalog::PgSqlType> type) = 0;
  virtual Result RegisterIndex(ObjectId database_id, ObjectId schema_id,
                               std::shared_ptr<Index> index) = 0;
  virtual Result RegisterIndexShard(std::shared_ptr<IndexShard> shard) = 0;

  virtual Result CreateDatabase(
    std::shared_ptr<catalog::Database> database) = 0;
  virtual Result CreateRole(std::shared_ptr<catalog::Role> role) = 0;
  virtual Result CreateSchema(ObjectId database_id,
                              std::shared_ptr<catalog::Schema> schema) = 0;
  virtual Result CreateView(ObjectId database_id, std::string_view schema,
                            std::shared_ptr<catalog::PgSqlView> view,
                            bool replace) = 0;
  virtual Result CreateSequence(ObjectId database_id, std::string_view schema,
                                std::shared_ptr<catalog::Sequence> sequence,
                                bool if_not_exists) = 0;
  virtual Result CreateFunction(
    ObjectId database_id, std::string_view schema,
    std::shared_ptr<catalog::PgSqlFunction> function, bool replace) = 0;
  virtual Result CreateTokenizer(ObjectId database_id, std::string_view schema,
                                 std::shared_ptr<Tokenizer> dict) = 0;
  virtual Result CreateType(ObjectId database_id, std::string_view schema,
                            std::shared_ptr<PgSqlType> type) = 0;
  virtual Result CreateTable(ObjectId database_id, std::string_view schema,
                             CreateTableOptions options,
                             CreateTableOperationOptions operation_options) = 0;
  virtual Result CreateSecondaryIndex(
    ObjectId database_id, std::string_view schema, std::string_view relation,
    std::string name, std::vector<CreateIndexColumn>&& columns, bool unique,
    CreateIndexOperationOptions operation_options) = 0;
  virtual Result CreateInvertedIndex(
    ObjectId database_id, std::string_view schema, std::string_view relation,
    std::string name, std::vector<CreateIndexColumn>&& columns,
    InvertedIndexOptions options,
    CreateIndexOperationOptions operation_options) = 0;

  virtual Result RenameTable(ObjectId database_id, std::string_view schema,
                             std::string_view name,
                             std::string_view new_name) = 0;
  virtual Result RenameView(ObjectId database_id, std::string_view schema,
                            std::string_view name,
                            std::string_view new_name) = 0;
  virtual Result RenameIndex(ObjectId database_id, std::string_view schema,
                             std::string_view name,
                             std::string_view new_name) = 0;
  virtual Result RenameRelation(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) = 0;
  virtual Result RenameFunction(ObjectId database_id, std::string_view schema,
                                std::string_view name,
                                std::string_view new_name) = 0;

  virtual Result ChangeView(ObjectId database_id, std::string_view schema,
                            std::string_view name,
                            ChangeCallback<catalog::PgSqlView> callback) = 0;
  virtual Result ChangeTable(ObjectId database_id, std::string_view schema,
                             std::string_view name,
                             ChangeCallback<catalog::Table> callback) = 0;
  virtual Result ChangeRole(std::string_view name,
                            ChangeCallback<catalog::Role> callback) = 0;

  virtual Result DropDatabase(std::string_view name,
                              duckdb::shared_ptr<void> keep_alive) = 0;
  virtual Result DropRole(std::string_view name) = 0;
  virtual Result DropSchema(std::string_view database, std::string_view name,
                            bool cascade) = 0;
  virtual Result DropFunction(std::string_view database,
                              std::string_view schema, std::string_view name,
                              bool cascade) = 0;
  virtual Result DropTokenizer(std::string_view database,
                               std::string_view schema, std::string_view name,
                               bool cascade) = 0;
  virtual Result DropView(std::string_view database, std::string_view schema,
                          std::string_view name, bool cascade) = 0;
  virtual Result DropSequence(std::string_view database,
                              std::string_view schema, std::string_view name,
                              bool if_exists, bool cascade) = 0;
  virtual Result DropType(std::string_view database, std::string_view schema,
                          std::string_view name, bool cascade) = 0;
  virtual Result DropTable(std::string_view database, std::string_view schema,
                           std::string_view name, bool cascade) = 0;
  virtual Result DropIndex(std::string_view database, std::string_view schema,
                           std::string_view name, bool cascade) = 0;

  virtual Result RemoveTombstone(ObjectId database_id, std::string_view schema,
                                 std::string_view name) = 0;

  virtual Result FinalizeLoad() = 0;

  virtual std::shared_ptr<const Snapshot> GetCatalogSnapshot() const = 0;
};

class CatalogFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Catalog"; }

  explicit CatalogFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void start() final;
  void unprepare() final;
  void prepare() final;

  void Cleanup() {
    _local.reset();
    _global.reset();
  }

  Result Open();

  LogicalCatalog& Global() const noexcept {
    SDB_ASSERT(_global, "Global catalog is not initialized");
    return *_global;
  }

  LogicalCatalog& Local() const noexcept {
    SDB_ASSERT(_local, "Local catalog is not initialized");
    return *_local;
  }

#ifdef SDB_GTEST
  auto& GlobalPtr() noexcept { return _global; }
  auto& LocalPtr() noexcept { return _local; }
#endif

 private:
  std::shared_ptr<LogicalCatalog> _global;
  std::shared_ptr<LogicalCatalog> _local;
  bool _skip_background_errors = false;
};

ResultOr<std::shared_ptr<Database>> GetDatabase(ObjectId database_id);
ResultOr<std::shared_ptr<Database>> GetDatabase(std::string_view name);
LogicalCatalog& GetCatalog();

}  // namespace sdb::catalog
