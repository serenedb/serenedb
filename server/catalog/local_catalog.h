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

#include <absl/base/thread_annotations.h>
#include <absl/synchronization/mutex.h>
#include <vpack/slice.h>

#include <memory>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/sequence.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/tokenizer.h"
#include "catalog/user_type.h"
#include "storage_engine/table_shard.h"

namespace sdb {

class RocksDBEngineCatalog;
}

namespace sdb::catalog {

class SnapshotImpl;

class LocalCatalog final : public LogicalCatalog,
                           public std::enable_shared_from_this<LocalCatalog> {
 public:
  explicit LocalCatalog();

  Result RegisterRole(std::shared_ptr<Role> role) final;
  Result RegisterDatabase(std::shared_ptr<Database> database) final;
  Result RegisterSchema(ObjectId database_id,
                        std::shared_ptr<Schema> schema) final;
  Result RegisterView(ObjectId schema_id,
                      std::shared_ptr<PgSqlView> view) final;
  Result RegisterSequence(ObjectId database_id, ObjectId schema_id,
                          std::shared_ptr<Sequence> sequence) final;
  Result RegisterFunction(ObjectId database_id, ObjectId schema_id,
                          std::shared_ptr<PgSqlFunction> function) final;
  Result RegisterTokenizer(ObjectId database_id, ObjectId schema_id,
                           std::shared_ptr<Tokenizer> tokenizer) final;
  Result RegisterType(ObjectId database_id, ObjectId schema_id,
                      std::shared_ptr<PgSqlType> type) final;
  Result RegisterTable(ObjectId database_id, ObjectId schema_id,
                       std::shared_ptr<Table> table) final;
  Result RegisterTableShard(std::shared_ptr<TableShard> shard) final;
  Result RegisterIndex(ObjectId database_id, ObjectId schema_id,
                       std::shared_ptr<Index> index) final;
  Result RegisterIndexShard(std::shared_ptr<IndexShard> shard) final;

  Result CreateDatabase(std::shared_ptr<Database> database) final;
  Result CreateRole(std::shared_ptr<Role> role) final;
  Result CreateView(ObjectId database_id, std::string_view schema,
                    std::shared_ptr<PgSqlView> view, bool replace) final;
  Result CreateSequence(ObjectId database_id, std::string_view schema,
                        std::shared_ptr<Sequence> sequence,
                        bool if_not_exists) final;
  Result CreateSchema(ObjectId database_id,
                      std::shared_ptr<Schema> schema) final;
  Result CreateFunction(ObjectId database_id, std::string_view schema,
                        std::shared_ptr<PgSqlFunction> function,
                        bool replace) final;
  Result CreateTable(ObjectId database_id, std::string_view schema,
                     CreateTableOptions table,
                     CreateTableOperationOptions operation_options) final;
  Result CreateSecondaryIndex(
    ObjectId database_id, std::string_view schema, std::string_view relation,
    std::string name, std::vector<CreateIndexColumn>&& columns, bool unique,
    CreateIndexOperationOptions operation_options) final;
  Result CreateInvertedIndex(
    ObjectId database_id, std::string_view schema, std::string_view relation,
    std::string name, std::vector<CreateIndexColumn>&& columns,
    InvertedIndexOptions options,
    CreateIndexOperationOptions operation_options) final;
  Result CreateTokenizer(ObjectId database_id, std::string_view schema,
                         std::shared_ptr<Tokenizer> dict) final;
  Result CreateType(ObjectId database_id, std::string_view schema,
                    std::shared_ptr<PgSqlType> type) final;

  Result RenameView(ObjectId database_id, std::string_view schema,
                    std::string_view name, std::string_view new_name) final;
  Result RenameTable(ObjectId database_id, std::string_view schema,
                     std::string_view name, std::string_view new_name) final;
  Result RenameIndex(ObjectId database_id, std::string_view schema,
                     std::string_view name, std::string_view new_name) final;
  Result RenameRelation(ObjectId database_id, std::string_view schema,
                        std::string_view name, std::string_view new_name) final;
  Result RenameFunction(ObjectId database_id, std::string_view schema,
                        std::string_view name, std::string_view new_name) final;

  Result ChangeView(ObjectId database_id, std::string_view schema,
                    std::string_view name,
                    ChangeCallback<PgSqlView> callback) final;
  Result ChangeTable(ObjectId database_id, std::string_view schema,
                     std::string_view name,
                     ChangeCallback<Table> callback) final;
  Result ChangeRole(std::string_view name, ChangeCallback<Role> callback) final;

  Result DropDatabase(std::string_view name,
                      duckdb::shared_ptr<void> keep_alive) final;
  Result DropRole(std::string_view role) final;
  Result DropSchema(std::string_view database, std::string_view name,
                    bool cascade) final;
  Result DropView(std::string_view database, std::string_view schema,
                  std::string_view name, bool cascade) final;
  Result DropSequence(std::string_view database, std::string_view schema,
                      std::string_view name, bool if_exists,
                      bool cascade) final;
  Result DropType(std::string_view database, std::string_view schema,
                  std::string_view name, bool cascade) final;
  Result DropFunction(std::string_view database, std::string_view schema,
                      std::string_view name, bool cascade) final;
  Result DropTokenizer(std::string_view database, std::string_view schema,
                       std::string_view name, bool cascade) final;
  Result DropTable(std::string_view database, std::string_view schema,
                   std::string_view name, bool cascade) final;
  Result DropIndex(std::string_view database, std::string_view schema,
                   std::string_view name, bool cascade) final;

  Result RemoveTombstone(ObjectId database_id, std::string_view schema,
                         std::string_view name) final;

  std::shared_ptr<const Snapshot> GetCatalogSnapshot() const noexcept final;

 private:
  Result CreateIndexImpl(std::string_view schema, std::shared_ptr<Index> index,
                         CreateIndexOperationOptions operation_options);

  template<typename T>
  Result RenameObjectImpl(ObjectId database_id, std::string_view schema,
                          std::string_view name, std::string_view new_name);

  template<typename T>
  Result RenameObjectImpl(ObjectId schema_id, std::string_view name,
                          std::string_view new_name, std::shared_ptr<T> object);

  mutable absl::Mutex _mutex;
  std::shared_ptr<const SnapshotImpl> _snapshot;
  RocksDBEngineCatalog* _engine;
};

}  // namespace sdb::catalog
