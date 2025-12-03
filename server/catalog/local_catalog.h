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

#include <memory>
#include <thread>

#include "basics/read_write_lock.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "storage_engine/storage_engine.h"
#include "storage_engine/table_shard.h"

namespace sdb::catalog {

class Snapshot;

class LocalCatalog final : public LogicalCatalog,
                           public PhysicalCatalog,
                           public std::enable_shared_from_this<LocalCatalog> {
 public:
  explicit LocalCatalog(StorageEngine& engine, bool skip_background_errors);

  Result RegisterRole(std::shared_ptr<catalog::Role> role) final;
  Result RegisterDatabase(std::shared_ptr<Database> database) final;
  Result RegisterSchema(ObjectId database_id,
                        std::shared_ptr<Schema> schema) final;
  Result RegisterView(ObjectId database_id, std::string_view schema,
                      std::shared_ptr<catalog::View> view) final;
  Result RegisterFunction(ObjectId database_id, std::string_view schema,
                          std::shared_ptr<catalog::Function> function) final;
  Result RegisterTable(ObjectId database_id, std::string_view schema,
                       CreateTableOptions collection) final;

  Result CreateDatabase(std::shared_ptr<catalog::Database> database) final;
  Result CreateRole(std::shared_ptr<catalog::Role> role) final;
  Result CreateView(ObjectId database_id, std::string_view schema,
                    std::shared_ptr<catalog::View> view) final;
  Result CreateSchema(ObjectId database_id,
                      std::shared_ptr<catalog::Schema> schema) final;
  Result CreateFunction(ObjectId database_id, std::string_view schema,
                        std::shared_ptr<catalog::Function> function) final;
  Result CreateTable(ObjectId database_id, std::string_view schema,
                     CreateTableOptions collection,
                     CreateTableOperationOptions operation_options) final;

  Result RenameView(ObjectId database_id, std::string_view schema,
                    std::string_view name, std::string_view new_name) final;
  Result RenameTable(ObjectId database_id, std::string_view schema,
                     std::string_view name, std::string_view new_name) final;
  Result ChangeView(ObjectId database_id, std::string_view schema,
                    std::string_view name,
                    ChangeCallback<catalog::View> callback) final;
  Result ChangeTable(ObjectId database_id, std::string_view schema,
                     std::string_view name,
                     ChangeCallback<catalog::Table> callback) final;
  Result ChangeRole(std::string_view name,
                    ChangeCallback<catalog::Role> callback) final;

  Result DropDatabase(std::string_view name, AsyncResult* async_result) final;
  Result DropRole(std::string_view role) final;
  Result DropSchema(ObjectId database_id, std::string_view name) final;
  Result DropView(ObjectId database_id, std::string_view schema,
                  std::string_view name) final;
  Result DropFunction(ObjectId database_id, std::string_view schema,
                      std::string_view name) final;
  Result DropTable(ObjectId database_id, std::string_view schema,
                   std::string_view name, AsyncResult* async_result) final;

  std::shared_ptr<catalog::Role> GetRole(std::string_view name) const final;
  std::shared_ptr<catalog::View> GetView(ObjectId database_id,
                                         std::string_view schema,
                                         std::string_view name) const final;
  std::shared_ptr<catalog::Function> GetFunction(
    ObjectId database_id, std::string_view schema,
    std::string_view name) const final;
  std::shared_ptr<catalog::Table> GetTable(ObjectId database_id,
                                           std::string_view schema,
                                           std::string_view name) const final;
  std::shared_ptr<Database> GetDatabase(std::string_view name) const final;
  std::shared_ptr<Schema> GetSchema(ObjectId database_id,
                                    std::string_view schema) const final;

  Result GetRoles(
    std::vector<std::shared_ptr<catalog::Role>>& roles) const final;
  Result GetViews(
    ObjectId database_id, std::string_view schema,
    std::vector<std::shared_ptr<catalog::View>>& views) const final;
  Result GetFunctions(
    ObjectId database_id, std::string_view schema,
    std::vector<std::shared_ptr<catalog::Function>>& functions) const final;
  Result GetTables(ObjectId database_id, std::string_view schema,
                   std::vector<std::pair<std::shared_ptr<catalog::Table>,
                                         std::shared_ptr<TableShard>>>&
                     collections) const final;
  std::vector<std::shared_ptr<Database>> GetDatabases() const final;
  Result GetSchemas(ObjectId database_id,
                    std::vector<std::shared_ptr<Schema>>& schemas) const final;

  std::shared_ptr<Object> GetObject(ObjectId id) const final;

  void RegisterTableDrop(TableTombstone tombstone) final;
  void RegisterDatabaseDrop(ObjectId database_id) final;
  std::shared_ptr<TableShard> GetTableShard(ObjectId id) const final;
  std::vector<std::shared_ptr<TableShard>> GetTableShards() final;
  void DropTableShard(ObjectId id);

  bool GetSkipBackgroundErrors() const noexcept {
    return _skip_background_errors;
  }

 private:
  // TODO(gnusi): use absl::Mutex, need this because of Update/Rename causing
  // recursive access in cases where it needs object resolution
  mutable basics::ReadWriteLock _mutex;
  mutable std::atomic<std::thread::id> _owner;
  std::shared_ptr<Snapshot> _snapshot;
  containers::FlatHashMap<ObjectId, std::shared_ptr<TableShard>> _collections;
  StorageEngine* const _engine;
  bool _skip_background_errors;
};

}  // namespace sdb::catalog
