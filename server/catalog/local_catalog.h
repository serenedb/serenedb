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
#include <vector>

#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/index.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "storage_engine/storage_engine.h"
#include "storage_engine/table_shard.h"

namespace sdb::catalog {

class SnapshotImpl;

class LocalCatalog final : public LogicalCatalog,
                           public std::enable_shared_from_this<LocalCatalog> {
 public:
  explicit LocalCatalog(StorageEngine& engine, bool skip_background_errors);

  Result RegisterRole(std::shared_ptr<Role> role) final;
  Result RegisterDatabase(std::shared_ptr<Database> database) final;
  Result RegisterSchema(ObjectId database_id,
                        std::shared_ptr<Schema> schema) final;
  Result RegisterView(ObjectId database_id, std::string_view schema,
                      std::shared_ptr<View> view) final;
  Result RegisterFunction(ObjectId database_id, std::string_view schema,
                          std::shared_ptr<Function> function) final;
  Result RegisterTable(ObjectId database_id, std::string_view schema,
                       CreateTableOptions table) final;
  Result RegisterIndex(ObjectId database_id, std::string_view schema,
                       IndexFactory index) final;

  Result CreateDatabase(std::shared_ptr<Database> database) final;
  Result CreateRole(std::shared_ptr<Role> role) final;
  Result CreateView(ObjectId database_id, std::string_view schema,
                    std::shared_ptr<View> view, bool replace) final;
  Result CreateSchema(ObjectId database_id,
                      std::shared_ptr<Schema> schema) final;
  Result CreateFunction(ObjectId database_id, std::string_view schema,
                        std::shared_ptr<Function> function, bool replace) final;
  Result CreateTable(ObjectId database_id, std::string_view schema,
                     CreateTableOptions table,
                     CreateTableOperationOptions operation_options) final;

  Result CreateIndex(ObjectId database_id, std::string_view schema,
                     std::string_view relation,
                     IndexFactory index_factory) final;

  Result RenameView(ObjectId database_id, std::string_view schema,
                    std::string_view name, std::string_view new_name) final;
  Result RenameTable(ObjectId database_id, std::string_view schema,
                     std::string_view name, std::string_view new_name) final;
  Result ChangeView(ObjectId database_id, std::string_view schema,
                    std::string_view name, ChangeCallback<View> callback) final;
  Result ChangeTable(ObjectId database_id, std::string_view schema,
                     std::string_view name,
                     ChangeCallback<Table> callback) final;
  Result ChangeRole(std::string_view name, ChangeCallback<Role> callback) final;

  Result DropDatabase(std::string_view name, AsyncResult* async_result) final;
  Result DropRole(std::string_view role) final;
  Result DropSchema(ObjectId database_id, std::string_view name, bool cascade,
                    AsyncResult* async_result) final;
  Result DropView(ObjectId database_id, std::string_view schema,
                  std::string_view name) final;
  Result DropFunction(ObjectId database_id, std::string_view schema,
                      std::string_view name) final;
  Result DropTable(ObjectId database_id, std::string_view schema,
                   std::string_view name, AsyncResult* async_result) final;
  Result DropIndex(ObjectId database_id, std::string_view schema,
                   std::string_view name) final;
  std::shared_ptr<Snapshot> GetSnapshot() const noexcept final;

  void RegisterTableDrop(TableTombstone tombstone) final;
  void RegisterScopeDrop(ObjectId database_id, ObjectId schema_id) final;
  void DropTableShard(ObjectId id);

  bool GetSkipBackgroundErrors() const noexcept {
    return _skip_background_errors;
  }

 private:
  mutable absl::Mutex _mutex;
  std::shared_ptr<SnapshotImpl> _snapshot;
  StorageEngine* _engine;
  bool _skip_background_errors;
};

}  // namespace sdb::catalog
