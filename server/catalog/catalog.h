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

#include <absl/synchronization/mutex.h>

#include <expected>
#include <functional>
#include <memory>
#include <vector>

#include "basics/containers/flat_hash_map.h"
#include "basics/down_cast.h"
#include "catalog/database.h"
#include "catalog/function.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "catalog/role.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/table_options.h"
#include "catalog/types.h"
#include "catalog/view.h"
#include "storage_engine/storage_engine.h"

namespace sdb::catalog {

using AsyncResult = yaclib::Future<Result>;

template<typename T>
using ChangeCallback = absl::FunctionRef<Result(const T&, std::shared_ptr<T>&)>;

struct CreateTableOperationOptions {
  bool wait_for_sync_replication = false;
  bool enforce_replication_factor = false;
};

template<typename T>
constexpr ObjectType GetObjectType() noexcept {
  if constexpr (std::is_same_v<T, View>) {
    return ObjectType::View;
  } else if constexpr (std::is_same_v<T, Schema>) {
    return ObjectType::Schema;
  } else if constexpr (std::is_same_v<T, catalog::Role>) {
    return ObjectType::Role;
  } else if constexpr (std::is_same_v<T, Database>) {
    return ObjectType::Database;
  } else if constexpr (std::is_same_v<T, catalog::Function>) {
    return ObjectType::Function;
  } else if constexpr (std::is_same_v<T, catalog::Table>) {
    return ObjectType::Table;
  } else {
    static_assert(false);
  }
}

struct LogicalCatalog {
  virtual ~LogicalCatalog() = default;

  virtual Result RegisterRole(std::shared_ptr<catalog::Role> role) = 0;
  virtual Result RegisterDatabase(std::shared_ptr<Database> database) = 0;
  virtual Result RegisterSchema(ObjectId database,
                                std::shared_ptr<catalog::Schema> schema) = 0;
  virtual Result RegisterView(ObjectId database_id, std::string_view schema,
                              std::shared_ptr<catalog::View> view) = 0;
  virtual Result RegisterTable(ObjectId database_id, std::string_view schema,
                               CreateTableOptions options) = 0;
  virtual Result RegisterFunction(
    ObjectId database_id, std::string_view schema,
    std::shared_ptr<catalog::Function> function) = 0;

  virtual Result CreateDatabase(
    std::shared_ptr<catalog::Database> database) = 0;
  virtual Result CreateRole(std::shared_ptr<catalog::Role> role) = 0;
  virtual Result CreateSchema(ObjectId database_id,
                              std::shared_ptr<catalog::Schema> schema) = 0;
  virtual Result CreateView(ObjectId database_id, std::string_view schema,
                            std::shared_ptr<catalog::View> view) = 0;
  virtual Result CreateFunction(
    ObjectId database_id, std::string_view schema,
    std::shared_ptr<catalog::Function> function) = 0;

  virtual Result CreateTable(ObjectId database_id, std::string_view schema,
                             CreateTableOptions options,
                             CreateTableOperationOptions operation_options) = 0;

  virtual Result RenameTable(ObjectId database_id, std::string_view schema,
                             std::string_view name,
                             std::string_view new_name) = 0;
  virtual Result RenameView(ObjectId database_id, std::string_view schema,
                            std::string_view name,
                            std::string_view new_name) = 0;

  virtual Result ChangeView(ObjectId database_id, std::string_view schema,
                            std::string_view name,
                            ChangeCallback<catalog::View> callback) = 0;
  virtual Result ChangeTable(ObjectId database_id, std::string_view schema,
                             std::string_view name,
                             ChangeCallback<catalog::Table> callback) = 0;
  virtual Result ChangeRole(std::string_view name,
                            ChangeCallback<catalog::Role> callback) = 0;

  virtual Result DropDatabase(std::string_view name,
                              AsyncResult* async_result) = 0;
  virtual Result DropRole(std::string_view name) = 0;
  virtual Result DropSchema(ObjectId database, std::string_view name,
                            bool cascade, AsyncResult* async_result) = 0;
  virtual Result DropFunction(ObjectId database, std::string_view schema,
                              std::string_view name) = 0;
  virtual Result DropView(ObjectId database, std::string_view schema,
                          std::string_view name) = 0;
  virtual Result DropTable(ObjectId database, std::string_view schema,
                           std::string_view name,
                           AsyncResult* async_result) = 0;

  virtual std::shared_ptr<catalog::Role> GetRole(
    std::string_view name) const = 0;
  virtual std::shared_ptr<catalog::View> GetView(
    ObjectId database, std::string_view schema,
    std::string_view name) const = 0;
  virtual std::shared_ptr<catalog::Function> GetFunction(
    ObjectId database, std::string_view schema,
    std::string_view name) const = 0;
  // TODO(gnusi): add options to return Table + TableShard together
  virtual std::shared_ptr<catalog::Table> GetTable(
    ObjectId database, std::string_view schema,
    std::string_view name) const = 0;
  virtual std::shared_ptr<Database> GetDatabase(
    std::string_view name) const = 0;

  virtual Result GetRoles(
    std::vector<std::shared_ptr<catalog::Role>>& roles) const = 0;
  virtual Result GetViews(
    ObjectId database_id, std::string_view schema,
    std::vector<std::shared_ptr<catalog::View>>& views) const = 0;
  virtual Result GetFunctions(
    ObjectId database_id, std::string_view schema,
    std::vector<std::shared_ptr<catalog::Function>>& functions) const = 0;
  virtual Result GetTables(
    ObjectId database_id, std::string_view schema,
    std::vector<std::pair<std::shared_ptr<catalog::Table>,
                          std::shared_ptr<TableShard>>>& tables) const = 0;
  virtual std::vector<std::shared_ptr<Database>> GetDatabases() const = 0;
  virtual Result GetSchemas(
    ObjectId database_id,
    std::vector<std::shared_ptr<Schema>>& schemas) const = 0;
  virtual std::shared_ptr<Schema> GetSchema(ObjectId database_id,
                                            std::string_view schema) const = 0;

  virtual std::shared_ptr<Object> GetObject(ObjectId id) const = 0;

  template<typename T>
  std::shared_ptr<T> GetObject(ObjectId id) const {
    auto obj = GetObject(id);
    if (obj && obj->GetType() == GetObjectType<T>()) {
      return basics::downCast<T>(obj);
    }
    return nullptr;
  }
};

class PhysicalCatalog {
 public:
  virtual ~PhysicalCatalog() = default;
  virtual std::shared_ptr<TableShard> GetTableShard(ObjectId id) const = 0;
  virtual void RegisterTableDrop(TableTombstone tombstone) = 0;
  virtual void RegisterScopeDrop(ObjectId database_id, ObjectId schema_id) = 0;
  virtual std::vector<std::shared_ptr<TableShard>> GetTableShards() = 0;
};

class CatalogFeature final : public SerenedFeature {
 public:
  static constexpr std::string_view name() noexcept { return "Catalog"; }

  explicit CatalogFeature(Server& server);

  void collectOptions(std::shared_ptr<options::ProgramOptions>) final;
  void start() final;
  void beginShutdown() final;
  void stop() final;
  void unprepare() final;
  void prepare() final;

  void Cleanup() {
    _local.reset();
    _global.reset();
    _physical.reset();
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

  PhysicalCatalog& Physical(this auto& self) noexcept {
    SDB_ASSERT(self._physical, "Physical catalog is not initialized");
    return *self._physical;
  }

#ifdef SDB_GTEST
  auto& GlobalPtr() noexcept { return _global; }
  auto& LocalPtr() noexcept { return _local; }
#endif

 private:
  Result OpenDatabase(DatabaseOptions database);
  Result AddDatabase(const DatabaseOptions& database);
  Result AddSchemas(ObjectId database_id, std::string_view database_name,
                    std::vector<std::shared_ptr<Schema>>& schemas);
  Result AddViews(ObjectId database_id, const Schema& schema,
                  std::string_view database_name);
  Result AddFunctions(ObjectId database_id, const Schema& schema,
                      std::string_view database_name);
  Result AddRoles();
  Result AddTables(ObjectId database_id, const Schema& schema,
                   std::string_view database_name);
  Result ProcessTombstones();

  std::shared_ptr<LogicalCatalog> _global;
  std::shared_ptr<LogicalCatalog> _local;
  std::shared_ptr<PhysicalCatalog> _physical;
  bool _skip_background_errors = false;
};

ResultOr<std::shared_ptr<Database>> GetDatabase(ObjectId database_id);
ResultOr<std::shared_ptr<Database>> GetDatabase(std::string_view name);

}  // namespace sdb::catalog
