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

#include <absl/strings/substitute.h>

#include <chrono>
#include <exception>
#include <limits>
#include <memory>
#include <yaclib/async/future.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object_dependency.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "catalog/types.h"
#include "general_server/scheduler.h"
#include "rest_server/serened_single.h"
#include "storage_engine/table_shard.h"

namespace sdb::catalog {

using AsyncResult = yaclib::Future<Result>;

inline constexpr uint32_t kInitialDelay = 125;
inline constexpr uint32_t kMaxDelay = kInitialDelay << 7;

class DropTask {
 public:
  // DropTask on reboot, since there is no Object
  DropTask(ObjectId id, ObjectId parent_id, bool is_root = false)
    : _parent_id{parent_id}, _id{id}, _is_root{is_root} {}

  DropTask(const std::shared_ptr<Object>& object, ObjectId parent_id,
           bool is_root = false)
    : _parent_id{parent_id},
      _id{object->GetId()},
      _is_root{is_root},
      _object{object} {}

  static AsyncResult Schedule(std::shared_ptr<DropTask> task) noexcept;

  static AsyncResult ExecuteTask(std::shared_ptr<DropTask> task) {
    SDB_ASSERT(task);
    if (!task->_object.expired()) {
      SDB_INFO("xxxxx", Logger::ROCKSDB,
               "Waiting till the snapshots will free the object");
      return Schedule(std::move(task));
    }
    return task->Execute();
  }

  virtual AsyncResult Execute() = 0;
  virtual std::string_view GetName() const noexcept = 0;
  virtual std::string GetContext() const noexcept = 0;

 protected:
  ObjectId _parent_id;
  ObjectId _id;
  bool _is_root;
  uint32_t _delay = kInitialDelay;  // delay in microseconds
  std::weak_ptr<Object> _object;
};

class TableShardDrop final : public DropTask,
                             std::enable_shared_from_this<TableShardDrop> {
 public:
  TableShardDrop(ObjectId id, ObjectId parent_id, uint64_t size)
    : DropTask{id, parent_id}, _size{size} {}

  TableShardDrop(const std::shared_ptr<TableShard>& shard, ObjectId parent_id,
                 uint64_t size)
    : DropTask{shard, parent_id}, _size{size} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("TableShardDrop(table $0 shard $1)",
                            _parent_id.id(), _id.id());
  }

  std::string_view GetName() const noexcept final { return "table shard drop"; }

  AsyncResult Execute() final;

 private:
  uint64_t _size;
};

struct IndexDrop final : public DropTask,
                         std::enable_shared_from_this<IndexDrop> {
 public:
  IndexDrop(ObjectId id, IndexType type, ObjectId db_id, ObjectId schema_id,
            ObjectId table_id, ObjectId shard_id, bool is_root = false)
    : DropTask{id, table_id, is_root},
      _db_id{db_id},
      _schema_id{schema_id},
      _shard_id{shard_id},
      _type{type} {}

  IndexDrop(const std::shared_ptr<Index>& index, ObjectId db_id,
            ObjectId schema_id, ObjectId table_id, ObjectId shard_id,
            bool is_root = false)
    : DropTask{index, table_id, is_root},
      _db_id{db_id},
      _schema_id{schema_id},
      _shard_id{shard_id},
      _type{index->GetIndexType()} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("IndexDrop(schema $0 index $1)", _parent_id.id(),
                            _id.id());
  }

  std::string_view GetName() const noexcept final { return "index drop"; }

  ObjectId GetDatabaseId() const { return _db_id; }

  AsyncResult Execute() final;
  Result Finalize();

 private:
  ObjectId _db_id;
  ObjectId _schema_id;
  ObjectId _shard_id;
  IndexType _type;
};

struct TableDrop final : public DropTask,
                         std::enable_shared_from_this<TableDrop> {
 public:
  static constexpr std::string_view kName = "table drop";

  TableDrop(ObjectId id, TableType type, ObjectId shard_id, uint64_t table_size,
            std::vector<std::shared_ptr<IndexDrop>> indexes, ObjectId schema_id,
            bool is_root = false)
    : DropTask{id, schema_id, is_root},
      _type{type},
      _indexes{std::move(indexes)},
      _shard_drop{std::make_shared<TableShardDrop>(shard_id, id, table_size)} {}

  TableDrop(const std::shared_ptr<Table>& table,
            const std::shared_ptr<TableShard>& shard,
            std::vector<std::shared_ptr<IndexDrop>> indexes, ObjectId schema_id,
            bool is_root = false)
    : DropTask{table, schema_id, is_root},
      _type{table->GetTableType()},
      _indexes{std::move(indexes)},
      _shard_drop{std::make_shared<TableShardDrop>(
        shard, table->GetId(),
        table->Columns().size() * shard->GetTableStats().num_rows)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("TableDrop(schema $0 table $1)", _parent_id.id(),
                            _id.id());
  }

  std::string_view GetName() const noexcept final { return "table drop"; }

  AsyncResult Execute() final;
  Result Finalize();

 private:
  TableType _type;
  std::vector<std::shared_ptr<IndexDrop>> _indexes;
  std::shared_ptr<TableShardDrop> _shard_drop;
};

struct SchemaDrop final : public DropTask,
                          std::enable_shared_from_this<SchemaDrop> {
 public:
  SchemaDrop(ObjectId schema_id, std::vector<std::shared_ptr<TableDrop>> tables,
             ObjectId db_id, bool is_root = false)
    : DropTask{schema_id, db_id, is_root}, _tables{std::move(tables)} {}

  SchemaDrop(const std::shared_ptr<Schema>& schema,
             std::vector<std::shared_ptr<TableDrop>> tables, ObjectId db_id,
             bool is_root = false)
    : DropTask{schema, db_id, is_root}, _tables{std::move(tables)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("SchemaDrop(database $0 schema $1)",
                            _parent_id.id(), _id.id());
  }

  std::string_view GetName() const noexcept final { return "schema drop"; }

  AsyncResult Execute() final;
  Result Finalize();

 private:
  std::vector<std::shared_ptr<TableDrop>> _tables;
};

struct DatabaseDrop final : public DropTask,
                            std::enable_shared_from_this<DatabaseDrop> {
 public:
  DatabaseDrop(ObjectId db_id, std::vector<std::shared_ptr<SchemaDrop>> schemas)
    : DropTask{db_id, id::kInstance, true}, _schemas{std::move(schemas)} {}

  DatabaseDrop(const std::shared_ptr<Database>& db,
               std::vector<std::shared_ptr<SchemaDrop>> schemas)
    : DropTask{db, id::kInstance, true}, _schemas{std::move(schemas)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("DatabaseDrop(database $0)", _id.id());
  }

  std::string_view GetName() const noexcept final { return "database drop"; }

  AsyncResult Execute() final;
  Result Finalize();

 private:
  std::vector<std::shared_ptr<SchemaDrop>> _schemas;
};

}  // namespace sdb::catalog
