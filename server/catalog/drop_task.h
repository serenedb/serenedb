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
#include <yaclib/async/future.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object_dependency.h"
#include "general_server/scheduler.h"
#include "rest_server/serened_single.h"

namespace sdb::catalog {
using AsyncResult = yaclib::Future<Result>;

inline constexpr uint32_t kInitialDelay = 125;
inline constexpr uint32_t kMaxDelay = kInitialDelay << 7;

struct DropTask {
  ObjectId parent_id;
  ObjectId id;
  bool is_root;
  uint32_t delay = kInitialDelay;  // delay in microseconds

  DropTask(ObjectId parent_id, ObjectId id, bool is_root = false)
    : parent_id{parent_id}, id{id}, is_root{is_root} {}

  static AsyncResult Schedule(std::shared_ptr<DropTask> task) noexcept;
  virtual AsyncResult operator()() = 0;
  virtual std::string_view GetName() const noexcept = 0;
  virtual std::string GetContext() const noexcept = 0;
};

struct TableShardDrop final : public DropTask,
                              std::enable_shared_from_this<TableShardDrop> {
  TableShardDrop(ObjectId parent_id, ObjectId id) : DropTask{parent_id, id} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("TableShardDrop(table $0 shard $1)", parent_id.id(),
                            id.id());
  }

  std::string_view GetName() const noexcept final { return "table shard drop"; }

  AsyncResult operator()() final;
};

struct IndexDrop final : public DropTask,
                         std::enable_shared_from_this<IndexDrop> {
  ObjectId db_id;
  ObjectId schema_id;
  ObjectId shard_id;
  IndexType type;

  IndexDrop(ObjectId db_id, ObjectId schema_id, ObjectId table_id, ObjectId id,
            IndexType type, bool is_root = false)
    : DropTask{table_id, id, is_root},
      db_id{db_id},
      schema_id{schema_id},
      type{type} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("IndexDrop(schema $0 index $1)", parent_id.id(),
                            id.id());
  }

  std::string_view GetName() const noexcept final { return "index drop"; }

  AsyncResult operator()() final;
  Result Finalize();
};

struct TableDrop final : public DropTask,
                         std::enable_shared_from_this<TableDrop> {
  static constexpr std::string_view kName = "table drop";

  ObjectId shard_id;
  std::vector<std::shared_ptr<IndexDrop>> indexes;

  TableDrop(ObjectId schema_id, ObjectId id, bool is_root = false)
    : DropTask{schema_id, id, is_root} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("TableDrop(schema $0 table $1)", parent_id.id(),
                            id.id());
  }

  std::string_view GetName() const noexcept final { return "table drop"; }

  AsyncResult operator()() final;
  Result Finalize();
};

struct SchemaDrop final : public DropTask,
                          std::enable_shared_from_this<SchemaDrop> {
  std::vector<std::shared_ptr<TableDrop>> tables;

  SchemaDrop(ObjectId db_id, ObjectId id, bool is_root = false)
    : DropTask{db_id, id, is_root} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("SchemaDrop(database $0 schema $1)", parent_id.id(),
                            id.id());
  }

  std::string_view GetName() const noexcept final { return "schema drop"; }

  AsyncResult operator()() final;
  Result Finalize();
};

struct DatabaseDrop final : public DropTask,
                            std::enable_shared_from_this<DatabaseDrop> {
  std::vector<std::shared_ptr<SchemaDrop>> schemas;

  DatabaseDrop(ObjectId id) : DropTask{id::kInstance, id, true} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("DatabaseDrop(database $0)", parent_id.id());
  }

  std::string_view GetName() const noexcept final { return "database drop"; }

  AsyncResult operator()() final;
  Result Finalize();
};

}  // namespace sdb::catalog
