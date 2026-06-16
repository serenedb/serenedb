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
#include <duckdb/main/database_manager.hpp>
#include <exception>
#include <limits>
#include <memory>
#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "basics/errors.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/object_dependency.h"
#include "catalog/schema.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "general_server/scheduler.h"
#include "search/inverted_index_storage.h"
namespace sdb::catalog {

using AsyncResult = yaclib::Future<Result>;

inline constexpr auto kInitialDelay = std::chrono::milliseconds{1};
inline constexpr auto kMaxDelay = std::chrono::milliseconds{1000};

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
    if (!task->AllowToDrop()) {
      SDB_TRACE(STORAGE, "Waiting till the snapshots will free the object ",
                task->GetContext());
      return yaclib::MakeFuture<Result>(ERROR_LOCKED);
    }
    task->_object.reset();
    return task->Execute();
  }

  virtual bool AllowToDrop() const noexcept {
    return _object.expired() && AllowToDropDependencies();
  }

  virtual AsyncResult Execute() = 0;
  virtual std::string_view GetName() const noexcept = 0;
  virtual std::string GetContext() const noexcept = 0;
  virtual bool AllowToDropDependencies() const noexcept = 0;
  virtual ~DropTask() = default;

 protected:
  ObjectId _parent_id;
  ObjectId _id;
  bool _is_root;
  std::chrono::milliseconds _delay = kInitialDelay;
  std::weak_ptr<Object> _object;
};

struct IndexDrop final : public DropTask {
 public:
  IndexDrop(ObjectId id, ObjectType type, ObjectId db_id, ObjectId schema_id,
            ObjectId table_id, bool is_root = false)
    : DropTask{id, table_id, is_root},
      _db_id{db_id},
      _schema_id{schema_id},
      _type{type} {}

  IndexDrop(const std::shared_ptr<Index>& index, ObjectId db_id,
            ObjectId schema_id, ObjectId table_id,
            std::weak_ptr<search::InvertedIndexStorage> data,
            bool is_root = false)
    : DropTask{index, table_id, is_root},
      _db_id{db_id},
      _schema_id{schema_id},
      _type{index->GetType()},
      _data{std::move(data)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("IndexDrop(schema $0 index $1)", _parent_id.id(),
                            _id.id());
  }

  // The store-index DROP is the gate; the inverted index's iresearch storage is
  // drained inside Execute (weak_ptr wait) before the directory removal.
  bool AllowToDropDependencies() const noexcept final { return true; }

  std::string_view GetName() const noexcept final { return "index drop"; }

  ObjectId GetDatabaseId() const { return _db_id; }

  AsyncResult Execute() final;
  Result Finalize();

 private:
  ObjectId _db_id;
  ObjectId _schema_id;
  ObjectType _type;
  std::weak_ptr<search::InvertedIndexStorage> _data;
};

struct TableDrop final : public DropTask {
 public:
  TableDrop(ObjectId id, std::vector<std::shared_ptr<IndexDrop>> indexes,
            std::vector<ObjectId> owned_sequences, ObjectId schema_id,
            bool is_root = false)
    : DropTask{id, schema_id, is_root},
      _indexes{std::move(indexes)},
      _owned_sequences{std::move(owned_sequences)} {}

  TableDrop(const std::shared_ptr<Table>& table,
            std::vector<std::shared_ptr<IndexDrop>> indexes,
            std::vector<ObjectId> owned_sequences, ObjectId schema_id,
            std::string store_name,
            std::vector<std::string> fk_referenced_store_names,
            bool is_root = false)
    : DropTask{table, schema_id, is_root},
      _store_name{std::move(store_name)},
      _fk_referenced_store_names{std::move(fk_referenced_store_names)},
      _indexes{std::move(indexes)},
      _owned_sequences{std::move(owned_sequences)} {}

  // FK linkage entries must go before ANY table drop in the transaction:
  // a live back-reference makes duckdb refuse dropping the main-key table,
  // and the cascade emission order is arbitrary. Removing both directions
  // up front makes the drops order-independent.
  void EmitStoreFkCleanups(CatalogStore::WriteContext& ctx) const {
    if (_store_name.empty()) {
      return;
    }
    for (const auto& referenced : _fk_referenced_store_names) {
      ctx.DropStoreForeignKey(referenced, _store_name);
      ctx.DropStoreForeignKey(_store_name, referenced);
    }
  }

  // Drops the store table synchronously in the same transaction that
  // tombstones the drop, freeing the public name immediately (renames are
  // unsafe for FK-involved tables: duckdb keeps back-references by name).
  // No-op when the table has no store table (Fast engine) or lives under
  // the dropped name (CTAS); Finalize's drop-by-id covers the latter.
  void EmitStoreDrops(CatalogStore::WriteContext& ctx) const {
    if (!_store_name.empty()) {
      ctx.DropStoreTable(_store_name);
    }
  }

  std::string GetContext() const noexcept final {
    return absl::Substitute("TableDrop(schema $0 table $1)", _parent_id.id(),
                            _id.id());
  }

  std::string_view GetName() const noexcept final { return "table drop"; }

  AsyncResult Execute() final;
  Result Finalize();

  bool AllowToDropDependencies() const noexcept final {
    return absl::c_all_of(_indexes, [](const auto& index) {
      SDB_ASSERT(index);
      return index->AllowToDrop();
    });
  }

 private:
  std::string _store_name;
  std::vector<std::string> _fk_referenced_store_names;
  std::vector<std::shared_ptr<IndexDrop>> _indexes;
  std::vector<ObjectId> _owned_sequences;
};

struct SchemaDrop final : public DropTask {
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

  void EmitStoreFkCleanups(CatalogStore::WriteContext& ctx) const {
    for (const auto& table : _tables) {
      table->EmitStoreFkCleanups(ctx);
    }
  }
  void EmitStoreDrops(CatalogStore::WriteContext& ctx) const {
    for (const auto& table : _tables) {
      table->EmitStoreDrops(ctx);
    }
  }

  AsyncResult Execute() final;
  Result Finalize();

  bool AllowToDropDependencies() const noexcept final {
    return absl::c_all_of(_tables, [](const auto& table) {
      SDB_ASSERT(table);
      return table->AllowToDrop();
    });
  }

 private:
  std::vector<std::shared_ptr<TableDrop>> _tables;
};

struct DatabaseDrop final : public DropTask {
 public:
  DatabaseDrop(ObjectId db_id, std::vector<std::shared_ptr<SchemaDrop>> schemas)
    : DropTask{db_id, id::kInstance, true}, _schemas{std::move(schemas)} {}

  DatabaseDrop(const std::shared_ptr<Database>& db,
               std::vector<std::shared_ptr<SchemaDrop>> schemas,
               duckdb::shared_ptr<void> keep_alive)
    : DropTask{db, id::kInstance, true},
      _keep_alive{std::move(keep_alive)},
      _schemas{std::move(schemas)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("DatabaseDrop(database $0)", _id.id());
  }

  std::string_view GetName() const noexcept final { return "database drop"; }

  void EmitStoreFkCleanups(CatalogStore::WriteContext& ctx) const {
    for (const auto& schema : _schemas) {
      schema->EmitStoreFkCleanups(ctx);
    }
  }
  void EmitStoreDrops(CatalogStore::WriteContext& ctx) const {
    for (const auto& schema : _schemas) {
      schema->EmitStoreDrops(ctx);
    }
  }

  AsyncResult Execute() final;
  Result Finalize();

  bool AllowToDropDependencies() const noexcept final {
    return absl::c_all_of(_schemas, [](const auto& schema) {
      SDB_ASSERT(schema);
      return schema->AllowToDrop();
    });
  }

 private:
  duckdb::shared_ptr<void> _keep_alive;
  std::vector<std::shared_ptr<SchemaDrop>> _schemas;
};

}  // namespace sdb::catalog
