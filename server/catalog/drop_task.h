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

#include <absl/algorithm/container.h>
#include <absl/strings/substitute.h>

#include <chrono>
#include <cstdint>
#include <duckdb/main/database_manager.hpp>
#include <exception>
#include <limits>
#include <memory>
#include <vector>
#include <yaclib/async/future.hpp>
#include <yaclib/async/make.hpp>

#include "app/app_server.h"
#include "basics/assert.h"
#include "catalog/database.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/index.h"
#include "catalog/schema.h"
#include "catalog/store/data_store.h"
#include "catalog/store/store.h"
#include "catalog/table.h"
#include "search/inverted_index_storage.h"
namespace sdb::catalog {

enum class DropOutcome : uint8_t {
  Done,
  Retry,
  // Shutdown observed while the task still needed a retry. The tombstone is
  // durable and boot reschedules the drop, so giving up here loses nothing.
  Abandoned,
};

using AsyncResult = yaclib::Future<DropOutcome>;

inline constexpr auto kInitialDelay = std::chrono::milliseconds{1};
inline constexpr auto kMaxDelay = std::chrono::milliseconds{1000};

class DropTask {
 public:
  // A drop rescheduled at boot, which has no definition left to gate on: the
  // record that opened the bracket is all there is.
  DropTask(ObjectId id, ObjectId parent_id, bool is_root = false)
    : _parent_id{parent_id}, _id{id}, _is_root{is_root} {}

  // `definition` is what a live reader still holds -- an entry version, a
  // pinned plan -- and the sweep waits for the last of them to let go before
  // it removes anything on disk.
  DropTask(ObjectId id, std::weak_ptr<const duckdb::CreateInfo> definition,
           ObjectId parent_id, bool is_root = false)
    : _parent_id{parent_id},
      _id{id},
      _is_root{is_root},
      _object{std::move(definition)} {}

  static AsyncResult Schedule(std::shared_ptr<DropTask> task) noexcept;

  // Once shutdown starts BackgroundScheduler::Delay stops waiting, so a retry
  // that keeps going spins on a satisfied delay instead of backing off. Every
  // retry point polls this: the loop in Schedule, and each Execute that
  // resumes after awaiting its children (a child that abandoned must not be
  // finalized over by its parent -- the whole subtree is redone at boot).
  static bool ShouldStop() noexcept;

  static AsyncResult ExecuteTask(std::shared_ptr<DropTask> task) {
    SDB_ASSERT(task);
    // Drops touch the data DB; boot schedules them (from tombstones) before
    // it is attached and reconciled.
    if (!DataStore::IsReady()) {
      return yaclib::MakeFuture<DropOutcome>(DropOutcome::Retry);
    }
    if (!task->AllowToDrop()) {
      SDB_TRACE(STORAGE, "Waiting till the snapshots will free the object ",
                task->GetContext());
      return yaclib::MakeFuture<DropOutcome>(DropOutcome::Retry);
    }
    task->_object.reset();
    return task->Execute();
  }

  virtual bool AllowToDrop() const noexcept {
    return _object.expired() &&
           absl::c_all_of(_attached,
                          [](const auto& task) { return task.expired(); }) &&
           AllowToDropDependencies();
  }

  void SetAttached(std::vector<std::weak_ptr<DropTask>> attached) noexcept {
    _attached = std::move(attached);
  }

  virtual AsyncResult Execute() = 0;
  virtual std::string_view GetName() const noexcept = 0;
  virtual std::string GetContext() const noexcept = 0;
  virtual bool AllowToDropDependencies() const noexcept = 0;
  virtual ~DropTask() = default;

  ObjectId GetId() const noexcept { return _id; }

  // Appends this task's own node under `parent_id` and then everything below
  // it, so the DropPrepare that opens the bracket carries the whole
  // reclamation and a boot rebuilds the task tree from the record instead of
  // from definitions the drop has already removed.
  virtual void DescribeSubtree(ObjectId parent_id,
                               std::vector<wal::DropNode>& out) const = 0;

 protected:
  ObjectId _parent_id;
  ObjectId _id;
  bool _is_root;
  std::chrono::milliseconds _delay = kInitialDelay;
  std::weak_ptr<const duckdb::CreateInfo> _object;
  std::vector<std::weak_ptr<DropTask>> _attached;
};

struct IndexDrop final : public DropTask {
 public:
  // Without a name: a drop reopened at boot, rebuilt from the record, which
  // names no physical index. The frame that opened the drop carried the store
  // removal, so there is nothing left for this one to take off the list.
  IndexDrop(ObjectId id, bool inverted, ObjectId db_id, ObjectId schema_id,
            ObjectId table_id, bool is_root = false)
    : DropTask{id, table_id, is_root},
      _db_id{db_id},
      _schema_id{schema_id},
      _inverted{inverted} {}

  // An index has no definition a reader pins: what one holds is the iresearch
  // storage, and `_data` is the gate for it.
  IndexDrop(const CreateIndexInfoBase& index, ObjectId db_id,
            ObjectId schema_id, ObjectId table_id,
            std::weak_ptr<search::InvertedIndexStorage> data,
            bool is_root = false)
    : DropTask{index.GetId(), table_id, is_root},
      _db_id{db_id},
      _schema_id{schema_id},
      _inverted{index.IsInverted()},
      _name{std::string{index.GetName()}},
      _data{std::move(data)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("IndexDrop(schema $0 index $1)", _parent_id.id(),
                            _id.id());
  }

  // Gate the drop on the iresearch storage being fully released -- no catalog
  // snapshot, query, replay session, or background refresh/compaction still
  // holds it. This must live here (not only in Execute) so an ancestor drop
  // (TableDrop / SchemaDrop / DatabaseDrop), which removes the directory at its
  // own level and gates on this index's AllowToDrop, also waits for the storage
  // before deleting the dir out from under a running task. A SecondaryIndex has
  // no iresearch storage, so its empty weak is already expired.
  bool AllowToDropDependencies() const noexcept final {
    return _data.expired();
  }

  std::string_view GetName() const noexcept final { return "index drop"; }

  ObjectId GetDatabaseId() const { return _db_id; }

  bool IsInverted() const noexcept { return _inverted; }

  void DescribeSubtree(ObjectId parent_id,
                       std::vector<wal::DropNode>& out) const final {
    out.push_back({.parent_id = parent_id,
                   .id = _id,
                   .type = duckdb::CatalogType::INDEX_ENTRY,
                   .inverted = _inverted});
  }

  AsyncResult Execute() final;
  void Finalize();

 private:
  ObjectId _db_id;
  ObjectId _schema_id;
  bool _inverted;
  std::string _name;
  std::weak_ptr<search::InvertedIndexStorage> _data;
};

struct TableDropBase : public DropTask {
 public:
  virtual void EmitStoreDrops(CatalogStore::WriteContext&) const {}

  void Finalize();

  virtual TableEngine GetEngine() const noexcept = 0;

  void DescribeSubtree(ObjectId parent_id,
                       std::vector<wal::DropNode>& out) const override {
    out.push_back({.parent_id = parent_id,
                   .id = _id,
                   .type = duckdb::CatalogType::TABLE_ENTRY,
                   .engine = GetEngine()});
    for (const auto seq_id : _owned_sequences) {
      out.push_back({.parent_id = _id,
                     .id = seq_id,
                     .type = duckdb::CatalogType::SEQUENCE_ENTRY});
    }
  }

 protected:
  TableDropBase(ObjectId id, ObjectId db_id,
                std::vector<ObjectId> owned_sequences, ObjectId schema_id,
                bool is_root)
    : DropTask{id, schema_id, is_root},
      _db_id{db_id},
      _owned_sequences{std::move(owned_sequences)} {}

  TableDropBase(const TableInfoRef& table, ObjectId db_id,
                std::vector<ObjectId> owned_sequences, ObjectId schema_id,
                bool is_root)
    : DropTask{catalog::IdOf(*table), table, schema_id, is_root},
      _db_id{db_id},
      _owned_sequences{std::move(owned_sequences)} {}

  virtual void FinalizeStore(CatalogStore::WriteContext&) const {}

  // The database whose attachment holds the rows.
  ObjectId _db_id;
  std::vector<ObjectId> _owned_sequences;
};

struct TableDrop final : public TableDropBase {
 public:
  TableDrop(ObjectId id, ObjectId db_id,
            std::vector<std::shared_ptr<IndexDrop>> indexes,
            std::vector<ObjectId> owned_sequences, ObjectId schema_id,
            bool is_root = false)
    : TableDropBase{id, db_id, std::move(owned_sequences), schema_id, is_root},
      _indexes{std::move(indexes)} {}

  TableDrop(const TableInfoRef& table, ObjectId db_id,
            std::vector<std::shared_ptr<IndexDrop>> indexes,
            std::vector<ObjectId> owned_sequences, ObjectId schema_id,
            bool is_root = false)
    : TableDropBase{table, db_id, std::move(owned_sequences), schema_id,
                    is_root},
      _indexes{std::move(indexes)},
      _has_store_table{true} {}

  // Drops the store table synchronously in the same transaction that
  // tombstones the drop, freeing the public name immediately (renames are
  // unsafe for FK-involved tables: duckdb keeps back-references by name).
  // The id-only constructor (a drop rescheduled at boot) knows of no store
  // table; Finalize's drop-by-id covers it.
  void EmitStoreDrops(CatalogStore::WriteContext& ctx) const override {
    if (_has_store_table) {
      ctx.store().DropTable(_db_id, _id);
    }
  }

  std::string GetContext() const noexcept final {
    return absl::Substitute("TableDrop(schema $0 table $1)", _parent_id.id(),
                            _id.id());
  }

  std::string_view GetName() const noexcept final { return "table drop"; }

  AsyncResult Execute() final;

  bool AllowToDropDependencies() const noexcept final {
    return absl::c_all_of(_indexes, [](const auto& index) {
      SDB_ASSERT(index);
      return index->AllowToDrop();
    });
  }

  TableEngine GetEngine() const noexcept final {
    return TableEngine::Transactional;
  }

  void DescribeSubtree(ObjectId parent_id,
                       std::vector<wal::DropNode>& out) const final {
    TableDropBase::DescribeSubtree(parent_id, out);
    for (const auto& index : _indexes) {
      index->DescribeSubtree(_id, out);
    }
  }

 private:
  void FinalizeStore(CatalogStore::WriteContext& ctx) const override {
    ctx.store().DropTable(_db_id, _id);
  }

  std::vector<std::shared_ptr<IndexDrop>> _indexes;
  bool _has_store_table = false;
};

struct SearchTableDrop final : public TableDropBase {
 public:
  SearchTableDrop(const TableInfoRef& table,
                  std::shared_ptr<search::SearchTable> search_data,
                  ObjectId db_id, std::vector<ObjectId> owned_sequences,
                  ObjectId schema_id, bool is_root = false)
    : TableDropBase{table, db_id, std::move(owned_sequences), schema_id,
                    is_root},
      _search_data{std::move(search_data)} {}

  SearchTableDrop(ObjectId id, ObjectId db_id,
                  std::vector<ObjectId> owned_sequences, ObjectId schema_id,
                  bool is_root = false)
    : TableDropBase{id, db_id, std::move(owned_sequences), schema_id, is_root} {
  }

  std::string GetContext() const noexcept final {
    return absl::Substitute("SearchTableDrop(schema $0 table $1)",
                            _parent_id.id(), _id.id());
  }

  std::string_view GetName() const noexcept final {
    return "search table drop";
  }

  AsyncResult Execute() final;

  bool AllowToDropDependencies() const noexcept final {
    return _search_data.expired();
  }

  TableEngine GetEngine() const noexcept final { return TableEngine::Search; }

 private:
  std::weak_ptr<search::SearchTable> _search_data;
};

struct SchemaDrop final : public DropTask {
 public:
  // `sequences` are the ones standing on their own under the schema; a table's
  // owned sequences go with that table's drop.
  SchemaDrop(ObjectId schema_id,
             std::vector<std::shared_ptr<TableDropBase>> tables,
             std::vector<ObjectId> sequences, ObjectId db_id,
             bool is_root = false)
    : DropTask{schema_id, db_id, is_root},
      _sequences{std::move(sequences)},
      _tables{std::move(tables)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("SchemaDrop(database $0 schema $1)",
                            _parent_id.id(), _id.id());
  }

  std::string_view GetName() const noexcept final { return "schema drop"; }

  void EmitStoreDrops(CatalogStore::WriteContext& ctx) const {
    for (const auto& table : _tables) {
      table->EmitStoreDrops(ctx);
    }
  }

  AsyncResult Execute() final;
  void Finalize();

  bool AllowToDropDependencies() const noexcept final {
    return absl::c_all_of(_tables, [](const auto& table) {
      SDB_ASSERT(table);
      return table->AllowToDrop();
    });
  }

  void DescribeSubtree(ObjectId parent_id,
                       std::vector<wal::DropNode>& out) const final {
    out.push_back({.parent_id = parent_id,
                   .id = _id,
                   .type = duckdb::CatalogType::SCHEMA_ENTRY});
    for (const auto seq_id : _sequences) {
      out.push_back({.parent_id = _id,
                     .id = seq_id,
                     .type = duckdb::CatalogType::SEQUENCE_ENTRY});
    }
    for (const auto& table : _tables) {
      table->DescribeSubtree(_id, out);
    }
  }

 private:
  std::vector<ObjectId> _sequences;
  std::vector<std::shared_ptr<TableDropBase>> _tables;
};

struct DatabaseDrop final : public DropTask {
 public:
  DatabaseDrop(ObjectId db_id, std::vector<std::shared_ptr<SchemaDrop>> schemas)
    : DropTask{db_id, id::kInstance, true}, _schemas{std::move(schemas)} {}

  DatabaseDrop(ObjectId db_id, std::vector<std::shared_ptr<SchemaDrop>> schemas,
               duckdb::shared_ptr<void> keep_alive)
    : DropTask{db_id, id::kInstance, true},
      _keep_alive{std::move(keep_alive)},
      _schemas{std::move(schemas)} {}

  std::string GetContext() const noexcept final {
    return absl::Substitute("DatabaseDrop(database $0)", _id.id());
  }

  std::string_view GetName() const noexcept final { return "database drop"; }

  void EmitStoreDrops(CatalogStore::WriteContext& ctx) const {
    for (const auto& schema : _schemas) {
      schema->EmitStoreDrops(ctx);
    }
  }

  AsyncResult Execute() final;
  void Finalize();

  bool AllowToDropDependencies() const noexcept final {
    return absl::c_all_of(_schemas, [](const auto& schema) {
      SDB_ASSERT(schema);
      return schema->AllowToDrop();
    });
  }

  void DescribeSubtree(ObjectId parent_id,
                       std::vector<wal::DropNode>& out) const final {
    out.push_back({.parent_id = parent_id,
                   .id = _id,
                   .type = duckdb::CatalogType::DATABASE_ENTRY});
    for (const auto& schema : _schemas) {
      schema->DescribeSubtree(_id, out);
    }
  }

 private:
  duckdb::shared_ptr<void> _keep_alive;
  std::vector<std::shared_ptr<SchemaDrop>> _schemas;
};

}  // namespace sdb::catalog
