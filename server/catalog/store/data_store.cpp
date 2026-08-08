////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include "catalog/store/data_store.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/execution/index/art/art.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <exception>
#include <optional>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/deferred_writes.h"
#include "catalog/duckdb_catalog.h"
#include "catalog/duckdb_catalog_sets.h"
#include "catalog/duckdb_index_entry.h"
#include "catalog/duckdb_table_entry.h"
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

// The relation as the binder resolves it: the ops that go through the planner
// name their target the way a session would.
duckdb::QualifiedName NameOf(const duckdb::TableCatalogEntry& entry) {
  return duckdb::QualifiedName{entry.ParentCatalog().GetName(),
                               entry.ParentSchema().name, entry.name};
}

// A concurrent statement reshaped the same rows first. duckdb reports it as an
// internal catalog conflict naming the store table; Aborted is what carries
// "retry this transaction" up to RunStoreOps, which says so in the user's own
// terms.
absl::Status StoreFailure(std::string error) {
  if (absl::StrContains(error, "write-write conflict")) {
    return absl::AbortedError(std::move(error));
  }
  return absl::InternalError(std::move(error));
}

// A conflict duckdb spells out in prose rather than as a write-write one -- an
// index build whose table another transaction reshaped is the reachable shape
// -- is still a conflict, and the type says so where the text does not.
absl::Status StoreFailure(const duckdb::ErrorData& error) {
  if (error.Type() == duckdb::ExceptionType::TRANSACTION) {
    return absl::AbortedError(error.Message());
  }
  return StoreFailure(error.Message());
}

// A duckdb exception's what() is the JSON form when it carries extra info, so
// a thrown error and one read off a QueryResult would reach the user spelled
// differently. This is the spelling QueryResult::GetError() produces.
absl::Status StoreFailure(const std::exception& e) {
  return StoreFailure(duckdb::ErrorData{e});
}

template<typename Fn>
absl::Status RunInTransaction(duckdb::Connection& conn, Fn&& fn) {
  try {
    conn.BeginTransaction();
  } catch (const std::exception& e) {
    return absl::InternalError(e.what());
  }
  // A throw out of `fn` must still reach the rollback below, so catch it here
  // and turn it into the failure status the caller already handles. Every op
  // is expected to report errors as a status; this is the backstop.
  absl::Status r;
  try {
    r = fn();
  } catch (const std::exception& e) {
    r = absl::InternalError(e.what());
  }
  try {
    if (r.ok()) {
      conn.Commit();
    } else {
      conn.Rollback();
    }
  } catch (const std::exception& e) {
    if (r.ok()) {
      return absl::InternalError(e.what());
    }
  }
  return r;
}

}  // namespace

// Routes the store connection's storage access for one database's attachment
// onto the statement's transaction, so every op in the batch lands in the undo
// buffer the statement commits or rolls back. The statement's transaction is
// not part of the store connection's own commit set, so the rollback below
// only discards the empty shell the connection needs to hold the override.
class DataStore::StatementTransaction {
 public:
  StatementTransaction(duckdb::Connection& conn, duckdb::ClientContext& context,
                       duckdb::AttachedDatabase& store_db)
    : _conn(conn),
      _store_db(store_db),
      _txn(duckdb::MetaTransaction::Get(context).GetTransaction(_store_db)) {
    Open();
  }

  ~StatementTransaction() { Close(); }

  StatementTransaction(const StatementTransaction&) = delete;
  StatementTransaction& operator=(const StatementTransaction&) = delete;

  // A failed statement leaves duckdb's meta transaction invalidated, and the
  // ops that recover from a failure -- ADD COLUMN whose DEFAULT does not bind
  // store-side, CREATE TABLE whose CHECK does not -- need a usable one to
  // retry on. Only the store connection's shell is replaced; the statement's
  // transaction carries the work across.
  void Recycle() {
    Close();
    Open();
  }

 private:
  void Open() {
    _conn.BeginTransaction();
    duckdb::MetaTransaction::Get(*_conn.context)
      .PushTransactionOverride(_store_db, _txn);
  }

  void Close() noexcept try {
    duckdb::MetaTransaction::Get(*_conn.context)
      .PopTransactionOverride(_store_db);
    _conn.Rollback();
  } catch (const std::exception& e) {
    SDB_FATAL(GENERAL,
              "data store: releasing the statement routing failed: ", e.what());
  }

  duckdb::Connection& _conn;
  duckdb::AttachedDatabase& _store_db;
  duckdb::Transaction& _txn;
};

DataStore::DataStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
}

DataStore::~DataStore() { gInstance = nullptr; }

void DataStore::Initialize() {
  _conn = DuckDBEngine::Instance().CreateConnection();
}

void DataStore::MarkReady() { _ready.store(true, std::memory_order_release); }

void DataStore::ForgetDatabase(ObjectId database_id) {
  if (gInstance != nullptr) {
    absl::MutexLock lock{&gInstance->_bind_mutex};
    gInstance->_bind_contexts.erase(database_id);
  }
}

void DataStore::Shutdown() {
  _ready.store(false, std::memory_order_release);
  // The bind connections hold references to the attachments; leaving them open
  // would block the detach that follows, and WithStoreBindContext gates on the
  // store being usable, so they must be cleared here.
  {
    absl::MutexLock lock{&_bind_mutex};
    _bind_contexts.clear();
  }
  _conn.reset();
}

absl::Status DataStore::ApplyStoreOps(duckdb::ClientContext* context,
                                      std::span<const store_op::Targeted> ops,
                                      uint64_t catalog_position) {
  SDB_ASSERT(_conn, "store DDL before the data store is up");
  // Boot recovery, background drop tasks and teardown have no statement to
  // run on, so they get a transaction of the store connection's own.
  if (context == nullptr || !context->transaction.HasActiveTransaction()) {
    return RunInTransaction(*_conn, [&] {
      auto r = ExecuteStoreOps(nullptr, ops);
      if (r.ok() && catalog_position != 0 && !ops.empty()) {
        RecordCatalogPositionOnCommit(*_conn->context, ops.front().database_id,
                                      catalog_position);
      }
      return r;
    });
  }
  // Every op reports failure as a status; this is the backstop, and it must
  // not escape past the scope that releases the routing below.
  try {
    return ExecuteStoreOps(context, ops);
  } catch (const std::exception& e) {
    return StoreFailure(e);
  }
}

absl::Status DataStore::ExecuteStoreOps(
  duckdb::ClientContext* context, std::span<const store_op::Targeted> ops) {
  // Published before the first op, not as each one runs: a batch can drop
  // several columns, and the rebuild triggered by the first already has to
  // know about all of them. The catalog half of the batch lands after the
  // store ops, so the definition still names the column being taken away --
  // and a replay, where the catalog is already past it, needs none of this.
  for (const auto& targeted : ops) {
    if (!targeted.info ||
        targeted.info->info_type != duckdb::ParseInfoType::ALTER_INFO) {
      continue;
    }
    const auto& alter = targeted.info->Cast<duckdb::AlterInfo>();
    if (alter.type != duckdb::AlterType::ALTER_TABLE ||
        alter.Cast<duckdb::AlterTableInfo>().alter_table_type !=
          duckdb::AlterTableType::REMOVE_COLUMN) {
      continue;
    }
    const auto* table = context != nullptr ? catalog::FindSessionTable(
                                               *context, targeted.relation_id)
                                           : nullptr;
    if (table == nullptr) {
      continue;
    }
    const auto* column = catalog::ColumnByName(
      *table->Definition(), alter.GetColumnName().GetIdentifierName());
    if (column != nullptr) {
      _dropping_columns.insert(ObjectId{column->CatalogOid()});
    }
  }
  const absl::Cleanup dropping_done = [&] { _dropping_columns.clear(); };
  // The routing of the store connection onto the statement's transaction is
  // per attachment, and duckdb allows one override at a time -- so it is
  // opened for the database the current op names and reopened when that
  // changes. Batches are single-database in practice; a cross-database one
  // still runs each op on the statement's transaction for its own database.
  std::optional<StatementTransaction> statement;
  // Rendering a type name back into a LogicalType resolves user-defined types
  // through the catalog, which needs a transaction to read. The batch's own is
  // enough -- nothing here writes through this connection.
  const bool own_transaction =
    !_conn->context->transaction.HasActiveTransaction();
  if (own_transaction) {
    _conn->BeginTransaction();
  }
  const absl::Cleanup routing_done = [&] {
    _statement = nullptr;
    _exec_conn = nullptr;
    _target = nullptr;
    if (own_transaction) {
      _conn->Rollback();
    }
  };
  ObjectId resolved;
  for (const auto& targeted : ops) {
    if (targeted.database_id != resolved) {
      // Resolved through the statement's own view when there is one: a
      // database created earlier in the same transaction is not in the
      // committed catalog yet, and the store connection would not find its
      // name.
      auto target = TryStoreDatabase(
        context != nullptr ? *context : *_conn->context, targeted.database_id);
      if (!target) {
        // The database is detached: DROP DATABASE removes the attachment (and
        // the file with it) synchronously, while the cascade's per-table drops
        // run asynchronously afterwards. Their target is already gone.
        if (store_op::IsDestructive(targeted)) {
          continue;
        }
        return absl::InternalError(absl::StrCat(
          "database ", targeted.database_id.id(), " is not attached"));
      }
      resolved = targeted.database_id;
      _target = target.get();
      // The index builds are run as statements, and a statement that names a
      // relation has to resolve it the way a session would -- so they go on the
      // connection that carries this database's own client state, not on the
      // bare one. It is marked as the store's, which is what routes those
      // statements to duckdb's native catalog paths rather than back into the
      // mutators that emitted them.
      _exec_conn = BindConnection(*_target);
      if (context != nullptr && _exec_conn != nullptr) {
        statement.reset();
        statement.emplace(*_exec_conn, *context, *_target);
        _statement = &*statement;
      }
    }
    if (auto r = ExecuteStoreOp(context, targeted); !r.ok()) {
      return r;
    }
  }
  return absl::OkStatus();
}

absl::Status DataStore::Run(
  duckdb::unique_ptr<duckdb::SQLStatement> statement) {
  SDB_ENSURE(_exec_conn != nullptr, "store statement with no connection");
  auto res = _exec_conn->Query(std::move(statement));
  if (!res->HasError()) {
    return absl::OkStatus();
  }
  auto error = res->GetErrorObject();
  if (_statement != nullptr) {
    _statement->Recycle();
  }
  return StoreFailure(error);
}

// The rows an op leaves behind travel with the transaction that produced them
// rather than being looked up again: only that transaction can see them, and
// the entry version that carries them is not built until the write.
absl::Status DataStore::Alter(duckdb::ClientContext* context,
                              duckdb::AlterInfo& info) try {
  auto& catalog = _target->GetCatalog().Cast<catalog::SereneDBCatalog>();
  auto reshaped = absl::Status{};
  auto apply = [&](duckdb::ClientContext& reshape_context) {
    try {
      // Versioned for a statement: the reshape is an alter of the entry, and
      // going through its set is what records it in the data WAL. Boot's gap
      // replay is the same reshape arriving without a statement, into a
      // definition the catalog log has already settled.
      catalog.AlterStorage(catalog.GetCatalogTransaction(reshape_context), info,
                           /*versioned=*/context != nullptr);
    } catch (const std::exception& e) {
      reshaped = StoreFailure(e);
    }
  };
  if (context != nullptr) {
    // The statement's own context: the row versions the reshape moves are that
    // transaction's, and its local storage is where they live.
    apply(*context);
  } else {
    WithBindContext(*_target, apply);
  }
  return reshaped;
} catch (const std::exception& e) {
  return StoreFailure(e);
}

duckdb::optional_ptr<duckdb::DuckTableEntry> DataStore::ResolveTable(
  duckdb::ClientContext& context, ObjectId table_id) {
  auto& catalog = _target->GetCatalog();
  auto entry = catalog.LookupTableById(catalog.GetCatalogTransaction(context),
                                       table_id.id());
  if (!entry || !entry->TryGetStorage()) {
    return nullptr;
  }
  return &entry->Cast<duckdb::DuckTableEntry>();
}

absl::Status DataStore::ExecuteStoreOp(duckdb::ClientContext* context,
                                       const store_op::Targeted& op) {
  if (!op.info) {
    return ExecuteCreateStoreTable(context, op.relation_id);
  }
  switch (op.info->info_type) {
    case duckdb::ParseInfoType::ALTER_INFO: {
      // A copy per execution: AlterStorage resolves the target into the info,
      // and a replayed batch is held for as long as the database is behind.
      auto info = op.info->Cast<duckdb::AlterInfo>().Copy();
      if (info->type == duckdb::AlterType::ALTER_TABLE &&
          info->Cast<duckdb::AlterTableInfo>().alter_table_type ==
            duckdb::AlterTableType::RENAME_TABLE) {
        return ExecuteRenameStoreIndex(context, op.relation_id,
                                       info->Cast<duckdb::RenameTableInfo>());
      }
      if (info->IsAddIndexedConstraint()) {
        // The constraint is backed by an index, and an index over existing
        // rows is a physical plan -- so this one goes through the planner
        // rather than straight onto the entry.
        return RunAlter(context, op.relation_id, std::move(info));
      }
      if (info->type == duckdb::AlterType::ALTER_TABLE &&
          info->Cast<duckdb::AlterTableInfo>().alter_table_type ==
            duckdb::AlterTableType::ADD_COLUMN) {
        return ExecuteAddStoreColumn(context, op.relation_id, std::move(info));
      }
      return Alter(context, *info);
    }
    case duckdb::ParseInfoType::CREATE_INFO:
      return ExecuteCreateStoreIndex(context, op);
    case duckdb::ParseInfoType::DROP_INFO: {
      const auto& drop = op.info->Cast<duckdb::DropInfo>();
      // The rows of a dropped table go with the entry, whose drop is a duckdb
      // table drop, so the blocks are reclaimed at commit.
      if (drop.type != duckdb::CatalogType::INDEX_ENTRY) {
        return absl::OkStatus();
      }
      return ExecuteDropStoreIndex(context, op.relation_id,
                                   drop.GetQualifiedName().Name());
    }
    default:
      return absl::InternalError(absl::StrCat(
        "store op ", static_cast<int>(op.info->info_type), " has no executor"));
  }
}

// The arms with real bodies, lifted out so the dispatch above reads as the
// table it is.
absl::Status DataStore::ExecuteAddStoreColumn(
  duckdb::ClientContext* context, ObjectId table_id,
  duckdb::unique_ptr<duckdb::AlterInfo> info) {
  auto& add = info->Cast<duckdb::AddColumnInfo>();
  if (!add.new_column.HasDefaultValue()) {
    return Alter(context, add);
  }
  auto bare = info->Copy();
  auto r = Alter(context, add);
  if (r.ok()) {
    return r;
  }
  // The DEFAULT may name something the reshape cannot bind here. Add the column
  // without it; existing rows get NULL and the definition still carries the
  // default, which is what fills it on insert.
  SDB_WARN(GENERAL, "relation ", table_id.id(), ": ADD COLUMN \"",
           add.new_column.Name().GetIdentifierName(),
           "\" DEFAULT not backfilled: ", r.message());
  auto& retry = bare->Cast<duckdb::AddColumnInfo>();
  retry.new_column.SetDefaultValue(nullptr);
  return Alter(context, retry);
}

// An index build, not a reshape: the entry alter only records the constraint,
// while the ART over the existing rows is a physical plan, and a plan is
// reached by running a statement.
absl::Status DataStore::ExecuteCreateStoreIndex(duckdb::ClientContext* context,
                                                const store_op::Targeted& op) {
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  auto table_entry = ResolveTable(resolve_context, op.relation_id);
  if (!table_entry) {
    return absl::InternalError(
      absl::StrCat("relation ", op.relation_id.id(), " has no rows to index"));
  }
  const auto& create = op.info->Cast<duckdb::CreateIndexInfo>();
  if (IsPlainStoreIndex(create)) {
    auto statement = duckdb::make_uniq<duckdb::CreateStatement>();
    auto info =
      duckdb::unique_ptr_cast<duckdb::CreateInfo, duckdb::CreateIndexInfo>(
        create.Copy());
    // The relation the op named by id, as the binder resolves it: the current
    // entry, not the one the op was written against.
    info->table = table_entry->name;
    info->SetQualifiedName(NameOf(*table_entry).WithName(info->GetIndexName()));
    // The keys are persisted as parsed_expressions; the binder reads the other
    // list, which duckdb's own CREATE INDEX fills the same way.
    for (const auto& key : info->parsed_expressions) {
      info->expressions.push_back(key->Copy());
    }
    statement->info = std::move(info);
    return Run(std::move(statement));
  }
  // The physical operator that ran the statement already published this one
  // into the live list, under the store table's checkpoint lock.
  auto& storage = table_entry->GetStorage();
  auto& list = storage.GetDataTableInfo()->GetIndexes();
  if (!list.NameIsUnique(std::string{create.GetIndexName()})) {
    return absl::OkStatus();
  }
  // A replayed op carries ids, not objects: the two definitions are written by
  // the PutTable and PutEntry records of the very same frame, so re-resolving
  // them by id is exact rather than approximate. That is what keeps the record
  // reconstructable without the log storing each definition twice.
  auto table = op.table;
  auto index = op.index;
  const ObjectId index_id{create.oid};
  if (!table || !index) {
    if (const auto* found = catalog::FindTableIn(nullptr, _target->GetCatalog(),
                                                 op.relation_id)) {
      table = found->Definition();
    }
    if (const auto* found =
          catalog::FindIndexIn(nullptr, _target->GetCatalog(), index_id);
        found != nullptr && found->IsInverted()) {
      index = found->Definition();
    }
    if (!table || !index) {
      return absl::InternalError(absl::StrCat(
        "inverted index ", index_id.id(), " on relation ", op.relation_id.id(),
        " has no catalog definition to rebuild it from"));
    }
  }
  WithBindContext(*_target, [&](duckdb::ClientContext& bind_ctx) {
    connector::AddInjectedInvertedIndex(
      list,
      connector::MakeInjectedInvertedIndex(&bind_ctx, storage, table, index));
  });
  return absl::OkStatus();
}

absl::Status DataStore::ExecuteDropStoreIndex(duckdb::ClientContext* context,
                                              ObjectId table_id,
                                              const duckdb::Identifier& name) {
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  // The table itself may already be gone -- index drops ride table and schema
  // drops -- and then its indexes went with its rows.
  auto table_entry = ResolveTable(resolve_context, table_id);
  if (!table_entry) {
    return absl::OkStatus();
  }
  auto& info = *table_entry->GetStorage().GetDataTableInfo();
  // Indexes deserialized by the attach stay unbound until first use, and Find
  // refuses to hand one out; the first DDL after a restart would hit that
  // instead of the drop.
  info.BindIndexes(*_conn->context);
  // Neither kind has a duckdb entry: an inverted index is injected into the
  // list and an ART is built onto it, so for both the list is where the index
  // is and taking it off is what ends it.
  info.GetIndexes().RemoveIndex(name);
  return absl::OkStatus();
}

// The physical index mirrors the catalog name, so a rename has to move it --
// including the one a checkpoint already wrote down, which is why the live
// index is renamed rather than dropped and rebuilt.
absl::Status DataStore::ExecuteRenameStoreIndex(
  duckdb::ClientContext* context, ObjectId table_id,
  const duckdb::RenameTableInfo& info) {
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  auto table_entry = ResolveTable(resolve_context, table_id);
  if (!table_entry) {
    return absl::OkStatus();
  }
  table_entry->GetStorage().GetDataTableInfo()->GetIndexes().RenameIndex(
    info.GetQualifiedName().Name(), info.new_table_name);
  return absl::OkStatus();
}

// A reshape duckdb can only reach through its planner: ADD PRIMARY KEY and ADD
// UNIQUE build an index over the rows already there.
absl::Status DataStore::RunAlter(duckdb::ClientContext* context,
                                 ObjectId table_id,
                                 duckdb::unique_ptr<duckdb::AlterInfo> info) {
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  auto table_entry = ResolveTable(resolve_context, table_id);
  if (!table_entry) {
    return absl::InternalError(
      absl::StrCat("relation ", table_id.id(), " has no rows to reshape"));
  }
  info->SetQualifiedName(NameOf(*table_entry));
  auto statement = duckdb::make_uniq<duckdb::AlterStatement>();
  statement->info = std::move(info);
  return Run(std::move(statement));
}

// The rows of a table the entry in front of them was built without: boot's
// store-op gap replay, where the entry came from the catalog log before the
// file holding its rows was open. A live create needs nothing here -- the entry
// builds its own rows as it is written.
absl::Status DataStore::ExecuteCreateStoreTable(duckdb::ClientContext* context,
                                                ObjectId table_id) try {
  if (context != nullptr) {
    return absl::OkStatus();
  }
  auto& catalog = _target->GetCatalog().Cast<catalog::SereneDBCatalog>();
  auto entry = catalog.LookupTableById(catalog.CommittedRead(), table_id.id());
  if (!entry) {
    return absl::InternalError(
      absl::StrCat("relation ", table_id.id(), " has no definition"));
  }
  auto& table = entry->Cast<duckdb::DuckTableEntry>();
  if (table.TryGetStorage()) {
    return absl::OkStatus();
  }
  auto info = table.GetInfo();
  // The entry reports the id off the rows it does not have yet, so it reports
  // none. Rows filed under id 0 are rows the checkpoint will not write and a
  // WAL record cannot name.
  info->oid = table_id.id();
  auto bound = duckdb::Binder::BindCreateTableCheckpoint(std::move(info),
                                                         table.ParentSchema());
  table.AdoptStorage(*bound);
  return absl::OkStatus();
} catch (const std::exception& e) {
  return StoreFailure(e);
}

duckdb::Connection* DataStore::BindConnection(duckdb::AttachedDatabase& db) {
  const auto database_id = StoreDatabaseId(db);
  if (!database_id.isSet()) {
    return nullptr;
  }
  absl::MutexLock lock{&_bind_mutex};
  auto it = _bind_contexts.find(database_id);
  if (it == _bind_contexts.end()) {
    auto database = catalog::FindDatabase(nullptr, database_id);
    if (!database) {
      // Without it every indexed expression stays unbound, which shows up much
      // later as a replay that indexes nothing. The catalog is loaded before
      // any database is attached, so this should not happen -- say so if it
      // does.
      SDB_WARN(STARTUP, "data store: database ", database_id.id(),
               " has no catalog record; indexed expressions will not bind");
      return nullptr;
    }
    const std::string name{database.Name()};
    BindContext bind;
    // Two connections, not one. An injection borrows the first for the length
    // of a callback, opening and rolling back a transaction of its own, while a
    // store batch holds the second across every op it runs -- routed onto the
    // statement's transaction. Sharing one made each end the other's
    // transaction out from under it.
    const auto open = [&](duckdb::unique_ptr<duckdb::Connection>& conn,
                          std::shared_ptr<ConnectionContext>& ctx) {
      conn = DuckDBEngine::Instance().CreateConnection();
      ctx = std::make_shared<ConnectionContext>(
        *conn->context, StaticStrings::kDefaultUser, ObjectId{}, name,
        database_id, nullptr, nullptr, 0, nullptr);
      ctx->MarkStorageConnection();
      connector::SereneDBClientState::Register(*conn->context, ctx);
      // Same search path a session gets: an indexed expression names its
      // dictionary unqualified, so resolving it needs `public` on the path.
      conn->context->session_user = std::string{StaticStrings::kDefaultUser};
      std::vector<duckdb::CatalogSearchEntry> paths{
        duckdb::CatalogSearchEntry{duckdb::Identifier{name},
                                   duckdb::Identifier{"$user"}},
        duckdb::CatalogSearchEntry{duckdb::Identifier{name},
                                   duckdb::Identifier{"public"}},
      };
      conn->context->client_data->catalog_search_path->SetDefaultPaths(
        std::vector{paths});
      conn->context->client_data->catalog_search_path->Set(
        std::move(paths), duckdb::CatalogSetPathType::SET_DIRECTLY);
    };
    open(bind.conn, bind.ctx);
    open(bind.exec_conn, bind.exec_ctx);
    it = _bind_contexts.emplace(database_id, std::move(bind)).first;
  }
  return it->second.exec_conn.get();
}

void DataStore::WithBindContext(
  duckdb::AttachedDatabase& db,
  absl::FunctionRef<void(duckdb::ClientContext&)> fn) {
  duckdb::Connection* connection = nullptr;
  if (BindConnection(db) != nullptr) {
    absl::MutexLock lock{&_bind_mutex};
    const auto it = _bind_contexts.find(StoreDatabaseId(db));
    connection = it == _bind_contexts.end() ? nullptr : it->second.conn.get();
  }
  if (connection == nullptr) {
    return;
  }
  auto& conn = *connection;
  conn.BeginTransaction();
  const absl::Cleanup done = [&] { conn.Rollback(); };
  // The injection runs from inside the attach of `db`, so the database manager
  // does not list it yet and a name lookup from this connection would fail.
  // Referencing it on the bind transaction is what breaks that cycle: the
  // catalog reads resolve without the attach having to complete first.
  auto shared_db = db.shared_from_this();
  duckdb::MetaTransaction::Get(*conn.context).UseDatabase(shared_db);
  fn(*conn.context);
}

void JoinStoreTransaction(duckdb::ClientContext* context) {
  if (context == nullptr) {
    return;
  }
  if (auto* session = connector::GetSereneDBContextPtr(*context)) {
    JoinStoreTransaction(*context, session->GetDatabaseId());
  }
}

void JoinStoreTransaction(duckdb::ClientContext& context,
                          ObjectId database_id) {
  if (!DataStore::IsReady() || !context.transaction.HasActiveTransaction()) {
    return;
  }
  auto& meta = duckdb::MetaTransaction::Get(context);
  // duckdb claims the database a statement writes before it executes, and for
  // DDL naming another database that is not the one the session connected to
  // -- so prefer its answer over the session's own database, which is only a
  // fallback for the DDL that arrives as a pragma and registers nothing.
  duckdb::optional_ptr<duckdb::AttachedDatabase> db = meta.ModifiedDatabase();
  if (!db) {
    db = TryStoreDatabase(context, database_id);
  }
  if (!db) {
    return;
  }
  meta.ModifyDatabase(*db,
                      duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
}

void DataStore::RebuildMissingIndexes(ObjectId database_id) {
  auto attachment = TryStoreDatabase(database_id);
  if (!attachment || !attachment->HasStorageManager()) {
    return;
  }
  auto& catalog = attachment->GetCatalog().Cast<catalog::SereneDBCatalog>();
  // Ids first, definitions after: resolving a relation from inside a walk
  // re-enters the very set the walk holds. One pass over the indexes rather
  // than one per table, so boot stays linear in the catalog.
  std::vector<ObjectId> table_ids;
  catalog::VisitTables(
    nullptr, database_id, [&](const TableInfoRef& table, const Permissions&) {
      if (catalog::TableEngineOf(*table) == TableEngine::Transactional) {
        table_ids.push_back(catalog::IdOf(*table));
      }
    });
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> index_ids;
  catalog::VisitIndexes(nullptr, database_id, [&](const IndexInfoRef& index) {
    index_ids[index->GetRelationId()].push_back(index->GetId());
  });
  std::vector<store_op::Targeted> ops;
  for (const auto table_id : table_ids) {
    const auto* table_entry = catalog::FindTableIn(nullptr, catalog, table_id);
    auto entry =
      catalog.LookupTableById(catalog.CommittedRead(), table_id.id());
    if (table_entry == nullptr || !entry || !entry->TryGetStorage()) {
      continue;
    }
    const auto table = table_entry->Definition();
    auto& list = entry->GetStorage().GetDataTableInfo()->GetIndexes();
    const auto on_table = index_ids.find(table_id);
    if (on_table != index_ids.end()) {
      for (const auto index_id : on_table->second) {
        const auto* index_entry =
          catalog::FindIndexIn(nullptr, catalog, index_id);
        if (index_entry == nullptr) {
          continue;
        }
        auto info = MakeStoreIndexInfo(*table, *index_entry->Definition());
        if (!info || !IsPlainStoreIndex(*info) ||
            !list.NameIsUnique(std::string{info->GetIndexName()})) {
          continue;
        }
        ops.emplace_back(database_id, table_id, std::move(info));
      }
    }
    // A key constraint's ART is named after the constraint rather than the
    // index object, and it is built by the ALTER that states it.
    for (const auto& constraint : table->constraints) {
      if (constraint->type != duckdb::ConstraintType::UNIQUE) {
        continue;
      }
      const auto& unique = constraint->Cast<duckdb::UniqueConstraint>();
      const auto name = unique.GetName(entry->name);
      if (!list.NameIsUnique(name.GetIdentifierName())) {
        continue;
      }
      duckdb::vector<duckdb::Identifier> columns;
      for (const auto& logical : unique.GetLogicalIndexes(table->columns)) {
        columns.emplace_back(table->columns.GetColumn(logical).Name());
      }
      if (columns.empty()) {
        continue;
      }
      auto key = duckdb::make_uniq<duckdb::UniqueConstraint>(
        std::move(columns), unique.IsPrimaryKey());
      key->constraint_name = constraint->constraint_name;
      auto add = duckdb::make_uniq<duckdb::AddConstraintInfo>(StoreTarget(),
                                                              std::move(key));
      add->oid = table_id.id();
      ops.emplace_back(database_id, table_id, std::move(add));
    }
  }
  if (ops.empty()) {
    return;
  }
  SDB_INFO(STARTUP, "database ", database_id.id(), ": rebuilding ", ops.size(),
           " index(es) no checkpoint captured");
  if (auto r = ApplyStoreOps(nullptr, ops); !r.ok()) {
    SDB_WARN(STARTUP, "database ", database_id.id(),
             ": rebuilding indexes failed: ", r.message());
  }
}

void WithStoreBindContext(duckdb::AttachedDatabase& db,
                          absl::FunctionRef<void(duckdb::ClientContext&)> fn) {
  if (auto* store = DataStore::gInstance; store != nullptr && store->_conn) {
    store->WithBindContext(db, fn);
  }
}

DataStore& GetDataStore() { return *DataStore::gInstance; }

}  // namespace sdb::catalog
