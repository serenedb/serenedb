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

#include "catalog/log/data_store.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/alter_statement.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/storage/data_table.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <exception>
#include <optional>
#include <utility>

#include "basics/assert.h"
#include "basics/containers/flat_hash_map.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_index_entry.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/entry/duckdb_table_entry.h"
#include "catalog/index.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "catalog/table.h"
#include "connector/duckdb_client_state.h"
#include "connector/inverted_store_index.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

thread_local bool gInBatch = false;

duckdb::QualifiedName NameOf(const duckdb::TableCatalogEntry& entry) {
  return duckdb::QualifiedName{entry.ParentCatalog().GetName(),
                               entry.ParentSchema().name, entry.name};
}

// A write-write conflict means a concurrent statement reshaped the same rows;
// Aborted is what carries "retry this transaction" up to RunStoreOps.
absl::Status StoreFailure(std::string error) {
  if (absl::StrContains(error, "write-write conflict")) {
    return absl::AbortedError(std::move(error));
  }
  return absl::InternalError(std::move(error));
}

// A TRANSACTION-typed error is a conflict even when the text does not say
// write-write.
absl::Status StoreFailure(const duckdb::ErrorData& error) {
  if (error.Type() == duckdb::ExceptionType::TRANSACTION) {
    return absl::AbortedError(error.Message());
  }
  return StoreFailure(error.Message());
}

// what() is the JSON form when it carries extra info; ErrorData restores the
// spelling QueryResult::GetError() produces.
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
  // Backstop: a throw out of `fn` must still reach the rollback below.
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
// onto the statement's transaction, so every op in the batch commits or rolls
// back with the statement. The rollback below only discards the empty shell
// the connection needs to hold the override.
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

  // A failed statement invalidates duckdb's meta transaction; ops that retry
  // after a failure need a usable one. Only the store connection's shell is
  // replaced; the statement's transaction carries the work across.
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
  // would block the detach that follows.
  {
    absl::MutexLock lock{&_bind_mutex};
    _bind_contexts.clear();
  }
  _conn.reset();
}

absl::Status DataStore::ApplyStoreOps(duckdb::ClientContext* context,
                                      std::span<const store_op::Targeted> ops) {
  SDB_ASSERT(_conn, "store DDL before the data store is up");
  // One batch at a time: the routing a batch sets up is this object's state.
  // Reentrant, because a reshape can emit a store op of its own, run by the
  // thread already inside the batch.
  const bool nested = std::exchange(gInBatch, true);
  const absl::Cleanup batch_done = [&] { gInBatch = nested; };
  std::optional<absl::MutexLock> batch;
  if (!nested) {
    batch.emplace(&_ops_mutex);
  }
  // Boot recovery, background drop tasks and teardown have no statement to
  // run on, so they get a transaction of the store connection's own.
  if (context == nullptr || !context->transaction.HasActiveTransaction()) {
    return RunInTransaction(*_conn,
                            [&] { return ExecuteStoreOps(nullptr, ops); });
  }
  // A failure under a statement is rethrown as duckdb's own error; the routing
  // is released by a Cleanup, so an escaping throw is safe.
  return ExecuteStoreOps(context, ops);
}

absl::Status DataStore::ExecuteStoreOps(
  duckdb::ClientContext* context, std::span<const store_op::Targeted> ops) {
  // Published before the first op: a batch can drop several columns, and the
  // rebuild triggered by the first already has to know about all of them. A
  // replay, where the catalog is already past the drop, needs none of this.
  for (const auto& targeted : ops) {
    if (targeted.info->info_type != duckdb::ParseInfoType::ALTER_INFO) {
      continue;
    }
    const auto& alter = targeted.info->Cast<duckdb::AlterInfo>();
    if (alter.type != duckdb::AlterType::ALTER_TABLE ||
        alter.Cast<duckdb::AlterTableInfo>().alter_table_type !=
          duckdb::AlterTableType::REMOVE_COLUMN) {
      continue;
    }
    const auto* table = context != nullptr
                          ? catalog::FindSession<SereneDBTableEntry>(
                              *context, targeted.relation_id)
                          : nullptr;
    if (table == nullptr) {
      continue;
    }
    const auto* column =
      table->GetColumns().TryGetColumn(alter.GetColumnName()).get();
    if (column != nullptr) {
      _dropping_columns.insert(ObjectId{column->CatalogOid()});
    }
  }
  const absl::Cleanup dropping_done = [&] { _dropping_columns.clear(); };
  // The routing onto the statement's transaction is per attachment, and duckdb
  // allows one override at a time -- opened for the database the current op
  // names, reopened when that changes.
  std::optional<StatementTransaction> statement;
  // Resolving user-defined types reads the catalog, which needs a transaction;
  // nothing here writes through this connection.
  const bool own_transaction =
    !_conn->context->transaction.HasActiveTransaction();
  if (own_transaction) {
    _conn->BeginTransaction();
  }
  const absl::Cleanup routing_done = [&] {
    _statement = nullptr;
    _exec_conn = nullptr;
    _target.reset();
    if (own_transaction) {
      _conn->Rollback();
    }
  };
  ObjectId resolved;
  for (const auto& targeted : ops) {
    if (targeted.database_id != resolved) {
      // Resolved through the statement's own view: a database created earlier
      // in the same transaction is not in the committed catalog yet.
      auto target = TryStoreDatabase(
        context != nullptr ? *context : *_conn->context, targeted.database_id);
      if (!target) {
        // DROP DATABASE removes the attachment synchronously while the
        // cascade's per-table drops run asynchronously afterwards; their
        // target is already gone.
        if (store_op::IsDestructive(targeted)) {
          continue;
        }
        return absl::InternalError(absl::StrCat(
          "database ", targeted.database_id.id(), " is not attached"));
      }
      resolved = targeted.database_id;
      _target = std::move(target);
      // Index builds run as statements, so they go on the connection carrying
      // this database's client state, marked as the store's -- which routes
      // them to duckdb's native catalog paths, not back into the mutators.
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
  // Under a statement the failure is rethrown rather than rendered: the client
  // sees the same code and text as for the identical failure on an INSERT.
  if (_statement != nullptr && error.Type() != duckdb::ExceptionType::INVALID) {
    error.Throw();
  }
  return StoreFailure(error);
}

// The reshaped rows travel with the transaction that produced them: only it
// can see them, and the entry version that carries them is built at the write.
absl::Status DataStore::Alter(duckdb::ClientContext* context,
                              duckdb::AlterInfo& info) {
  auto& catalog = _target->GetCatalog().Cast<catalog::SereneDBCatalog>();
  if (context != nullptr) {
    // Versioned, so the reshape is recorded in the data WAL. A failure is
    // thrown, not reported: it is this statement's error.
    catalog.AlterStorage(catalog.GetCatalogTransaction(*context), info,
                         /*versioned=*/true);
    return absl::OkStatus();
  }
  // Boot: no statement, and the catalog log has already settled the
  // definition. Nobody is listening for a throw here.
  auto reshaped = absl::Status{};
  WithBindContext(*_target, [&](duckdb::ClientContext& reshape_context) {
    try {
      catalog.AlterStorage(catalog.GetCatalogTransaction(reshape_context), info,
                           /*versioned=*/false);
    } catch (const std::exception& e) {
      reshaped = StoreFailure(e);
    }
  });
  return reshaped;
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
  switch (op.info->info_type) {
    case duckdb::ParseInfoType::ALTER_INFO: {
      // A copy per execution: AlterStorage resolves the target into the info,
      // and a replayed batch is held for as long as the database is behind.
      auto info = op.info->Cast<duckdb::AlterInfo>().Copy();
      if (info->type == duckdb::AlterType::ALTER_TABLE &&
          info->Cast<duckdb::AlterTableInfo>().alter_table_type ==
            duckdb::AlterTableType::RENAME_TABLE) {
        // The physical index names mirror the relation name and move first;
        // the alter below re-files the entry itself.
        if (auto renamed = ExecuteRenameStoreIndex(
              context, op.relation_id, info->Cast<duckdb::RenameTableInfo>());
            !renamed.ok()) {
          return renamed;
        }
        return Alter(context, *info);
      }
      if (info->IsAddIndexedConstraint()) {
        // The build goes through the planner first and the entry takes the
        // constraint below, sharing the index the plan added. A search table
        // has no rows of its own and records the key on the definition alone.
        auto& resolve_context = context != nullptr ? *context : *_conn->context;
        if (ResolveTable(resolve_context, op.relation_id)) {
          auto alter_copy = info->Copy();
          if (auto built =
                RunAlter(context, op.relation_id, std::move(alter_copy));
              !built.ok()) {
            return built;
          }
        }
        return Alter(context, *info);
      }
      return Alter(context, *info);
    }
    case duckdb::ParseInfoType::CREATE_INFO:
      return ExecuteCreateStoreIndex(context, op);
    case duckdb::ParseInfoType::DROP_INFO: {
      const auto& drop = op.info->Cast<duckdb::DropInfo>();
      SDB_ASSERT(drop.type == duckdb::CatalogType::INDEX_ENTRY);
      return ExecuteDropStoreIndex(context, op.relation_id,
                                   drop.GetQualifiedName().Name());
    }
    default:
      return absl::InternalError(absl::StrCat(
        "store op ", static_cast<int>(op.info->info_type), " has no executor"));
  }
}

// An ART over existing rows is a physical plan, and a plan is reached by
// running a statement.
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
  // A replayed op carries ids, not objects: the definitions are written by the
  // PutEntry records of the same frame, so re-resolving them by id is exact
  // and the log need not store each definition twice.
  auto table = op.table ? catalog::Clone(*op.table) : nullptr;
  std::shared_ptr<const catalog::Index> index = op.index;
  const ObjectId index_id{create.oid};
  if (!table || !index) {
    if (const auto* found = catalog::FindIn<SereneDBTableEntry>(
          nullptr, _target->GetCatalog(), op.relation_id)) {
      table = found->Definition();
    }
    if (const auto* found = catalog::FindIn<SereneDBIndexEntry>(
          nullptr, _target->GetCatalog(), index_id);
        found != nullptr && found->IsInverted()) {
      index = found->DefinitionPtr();
    }
    if (!table || !index) {
      return absl::InternalError(absl::StrCat(
        "inverted index ", index_id.id(), " on relation ", op.relation_id.id(),
        " has no catalog definition to rebuild it from"));
    }
  }
  WithBindContext(*_target, [&](duckdb::ClientContext& bind_ctx) {
    connector::AddInjectedInvertedIndex(
      list, connector::MakeInjectedInvertedIndex(bind_ctx, storage, *table,
                                                 index, op.storage));
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
  // Indexes deserialized by the attach stay unbound until first use; the first
  // DDL after a restart would hit that instead of the drop.
  info.BindIndexes(*_conn->context);
  // Neither kind has a duckdb entry: the index list is where both live, and
  // taking one off is what ends it.
  info.GetIndexes().RemoveIndex(name);
  return absl::OkStatus();
}

// The physical index name mirrors the catalog name; it is renamed live rather
// than dropped and rebuilt, since a checkpoint may already have written it.
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
    const std::string name{database->name.GetIdentifierName()};
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
        database_id, nullptr, 0, nullptr);
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
  // Starting the transaction is what takes the checkpoint lock; claiming a
  // write is not this function's business. A statement that really writes this
  // database has already been claimed by duckdb before it executed, and one
  // that does not -- DROP DATABASE names another -- must not be made to look
  // like it writes two.
  meta.GetTransaction(*db);
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
  catalog::Visit<catalog::SereneDBTableEntry>(
    nullptr, database_id, [&](const catalog::SereneDBTableEntry& table) {
      if (table.GetEngine() == TableEngine::Transactional) {
        table_ids.push_back(catalog::IdOf(table));
      }
    });
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> index_ids;
  catalog::Visit<catalog::SereneDBIndexEntry>(
    nullptr, database_id, [&](const catalog::SereneDBIndexEntry& index) {
      index_ids[index.GetRelationId()].push_back(catalog::IdOf(index));
    });
  std::vector<store_op::Targeted> ops;
  for (const auto table_id : table_ids) {
    const auto* table_entry =
      catalog::FindIn<SereneDBTableEntry>(nullptr, catalog, table_id);
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
          catalog::FindIn<SereneDBIndexEntry>(nullptr, catalog, index_id);
        if (index_entry == nullptr) {
          continue;
        }
        auto record = index_entry->GetInfo();
        auto info =
          MakeStoreIndexInfo(*table, record->Cast<catalog::CreateIndexInfo>());
        if (!info || !IsPlainStoreIndex(*info) ||
            !list.NameIsUnique(std::string{info->GetIndexName()})) {
          continue;
        }
        ops.emplace_back(database_id, table_id, std::move(info));
      }
    }
    // A key constraint's ART is named after the constraint (GetName prefers
    // the constraint's own name, whichever statement built the ART), and it
    // is built by the ALTER that states it.
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
