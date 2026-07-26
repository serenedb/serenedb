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
#include <absl/strings/str_join.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/execution/index/art/art.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parser.hpp>
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
#include "catalog/index.h"
#include "catalog/inverted_index.h"
#include "catalog/table.h"
#include "connector/duckdb_catalog.h"
#include "connector/duckdb_catalog_sets.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_index_entry.h"
#include "connector/duckdb_table_entry.h"
#include "connector/inverted_store_index.h"
#include "pg/connection_context.h"

namespace sdb::catalog {
namespace {

std::string QuotedIdent(const std::string& name) {
  return duckdb::KeywordHelper::WriteQuoted(name, '"');
}

// `"postgres"."public"."orders"` -- an index build is the one op still spelled
// as SQL, and it names the relation the way a session would.
std::string QualifiedTable(const duckdb::TableCatalogEntry& entry) {
  return absl::StrCat(
    QuotedIdent(entry.ParentCatalog().GetName().GetIdentifierName()), ".",
    QuotedIdent(entry.ParentSchema().name.GetIdentifierName()), ".",
    QuotedIdent(entry.name.GetIdentifierName()));
}

std::string QuotedColumns(std::span<const std::string> columns) {
  return absl::StrJoin(columns, ", ",
                       [](std::string* out, const std::string& c) {
                         absl::StrAppend(out, QuotedIdent(c));
                       });
}

// One expression of rendered SQL. The op carries the user's own text -- a
// DEFAULT, a CHECK body, an ALTER COLUMN TYPE USING -- and this is where it
// becomes a parse tree again; nothing else about a store op is SQL any more.
duckdb::unique_ptr<duckdb::ParsedExpression> ParseOne(const std::string& sql) {
  auto list = duckdb::Parser::ParseExpressionList(sql);
  SDB_ENSURE(list.size() == 1, "store op: \"", sql,
             "\" is not a single expression");
  return std::move(list.front());
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
  // know about all of them.
  for (const auto& targeted : ops) {
    if (const auto* drop = std::get_if<store_op::DropColumn>(&targeted.op)) {
      if (drop->column_id.isSet()) {
        _dropping_columns.insert(drop->column_id);
      }
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
        if (store_op::IsDestructive(targeted.op)) {
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
    if (auto r = ExecuteStoreOp(context, targeted.op); !r.ok()) {
      return r;
    }
  }
  return absl::OkStatus();
}

absl::Status DataStore::Exec(const std::string& sql) {
  SDB_ENSURE(_exec_conn != nullptr,
             "store statement with no connection: ", sql);
  auto res = _exec_conn->Query(sql);
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
absl::Status DataStore::Alter(duckdb::ClientContext* context, ObjectId table_id,
                              duckdb::AlterInfo& info) try {
  info.host_id = table_id.id();
  auto& catalog = _target->GetCatalog().Cast<connector::SereneDBCatalog>();
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

// An AlterInfo names its target twice: by identity, which is what resolves it,
// and by name, which is what an error message shows. An op carries only the
// identity, so the name is left for AlterStorage to fill in off the entry it
// resolves -- the current one, not the one the op was written against.
duckdb::AlterEntryData DataStore::OpTarget(bool missing_ok) {
  return duckdb::AlterEntryData{duckdb::QualifiedName{},
                                missing_ok
                                  ? duckdb::OnEntryNotFound::RETURN_NULL
                                  : duckdb::OnEntryNotFound::THROW_EXCEPTION};
}

absl::Status DataStore::ExecuteStoreOp(duckdb::ClientContext* context,
                                       const store_op::Op& op) {
  return std::visit(
    [this, context](const auto& o) -> absl::Status {
      using T = std::decay_t<decltype(o)>;
      if constexpr (std::is_same_v<T, store_op::CreateTable>) {
        return ExecuteCreateStoreTable(context, o.table_id);
      } else if constexpr (std::is_same_v<T, store_op::DropTable>) {
        // The rows go with the entry, whose drop is a duckdb table drop, so
        // the blocks are reclaimed at commit.
        return absl::OkStatus();
      } else if constexpr (std::is_same_v<T, store_op::DropColumn>) {
        duckdb::RemoveColumnInfo info{OpTarget(), o.column,
                                      /*if_column_exists=*/false,
                                      /*cascade=*/false};
        return Alter(context, o.table_id, info);
      } else if constexpr (std::is_same_v<T, store_op::RenameColumn>) {
        duckdb::RenameColumnInfo info{OpTarget(), duckdb::Identifier{o.column},
                                      duckdb::Identifier{o.new_name}};
        return Alter(context, o.table_id, info);
      } else if constexpr (std::is_same_v<T, store_op::DropNotNull>) {
        duckdb::DropNotNullInfo info{OpTarget(), duckdb::Identifier{o.column}};
        return Alter(context, o.table_id, info);
      } else if constexpr (std::is_same_v<T, store_op::AddNotNull>) {
        duckdb::SetNotNullInfo info{OpTarget(), duckdb::Identifier{o.column}};
        return Alter(context, o.table_id, info);
      } else if constexpr (std::is_same_v<T, store_op::AddCheck>) {
        duckdb::AddConstraintInfo info{
          OpTarget(),
          duckdb::make_uniq<duckdb::CheckConstraint>(ParseOne(o.expr))};
        return Alter(context, o.table_id, info);
      } else if constexpr (std::is_same_v<T, store_op::AddPrimaryKey> ||
                           std::is_same_v<T, store_op::AddUnique>) {
        return ExecuteAddKeyConstraint(
          context, o.table_id, o.constraint, o.columns,
          std::is_same_v<T, store_op::AddPrimaryKey>);
      } else if constexpr (std::is_same_v<T, store_op::DropCheck>) {
        duckdb::DropConstraintInfo info{OpTarget(/*missing_ok=*/true), o.expr,
                                        true, false};
        return Alter(context, o.table_id, info);
      } else if constexpr (std::is_same_v<T, store_op::CreateIndex>) {
        return ExecuteCreateStoreIndex(context, o);
      } else if constexpr (std::is_same_v<T, store_op::DropIndex>) {
        return ExecuteDropStoreIndex(context, o.def);
      } else if constexpr (std::is_same_v<T, store_op::RenameIndex>) {
        return ExecuteRenameStoreIndex(context, o);
      } else if constexpr (std::is_same_v<T, store_op::AddColumn>) {
        return ExecuteAddStoreColumn(context, o);
      } else if constexpr (std::is_same_v<T, store_op::ChangeColumnType>) {
        duckdb::ChangeColumnTypeInfo info{
          OpTarget(), duckdb::Identifier{o.column},
          duckdb::TransformStringToLogicalType(o.type_sql, *_conn->context),
          o.using_sql.empty() ? nullptr : ParseOne(o.using_sql)};
        return Alter(context, o.table_id, info);
      } else {
        // No catch-all: a new op falling through here would report success
        // for DDL that never ran.
        static_assert(false, "store op is not executed");
      }
    },
    op);
}

// The arms with real bodies, lifted out so the visit above reads as the
// dispatch table it is.
absl::Status DataStore::ExecuteAddStoreColumn(duckdb::ClientContext* context,
                                              const store_op::AddColumn& o) {
  const auto column = [&](bool with_default) {
    duckdb::ColumnDefinition cd{
      duckdb::Identifier{o.column},
      duckdb::TransformStringToLogicalType(o.type_sql, *_conn->context)};
    cd.SetCompressionType(o.compression);
    if (with_default) {
      cd.SetDefaultValue(ParseOne(o.default_sql));
    }
    return cd;
  };
  const auto add = [&](bool with_default) {
    duckdb::AddColumnInfo info{OpTarget(), column(with_default),
                               /*if_column_not_exists=*/false};
    return Alter(context, o.table_id, info);
  };
  if (o.default_sql.empty()) {
    return add(/*with_default=*/false);
  }
  auto r = add(/*with_default=*/true);
  if (r.ok()) {
    return r;
  }
  // The DEFAULT may name something the reshape cannot bind here. Add the column
  // without it; existing rows get NULL and the definition still carries the
  // default, which is what fills it on insert.
  SDB_WARN(GENERAL, "relation ", o.table_id.id(), ": ADD COLUMN \"", o.column,
           "\" DEFAULT not backfilled: ", r.message());
  return add(/*with_default=*/false);
}

// An index build, not a reshape: the entry alter only records the constraint,
// while the ART over the existing rows is a physical plan, and a plan is
// reached by running a statement.
absl::Status DataStore::ExecuteAddKeyConstraint(
  duckdb::ClientContext* context, ObjectId table_id,
  const std::string& constraint, std::span<const std::string> columns,
  bool primary_key) {
  auto entry =
    ResolveTable(context != nullptr ? *context : *_conn->context, table_id);
  if (!entry) {
    return absl::InternalError(
      absl::StrCat("relation ", table_id.id(), " has no rows to key"));
  }
  return Exec(absl::StrCat("ALTER TABLE ", QualifiedTable(*entry),
                           " ADD CONSTRAINT ", QuotedIdent(constraint),
                           primary_key ? " PRIMARY KEY (" : " UNIQUE (",
                           QuotedColumns(columns), ")"));
}

absl::Status DataStore::ExecuteCreateStoreIndex(
  duckdb::ClientContext* context, const store_op::CreateIndex& o) {
  const auto& def = o.def;
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  auto table_entry = ResolveTable(resolve_context, def.table_id);
  if (!table_entry) {
    return absl::InternalError(
      absl::StrCat("relation ", def.table_id.id(), " has no rows to index"));
  }
  if (def.kind != StoreIndexDef::Kind::Inverted) {
    // The index name is bare: CREATE INDEX takes its schema from the relation,
    // and spelling one here is a syntax error.
    return Exec(absl::StrCat("CREATE ", def.unique ? "UNIQUE " : "", "INDEX ",
                             QuotedIdent(def.name), " ON ",
                             QualifiedTable(*table_entry), " (",
                             absl::StrJoin(def.keys, ", "), ")"));
  }
  if (def.defer_injection) {
    return absl::OkStatus();
  }
  // A replayed op carries ids, not objects: the two definitions are written by
  // the PutTable and PutEntry records of the very same frame, so re-resolving
  // them by id is exact rather than approximate. That is what keeps the record
  // reconstructable without the log storing each definition twice.
  auto table = o.table;
  auto index = o.index;
  if (!table || !index) {
    if (const auto* found = connector::FindTableIn(
          nullptr, _target->GetCatalog(), def.table_id)) {
      table = found->Definition();
    }
    if (const auto* found =
          connector::FindIndexIn(nullptr, _target->GetCatalog(), def.index_id);
        found != nullptr && found->IsInverted()) {
      index = found->Definition();
    }
    if (!table || !index) {
      return absl::InternalError(absl::StrCat(
        "inverted index ", def.index_id.id(), " on relation ",
        def.table_id.id(), " has no catalog definition to rebuild it from"));
    }
  }
  auto& storage = table_entry->GetStorage();
  auto& list = storage.GetDataTableInfo()->GetIndexes();
  WithBindContext(*_target, [&](duckdb::ClientContext& bind_ctx) {
    connector::AddInjectedInvertedIndex(
      list,
      connector::MakeInjectedInvertedIndex(&bind_ctx, storage, table, index));
  });
  return absl::OkStatus();
}

absl::Status DataStore::ExecuteDropStoreIndex(duckdb::ClientContext* context,
                                              const StoreIndexDef& def) {
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  // The table itself may already be gone -- index drops ride table and schema
  // drops -- and then its indexes went with its rows.
  auto table_entry = ResolveTable(resolve_context, def.table_id);
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
  info.GetIndexes().RemoveIndex(duckdb::Identifier{def.name});
  return absl::OkStatus();
}

// The physical index mirrors the catalog name, so a rename has to move it --
// including the one a checkpoint already wrote down, which is why the live
// index is renamed rather than dropped and rebuilt.
absl::Status DataStore::ExecuteRenameStoreIndex(
  duckdb::ClientContext* context, const store_op::RenameIndex& o) {
  auto& resolve_context = context != nullptr ? *context : *_conn->context;
  auto table_entry = ResolveTable(resolve_context, o.table_id);
  if (!table_entry) {
    return absl::OkStatus();
  }
  auto& info = *table_entry->GetStorage().GetDataTableInfo();
  info.GetIndexes().RenameIndex(duckdb::Identifier{o.from},
                                duckdb::Identifier{o.to});
  return absl::OkStatus();
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
  auto& catalog = _target->GetCatalog().Cast<connector::SereneDBCatalog>();
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
    auto database = connector::FindDatabase(nullptr, database_id);
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
  auto& catalog = attachment->GetCatalog().Cast<connector::SereneDBCatalog>();
  // Ids first, definitions after: resolving a relation from inside a walk
  // re-enters the very set the walk holds. One pass over the indexes rather
  // than one per table, so boot stays linear in the catalog.
  std::vector<ObjectId> table_ids;
  connector::VisitTables(
    nullptr, database_id, [&](const TableInfoRef& table, const Permissions&) {
      if (table->GetEngine() == TableEngine::Transactional) {
        table_ids.push_back(table->GetId());
      }
    });
  containers::FlatHashMap<ObjectId, std::vector<ObjectId>> index_ids;
  connector::VisitIndexes(nullptr, database_id, [&](const IndexInfoRef& index) {
    index_ids[index->GetRelationId()].push_back(index->GetId());
  });
  std::vector<store_op::Targeted> ops;
  for (const auto table_id : table_ids) {
    const auto* table_entry =
      connector::FindTableIn(nullptr, catalog, table_id);
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
          connector::FindIndexIn(nullptr, catalog, index_id);
        if (index_entry == nullptr) {
          continue;
        }
        auto def = MakeStoreIndexDef(*table, *index_entry->Definition());
        if (!def || def->kind != StoreIndexDef::Kind::Plain ||
            !list.NameIsUnique(def->name)) {
          continue;
        }
        ops.push_back(
          {database_id, store_op::CreateIndex{*def, nullptr, nullptr}});
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
      std::vector<std::string> columns;
      for (const auto& logical : unique.GetLogicalIndexes(table->columns)) {
        columns.emplace_back(
          table->columns.GetColumn(logical).Name().GetIdentifierName());
      }
      if (columns.empty()) {
        continue;
      }
      auto op = store_op::AddUnique{table_id, constraint->constraint_name,
                                    std::move(columns)};
      if (unique.IsPrimaryKey()) {
        ops.push_back(
          {database_id, store_op::AddPrimaryKey{op.table_id, op.constraint,
                                                std::move(op.columns)}});
        continue;
      }
      ops.push_back({database_id, std::move(op)});
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
