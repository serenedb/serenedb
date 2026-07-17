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

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/time/time.h>
#include <fast_float/fast_float.h>

#include <chrono>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/duck_table_entry.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/parser/column_definition.hpp>
#include <duckdb/parser/constraints/check_constraint.hpp>
#include <duckdb/parser/constraints/foreign_key_constraint.hpp>
#include <duckdb/parser/constraints/not_null_constraint.hpp>
#include <duckdb/parser/constraints/unique_constraint.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_table_info.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <exception>
#include <filesystem>
#include <utility>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/database.h"
#include "catalog/index.h"
#include "catalog/schema.h"
#include "catalog/table.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

constexpr std::string_view kStoreAlias = kStoreDatabaseName;
constexpr std::string_view kStoreFile = "data.db";

void ExecOrFatal(duckdb::Connection& conn, const std::string& sql) {
  auto res = conn.Query(sql);
  if (res->HasError()) {
    SDB_FATAL(STARTUP, "data store: '", sql, "' failed: ", res->GetError());
  }
}

std::string QuotedIdent(const std::string& name) {
  return duckdb::KeywordHelper::WriteQuoted(name, '"');
}

template<typename Fn>
absl::Status RunInTransaction(duckdb::Connection& conn, Fn&& fn) {
  try {
    conn.BeginTransaction();
  } catch (const std::exception& e) {
    return absl::InternalError(e.what());
  }
  auto r = [&]() noexcept { return fn(); }();
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

DataStore::DataStore() {
  SDB_ASSERT(gInstance == nullptr);
  gInstance = this;
}

DataStore::~DataStore() { gInstance = nullptr; }

void DataStore::Initialize(std::string_view database_directory) {
  namespace fs = std::filesystem;
  _database_directory = std::string{database_directory};
  const auto dir = fs::path{database_directory} / StaticStrings::kDataStoreRoot;
  std::error_code ec;
  fs::create_directories(dir, ec);
  if (ec) {
    SDB_FATAL(STARTUP, "data store: cannot create directory '", dir.string(),
              "': ", ec.message());
  }
  const auto file = (dir / kStoreFile).string();

  _conn = DuckDBEngine::Instance().CreateConnection();
  // The attach replays the data WAL; the catalog is already initialized, so
  // this is the "catalog first, then data" boot point. HIDDEN keeps the
  // store out of catalog enumeration; qualified lookups still resolve.
  ExecOrFatal(*_conn, absl::StrCat("ATTACH '",
                                   absl::StrReplaceAll(file, {{"'", "''"}}),
                                   "' AS \"", kStoreAlias,
                                   "\" (HIDDEN true)"));

  ResolvePendingAlter();
  Reconcile();
  _ready.store(true, std::memory_order_release);
}

void DataStore::Shutdown() {
  _ready.store(false, std::memory_order_release);
  _conn.reset();
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto res = conn->Query(absl::StrCat("DETACH \"", kStoreAlias, "\""));
  if (res->HasError()) {
    SDB_WARN(STARTUP, "data store: detach failed: ", res->GetError());
  }
}

absl::Status DataStore::ApplyStoreOps(
  std::span<const CatalogStore::Entry> entries) {
  SDB_ASSERT(_conn, "store DDL before the data store is attached");
  return RunInTransaction(*_conn, [&]() -> absl::Status {
    for (const auto& entry : entries) {
      if (auto r = ExecuteEntry(entry); !r.ok()) {
        return r;
      }
    }
    return absl::OkStatus();
  });
}

absl::Status DataStore::ExecuteEntry(const CatalogStore::Entry& entry) {
  using Op = CatalogStore::Op;
  switch (entry.op) {
    case Op::CreateStoreTable:
      return ExecuteCreateStoreTable(entry.store_table);
    case Op::DropStoreTable: {
      auto res =
        _conn->Query(absl::StrCat("DROP TABLE IF EXISTS \"", kStoreAlias,
                                  "\".main.", QuotedIdent(entry.store_table.name)));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::DropStoreCheck: {
      try {
        auto& context = *_conn->context;
        duckdb::AlterEntryData data{
          duckdb::QualifiedName{duckdb::Identifier{kStoreAlias},
                                duckdb::Identifier{"main"},
                                duckdb::Identifier{entry.store_table.name}},
          duckdb::OnEntryNotFound::RETURN_NULL};
        duckdb::DropConstraintInfo info{std::move(data), entry.name_a, true,
                                        false};
        auto& catalog =
          duckdb::Catalog::GetCatalog(context, duckdb::Identifier{kStoreAlias});
        catalog.Alter(context, info);
      } catch (const std::exception& e) {
        return absl::InternalError(e.what());
      }
      break;
    }
    case Op::DropStoreNotNull: {
      auto res = _conn->Query(
        absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                     QuotedIdent(entry.store_table.name), " ALTER COLUMN ",
                     QuotedIdent(entry.name_a), " DROP NOT NULL"));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::AddStoreNotNull: {
      auto res = _conn->Query(
        absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                     QuotedIdent(entry.store_table.name), " ALTER COLUMN ",
                     QuotedIdent(entry.name_a), " SET NOT NULL"));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::AddStoreCheck: {
      auto res =
        _conn->Query(absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                                  QuotedIdent(entry.store_table.name),
                                  " ADD CHECK (", entry.name_a, ")"));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::AddStorePrimaryKey: {
      std::string cols;
      for (const auto& c : entry.store_table.pk_columns) {
        if (!cols.empty()) {
          cols += ", ";
        }
        cols += QuotedIdent(c);
      }
      auto res =
        _conn->Query(absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                                  QuotedIdent(entry.store_table.name),
                                  " ADD PRIMARY KEY (", cols, ")"));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::AddStoreUnique: {
      std::string cols;
      for (const auto& c : entry.store_table.unique_constraints.front()) {
        if (!cols.empty()) {
          cols += ", ";
        }
        cols += QuotedIdent(c);
      }
      auto res = _conn->Query(absl::StrCat(
        "ALTER TABLE \"", kStoreAlias, "\".main.",
        QuotedIdent(entry.store_table.name), " ADD UNIQUE (", cols, ")"));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::CreateStoreIndex: {
      const auto& def = entry.store_index;
      std::string sql;
      if (def.kind == StoreIndexDef::Kind::Inverted) {
        std::string cols;
        for (const auto& name : def.columns) {
          if (!cols.empty()) {
            cols += ", ";
          }
          cols += QuotedIdent(name);
        }
        sql = absl::StrCat(
          "CREATE INDEX ", QuotedIdent(StoreIndexName(def.index_id)), " ON \"",
          kStoreAlias, "\".main.", QuotedIdent(def.table), " USING inverted(",
          cols, ") WITH (sdb_table_id=", def.table_id.id(),
          ", sdb_index_id=", def.index_id.id(), ")");
      } else {
        // def.keys already holds per-key SQL in order (quoted column
        // identifiers or parenthesized expressions); join verbatim.
        std::string cols;
        for (const auto& key : def.keys) {
          if (!cols.empty()) {
            cols += ", ";
          }
          cols += key;
        }
        sql = absl::StrCat("CREATE ", def.unique ? "UNIQUE " : "", "INDEX ",
                           QuotedIdent(StoreIndexName(def.index_id)), " ON \"",
                           kStoreAlias, "\".main.", QuotedIdent(def.table),
                           " (", cols, ")");
      }
      auto res = _conn->Query(sql);
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::DropStoreIndex: {
      auto res = _conn->Query(
        absl::StrCat("DROP INDEX IF EXISTS \"", kStoreAlias, "\".main.",
                     QuotedIdent(StoreIndexName(entry.store_index.index_id))));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::DropStoreForeignKey: {
      try {
        auto& context = *_conn->context;
        duckdb::AlterEntryData data{
          duckdb::QualifiedName{duckdb::Identifier{kStoreAlias},
                                duckdb::Identifier{"main"},
                                duckdb::Identifier{entry.store_table.name}},
          duckdb::OnEntryNotFound::RETURN_NULL};
        duckdb::AlterForeignKeyInfo info{
          std::move(data),
          duckdb::Identifier{entry.name_a},
          {},
          {},
          {},
          {},
          duckdb::AlterForeignKeyType::AFT_DELETE};
        auto& catalog =
          duckdb::Catalog::GetCatalog(context, duckdb::Identifier{kStoreAlias});
        catalog.Alter(context, info);
      } catch (const std::exception& e) {
        return absl::InternalError(e.what());
      }
      break;
    }
    case Op::DropStoreColumn: {
      auto res = _conn->Query(
        absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                     QuotedIdent(entry.store_table.name), " DROP COLUMN ",
                     QuotedIdent(entry.name_a)));
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::AddStoreColumn: {
      std::string base =
        absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                     QuotedIdent(entry.store_table.name), " ADD COLUMN ",
                     QuotedIdent(entry.name_a), " ", entry.name_b);
      std::string sql = base;
      if (!entry.def.empty()) {
        absl::StrAppend(&sql, " DEFAULT ", entry.def);
      }
      auto res = _conn->Query(sql);
      if (res->HasError() && !entry.def.empty()) {
        // The DEFAULT may call a function the store connection can't
        // resolve (sequences, user macros bind facade-side). Add the
        // column without it; existing rows get NULL and the facade fills
        // the default for future inserts.
        SDB_WARN(GENERAL, "store table \"", entry.store_table.name,
                 "\": ADD COLUMN \"", entry.name_a,
                 "\" DEFAULT not mirrored: ", res->GetError());
        res = _conn->Query(base);
      }
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    case Op::ChangeStoreColumnType: {
      std::string sql =
        absl::StrCat("ALTER TABLE \"", kStoreAlias, "\".main.",
                     QuotedIdent(entry.store_table.name), " ALTER COLUMN ",
                     QuotedIdent(entry.name_a), " TYPE ", entry.name_b);
      if (!entry.def.empty()) {
        absl::StrAppend(&sql, " USING ", entry.def);
      }
      auto res = _conn->Query(sql);
      if (res->HasError()) {
        return absl::InternalError(res->GetError());
      }
      break;
    }
    default:
      SDB_ASSERT(false, "catalog record reached the data executor");
      break;
  }
  return absl::OkStatus();
}

absl::Status DataStore::ExecuteCreateStoreTable(const StoreTableDef& def) {
  auto r = ExecuteCreateStoreTableImpl(def, /*with_checks=*/true);
  if (!r.ok() && !def.checks.empty()) {
    // CHECK expressions may reference facade-only types or functions the store
    // catalog cannot bind. The store table omits them; the facade-bound CHECK
    // is carried into every write plan instead (RetargetStoreConstraints), so
    // it is still enforced on INSERT/UPDATE/upsert.
    auto retry = ExecuteCreateStoreTableImpl(def, /*with_checks=*/false);
    if (retry.ok()) {
      SDB_TRACE(GENERAL, "store table \"", def.name,
                "\": CHECK constraints kept facade-side: ", r.message());
      return retry;
    }
  }
  return r;
}

absl::Status DataStore::ExecuteCreateStoreTableImpl(const StoreTableDef& def,
                                                    bool with_checks) try {
  auto info = duckdb::make_uniq<duckdb::CreateTableInfo>(duckdb::QualifiedName{
    duckdb::Identifier{kStoreAlias}, duckdb::Identifier{"main"},
    duckdb::Identifier{def.name}});
  for (const auto& col : def.columns) {
    info->columns.AddColumn(
      duckdb::ColumnDefinition{duckdb::Identifier{col.name}, col.type});
  }
  for (auto idx : def.not_null) {
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::NotNullConstraint>(duckdb::LogicalIndex{idx}));
  }
  if (!def.pk_columns.empty()) {
    duckdb::vector<duckdb::Identifier> pk;
    pk.reserve(def.pk_columns.size());
    for (const auto& name : def.pk_columns) {
      pk.emplace_back(name);
    }
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::UniqueConstraint>(std::move(pk), true));
  }
  for (const auto& unique : def.unique_constraints) {
    duckdb::vector<duckdb::Identifier> names;
    names.reserve(unique.size());
    for (const auto& name : unique) {
      names.emplace_back(name);
    }
    info->constraints.push_back(
      duckdb::make_uniq<duckdb::UniqueConstraint>(std::move(names), false));
  }
  for (const auto& fk : def.foreign_keys) {
    duckdb::vector<duckdb::Identifier> fk_cols;
    duckdb::vector<duckdb::Identifier> pk_cols;
    for (const auto& name : fk.columns) {
      fk_cols.emplace_back(name);
    }
    for (const auto& name : fk.referenced_columns) {
      pk_cols.emplace_back(name);
    }
    duckdb::ForeignKeyInfo fk_info;
    fk_info.type = duckdb::ForeignKeyType::FK_TYPE_FOREIGN_KEY_TABLE;
    fk_info.table = duckdb::Identifier{fk.referenced_table};
    info->constraints.push_back(duckdb::make_uniq<duckdb::ForeignKeyConstraint>(
      std::move(pk_cols), std::move(fk_cols), std::move(fk_info)));
  }
  if (with_checks) {
    for (const auto& check : def.checks) {
      info->constraints.push_back(
        duckdb::make_uniq<duckdb::CheckConstraint>(check->Copy()));
    }
  }
  auto& context = *_conn->context;
  auto& catalog =
    duckdb::Catalog::GetCatalog(context, duckdb::Identifier{kStoreAlias});
  // Acquire the store's (shared) checkpoint lock before creating the table.
  // The direct catalog.CreateTable() call bypasses the statement-execution
  // path that normally registers the modification and takes this lock (which
  // serenedb's DROP/ALTER do go through, via _conn->Query). Without it, a
  // store table can be created concurrently with a store checkpoint; the new
  // table is then not in the checkpoint's snapshot, so MergeCheckpointDeltas
  // never merges its index's added_data delta -> the entry is stranded -> a
  // later delete fails "0 out of 1" -> "Failed to rollback transaction".
  duckdb::MetaTransaction::Get(context).ModifyDatabase(
    catalog.GetAttached(),
    duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY);
  SDB_WAIT_ON_FAILURE("pause_store_create_table");
  catalog.CreateTable(context, std::move(info));
  return absl::OkStatus();
} catch (const std::exception& e) {
  return absl::InternalError(e.what());
}

namespace {

duckdb::optional_ptr<duckdb::CatalogEntry> LookupStoreTable(
  duckdb::ClientContext& context, const std::string& name) {
  duckdb::EntryLookupInfo lookup{
    duckdb::CatalogType::TABLE_ENTRY,
    duckdb::QualifiedName{duckdb::Identifier{kStoreAlias},
                          duckdb::Identifier{"main"},
                          duckdb::Identifier{name}}};
  return duckdb::Catalog::GetEntry(context, lookup,
                                   duckdb::OnEntryNotFound::RETURN_NULL);
}

// Store shape of `table` matches its expected def exactly (columns by name
// and type). Used to decide a PendingAlter's direction.
bool StoreShapeMatches(duckdb::ClientContext& context,
                       const StoreTableDef& def) {
  auto entry = LookupStoreTable(context, def.name);
  if (!entry) {
    return false;
  }
  const auto& columns = entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
  if (columns.LogicalColumnCount() != def.columns.size()) {
    return false;
  }
  duckdb::idx_t i = 0;
  for (const auto& col : columns.Logical()) {
    if (col.Name().GetIdentifierName() != def.columns[i].name ||
        col.Type() != def.columns[i].type) {
      return false;
    }
    ++i;
  }
  return true;
}

}  // namespace

void DataStore::ResolvePendingAlter() {
  auto pending = GetCatalogStore().TakePendingAlter();
  if (!pending) {
    return;
  }
  // The flag's payload is the batch's final catalog records; the batch is
  // order-sensitive exactly because it retypes columns, so its table defs
  // decide the direction: store already matches them -> the data commit
  // happened, roll forward; otherwise the crash preceded it, roll back.
  bool matches = true;
  std::vector<std::pair<ObjectId, std::shared_ptr<Table>>> tables;
  auto snapshot = GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);
  _conn->BeginTransaction();
  for (const auto& record : *pending) {
    if (record.op != CatalogStore::Op::PutDefinition ||
        record.key.type != ObjectType::Table) {
      continue;
    }
    auto schema = snapshot->GetObject<Schema>(record.key.parent_id);
    auto table = catalog::DeserializeObject<Table>(
      record.def, {.id = record.key.id,
                   .database_id = schema ? schema->GetParentId() : ObjectId{},
                   .schema_id = record.key.parent_id});
    if (!table) {
      matches = false;
      break;
    }
    if (table->GetEngine() == TableEngine::Transactional &&
        !StoreShapeMatches(*_conn->context, MakeStoreTableDef(*table))) {
      matches = false;
      break;
    }
    tables.emplace_back(record.key.parent_id, std::move(table));
  }
  _conn->Rollback();

  if (!matches) {
    SDB_WARN(STARTUP,
             "pending alter: store does not match the interrupted batch, "
             "rolling it back");
    GetCatalogStore().AbortPendingAlter();
    return;
  }
  SDB_INFO(STARTUP, "pending alter: rolling the interrupted batch forward");
  for (auto& [schema_id, table] : tables) {
    GetCatalog().ReplaceRelationForRecovery(schema_id, table);
  }
  GetCatalogStore().CommitPendingAlter(std::move(*pending));
}

void DataStore::ValidateStoreTable(const StoreTableDef& def) {
#ifdef SDB_DEV
  try {
    _conn->BeginTransaction();
  } catch (const std::exception& e) {
    SDB_ASSERT(false, "store table validation: ", e.what());
    return;
  }
  try {
    auto entry = LookupStoreTable(*_conn->context, def.name);
    SDB_ASSERT(entry, "store table missing for ", def.name);
    if (entry) {
      const auto& columns =
        entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
      SDB_ASSERT(columns.LogicalColumnCount() == def.columns.size(),
                 "store table column count mismatch for ", def.name);
      duckdb::idx_t i = 0;
      for (const auto& col : columns.Logical()) {
        if (i >= def.columns.size()) {
          break;
        }
        SDB_ASSERT(col.Name().GetIdentifierName() == def.columns[i].name,
                   "store table column name mismatch for ", def.name, ": ",
                   col.Name().GetIdentifierName(), " vs ", def.columns[i].name);
        SDB_ASSERT(col.Type() == def.columns[i].type,
                   "store table column type mismatch for ", def.name, ".",
                   def.columns[i].name);
        ++i;
      }
    }
  } catch (const std::exception& e) {
    SDB_ASSERT(false, "store table validation: ", e.what());
  }
  try {
    _conn->Rollback();
  } catch (const std::exception&) {
  }
#endif
}

void DataStore::Reconcile() {
  auto begin = std::chrono::steady_clock::now();
  SweepOrphans();
  auto snapshot = GetCatalog().GetCatalogSnapshot();
  SDB_ASSERT(snapshot);
  for (const auto& db : snapshot->GetDatabases()) {
    for (const auto& schema : snapshot->GetSchemas(db->GetId())) {
      for (const auto& table :
           snapshot->GetTables(db->GetId(), schema->GetName())) {
        if (table->GetEngine() != TableEngine::Transactional ||
            table->Tombstoned()) {
          continue;
        }
        ReconcileTable(*table);
      }
    }
  }
  SweepSearchDirs();
  const auto duration =
    absl::FromChrono(std::chrono::steady_clock::now() - begin);
  SDB_INFO(STARTUP, "data store reconciled in ",
           absl::FormatDuration(duration));
}

void DataStore::SweepOrphans() {
  auto snapshot = GetCatalog().GetCatalogSnapshot();
  std::vector<std::string> drop_tables;
  std::vector<std::string> drop_indexes;
  _conn->BeginTransaction();
  try {
    auto& catalog = duckdb::Catalog::GetCatalog(
      *_conn->context, duckdb::Identifier{kStoreAlias});
    auto& schema =
      catalog.GetSchema(*_conn->context, duckdb::Identifier{"main"});
    schema.Scan(*_conn->context, duckdb::CatalogType::TABLE_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  const auto& name = entry.name.GetIdentifierName();
                  auto id = ParseStoreId('t', name);
                  if (!id) {
                    return;
                  }
                  if (!snapshot->GetObject<Table>(*id)) {
                    drop_tables.push_back(name);
                  }
                });
    schema.Scan(*_conn->context, duckdb::CatalogType::INDEX_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  const auto& name = entry.name.GetIdentifierName();
                  auto id = ParseStoreId('i', name);
                  if (!id) {
                    return;
                  }
                  if (!snapshot->GetObject<Index>(*id)) {
                    drop_indexes.push_back(name);
                  }
                });
  } catch (const std::exception& e) {
    _conn->Rollback();
    SDB_FATAL(STARTUP, "data store orphan scan failed: ", e.what());
  }
  _conn->Rollback();

  for (const auto& name : drop_indexes) {
    SDB_WARN(STARTUP, "data store: dropping orphaned index ", name);
    auto res = _conn->Query(absl::StrCat("DROP INDEX IF EXISTS \"", kStoreAlias,
                                         "\".main.", QuotedIdent(name)));
    if (res->HasError()) {
      SDB_WARN(STARTUP, "data store: dropping orphaned index ", name,
               " failed: ", res->GetError());
    }
  }
  for (const auto& name : drop_tables) {
    SDB_WARN(STARTUP, "data store: dropping orphaned table ", name);
    auto res = _conn->Query(absl::StrCat("DROP TABLE IF EXISTS \"", kStoreAlias,
                                         "\".main.", QuotedIdent(name)));
    if (res->HasError()) {
      SDB_WARN(STARTUP, "data store: dropping orphaned table ", name,
               " failed: ", res->GetError());
    }
  }
}

void DataStore::ReconcileTable(const Table& table) {
  const auto def = MakeStoreTableDef(table);

  struct ActualColumn {
    std::string name;
    duckdb::LogicalType type;
  };
  std::vector<ActualColumn> actual;
  bool exists = false;
  _conn->BeginTransaction();
  if (auto entry = LookupStoreTable(*_conn->context, def.name)) {
    exists = true;
    const auto& columns = entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
    actual.reserve(columns.LogicalColumnCount());
    for (const auto& col : columns.Logical()) {
      actual.push_back({col.Name().GetIdentifierName(), col.Type()});
    }
  }
  _conn->Rollback();

  // Creates commit on the data DB before the catalog append, so a def whose
  // store table is missing cannot be produced by any crash window.
  SDB_VERIFY(exists, "store table ", def.name, " for catalog table ",
             table.GetName(), " is missing");

  auto expected_of = [&](std::string_view name) -> const StoreTableColumn* {
    for (const auto& col : def.columns) {
      if (col.name == name) {
        return &col;
      }
    }
    return nullptr;
  };

  for (const auto& col : actual) {
    const auto* expected = expected_of(col.name);
    if (!expected) {
      SDB_WARN(STARTUP, "data store: dropping orphaned column ", def.name, ".",
               col.name);
      auto res = _conn->Query(absl::StrCat("ALTER TABLE \"", kStoreAlias,
                                           "\".main.", QuotedIdent(def.name),
                                           " DROP COLUMN ",
                                           QuotedIdent(col.name)));
      if (res->HasError()) {
        SDB_WARN(STARTUP, "data store: dropping column ", def.name, ".",
                 col.name, " failed: ", res->GetError());
      }
      continue;
    }
    SDB_VERIFY(expected->type == col.type, "store column type mismatch for ",
               def.name, ".", col.name,
               " outside a pending alter: ", col.type.ToString(), " vs ",
               expected->type.ToString());
  }

  for (const auto& col : def.columns) {
    if (absl::c_any_of(actual,
                       [&](const auto& a) { return a.name == col.name; })) {
      continue;
    }
    SDB_WARN(STARTUP, "data store: re-adding missing column ", def.name, ".",
             col.name);
    // Backfill default rendering matches what the interrupted ALTER carried.
    std::string default_sql;
    if (auto id = ParseStoreId('c', col.name)) {
      if (const auto* facade = table.ColumnById(*id)) {
        if (facade->expr && facade->expr->HasExpr()) {
          default_sql = facade->expr->GetExpr().ToString();
        }
      }
    }
    CatalogStore::Entry add;
    add.op = CatalogStore::Op::AddStoreColumn;
    add.store_table.name = def.name;
    add.name_a = col.name;
    add.name_b = col.type.ToString();
    add.def = std::move(default_sql);
    if (auto r = ApplyStoreOps({&add, 1}); !r.ok()) {
      SDB_WARN(STARTUP, "data store: re-adding column ", def.name, ".",
               col.name, " failed: ", r.message());
    }
  }

  // Mirrored indexes: recreate missing ones (extra i<id> entries were swept
  // as orphans above).
  auto snapshot = GetCatalog().GetCatalogSnapshot();
  for (const auto& index : snapshot->GetIndexesByRelation(table.GetId())) {
    auto index_def = MakeStoreIndexDef(table, *index);
    if (!index_def) {
      continue;
    }
    bool index_exists = false;
    _conn->BeginTransaction();
    try {
      duckdb::EntryLookupInfo lookup{
        duckdb::CatalogType::INDEX_ENTRY,
        duckdb::QualifiedName{
          duckdb::Identifier{kStoreAlias}, duckdb::Identifier{"main"},
          duckdb::Identifier{StoreIndexName(index->GetId())}}};
      index_exists = duckdb::Catalog::GetEntry(
                       *_conn->context, lookup,
                       duckdb::OnEntryNotFound::RETURN_NULL) != nullptr;
    } catch (const std::exception&) {
    }
    _conn->Rollback();
    if (index_exists) {
      continue;
    }
    SDB_WARN(STARTUP, "data store: recreating missing index ",
             StoreIndexName(index->GetId()), " on ", def.name);
    CatalogStore::Entry create;
    create.op = CatalogStore::Op::CreateStoreIndex;
    create.store_index = std::move(*index_def);
    if (auto r = ApplyStoreOps({&create, 1}); !r.ok()) {
      SDB_WARN(STARTUP, "data store: recreating index ",
               StoreIndexName(index->GetId()), " failed: ", r.message());
    }
  }
}

void DataStore::SweepSearchDirs() {
  namespace fs = std::filesystem;
  auto snapshot = GetCatalog().GetCatalogSnapshot();
  const auto root =
    fs::path{_database_directory} / StaticStrings::kSearchRoot;
  std::error_code ec;
  if (!fs::is_directory(root, ec)) {
    return;
  }
  auto object_exists = [&](const fs::path& name) {
    uint64_t value = 0;
    const auto text = name.string();
    const auto* begin = text.data();
    const auto* end = begin + text.size();
    const auto [ptr, parse_ec] = fast_float::from_chars(begin, end, value);
    if (parse_ec != std::errc{} || ptr != end) {
      return true;  // not an object dir; leave it alone
    }
    return snapshot->GetObject(ObjectId{value}) != nullptr;
  };
  auto sweep = [&](this auto& self, const fs::path& dir, int depth) -> void {
    for (const auto& entry : fs::directory_iterator{dir, ec}) {
      if (!entry.is_directory()) {
        continue;
      }
      const auto name = entry.path().filename();
      if (name == "wal") {
        continue;
      }
      if (!object_exists(name)) {
        SDB_WARN(STARTUP, "data store: removing orphaned search dir ",
                 entry.path().string());
        std::error_code remove_ec;
        fs::remove_all(entry.path(), remove_ec);
        if (remove_ec) {
          SDB_WARN(STARTUP, "data store: removing ", entry.path().string(),
                   " failed: ", remove_ec.message());
        }
        continue;
      }
      if (depth < 3) {
        self(entry.path(), depth + 1);
      }
    }
  };
  sweep(root, 0);
}

DataStore& GetDataStore() { return DataStore::instance(); }

}  // namespace sdb::catalog
