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

#include "catalog/catalog.h"

#include <absl/algorithm/container.h>
#include <absl/strings/match.h>

#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/catalog/default/default_schemas.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_foreign_server_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_subscription_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/operator/logical_alter.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/database_size.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <utility>
#include <vector>

#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/search_table.h"
#include "catalog/entry/system_table.h"
#include "catalog/foreign_database.h"
#include "catalog/record.h"
#include "catalog/transaction.h"
#include "connector/duckdb_physical_search_delete.h"
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_truncate.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/inverted_index_bind.h"
#include "connector/view_index_bind.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/oids.h"
#include "query/config.h"
#include "search/store.h"

namespace sdb::catalog {
namespace {

[[noreturn]] void RefuseTable(const duckdb::CatalogEntry& table) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
    ERR_MSG("\"", table.name.GetIdentifierName(), "\" is not a search table"));
}

const SearchTableEntry& SearchTable(const duckdb::TableCatalogEntry& table) {
  if (!IsSearchTable(table)) {
    RefuseTable(table);
  }
  return table.Cast<SearchTableEntry>();
}

bool IsReservedSchemaName(const duckdb::Identifier& name) {
  return !duckdb::DefaultSchemaGenerator::IsDefaultSchema(name) &&
         name.GetIdentifierName().starts_with("pg_");
}

[[noreturn]] void ThrowReservedSchemaName(const duckdb::Identifier& name) {
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_RESERVED_NAME),
    ERR_MSG("unacceptable schema name \"", name.GetIdentifierName(), "\""),
    ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
}

std::string_view AlterActionName(duckdb::AlterTableType type) {
  switch (type) {
    case duckdb::AlterTableType::ADD_COLUMN:
      return "ADD COLUMN";
    case duckdb::AlterTableType::REMOVE_COLUMN:
      return "DROP COLUMN";
    case duckdb::AlterTableType::ALTER_COLUMN_TYPE:
      return "ALTER COLUMN TYPE";
    case duckdb::AlterTableType::SET_DEFAULT:
      return "ALTER COLUMN SET DEFAULT";
    case duckdb::AlterTableType::SET_NOT_NULL:
      return "SET NOT NULL";
    case duckdb::AlterTableType::DROP_NOT_NULL:
      return "DROP NOT NULL";
    case duckdb::AlterTableType::ADD_CONSTRAINT:
    case duckdb::AlterTableType::FOREIGN_KEY_CONSTRAINT:
      return "ADD CONSTRAINT";
    case duckdb::AlterTableType::DROP_CONSTRAINT:
      return "DROP CONSTRAINT";
    default:
      return {};
  }
}

void RefuseViewAlter(const duckdb::AlterTableInfo& info,
                     std::string_view name) {
  switch (info.alter_table_type) {
    case duckdb::AlterTableType::RENAME_TABLE:
      return;
    case duckdb::AlterTableType::RENAME_COLUMN:
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("cannot rename columns of a non-table relation"));
    case duckdb::AlterTableType::RENAME_CONSTRAINT:
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("constraint \"",
                info.Cast<duckdb::RenameConstraintInfo>().old_name,
                "\" for table \"", name, "\" does not exist"));
    default:
      break;
  }
  const auto action = AlterActionName(info.alter_table_type);
  if (action.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", name, "\" is not a table"),
                    ERR_DETAIL("This operation is not supported for views."));
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                  ERR_MSG("ALTER action ", action,
                          " cannot be performed on relation \"", name, "\""),
                  ERR_DETAIL("This operation is not supported for views."));
}

}  // namespace

std::string SereneDBCatalog::GetDBPath() {
  return _directory ? _directory->Path().string() : std::string{};
}

void SereneDBCatalog::LogSequenceValue(const duckdb::SequenceValue& value) {
  if (auto* log = Transactions::Get().GetLog()) {
    log->Append(SequenceRecord(*this, value));
  }
}

void SereneDBCatalog::Initialize(bool) {
  auto data = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  duckdb::CreateSchemaInfo info;
  info.SetQualifiedName(
    duckdb::QualifiedName({*GetDefaultSchema()}, duckdb::Identifier()));
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  info.permissions = pg::SchemaPermissions();
  info.oid = GetAttached().IsTemporary()
               ? duckdb::DatabaseManager::Get(GetDatabase()).NextOid()
               : pg::kPgPublicSchema;
  BaseCatalog::CreateSchema(data, info);
  MountSystemSchemas(*this);
}

void SereneDBCatalog::FinalizeLoad(
  duckdb::optional_ptr<duckdb::ClientContext> context) {
  if (_foreign) {
    SDB_ASSERT(context);
    ReplayForeignDatabase(*context, *this, *_foreign);
  }
}

void SereneDBCatalog::OnDetach(duckdb::ClientContext& context) {
  _detached.store(true, std::memory_order_release);
  if (!_foreign) {
    std::vector<duckdb::Identifier> servers;
    _servers.ScanCommitted(
      [&](duckdb::CatalogEntry& entry) { servers.push_back(entry.name); });
    for (const auto& server : servers) {
      duckdb::DatabaseManager::Get(context).DetachDatabase(
        context, server, duckdb::OnEntryNotFound::RETURN_NULL);
    }
  }
  BaseCatalog::OnDetach(context);
}

EntrySet& SereneDBCatalog::Set(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
      return _servers;
    case duckdb::CatalogType::SUBSCRIPTION_ENTRY:
      return _subscriptions;
    default:
      return BaseCatalog::Set(type);
  }
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateForeignServer(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateForeignServerInfo& info) {
  auto result = AddTo(_servers, transaction,
                      duckdb::make_uniq<ForeignServerCatalogEntry>(*this, info),
                      info.on_conflict);
  if (result) {
    result->Cast<ForeignServerCatalogEntry>().Attach(transaction.GetContext());
  }
  return result;
}

void SereneDBCatalog::DropForeignServer(duckdb::CatalogTransaction transaction,
                                        duckdb::DropInfo& info) {
  DropFrom(_servers, transaction, duckdb::CatalogType::FOREIGN_SERVER_ENTRY,
           info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSubscription(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateSubscriptionInfo& info) {
  return AddTo(_subscriptions, transaction,
               duckdb::make_uniq<SubscriptionCatalogEntry>(*this, info),
               info.on_conflict);
}

void SereneDBCatalog::DropSubscription(duckdb::CatalogTransaction transaction,
                                       duckdb::DropInfo& info) {
  DropFrom(_subscriptions, transaction, duckdb::CatalogType::SUBSCRIPTION_ENTRY,
           info);
}

void SereneDBCatalog::ReplayCreate(
  duckdb::ClientContext& context, duckdb::unique_ptr<duckdb::CreateInfo> info) {
  switch (info->type) {
    case duckdb::CatalogType::FOREIGN_SERVER_ENTRY:
      AddTo(_servers, GetCatalogTransaction(context),
            duckdb::make_uniq<ForeignServerCatalogEntry>(
              *this, info->Cast<duckdb::CreateForeignServerInfo>()),
            info->on_conflict);
      return;
    case duckdb::CatalogType::SUBSCRIPTION_ENTRY:
      CreateSubscription(GetCatalogTransaction(context),
                         info->Cast<duckdb::CreateSubscriptionInfo>());
      return;
    default:
      BaseCatalog::ReplayCreate(context, std::move(info));
  }
}

void SereneDBCatalog::ReplayDrop(duckdb::ClientContext& context,
                                 duckdb::CatalogEntry& entry) {
  const auto type = entry.type;
  if (type != duckdb::CatalogType::FOREIGN_SERVER_ENTRY &&
      type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    BaseCatalog::ReplayDrop(context, entry);
    return;
  }
  const auto transaction = GetCatalogTransaction(context);
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  Set(type).Drop(transaction, entry.name);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  if (!info.internal && IsReservedSchemaName(info.SchemaName())) {
    ThrowReservedSchemaName(info.SchemaName());
  }
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  return BaseCatalog::CreateSchema(transaction, info);
}

void SereneDBCatalog::Alter(duckdb::CatalogTransaction transaction,
                            duckdb::AlterInfo& info) {
  if (const auto new_name = info.GetNewName();
      new_name && info.GetCatalogType() == duckdb::CatalogType::SCHEMA_ENTRY &&
      IsReservedSchemaName(*new_name)) {
    ThrowReservedSchemaName(*new_name);
  }
  if (const auto type = info.GetCatalogType();
      type == duckdb::CatalogType::FOREIGN_SERVER_ENTRY ||
      type == duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    DeclareModified(transaction, *this,
                    duckdb::DatabaseModificationType::ALTER_TABLE);
    const auto& name = info.GetQualifiedName().Name();
    auto& set = Set(type);
    auto entry = set.Get(transaction, name);
    if (!entry) {
      if (info.if_not_found == duckdb::OnEntryNotFound::RETURN_NULL) {
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG(type == duckdb::CatalogType::SUBSCRIPTION_ENTRY
                                ? "subscription \""
                                : "server \"",
                              name.GetIdentifierName(), "\" does not exist"));
    }
    catalog::AlterEntry(transaction, set, *entry, info);
    return;
  }
  if (transaction.HasContext() &&
      (info.type == duckdb::AlterType::ALTER_TABLE ||
       info.type == duckdb::AlterType::ALTER_INDEX)) {
    RefuseUnsupportedAlter(transaction.GetContext(), info);
  }
  BaseCatalog::Alter(transaction, info);
}

void SereneDBCatalog::RefuseUnsupportedAlter(duckdb::ClientContext& context,
                                             duckdb::AlterInfo& info) {
  duckdb::CatalogEntryRetriever retriever{context};
  const duckdb::EntryLookupInfo lookup_info{info.GetCatalogType(),
                                            info.GetQualifiedName()};
  const auto lookup =
    LookupEntry(retriever, lookup_info, duckdb::OnEntryNotFound::RETURN_NULL);
  if (!lookup.Found()) {
    return;
  }
  const auto& name = lookup.entry->name.GetIdentifierName();
  if (info.type == duckdb::AlterType::ALTER_INDEX) {
    if (!info.GetNewName() &&
        !dynamic_cast<const InvertedIndexEntry*>(lookup.entry.get())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", name, "\" is not an inverted index"));
    }
    return;
  }
  if (lookup.entry->type == duckdb::CatalogType::VIEW_ENTRY) {
    RefuseViewAlter(info.Cast<duckdb::AlterTableInfo>(), name);
  }
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBCatalog::MakeTable(
  duckdb::CatalogTransaction transaction, Schema& schema,
  duckdb::BoundCreateTableInfo& info) {
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  auto entry =
    duckdb::make_uniq<SearchTableEntry>(*this, schema, info, transaction);
  if (info.Base().oid == 0) {
    entry->Storage()->StartTasks();
  }
  return entry;
}

duckdb::unique_ptr<duckdb::CatalogEntry> SereneDBCatalog::MakeIndex(
  duckdb::CatalogTransaction transaction, Schema& schema,
  duckdb::CreateIndexInfo& info, duckdb::CatalogEntry& relation) {
  if (info.index_type != kInvertedIndexTypeName) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("access method \"", info.index_type,
                            "\" is not supported: only inverted indexes are"));
  }
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table;
  if (relation.type == duckdb::CatalogType::TABLE_ENTRY) {
    table = &relation.Cast<duckdb::TableCatalogEntry>();
  }
  auto entry =
    duckdb::make_uniq<InvertedIndexEntry>(*this, schema, info, table);
  if (info.oid == 0) {
    return entry;
  }
  if (const auto& store = entry->SearchStore()) {
    store->MergeIndexConfig(entry->oid, entry->Config());
  } else {
    entry->AdoptStorage(std::make_shared<search::Store>(
      _directory, InMemory(), GetAttached().IsReadOnly(), GetOid(), entry->oid,
      ResolveSettings(entry->options), entry->Config()->top_k_scorer, false));
  }
  return entry;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  SDB_ASSERT(plan);
  auto& insert = planner.Make<connector::SereneDBSearchInsert>(
    SearchTable(op.table), std::move(op.bound_constraints), op.types,
    op.estimated_cardinality, op.return_chunk);
  insert.children.emplace_back(*plan);
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  const auto& entry = SearchTable(op.table);
  if (op.is_truncate) {
    return planner.Make<connector::SereneDBSearchTruncate>(
      entry, op.estimated_cardinality);
  }
  auto& del = planner.Make<connector::SereneDBSearchDelete>(
    entry, std::move(op.expressions), op.types, op.estimated_cardinality,
    op.return_chunk, std::move(op.return_columns), op.returning_virtual_columns,
    op.row_id_handling);
  del.children.emplace_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  auto& insert = planner.Make<connector::SereneDBSearchInsert>(
    std::move(op.info), op.estimated_cardinality);
  insert.children.emplace_back(plan);
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext&, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  auto& update = planner.Make<connector::SereneDBSearchUpdate>(
    SearchTable(op.table), op.columns, std::move(op.expressions),
    std::move(op.bound_constraints), op.types, op.estimated_cardinality,
    op.return_chunk, op.row_id_handling);
  update.children.emplace_back(plan);
  return update;
}

void SereneDBCatalog::BindIndexDefinition(duckdb::Binder& binder,
                                          duckdb::CreateStatement& stmt,
                                          duckdb::CatalogEntry& table) {
  auto& info = stmt.info->Cast<duckdb::CreateIndexInfo>();
  if (info.index_type == "ART" ||
      absl::EqualsIgnoreCase(info.index_type, "btree")) {
    info.index_type = kInvertedIndexTypeName;
  }
  if (info.on_conflict == duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
    if (const auto holder =
          table.ParentSchema(binder.context)
            .Cast<Schema>()
            .NamespaceEntry(GetCatalogTransaction(binder.context),
                            duckdb::CatalogType::INDEX_ENTRY,
                            info.GetIndexName())) {
      ThrowEntryExists(holder->type, info.GetIndexName());
    }
  }
  const bool inverted = info.index_type == kInvertedIndexTypeName;
  const auto names = connector::SortedOptionNames(info.options);
  for (const auto name : names) {
    if (!inverted || !IsKnownInvertedIndexOption(name)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized parameter \"", name, "\""));
    }
  }
  const bool search_table = IsSearchTable(table);
  if (search_table) {
    for (const auto name : names) {
      RequireSearchTableIndexOption(name);
    }
  }
  for (auto* expressions : {&info.expressions, &info.parsed_expressions}) {
    for (auto& expression : *expressions) {
      binder.QualifyUserFunctions(table.ParentCatalog(), *expression);
    }
  }
  if (info.where_clause) {
    binder.QualifyUserFunctions(table.ParentCatalog(), *info.where_clause);
  }
  if (info.where_clause) {
    duckdb::IndexBinder where_binder(binder, binder.context);
    auto where_copy = info.where_clause->Copy();
    auto where = where_binder.Bind(where_copy);
    if (where->IsFoldable() && where->IsConsistent()) {
      const auto value = duckdb::ExpressionExecutor::EvaluateScalar(
        binder.context,
        *duckdb::BoundCastExpression::AddCastToType(
          binder.context, std::move(where), duckdb::LogicalType::BOOLEAN));
      if (value.IsNull()) {
        info.where_clause = duckdb::ConstantExpression::Null();
      } else if (value.GetValue<bool>()) {
        info.where_clause = nullptr;
      } else {
        info.where_clause = duckdb::ConstantExpression::Boolean(false);
      }
    } else if (const auto& type = where->GetReturnType();
               type != duckdb::LogicalType::BOOLEAN &&
               type.id() != duckdb::LogicalTypeId::SQLNULL) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_DATATYPE_MISMATCH),
        ERR_MSG("argument of WHERE must be type boolean, not type ",
                type.ToString()));
    }
  }
  if (inverted) {
    BindInvertedIndexOptions(binder.context, info.options,
                             table.type == duckdb::CatalogType::VIEW_ENTRY,
                             search_table);
  }
  for (size_t i = 0; i < info.column_opclasses.size(); ++i) {
    if (info.column_opclasses[i].empty() || info.column_opclass_options[i]) {
      continue;
    }
    auto entry = connector::ResolveOpclassTokenizer(
      binder.context, table.ParentSchema(binder.context),
      info.column_opclasses[i]);
    if (entry && &entry->ParentCatalog() == this) {
      info.dependencies.AddDependency(*entry);
    }
  }
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::TableCatalogEntry& table,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  if (table.internal) {
    RefuseSystemCatalog(table);
  }
  if (!IsSearchTable(table)) {
    RefuseTable(table);
  }
  const bool plain = stmt.info->Cast<duckdb::CreateIndexInfo>().index_type !=
                     kInvertedIndexTypeName;
  BindIndexDefinition(binder, stmt, table);
  return connector::BindCreateIndexOnSearchTable(
    binder, stmt, table.Cast<SearchTableEntry>(), std::move(plan), plain);
}

duckdb::unique_ptr<duckdb::LogicalOperator>
SereneDBCatalog::BindCreateViewIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::ViewCatalogEntry& view,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  if (view.internal) {
    RefuseSystemCatalog(view);
  }
  if (stmt.info->Cast<duckdb::CreateIndexInfo>().constraint_type ==
      duckdb::IndexConstraintType::UNIQUE) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("unique indexes on views are not supported"));
  }
  BindIndexDefinition(binder, stmt, view);
  return connector::BindCreateIndexOnView(binder, stmt, view, std::move(plan));
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindAlterAddIndex(
  duckdb::Binder&, duckdb::TableCatalogEntry&,
  duckdb::unique_ptr<duckdb::LogicalOperator>,
  duckdb::unique_ptr<duckdb::CreateIndexInfo>,
  duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) {
  return duckdb::make_uniq<duckdb::LogicalAlter>(
    duckdb::unique_ptr<duckdb::AlterInfo>{std::move(alter_info)});
}

duckdb::ErrorData SereneDBCatalog::SupportsCreateTable(
  duckdb::BoundCreateTableInfo& info) {
  for (const auto name : connector::SortedOptionNames(info.Base().options)) {
    if (!absl::c_contains(kSearchTableOptions, name)) {
      return duckdb::ErrorData{duckdb::BinderException(
        "unrecognized parameter \"%s\"", std::string{name})};
    }
  }
  return {};
}

std::shared_ptr<const CatalogSnapshot> CatalogSnapshots::Get(
  duckdb::ClientContext& context, EntrySet& set) {
  const auto& session = SessionOf(context, _catalog);
  const auto version = session.Snapshot();
  const auto cache = session.Changes().empty();
  const auto collect = [&] {
    auto snapshot = std::make_shared<CatalogSnapshot>();
    set.Scan(_catalog.GetCatalogTransaction(context),
             [&](duckdb::CatalogEntry& entry) {
               snapshot->entries.emplace_back(&entry);
             });
    return snapshot;
  };
  if (!cache) {
    return collect();
  }
  {
    absl::ReaderMutexLock lock{&_mutex};
    if (_version == version) {
      if (const auto it = _snapshots.find(&set); it != _snapshots.end()) {
        return it->second;
      }
    }
  }
  auto snapshot = collect();
  absl::MutexLock lock{&_mutex};
  if (_version > version) {
    return snapshot;
  }
  if (_version < version) {
    _snapshots.clear();
    _version = version;
  }
  return _snapshots.try_emplace(&set, std::move(snapshot)).first->second;
}

duckdb::vector<duckdb::shared_ptr<duckdb::AttachedDatabase>> SereneDBDatabases(
  duckdb::DatabaseManager& manager) {
  auto databases = manager.GetDatabases();
  std::erase_if(databases, [](const auto& db) {
    return !IsSereneDB(db->GetCatalog()) || db->IsReadOnly();
  });
  return databases;
}

void ForEachSearchStorage(
  SereneDBCatalog& catalog, absl::FunctionRef<void(SearchTableEntry&)> table,
  std::optional<absl::FunctionRef<void(InvertedIndexEntry&)>> index) {
  catalog.ScanSchemas([&](Schema& schema) {
    schema.Scan(duckdb::CatalogType::TABLE_ENTRY,
                [&](duckdb::CatalogEntry& entry) {
                  if (IsSearchTable(entry)) {
                    table(entry.Cast<SearchTableEntry>());
                  }
                });
    if (!index) {
      return;
    }
    schema.Scan(
      duckdb::CatalogType::INDEX_ENTRY, [&](duckdb::CatalogEntry& entry) {
        if (auto* inverted = dynamic_cast<InvertedIndexEntry*>(&entry);
            inverted && inverted->Storage()) {
          (*index)(*inverted);
        }
      });
  });
}

duckdb::DatabaseSize SereneDBCatalog::GetDatabaseSize(
  duckdb::ClientContext& context) {
  duckdb::DatabaseSize size;
  size.block_size =
    duckdb::Settings::Get<duckdb::DefaultBlockSizeSetting>(context);
  ReadDatabase(GetName().GetIdentifierName(),
               [&](const DatabaseCatalogEntry& database) {
                 const auto& options = database.Options();
                 if (const auto it = options.find(kBlockSizeSetting);
                     it != options.end()) {
                   size.block_size = it->second.GetValue<duckdb::idx_t>();
                 }
               });
  ForEachSearchStorage(
    *this,
    [&](SearchTableEntry& table) {
      size.bytes += table.Storage()->GetStats().indexSize;
    },
    [&](InvertedIndexEntry& index) {
      size.bytes += index.Storage()->GetStats().indexSize;
    });
  return size;
}

std::vector<std::shared_ptr<search::Store>> SearchTableStorages(
  SereneDBCatalog& catalog) {
  std::vector<std::shared_ptr<search::Store>> tables;
  ForEachSearchStorage(
    catalog,
    [&](SearchTableEntry& table) { tables.push_back(table.Storage()); },
    std::nullopt);
  return tables;
}

duckdb::optional_ptr<SereneDBCatalog> DefaultSereneDBCatalog(
  duckdb::ClientContext& context) {
  auto catalog = duckdb::Catalog::GetCatalogEntry(
    context, duckdb::DatabaseManager::GetDefaultDatabase(context));
  if (!catalog || !IsSereneDB(*catalog)) {
    return nullptr;
  }
  return catalog->Cast<SereneDBCatalog>();
}

bool IsSereneDB(duckdb::Catalog& catalog) {
  return catalog.GetCatalogType() == SereneDBCatalog::kStorageType;
}

}  // namespace sdb::catalog
