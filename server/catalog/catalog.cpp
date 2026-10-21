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
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>

#include <algorithm>
#include <duckdb/catalog/catalog_entry_retriever.hpp>
#include <duckdb/catalog/default/default_schemas.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/exception/catalog_exception.hpp>
#include <duckdb/execution/expression_executor.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/expression/constant_expression.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_cast_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_create_table.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_merge_into.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/storage/storage_manager.hpp>
#include <duckdb/storage/write_ahead_log.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <filesystem>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <utility>
#include <vector>

#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
#include "catalog/entry/job.h"
#include "catalog/entry/role.h"
#include "catalog/entry/search_table.h"
#include "catalog/entry/system_table.h"
#include "catalog/entry/tokenizer.h"
#include "connector/duckdb_client_state.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_physical_search_delete.h"
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_truncate.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/inverted_store_index.h"
#include "connector/primary_key.h"
#include "connector/view_index_bind.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "pg/tsdictionary.h"
#include "scheduler/job_scheduler.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::catalog {
namespace {

constexpr uint64_t kPkSequenceCache = uint64_t{1} << 20;

}  // namespace

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type) {
  if (!transaction.context) {
    return;
  }
  duckdb::MetaTransaction::Get(transaction.GetContext())
    .ModifyDatabase(catalog.GetAttached(), type);
}

duckdb::optional_ptr<duckdb::DuckCatalog> DefaultDuckCatalog(
  duckdb::ClientContext& context) {
  auto catalog = duckdb::Catalog::GetCatalogEntry(
    context, duckdb::DatabaseManager::GetDefaultDatabase(context));
  if (!catalog || !catalog->IsDuckCatalog()) {
    return nullptr;
  }
  return catalog->Cast<duckdb::DuckCatalog>();
}

duckdb::optional_ptr<SereneDBCatalog> DefaultSereneDBCatalog(
  duckdb::ClientContext& context) {
  auto catalog = DefaultDuckCatalog(context);
  if (!catalog || catalog->GetCatalogType() != SereneDBCatalog::kStorageType) {
    return nullptr;
  }
  return catalog->Cast<SereneDBCatalog>();
}

duckdb::unique_ptr<duckdb::TableCatalogEntry> SereneDBCatalog::MakeTableEntry(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  duckdb::BoundCreateTableInfo& info) {
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  auto& options = info.Base().options;
  if (ReadStorageEngine(options) == TableEngine::Search) {
    auto entry =
      duckdb::make_uniq<SearchTableEntry>(*this, schema, info, transaction);
    if (info.Base().oid == 0) {
      if (!entry->PkSequenceName().empty()) {
        duckdb::CreateSequenceInfo sequence_info;
        sequence_info.SetQualification(GetName(), schema.name);
        sequence_info.SetSequenceName(entry->PkSequenceName());
        sequence_info.cache = kPkSequenceCache;
        sequence_info.permissions.owner = info.Base().permissions.owner;
        info.Base().dependencies.AddOwnedDependency(
          *schema.CreateSequence(transaction, sequence_info));
      }
      entry->Storage()->StartTasks();
    }
    return std::move(entry);
  }
  options.erase(kStorageOption);
  return duckdb::DuckCatalog::MakeTableEntry(transaction, schema, info);
}

duckdb::unique_ptr<duckdb::IndexCatalogEntry> SereneDBCatalog::MakeIndexEntry(
  duckdb::DuckSchemaEntry& schema, duckdb::CreateIndexInfo& info,
  duckdb::CatalogEntry& relation) {
  if (info.index_type != kInvertedIndexTypeName) {
    return duckdb::DuckCatalog::MakeIndexEntry(schema, info, relation);
  }
  duckdb::optional_ptr<duckdb::TableCatalogEntry> table;
  if (relation.type == duckdb::CatalogType::TABLE_ENTRY) {
    table = &relation.Cast<duckdb::TableCatalogEntry>();
  }
  auto entry =
    duckdb::make_uniq<InvertedIndexEntry>(*this, schema, info, table);
  if (info.oid == 0) {
    return std::move(entry);
  }
  if (const auto& store = entry->SearchStore()) {
    store->MergeIndexConfig(entry->oid, entry->Config());
  } else {
    entry->AdoptStorage(search::InvertedIndexStorage::Create(
      _directory, InMemory(), GetAttached().IsReadOnly(), GetOid(), entry->oid,
      ResolveSettings(entry->options), entry->Config()->top_k_scorer, false));
  }
  return std::move(entry);
}

duckdb::unique_ptr<duckdb::StandardEntry> SereneDBCatalog::MakeTokenizerEntry(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  duckdb::CreateTokenizerInfo& info) {
  if (info.config.empty()) {
    pg::CompileTokenizer(transaction.GetContext(), info);
  }
  return duckdb::make_uniq<TokenizerCatalogEntry>(*this, schema, info);
}

duckdb::unique_ptr<duckdb::StandardEntry> SereneDBCatalog::MakeJobEntry(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  duckdb::CreateJobInfo& info) {
  VerifySchedule(info.schedule);
  auto job = duckdb::make_uniq<JobCatalogEntry>(*this, schema, info,
                                                std::make_shared<JobState>());
  if (transaction.context) {
    job->ScheduleAtCommit(*transaction.context);
  }
  return job;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  const auto* entry = dynamic_cast<const SearchTableEntry*>(&op.table);
  if (!entry) {
    return duckdb::DuckCatalog::PlanInsert(context, planner, op, plan);
  }
  SDB_ASSERT(plan);
  auto& insert = planner.Make<connector::SereneDBSearchInsert>(
    *entry, op.types, op.estimated_cardinality, op.return_chunk);
  insert.children.emplace_back(*plan);
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  const auto* entry = dynamic_cast<const SearchTableEntry*>(&op.table);
  if (!entry) {
    return duckdb::DuckCatalog::PlanDelete(context, planner, op, plan);
  }
  if (op.is_truncate) {
    return planner.Make<connector::SereneDBSearchTruncate>(
      *entry, op.estimated_cardinality, context.transaction.IsAutoCommit());
  }
  auto& del = planner.Make<connector::SereneDBSearchDelete>(
    *entry, std::move(op.expressions), op.types, op.estimated_cardinality,
    op.return_chunk, std::move(op.return_columns), op.row_id_handling);
  del.children.emplace_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanCreateTableAs(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalCreateTable& op, duckdb::PhysicalOperator& plan) {
  if (ReadStorageEngine(op.info->Base().options) != TableEngine::Search) {
    return duckdb::DuckCatalog::PlanCreateTableAs(context, planner, op, plan);
  }
  auto& insert = planner.Make<connector::SereneDBSearchInsert>(
    std::move(op.info), op.estimated_cardinality);
  insert.children.emplace_back(plan);
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanMergeInto(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalMergeInto& op, duckdb::PhysicalOperator& plan) {
  if (dynamic_cast<const SearchTableEntry*>(&op.table)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
      ERR_MSG("MERGE INTO (and INSERT ... ON CONFLICT) is not yet supported on "
              "search-backed tables"));
  }
  return duckdb::DuckCatalog::PlanMergeInto(context, planner, op, plan);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  const auto* entry = dynamic_cast<const SearchTableEntry*>(&op.table);
  if (!entry) {
    return duckdb::DuckCatalog::PlanUpdate(context, planner, op, plan);
  }
  auto& update = planner.Make<connector::SereneDBSearchUpdate>(
    *entry, op.columns, std::move(op.expressions), op.types,
    op.estimated_cardinality, op.return_chunk, op.row_id_handling);
  update.children.emplace_back(plan);
  return update;
}

void SereneDBCatalog::BindIndexDefinition(duckdb::Binder& binder,
                                          duckdb::CreateStatement& stmt,
                                          duckdb::CatalogEntry& table) {
  auto& info = stmt.info->Cast<duckdb::CreateIndexInfo>();
  const bool inverted = info.index_type == kInvertedIndexTypeName;
  const auto unknown =
    absl::c_find_if_not(info.options, [&](const auto& option) {
      return inverted && IsKnownInvertedIndexOption(option.first);
    });
  if (unknown != info.options.end()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized parameter \"", unknown->first, "\""));
  }
  const bool search_table =
    dynamic_cast<const SearchTableEntry*>(&table) != nullptr;
  if (search_table) {
    for (const auto& [name, value] : info.options) {
      RequireSearchTableIndexOption(name);
    }
  }
  if (info.where_clause) {
    auto where_binder_owner = duckdb::Binder::CreateBinder(binder.context);
    duckdb::vector<duckdb::ColumnIndex> column_ids;
    auto* where_bind = &binder;
    if (table.type == duckdb::CatalogType::TABLE_ENTRY &&
        table.Cast<duckdb::TableCatalogEntry>().IsDuckTable()) {
      auto& columns = table.Cast<duckdb::TableCatalogEntry>().GetColumns();
      duckdb::vector<duckdb::Identifier> names;
      duckdb::vector<duckdb::LogicalType> types;
      for (const auto& column : columns.Logical()) {
        names.push_back(column.Name());
        types.push_back(column.Type());
      }
      where_binder_owner->bind_context.AddBaseTable(
        duckdb::TableIndex(0), duckdb::Identifier(), names, types, column_ids,
        table.Cast<duckdb::TableCatalogEntry>());
      where_bind = where_binder_owner.get();
    }
    duckdb::IndexBinder where_binder(*where_bind, binder.context);
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
  } else {
    for (auto i = info.column_opclasses.size(); i-- > 0;) {
      if (info.column_opclasses[i] != kIncludedKind) {
        continue;
      }
      info.column_opclasses.erase(info.column_opclasses.begin() + i);
      info.column_opclass_options.erase(info.column_opclass_options.begin() +
                                        i);
      info.expressions.erase(info.expressions.begin() + i);
      info.parsed_expressions.erase(info.parsed_expressions.begin() + i);
    }
  }
  for (const auto& opclass : info.column_opclasses) {
    if (opclass.empty() || opclass == kIncludedKind || opclass == kIVFKind ||
        opclass == kHNSWKind) {
      continue;
    }
    auto entry = duckdb::Catalog::GetEntry<TokenizerCatalogEntry>(
      binder.context, duckdb::QualifiedName::Parse(opclass),
      duckdb::OnEntryNotFound::RETURN_NULL);
    if (entry && &entry->ParentCatalog() == this) {
      info.dependencies.AddDependency(*entry);
    }
  }
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::TableCatalogEntry& table,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  BindIndexDefinition(binder, stmt, table);
  if (auto* search = dynamic_cast<SearchTableEntry*>(&table)) {
    return connector::BindCreateIndexOnSearchTable(binder, stmt, *search,
                                                   std::move(plan));
  }
  const bool inverted = stmt.info->Cast<duckdb::CreateIndexInfo>().index_type ==
                        kInvertedIndexTypeName;
  auto& scan = plan->Cast<duckdb::LogicalGet>()
                 .bind_data->Cast<duckdb::TableScanBindData>();
  auto result =
    duckdb::DuckCatalog::BindCreateIndex(binder, stmt, table, std::move(plan));
  if (inverted) {
    scan.is_create_index = false;
  }
  return result;
}

duckdb::unique_ptr<duckdb::LogicalOperator>
SereneDBCatalog::BindCreateViewIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::ViewCatalogEntry& view,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  BindIndexDefinition(binder, stmt, view);
  return connector::BindCreateIndexOnView(binder, stmt, view, std::move(plan));
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindAlterAddIndex(
  duckdb::Binder& binder, duckdb::TableCatalogEntry& table_entry,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan,
  duckdb::unique_ptr<duckdb::CreateIndexInfo> create_info,
  duckdb::unique_ptr<duckdb::AlterTableInfo> alter_info) {
  if (dynamic_cast<SearchTableEntry*>(&table_entry)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("ALTER TABLE ADD ",
                            create_info->constraint_type ==
                                duckdb::IndexConstraintType::PRIMARY
                              ? "PRIMARY KEY"
                              : "UNIQUE",
                            " on a search-backed table is not yet supported"));
  }
  return duckdb::DuckCatalog::BindAlterAddIndex(
    binder, table_entry, std::move(plan), std::move(create_info),
    std::move(alter_info));
}

duckdb::ErrorData SereneDBCatalog::SupportsCreateTable(
  duckdb::BoundCreateTableInfo& info) {
  const auto& options = info.Base().options;
  const bool search = ReadStorageEngine(options) == TableEngine::Search;
  auto unknown = absl::c_find_if(options, [search](const auto& option) {
    return option.first != kStorageOption &&
           !(search && absl::c_contains(kSearchTableOptions, option.first));
  });
  if (unknown != options.end()) {
    return duckdb::ErrorData{
      duckdb::BinderException("unrecognized parameter \"%s\"", unknown->first)};
  }
  return {};
}

std::string SereneDBCatalog::GetCatalogType() {
  return GetAttached().IsReadOnly() ? kReadOnlyStorageType : kStorageType;
}

duckdb::shared_ptr<duckdb::WriteAheadLog> SereneDBCatalog::CatalogLog() {
  if (GetAttached().IsReadOnly() || _detached.load(std::memory_order_acquire)) {
    return nullptr;
  }
  return ClusterOf(GetDatabase()).CatalogLog();
}

void SereneDBCatalog::FinalizeLoad(
  duckdb::optional_ptr<duckdb::ClientContext> context) {
  if (!GetAttached().IsReadOnly()) {
    return;
  }
  SDB_ASSERT(context);
  const auto& path = _directory->Path();
  const auto name = path.filename().string();
  duckdb::idx_t oid = 0;
  if (!absl::c_all_of(name, absl::ascii_isdigit) ||
      !absl::SimpleAtoi(name, &oid)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("\"", path.string(),
                            "\" is not a SereneDB database directory: its "
                            "name is not a database oid"));
  }
  const auto log_path = (path.parent_path() / kCatalogLog).string();
  _catalog_log_lock =
    duckdb::FileSystem::Get(GetAttached())
      .OpenFile(log_path, duckdb::FileFlags::FILE_FLAGS_READ |
                            duckdb::FileFlags::FILE_FLAGS_NULL_IF_NOT_EXISTS |
                            duckdb::FileLockType::READ_LOCK);
  if (!_catalog_log_lock) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_FILE),
      ERR_MSG("Cannot open catalog log \"", log_path,
              "\" in read-only mode: catalog log does not exist"));
  }
  auto& storage = GetAttached().GetStorageManager();
  duckdb::WriteAheadLog::ReplayCatalog(*context, storage, log_path, oid);
  for (auto& table : SearchTableStorages(*this)) {
    table->OpenReader();
  }
  for (auto& index : InvertedIndexEntries(*this)) {
    if (const auto& index_storage = index.get().Storage()) {
      index_storage->OpenReader();
    }
  }
  auto attached = GetAttached().shared_from_this();
  duckdb::MetaTransaction::Get(*context).UseDatabase(attached);
  storage.FinishLoad(*context);
}

bool SereneDBCatalog::AppendLocalIndexes(
  duckdb::DuckTransaction& transaction, duckdb::TableIndexList& index_list,
  duckdb::RowGroupCollection& source,
  const duckdb::vector<duckdb::StorageIndex>& mapped_column_ids,
  duckdb::row_t row_start, duckdb::ErrorData& error) {
  return connector::InvertedStoreIndex::AppendLocal(
    transaction, index_list, source, mapped_column_ids, row_start, error);
}

void SereneDBCatalog::Initialize(bool load_builtin) {
  duckdb::DuckCatalog::Initialize(load_builtin);
  auto data = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  duckdb::CreateSchemaInfo info;
  info.SetQualifiedName(duckdb::QualifiedName(
    {duckdb::Identifier{irs::StaticStrings::kPublic}}, duckdb::Identifier()));
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  info.permissions.owner = pg::kRootUser;
  info.permissions.acl = {
    {.grantee = pg::kRootUser,
     .grantor = pg::kRootUser,
     .privs = duckdb::AclMode::Usage | duckdb::AclMode::Create},
    {.grantee = pg::kPublicGrantee,
     .grantor = pg::kRootUser,
     .privs = duckdb::AclMode::Usage},
  };
  info.oid = pg::kPgPublicSchema;
  CreateSchema(data, info);
  MountSystemSchemas(*this);
}

duckdb::idx_t SereneDBCatalog::DefaultSchemaOid() const {
  return pg::kPgMainSchema;
}

void SereneDBCatalog::OnDetach(duckdb::ClientContext& context) {
  _detached.store(true, std::memory_order_release);
  if (GetAttached().IsReadOnly()) {
    duckdb::DuckCatalog::OnDetach(context);
    return;
  }
  ForEachJob(*this, [](JobCatalogEntry& job) { job.OnDrop(); });
  std::vector<duckdb::Identifier> servers;
  GetCatalogSet(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
    .Scan(
      [&](duckdb::CatalogEntry& entry) { servers.emplace_back(entry.name); });
  for (const auto& server : servers) {
    duckdb::DatabaseManager::Get(context).DetachDatabase(
      context, server, duckdb::OnEntryNotFound::RETURN_NULL);
  }
  duckdb::DuckCatalog::OnDetach(context);
}

static bool IsReservedSchemaName(const duckdb::Identifier& name) {
  return !duckdb::DefaultSchemaGenerator::IsDefaultSchema(name) &&
         name.GetIdentifierName().starts_with("pg_");
}

static std::string_view AlterActionName(duckdb::AlterTableType type) {
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

static void RefuseViewAlter(const duckdb::AlterTableInfo& info,
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

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSchema(
  duckdb::CatalogTransaction transaction, duckdb::CreateSchemaInfo& info) {
  const auto& name = info.GetQualifiedName().Schema();
  if (IsReservedSchemaName(name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("unacceptable schema name \"", name.GetIdentifierName(), "\""),
      ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
  }
  SDB_IF_FAILURE("unable_to_create") {
    THROW_SQL_ERROR(ERR_MSG("internal error"));
  }
  return duckdb::DuckCatalog::CreateSchema(transaction, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateForeignServer(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateForeignServerInfo& info) {
  DeclareModified(transaction, *this);
  auto entry = duckdb::DuckCatalog::CreateForeignServer(transaction, info);
  if (entry) {
    entry->Cast<ForeignServerCatalogEntry>().Attach(transaction.GetContext());
  }
  return entry;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateSubscription(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateSubscriptionInfo& info) {
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateSubscription(transaction, info);
}

void SereneDBCatalog::Alter(duckdb::CatalogTransaction transaction,
                            duckdb::AlterInfo& info) {
  const auto type = info.GetCatalogType();
  if (const auto new_name = info.GetNewName();
      new_name && type == duckdb::CatalogType::SCHEMA_ENTRY &&
      IsReservedSchemaName(*new_name)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("unacceptable schema name \"", new_name->GetIdentifierName(),
              "\""),
      ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
  }
  if (transaction.HasContext() &&
      (info.type == duckdb::AlterType::ALTER_TABLE ||
       info.type == duckdb::AlterType::ALTER_INDEX)) {
    RefuseUnsupportedAlter(transaction.GetContext(), info);
  }
  if (type != duckdb::CatalogType::FOREIGN_SERVER_ENTRY &&
      type != duckdb::CatalogType::SUBSCRIPTION_ENTRY) {
    duckdb::DuckCatalog::Alter(transaction, info);
    return;
  }
  DeclareModified(transaction, *this);
  const auto& name = info.GetQualifiedName().Name();
  if (!GetCatalogSet(type).AlterEntry(transaction, name, info)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG(duckdb::CatalogTypeToString(type), " with name ",
                            name.GetIdentifierName(), " does not exist!"));
  }
}

void SereneDBCatalog::DropForeignServer(duckdb::CatalogTransaction transaction,
                                        duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  duckdb::DuckCatalog::DropForeignServer(transaction, info);
}

void SereneDBCatalog::DropSubscription(duckdb::CatalogTransaction transaction,
                                       duckdb::DropInfo& info) {
  DeclareModified(transaction, *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  duckdb::DuckCatalog::DropSubscription(transaction, info);
}

}  // namespace sdb::catalog
