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

#include <algorithm>
#include <duckdb/catalog/default/default_schemas.hpp>
#include <duckdb/catalog/dependency_manager.hpp>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/common/exception/catalog_exception.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/function/table/table_scan.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <duckdb/parser/parsed_data/create_index_info.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_data/create_sequence_info.hpp>
#include <duckdb/parser/parsed_data/drop_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
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
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/assert.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <utility>

#include "catalog/cluster.h"
#include "catalog/entry/database.h"
#include "catalog/entry/foreign_server.h"
#include "catalog/entry/inverted_index.h"
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
#include "connector/primary_key.h"
#include "connector/view_index_bind.h"
#include "pg/connection_context.h"
#include "pg/pg_types.h"
#include "search/inverted_index_storage.h"
#include "search/search_table.h"

namespace sdb::catalog {

void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type) {
  if (!transaction.context) {
    return;
  }
  duckdb::MetaTransaction::Get(transaction.GetContext())
    .ModifyDatabase(catalog.GetAttached(), type);
}

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db, true} {}

duckdb::unique_ptr<duckdb::TableCatalogEntry> SereneDBCatalog::MakeTableEntry(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  duckdb::BoundCreateTableInfo& info) {
  auto& options = info.Base().options;
  if (ReadStorageEngine(options) == TableEngine::Search) {
    auto entry =
      duckdb::make_uniq<SearchTableEntry>(*this, schema, info, transaction);
    if (info.Base().oid == 0) {
      if (!entry->PkSequenceName().empty()) {
        duckdb::CreateSequenceInfo sequence_info;
        sequence_info.SetQualification(GetName(), schema.name);
        sequence_info.SetSequenceName(entry->PkSequenceName());
        info.dependencies.AddOwnedDependency(
          *schema.CreateSequence(transaction, sequence_info));
      }
      entry->Storage()->StartTasks();
    }
    return std::move(entry);
  }
  options.erase(std::string{kStorageOption});
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
      GetOid(), schema.oid, relation.oid, entry->oid,
      ResolveSettings(entry->options), entry->Config()->top_k_scorer, false));
  }
  return std::move(entry);
}

duckdb::optional_ptr<duckdb::SchemaCatalogEntry>
SereneDBCatalog::FindSchemaById(duckdb::ClientContext& context,
                                duckdb::idx_t id) {
  duckdb::optional_ptr<duckdb::SchemaCatalogEntry> result;
  GetSchemaCatalogSet().ScanWithReturn(
    context, [&](duckdb::CatalogEntry& entry) {
      if (entry.oid != id) {
        return true;
      }
      result = &entry.Cast<duckdb::SchemaCatalogEntry>();
      return false;
    });
  return result;
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::FindEntryById(
  duckdb::optional_ptr<duckdb::ClientContext> context, duckdb::CatalogType type,
  duckdb::idx_t id) {
  duckdb::optional_ptr<duckdb::CatalogEntry> result;
  const auto match = [&](duckdb::CatalogEntry& entry) {
    if (!result && !entry.internal && entry.oid == id) {
      result = &entry;
    }
  };
  const auto scan = [&](duckdb::SchemaCatalogEntry& schema) {
    if (context) {
      schema.Scan(*context, type, match);
    } else {
      schema.Scan(type, match);
    }
  };
  if (context) {
    duckdb::DuckCatalog::ScanSchemas(*context, scan);
  } else {
    duckdb::DuckCatalog::ScanSchemas(scan);
  }
  return result;
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
  insert.children.push_back(*plan);
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
      entry->Storage(), op.estimated_cardinality,
      context.transaction.IsAutoCommit());
  }
  auto& del = planner.Make<connector::SereneDBSearchDelete>(
    *entry, std::move(op.expressions), op.types, op.estimated_cardinality,
    op.return_chunk, std::move(op.return_columns));
  del.children.push_back(plan);
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
  insert.children.push_back(plan);
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
    op.estimated_cardinality, op.return_chunk);
  update.children.push_back(plan);
  return update;
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::CatalogEntry& table,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
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
  if (inverted) {
    BindInvertedIndexOptions(binder.context, info.options,
                             table.type == duckdb::CatalogType::VIEW_ENTRY);
  }
  for (const auto& opclass : info.column_opclasses) {
    if (opclass.empty() || opclass == kIncludedKind || opclass == kIVFKind ||
        opclass == kHNSWKind) {
      continue;
    }
    const duckdb::EntryLookupInfo dictionary{
      duckdb::CatalogType::TOKENIZER_ENTRY,
      duckdb::QualifiedName::Parse(opclass)};
    auto entry = duckdb::Catalog::GetEntry(
      binder.context, dictionary, duckdb::OnEntryNotFound::RETURN_NULL);
    if (entry && &entry->ParentCatalog() == this) {
      info.dependencies.AddDependency(*entry);
    }
  }
  if (table.type == duckdb::CatalogType::VIEW_ENTRY) {
    return connector::BindCreateIndexOnView(
      binder, stmt, table.Cast<duckdb::ViewCatalogEntry>(), std::move(plan));
  }
  auto& table_entry = table.Cast<duckdb::TableCatalogEntry>();
  if (auto* search = dynamic_cast<SearchTableEntry*>(&table_entry)) {
    return connector::BindCreateIndexOnSearchTable(binder, stmt, *search,
                                                   std::move(plan));
  }
  auto& scan = plan->Cast<duckdb::LogicalGet>()
                 .bind_data->Cast<duckdb::TableScanBindData>();
  auto result =
    duckdb::DuckCatalog::BindCreateIndex(binder, stmt, table, std::move(plan));
  if (inverted) {
    scan.is_create_index = false;
  }
  return result;
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

void SereneDBCatalog::Initialize(bool load_builtin) {
  duckdb::DuckCatalog::Initialize(load_builtin);
  auto data = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  duckdb::CreateSchemaInfo info;
  info.SetQualifiedName(duckdb::QualifiedName(
    {duckdb::Identifier{irs::StaticStrings::kPublic}}, duckdb::Identifier()));
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  info.permissions.owner = pg::kRootUser;
  info.oid = pg::kPgPublicSchema;
  CreateSchema(data, info);
  MountSystemSchemas(*this);
}

void SereneDBCatalog::OnDetach(duckdb::ClientContext& context) {
  std::vector<duckdb::Identifier> servers;
  GetCatalogSet(duckdb::CatalogType::FOREIGN_SERVER_ENTRY)
    .Scan([&](duckdb::CatalogEntry& entry) { servers.push_back(entry.name); });
  for (const auto& server : servers) {
    duckdb::DatabaseManager::Get(context).DetachDatabase(
      context, server, duckdb::OnEntryNotFound::RETURN_NULL);
  }
  if (context.transaction.HasActiveTransaction()) {
    auto& cluster = ClusterOf(context);
    duckdb::DropInfo info;
    info.type = duckdb::CatalogType::DATABASE_ENTRY;
    info.SetName(GetName());
    info.if_not_found = duckdb::OnEntryNotFound::RETURN_NULL;
    cluster.DropDatabase(cluster.GetCatalogTransaction(context), info);
  }
  duckdb::DuckCatalog::OnDetach(context);
}

static bool IsReservedSchemaName(const duckdb::Identifier& name) {
  return !duckdb::DefaultSchemaGenerator::IsDefaultSchema(name) &&
         name.GetIdentifierName().starts_with("pg_");
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
  return duckdb::DuckCatalog::CreateSchema(transaction, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateTokenizer(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  duckdb::CreateTokenizerInfo& info) {
  DeclareModified(transaction, *this);
  return schema.CreateTokenizer(transaction, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateForeignServer(
  duckdb::CatalogTransaction transaction,
  duckdb::CreateForeignServerInfo& info) {
  DeclareModified(transaction, *this);
  return duckdb::DuckCatalog::CreateForeignServer(transaction, info);
}

void SereneDBCatalog::Alter(duckdb::CatalogTransaction transaction,
                            duckdb::AlterInfo& info) {
  const auto type = info.GetCatalogType();
  if (type == duckdb::CatalogType::SCHEMA_ENTRY &&
      info.type == duckdb::AlterType::RENAME) {
    const auto& new_name = info.Cast<duckdb::RenameInfo>().new_name;
    if (IsReservedSchemaName(new_name)) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_RESERVED_NAME),
        ERR_MSG("unacceptable schema name \"", new_name.GetIdentifierName(),
                "\""),
        ERR_DETAIL("The prefix \"pg_\" is reserved for system schemas."));
    }
  }
  if (type != duckdb::CatalogType::FOREIGN_SERVER_ENTRY) {
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

}  // namespace sdb::catalog
