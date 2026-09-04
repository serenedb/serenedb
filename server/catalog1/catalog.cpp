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

#include "catalog1/catalog.h"

#include <algorithm>
#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/execution/physical_plan_generator.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/parser/expression/columnref_expression.hpp>
#include <duckdb/parser/parsed_data/create_schema_info.hpp>
#include <duckdb/parser/parsed_expression_iterator.hpp>
#include <duckdb/parser/statement/create_statement.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <duckdb/planner/expression_binder/index_binder.hpp>
#include <duckdb/planner/operator/logical_create_index.hpp>
#include <duckdb/planner/operator/logical_delete.hpp>
#include <duckdb/planner/operator/logical_filter.hpp>
#include <duckdb/planner/operator/logical_get.hpp>
#include <duckdb/planner/operator/logical_insert.hpp>
#include <duckdb/planner/operator/logical_update.hpp>
#include <duckdb/planner/parsed_data/bound_create_table_info.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <utility>

#include "basics/assert.h"
#include "basics/static_strings.h"
#include "catalog1/entry/database.h"
#include "catalog1/entry/foreign_server.h"
#include "catalog1/entry/inverted_index.h"
#include "catalog1/entry/role.h"
#include "catalog1/entry/search_table.h"
#include "catalog1/entry/tokenizer.h"
#include "connector/duckdb_physical_create_index.h"
#include "connector/duckdb_physical_search_delete.h"
#include "connector/view_index_bind.h"
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/search_table_dispatch.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {
namespace {

// duckdb refuses a CatalogSet write unless the meta transaction names this
// database as the one it modifies -- a DDL statement's binder does that, and
// these kinds are reached through pragmas and boot code that have no binder.
void DeclareModified(duckdb::CatalogTransaction transaction,
                     duckdb::Catalog& catalog,
                     duckdb::DatabaseModificationType type =
                       duckdb::DatabaseModificationType::CREATE_CATALOG_ENTRY) {
  if (!transaction.context) {
    return;
  }
  duckdb::MetaTransaction::Get(transaction.GetContext())
    .ModifyDatabase(catalog.GetAttached(), type);
}

}  // namespace

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db}, _foreign_servers{*this} {}

duckdb::unique_ptr<duckdb::TableCatalogEntry> SereneDBCatalog::MakeTableEntry(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  duckdb::BoundCreateTableInfo& info) {
  connector::ApplyStorageKind(transaction.context, info.Base());
  if (connector::ReadStorageEngine(info.Base().options) ==
      TableEngine::Search) {
    auto entry = duckdb::make_uniq<SearchTableEntry>(*this, schema, info);
    connector::EnsureGeneratedPkSequence(transaction, schema, *entry);
    return std::move(entry);
  }
  return duckdb::DuckCatalog::MakeTableEntry(transaction, schema, info);
}

duckdb::unique_ptr<duckdb::IndexCatalogEntry> SereneDBCatalog::MakeIndexEntry(
  duckdb::DuckSchemaEntry& schema, duckdb::CreateIndexInfo& info,
  duckdb::TableCatalogEntry& table) {
  if (info.index_type == kInvertedIndexTypeName) {
    return duckdb::make_uniq<InvertedIndexEntry>(*this, schema, info, table);
  }
  return duckdb::DuckCatalog::MakeIndexEntry(schema, info, table);
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanInsert(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalInsert& op,
  duckdb::optional_ptr<duckdb::PhysicalOperator> plan) {
  const auto* entry = dynamic_cast<const SearchTableEntry*>(&op.table);
  if (entry == nullptr) {
    return duckdb::DuckCatalog::PlanInsert(context, planner, op, plan);
  }
  SDB_ASSERT(plan);
  auto& insert = planner.Make<connector::SereneDBSearchInsert>(
    connector::ResolveSearchWriteTarget(context, *entry), op.types,
    op.estimated_cardinality, op.return_chunk);
  insert.children.push_back(*plan);
  return insert;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanDelete(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalDelete& op, duckdb::PhysicalOperator& plan) {
  const auto* entry = dynamic_cast<const SearchTableEntry*>(&op.table);
  if (entry == nullptr) {
    return duckdb::DuckCatalog::PlanDelete(context, planner, op, plan);
  }
  std::vector<duckdb::idx_t> pk_col_indices;
  pk_col_indices.reserve(op.expressions.size());
  for (const auto& expr : op.expressions) {
    pk_col_indices.push_back(
      expr->Cast<duckdb::BoundReferenceExpression>().Index());
  }
  duckdb::vector<duckdb::column_t> return_columns;
  if (op.return_chunk) {
    return_columns.assign(op.return_columns.begin(), op.return_columns.end());
  }
  auto& del = planner.Make<connector::SereneDBSearchDelete>(
    connector::ResolveSearchWriteTarget(context, *entry),
    std::move(pk_col_indices), op.types, std::move(return_columns),
    op.estimated_cardinality);
  del.children.push_back(plan);
  return del;
}

duckdb::PhysicalOperator& SereneDBCatalog::PlanUpdate(
  duckdb::ClientContext& context, duckdb::PhysicalPlanGenerator& planner,
  duckdb::LogicalUpdate& op, duckdb::PhysicalOperator& plan) {
  const auto* entry = dynamic_cast<const SearchTableEntry*>(&op.table);
  if (entry == nullptr) {
    return duckdb::DuckCatalog::PlanUpdate(context, planner, op, plan);
  }
  // The binder appends the row identifiers after the projected row, so they
  // are the trailing columns of what this operator is fed.
  const auto row_ids = op.table.GetRowIdColumns().size();
  const auto width = plan.GetTypes().size();
  SDB_ASSERT(row_ids <= width);
  std::vector<duckdb::idx_t> pk_col_indices;
  pk_col_indices.reserve(row_ids);
  for (auto i = width - row_ids; i < width; ++i) {
    pk_col_indices.push_back(i);
  }
  std::vector<duckdb::PhysicalIndex> update_columns(op.columns.begin(),
                                                    op.columns.end());
  auto& update = planner.Make<connector::SereneDBSearchUpdate>(
    connector::ResolveSearchWriteTarget(context, *entry),
    std::move(pk_col_indices), std::move(update_columns), op.types,
    op.estimated_cardinality, op.return_chunk);
  update.children.push_back(plan);
  return update;
}

duckdb::unique_ptr<duckdb::LogicalOperator> SereneDBCatalog::BindCreateIndex(
  duckdb::Binder& binder, duckdb::CreateStatement& stmt,
  duckdb::CatalogEntry& table,
  duckdb::unique_ptr<duckdb::LogicalOperator> plan) {
  if (table.type != duckdb::CatalogType::VIEW_ENTRY) {
    return duckdb::DuckCatalog::BindCreateIndex(binder, stmt, table,
                                                std::move(plan));
  }
  return connector::BindCreateIndexOnView(
    binder, stmt, table.Cast<duckdb::ViewCatalogEntry>());
}

duckdb::ErrorData SereneDBCatalog::SupportsCreateTable(
  duckdb::BoundCreateTableInfo&) {
  return {};
}

std::string SereneDBCatalog::GetDefaultSchema() const {
  return std::string{StaticStrings::kPublic};
}

void SereneDBCatalog::Initialize(bool load_builtin) {
  duckdb::DuckCatalog::Initialize(load_builtin);
  auto data = duckdb::CatalogTransaction::GetSystemTransaction(GetDatabase());
  duckdb::CreateSchemaInfo info;
  info.SetQualifiedName(duckdb::QualifiedName(
    {duckdb::Identifier{StaticStrings::kPublic}}, duckdb::Identifier()));
  info.on_conflict = duckdb::OnCreateConflict::IGNORE_ON_CONFLICT;
  CreateSchema(data, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateTokenizer(
  duckdb::CatalogTransaction transaction, duckdb::DuckSchemaEntry& schema,
  CreateTokenizerInfo& info) {
  DeclareModified(transaction, *this);
  auto entry = duckdb::make_uniq<TokenizerCatalogEntry>(*this, schema, info);
  return schema.AddEntry(transaction, std::move(entry), info.on_conflict);
}

void SereneDBCatalog::DropTokenizer(duckdb::ClientContext& context,
                                    duckdb::DropInfo& info) {
  DeclareModified(GetCatalogTransaction(context), *this,
                  duckdb::DatabaseModificationType::DROP_CATALOG_ENTRY);
  DropEntry(context, info);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::CreateForeignServer(
  duckdb::CatalogTransaction transaction, CreateForeignServerInfo& info) {
  const auto& entry_name = info.GetQualifiedName().Name();
  if (info.on_conflict != duckdb::OnCreateConflict::ERROR_ON_CONFLICT) {
    const auto existing = _foreign_servers.GetEntry(transaction, entry_name);
    if (existing) {
      if (info.on_conflict == duckdb::OnCreateConflict::IGNORE_ON_CONFLICT) {
        return nullptr;
      }
      _foreign_servers.DropEntry(transaction, entry_name, false);
    }
  }
  auto entry = duckdb::make_uniq<ForeignServerCatalogEntry>(*this, info);
  auto result = entry.get();
  if (!_foreign_servers.CreateEntry(transaction, entry_name, std::move(entry),
                                    info.dependencies)) {
    throw duckdb::CatalogException::EntryAlreadyExists(
      duckdb::CatalogType::FOREIGN_SERVER_ENTRY, entry_name);
  }
  return result;
}

bool SereneDBCatalog::DropForeignServer(duckdb::CatalogTransaction transaction,
                                        const duckdb::Identifier& name,
                                        bool cascade) {
  DeclareModified(transaction, *this);
  return _foreign_servers.DropEntry(transaction, name, cascade);
}

void SereneDBCatalog::ScanForeignServers(
  duckdb::CatalogTransaction transaction,
  const std::function<void(duckdb::CatalogEntry&)>& callback) {
  _foreign_servers.Scan(transaction, callback);
}

duckdb::optional_ptr<duckdb::CatalogEntry> SereneDBCatalog::LookupForeignServer(
  duckdb::CatalogTransaction transaction, const duckdb::Identifier& name) {
  return _foreign_servers.GetEntry(transaction, name);
}

}  // namespace sdb::catalog
