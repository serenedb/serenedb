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
#include "connector/duckdb_physical_search_insert.h"
#include "connector/duckdb_physical_search_update.h"
#include "connector/search_table_dispatch.h"
#include "connector/view_index_bind.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::catalog {

SereneDBCatalog::SereneDBCatalog(duckdb::AttachedDatabase& db)
  : duckdb::DuckCatalog{db}, _foreign_servers{*this} {}

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
