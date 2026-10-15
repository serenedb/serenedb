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

#include "connector/functions/ttl.h"

#include <duckdb/common/enums/database_modification_type.hpp>
#include <duckdb/common/types/interval.hpp>
#include <duckdb/function/pragma_function.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/client_data.hpp>
#include <duckdb/main/extension/extension_loader.hpp>
#include <duckdb/transaction/meta_transaction.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "catalog/entry/search_table.h"
#include "connector/column_id.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "search/search_table.h"

namespace sdb::connector {
namespace {

void ExpireById(duckdb::ClientContext& context,
                const duckdb::FunctionParameters& parameters) {
  const auto table_id = parameters.values[0].GetValue<int64_t>();
  const auto& home =
    duckdb::ClientData::Get(context).catalog_search_path->GetDefault();
  auto& catalog = duckdb::Catalog::GetCatalog(context, home.GetCatalog())
                    .Cast<catalog::SereneDBCatalog>();
  auto entry = catalog.FindIn<duckdb::TableCatalogEntry>(
    &context, static_cast<duckdb::idx_t>(table_id));
  if (!entry || !entry->GetLogStorage()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_TABLE),
      ERR_MSG("search table with oid ", table_id, " does not exist"));
  }
  auto& table = entry->Cast<catalog::SearchTableEntry>();
  auto& connection = GetSereneDBContext(context);
  const auto caller = auth::ClosureFor(&context, connection.GetRoleId());
  if (!caller->Can(duckdb::CatalogType::TABLE_ENTRY, table.permissions,
                   duckdb::AclMode::Delete)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied for table ", table.name.GetIdentifierName()));
  }
  if (!table.HasTtl()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
      ERR_MSG("table \"", table.name.GetIdentifierName(), "\" has no TTL"));
  }
  const auto& options = table.Options();
  const auto& column =
    table.GetColumns().GetColumn(duckdb::Identifier{options.ttl_column});
  const auto now = duckdb::MetaTransaction::Get(context).start_timestamp;
  auto cutoff =
    duckdb::Value::TIMESTAMP(
      duckdb::Interval::Add(now, duckdb::Interval::Invert(options.ttl)))
      .DefaultTryCastAs(column.Type());
  if (!cutoff) {
    return;
  }
  duckdb::MetaTransaction::Get(context).ModifyDatabase(
    table.catalog.GetAttached(), duckdb::DatabaseModificationType::DELETE_DATA);
  auto& search = connection.SearchTxn();
  search.RegisterWriter(table.Storage(), table);
  search.AddSearchExpiry(table.Storage(),
                         {.column = static_cast<ColumnId>(column.Oid()),
                          .cutoff = std::move(*cutoff)});
}

}  // namespace

void RegisterTtlFunctions(duckdb::DatabaseInstance& db) {
  duckdb::ExtensionLoader loader{db, "serenedb"};
  loader.RegisterFunction(duckdb::PragmaFunction::PragmaCall(
    duckdb::Identifier{kExpireByIdPragma}, ExpireById,
    {duckdb::LogicalType::BIGINT}));
}

}  // namespace sdb::connector
