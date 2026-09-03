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

#include "pg/commands/create_server.h"

#include <absl/strings/ascii.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/main/client_context.hpp>
#include <string>
#include <utility>

#include "catalog1/catalog.h"
#include "catalog1/entry/foreign_server.h"

namespace sdb::pg {
namespace {

catalog::SereneDBCatalog& CatalogOf(ConnectionContext& conn_ctx) {
  auto& context = conn_ctx.GetClientContext();
  const duckdb::Identifier name{conn_ctx.GetDatabase()};
  return duckdb::Catalog::GetCatalog(context, name)
    .Cast<catalog::SereneDBCatalog>();
}

catalog::ServerOptions MakeServerOptions(
  const duckdb::named_parameter_map_t& options) {
  catalog::ServerOptions result;
  result.reserve(options.size());
  for (const auto& [key, value] : options) {
    result.emplace(absl::AsciiStrToLower(key.GetIdentifierName()),
                   value.ToString());
  }
  return result;
}

}  // namespace

void CreateForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                         std::string_view fdw_name, bool if_not_exists,
                         const duckdb::named_parameter_map_t& options) {
  catalog::CreateForeignServerInfo info;
  info.SetName(duckdb::Identifier{name});
  info.fdw_name = std::string{fdw_name};
  info.options = MakeServerOptions(options);
  info.on_conflict = if_not_exists
                       ? duckdb::OnCreateConflict::IGNORE_ON_CONFLICT
                       : duckdb::OnCreateConflict::ERROR_ON_CONFLICT;

  auto& catalog = CatalogOf(conn_ctx);
  catalog.CreateForeignServer(
    catalog.GetCatalogTransaction(conn_ctx.GetClientContext()), info);
}

void DropForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                       bool missing_ok, bool cascade) {
  auto& catalog = CatalogOf(conn_ctx);
  const bool dropped = catalog.DropForeignServer(
    catalog.GetCatalogTransaction(conn_ctx.GetClientContext()),
    duckdb::Identifier{name}, cascade);
  if (!dropped && !missing_ok) {
    throw duckdb::CatalogException("server \"%s\" does not exist",
                                   std::string{name});
  }
}

}  // namespace sdb::pg
