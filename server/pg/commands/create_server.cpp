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

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "basics/duckdb_engine.h"
#include "catalog/ddl/catalog.h"
#include "catalog/ddl/duckdb_catalog.h"
#include "catalog/entry/duckdb_object_entry.h"
#include "catalog/foreign_server.h"
#include "catalog/log/data_store.h"
#include "catalog/read/duckdb_catalog_sets.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {
namespace {

// DROP SERVER's catalog half. A foreign server is a database child, as in
// postgres, so the drop is named by the database rather than by a schema.
bool DropForeignServerRow(ConnectionContext& conn_ctx, std::string_view name,
                          bool cascade, bool missing_ok) {
  auto& context = conn_ctx.GetClientContext();
  catalog::JoinStoreTransaction(&context);
  const auto database_id =
    catalog::FindDatabaseId(&context, conn_ctx.GetDatabase());
  return catalog::DropEntryObject(
    catalog::ActingAs(conn_ctx.GetRoleId(), context),
    duckdb::CatalogType::FOREIGN_SERVER_ENTRY, database_id, database_id, name,
    cascade, missing_ok);
}

// Lower-case the option keys and stringify the values into the parallel
// key/value vectors ForeignServer stores.
std::pair<std::vector<std::string>, std::vector<std::string>> MakeServerOptions(
  const duckdb::named_parameter_map_t& options) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(options.size());
  values.reserve(options.size());
  for (const auto& [key, value] : options) {
    keys.push_back(absl::AsciiStrToLower(key.GetIdentifierName()));
    values.push_back(value.ToString());
  }
  return {std::move(keys), std::move(values)};
}

// Establish the live attachment for a server (validates connectivity too).
uint64_t RunAttach(const catalog::CreateForeignServerInfo& server) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  const auto res = catalog::RunForeignServerAttach(*conn, server);
  using Status = catalog::ForeignServerAttachResult::Status;
  if (res.status == Status::Unsupported) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("foreign-data wrapper \"", server.GetFdwName(),
                            "\" is not supported"),
                    ERR_HINT("Use one of: ", catalog::SupportedFdwList(), "."));
  }
  if (res.status == Status::Failed) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
                    ERR_MSG("could not connect foreign server \"",
                            server.GetName(), "\": ", res.error));
  }
  return res.attachment_id;
}

}  // namespace

void CreateForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                         std::string_view fdw_name, bool if_not_exists,
                         const duckdb::named_parameter_map_t& options) {
  auto db_id = conn_ctx.GetDatabaseId();

  // Owner = the creating role; the default ACL then gives the owner USAGE and
  // the public nothing (auth::ClassPrivs/PublicDefaultPrivs).
  auto [option_keys, option_values] = MakeServerOptions(options);
  auto server = std::make_shared<catalog::CreateForeignServerInfo>(
    ObjectId{}, db_id, name, std::string{fdw_name}, std::move(option_keys),
    std::move(option_values));

  // The catalog validates everything under its mutex (privilege, supported
  // FDW, name collisions) and persists -- a denied or invalid CREATE never
  // touches the network. The live ATTACH runs after; a connect failure
  // compensates by dropping the just-created row, so a failed CREATE SERVER
  // still leaves nothing behind.
  auto& catalog = catalog::DatabaseCatalog(&conn_ctx.GetClientContext(), db_id);
  if (!catalog.CreateForeignServer(
        catalog::ActingAs(conn_ctx.GetRoleId(), conn_ctx.GetClientContext()),
        db_id, server, catalog::Permissions{conn_ctx.GetRoleId()},
        if_not_exists)) {
    return;
  }
  uint64_t attachment = 0;
  try {
    attachment = RunAttach(*server);
  } catch (...) {
    try {
      DropForeignServerRow(conn_ctx, name, /*cascade=*/true,
                           /*missing_ok=*/true);
    } catch (...) {
      // Surface the connect error, not the cleanup's -- boot replay heals a
      // row that outlives this (it re-attaches persisted servers).
    }
    throw;
  }

  // The attach is the slow part, and a concurrent DROP SERVER may have removed
  // our row while it ran -- that DROP saw no attachment yet, so it detached
  // nothing. Own what we attached: take it back down rather than leaving an
  // attachment holding the alias with no row to drop it by.
  if (!catalog::FindForeignServer(&conn_ctx.GetClientContext(), db_id, name)) {
    catalog::DetachForeignServerAttachment(name, attachment);
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("server \"", name,
                            "\" was dropped concurrently with its creation"));
  }
}

void DropForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                       bool missing_ok, bool cascade) {
  // Captured before the row goes: the detach afterwards removes this exact
  // attachment or nothing, so it cannot tear down one that a concurrent
  // same-named CREATE SERVER attached in the meantime.
  const auto attachment = catalog::ForeignServerAttachmentId(name);
  // The catalog drops the server row; absent + missing_ok returns false.
  if (!DropForeignServerRow(conn_ctx, name, cascade, missing_ok)) {
    return;
  }

  catalog::DetachForeignServerAttachment(name, attachment);
}

}  // namespace sdb::pg
