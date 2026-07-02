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
#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/errors.h"
#include "basics/log.h"
#include "basics/static_strings.h"
#include "catalog/catalog.h"
#include "catalog/foreign_server.h"
#include "catalog/user_mapping.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {
namespace {

// Servers are not schema-qualified in PG; serenedb keeps them in `public`.
constexpr std::string_view kSchema = StaticStrings::kPublic;

// "DETACH <ident>" -- built in several places (probe cleanup, the atomic
// re-attach swap, the revert path); the alias is double-quoted.
std::string DetachSql(std::string_view name) {
  return absl::StrCat("DETACH ", catalog::QuoteSqlIdentifier(name));
}

// Lower-case the option keys and stringify the values into parallel vectors
// (the storage shape shared by ForeignServer and UserMapping).
std::pair<std::vector<std::string>, std::vector<std::string>> SplitOptions(
  const duckdb::named_parameter_map_t& options) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(options.size());
  values.reserve(options.size());
  for (const auto& [key, value] : options) {
    keys.push_back(absl::AsciiStrToLower(key));
    values.push_back(value.ToString());
  }
  return {std::move(keys), std::move(values)};
}

// Establish the live attachment for a server (validates connectivity too).
void RunAttach(const catalog::ForeignServer& server) {
  auto sql = catalog::BuildForeignServerAttachSql(server);
  if (sql.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("foreign-data wrapper \"", server.GetFdwName(),
                            "\" is not supported"),
                    ERR_HINT("Use clickhouse_fdw or postgres_fdw."));
  }
  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto result = conn->Query(sql);
  if (result->HasError()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
                    ERR_MSG("could not connect foreign server \"",
                            server.GetName(), "\": ", result->GetError()));
  }
}

void RunDetach(std::string_view name) {
  // Best-effort: the attachment may be absent (e.g. boot replay skipped a down
  // remote). Ignore errors.
  auto conn = DuckDBEngine::Instance().CreateConnection();
  conn->Query(DetachSql(name));
}

// Resolve a USER MAPPING role spec: CURRENT_USER/USER -> the session user;
// PUBLIC stays "public"; otherwise the literal role name.
std::string ResolveRole(ConnectionContext& conn_ctx, std::string_view user) {
  const auto lower = absl::AsciiStrToLower(user);
  if (lower == "current_user" || lower == "user") {
    return std::string{conn_ctx.user()};
  }
  if (lower == "public") {
    return "public";
  }
  return std::string{user};
}

// Validate that a server (with the given PUBLIC mapping merged in) can actually
// connect, by attaching to a THROWAWAY alias rather than the live name. Returns
// the connect error on failure, or empty on success. Leaves nothing attached.
std::string ProbeAttach(const catalog::ForeignServer& server,
                        const catalog::UserMapping* pub) {
  const auto alias = absl::StrCat("__sdb_fdw_probe_", server.GetName());
  auto sql = catalog::BuildForeignServerAttachSql(server, pub, alias);
  if (sql.empty()) {
    return {};  // unsupported FDW is reported elsewhere; nothing to probe
  }
  auto conn = DuckDBEngine::Instance().CreateConnection();
  // Clear any stale probe left by a crash between attach and detach.
  conn->Query(DetachSql(alias));
  auto result = conn->Query(sql);
  if (result->HasError()) {
    return result->GetError();
  }
  conn->Query(DetachSql(alias));
  return {};
}

// Re-establish a server's attachment with current credentials (server OPTIONS
// merged with its PUBLIC user mapping, if any). Called when a PUBLIC mapping is
// added or removed. Atomic: the new credentials are validated on a throwaway
// alias FIRST, so a failed re-attach never detaches a currently-working server.
void ReattachServer(ObjectId db_id, std::string_view server_name,
                    bool throw_on_error) {
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto server = snapshot->GetForeignServer(db_id, kSchema, server_name);
  if (!server) {
    return;
  }
  auto pub = snapshot->GetUserMapping(
    db_id, kSchema, catalog::MakeUserMappingName("public", server_name));
  auto sql = catalog::BuildForeignServerAttachSql(*server, pub.get());
  if (sql.empty()) {
    return;
  }

  auto conn = DuckDBEngine::Instance().CreateConnection();
  auto err = ProbeAttach(*server, pub.get());
  if (!err.empty()) {
    if (throw_on_error) {
      // Adding/changing a PUBLIC mapping: the new credentials must connect.
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
                      ERR_MSG("could not connect foreign server \"",
                              server_name, "\": ", err));
    }
    // Revert path (e.g. a PUBLIC mapping was dropped): the remaining
    // server-only credentials do not connect. DETACH so the live attachment
    // matches the catalog rather than leaving the just-dropped mapping's
    // credentials live.
    conn->Query(DetachSql(server_name));
    SDB_WARN(GENERAL, "Detached foreign server \"", server_name,
             "\" after its credentials stopped connecting: ", err);
    return;
  }

  // Credentials verified by the probe: swap the live attachment.
  conn->Query(DetachSql(server_name));
  auto attach_result = conn->Query(sql);
  if (attach_result->HasError()) {
    // The probe connected but the live re-ATTACH failed (e.g. a transient).
    // Surface it instead of silently leaving the server detached.
    if (throw_on_error) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
                      ERR_MSG("re-attach of foreign server \"", server_name,
                              "\" failed after a successful probe: ",
                              attach_result->GetError()));
    }
    SDB_WARN(GENERAL, "Re-attach of foreign server \"", server_name,
             "\" failed after a successful probe: ", attach_result->GetError());
  }
}

}  // namespace

void CreateForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                         std::string_view fdw_name, bool if_not_exists,
                         const duckdb::named_parameter_map_t& options) {
  auto db_id = conn_ctx.GetDatabaseId();

  auto& catalog = catalog::GetCatalog();
  if (catalog.GetCatalogSnapshot()->GetForeignServer(db_id, kSchema, name)) {
    if (if_not_exists) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("server \"", name, "\" already exists"));
  }

  auto [keys, values] = SplitOptions(options);

  auto server = std::make_shared<catalog::ForeignServer>(
    ObjectId{}, ObjectId{}, name, std::string{fdw_name}, std::move(keys),
    std::move(values));

  // Validate + attach first; only persist if the connection works, so a failed
  // CREATE SERVER leaves nothing behind.
  RunAttach(*server);

  auto r = catalog.CreateForeignServer(db_id, kSchema, server);
  if (!r.ok()) {
    RunDetach(name);
    if (r.is(ERROR_SERVER_DUPLICATE_NAME)) {
      if (if_not_exists) {
        return;
      }
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                      ERR_MSG("server \"", name, "\" already exists"));
    }
    // Any other persist failure (e.g. schema unresolved, write error) must
    // surface as itself, not be mislabelled "already exists".
    SDB_THROW(std::move(r));
  }
}

void DropForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                       bool missing_ok) {
  auto& catalog = catalog::GetCatalog();
  auto r =
    catalog.DropForeignServer(conn_ctx.GetDatabase(), kSchema, name, false);

  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (!missing_ok) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("server \"", name, "\" does not exist"));
    }
    return;
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }

  // Cascade: drop the server's user mappings. Otherwise they are orphaned, and
  // a future server created with the same name would silently inherit the old
  // PUBLIC mapping's credentials (boot replay / ReattachServer merges by name).
  auto db_id = conn_ctx.GetDatabaseId();
  for (const auto& m :
       catalog.GetCatalogSnapshot()->GetUserMappings(db_id, kSchema)) {
    if (m->GetServerName() == name) {
      // Best-effort cleanup; the server itself is already dropped.
      (void)catalog.DropUserMapping(conn_ctx.GetDatabase(), kSchema,
                                    std::string{m->GetName()}, false);
    }
  }

  RunDetach(name);
}

void CreateUserMapping(ConnectionContext& conn_ctx, std::string_view user,
                       std::string_view server, bool if_not_exists,
                       const duckdb::named_parameter_map_t& options) {
  auto db_id = conn_ctx.GetDatabaseId();
  const auto role = ResolveRole(conn_ctx, user);

  auto& catalog = catalog::GetCatalog();
  auto snapshot = catalog.GetCatalogSnapshot();
  auto server_obj = snapshot->GetForeignServer(db_id, kSchema, server);
  if (!server_obj) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("server \"", server, "\" does not exist"));
  }

  const auto name = catalog::MakeUserMappingName(role, server);
  if (snapshot->GetUserMapping(db_id, kSchema, name)) {
    if (if_not_exists) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("user mapping for \"", role, "\" on server \"",
                            server, "\" already exists"));
  }

  auto [keys, values] = SplitOptions(options);

  auto mapping = std::make_shared<catalog::UserMapping>(
    ObjectId{}, ObjectId{}, name, std::string{server}, role, std::move(keys),
    std::move(values));

  // A PUBLIC mapping drives the live (instance-global) attachment. Validate the
  // merged credentials on a throwaway alias BEFORE persisting, so a bad mapping
  // neither detaches the working server nor leaves a broken catalog row behind.
  if (role == "public") {
    auto err = ProbeAttach(*server_obj, mapping.get());
    if (!err.empty()) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
                      ERR_MSG("could not connect foreign server \"", server,
                              "\" with the new user mapping: ", err));
    }
  }

  auto r = catalog.CreateUserMapping(db_id, kSchema, mapping);
  if (!r.ok()) {
    if (if_not_exists && r.is(ERROR_SERVER_DUPLICATE_NAME)) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("user mapping for \"", role, "\" on server \"",
                            server, "\" already exists"));
  }

  // A PUBLIC mapping supplies the credentials the (instance-global) attachment
  // uses, so re-attach with the merged options. Per-user mappings are stored
  // and visible but do not change the live connection (Phase 2a).
  if (role == "public") {
    ReattachServer(db_id, server, /*throw_on_error=*/true);
  }
}

void DropUserMapping(ConnectionContext& conn_ctx, std::string_view user,
                     std::string_view server, bool missing_ok) {
  auto db_id = conn_ctx.GetDatabaseId();
  const auto role = ResolveRole(conn_ctx, user);
  const auto name = catalog::MakeUserMappingName(role, server);

  auto& catalog = catalog::GetCatalog();
  auto r =
    catalog.DropUserMapping(conn_ctx.GetDatabase(), kSchema, name, false);
  if (r.is(ERROR_SERVER_ILLEGAL_NAME)) {
    if (!missing_ok) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("user mapping for \"", role, "\" on server \"",
                              server, "\" does not exist"));
    }
    return;
  }
  if (!r.ok()) {
    SDB_THROW(std::move(r));
  }
  // Dropping a PUBLIC mapping reverts the attachment to the server's own creds.
  if (role == "public") {
    ReattachServer(db_id, server, /*throw_on_error=*/false);
  }
}

}  // namespace sdb::pg
