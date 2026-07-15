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

#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "basics/duckdb_engine.h"
#include "basics/log.h"
#include "catalog/catalog.h"
#include "catalog/foreign_server.h"
#include "catalog/role.h"
#include "catalog/user_mapping.h"
#include "pg/errcodes.h"
#include "pg/sql_exception_macro.h"

namespace sdb::pg {
namespace {

// "DETACH <ident>" -- built in several places (probe cleanup, the atomic
// re-attach swap, the revert path); the alias is double-quoted.
std::string DetachSql(std::string_view name) {
  return absl::StrCat("DETACH ", catalog::QuoteSqlIdentifier(name));
}

// Lower-case the option keys and stringify the values into the Options
// storage shared by ForeignServer and UserMapping.
catalog::Options MakeCatalogOptions(
  const duckdb::named_parameter_map_t& options) {
  std::vector<std::string> keys;
  std::vector<std::string> values;
  keys.reserve(options.size());
  values.reserve(options.size());
  for (const auto& [key, value] : options) {
    keys.push_back(absl::AsciiStrToLower(key.GetIdentifierName()));
    values.push_back(value.ToString());
  }
  return catalog::Options{std::move(keys), std::move(values)};
}

// Establish the live attachment for a server (validates connectivity too).
void RunAttach(const catalog::ForeignServer& server) {
  auto conn = DuckDBEngine::Instance().CreateConnection();
  const auto secret = catalog::MakeForeignServerSecretName(server.GetName());
  auto sql =
    catalog::PrepareForeignServerAttach(*conn->context, secret, server);
  if (sql.empty()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("foreign-data wrapper \"", server.GetFdwName(),
                            "\" is not supported"),
                    ERR_HINT("Use clickhouse_fdw or postgres_fdw."));
  }
  auto result = conn->Query(sql);
  catalog::DropForeignServerSecret(*conn->context, secret);
  if (result->HasError()) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
      ERR_MSG("could not connect foreign server \"", server.GetName(),
              "\": ", catalog::RedactConnstrSecrets(result->GetError())));
  }
}

void RunDetach(std::string_view name) {
  catalog::DetachForeignServerAttachment(name);
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
  auto conn = DuckDBEngine::Instance().CreateConnection();
  const auto secret = catalog::MakeForeignServerSecretName(alias);
  auto sql = catalog::PrepareForeignServerAttach(*conn->context, secret, server,
                                                 pub, alias);
  if (sql.empty()) {
    return {};  // unsupported FDW is reported elsewhere; nothing to probe
  }
  // Clear any stale probe left by a crash between attach and detach.
  conn->Query(DetachSql(alias));
  auto result = conn->Query(sql);
  std::string err;
  if (result->HasError()) {
    // Redact here so every caller that surfaces this string (error or log) is
    // covered -- the postgres connector echoes the full DSN, password included.
    err = catalog::RedactConnstrSecrets(result->GetError());
  } else {
    conn->Query(DetachSql(alias));
  }
  catalog::DropForeignServerSecret(*conn->context, secret);
  return err;
}

// Re-establish a server's attachment with current credentials (server OPTIONS
// merged with its PUBLIC user mapping, if any). Called when a PUBLIC mapping is
// added or removed. Atomic: the new credentials are validated on a throwaway
// alias FIRST, so a failed re-attach never detaches a currently-working server.
void ReattachServer(ObjectId db_id, std::string_view server_name,
                    bool throw_on_error) {
  auto snapshot = catalog::GetCatalog().GetCatalogSnapshot();
  auto server = snapshot->GetForeignServer(db_id, server_name);
  if (!server) {
    return;
  }
  auto pub = snapshot->GetUserMapping(server->GetId(), "public");

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

  // Credentials verified by the probe: swap the live attachment under a
  // freshly-registered secret.
  const auto secret = catalog::MakeForeignServerSecretName(server_name);
  auto sql = catalog::PrepareForeignServerAttach(*conn->context, secret,
                                                 *server, pub.get());
  if (sql.empty()) {
    return;
  }
  conn->Query(DetachSql(server_name));
  auto attach_result = conn->Query(sql);
  catalog::DropForeignServerSecret(*conn->context, secret);
  if (attach_result->HasError()) {
    // The probe connected but the live re-ATTACH failed (e.g. a transient).
    // Surface it instead of silently leaving the server detached.
    auto err = catalog::RedactConnstrSecrets(attach_result->GetError());
    if (throw_on_error) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_CONNECTION_EXCEPTION),
                      ERR_MSG("re-attach of foreign server \"", server_name,
                              "\" failed after a successful probe: ", err));
    }
    SDB_WARN(GENERAL, "Re-attach of foreign server \"", server_name,
             "\" failed after a successful probe: ", err);
  }
}

}  // namespace

void CreateForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                         std::string_view fdw_name, bool if_not_exists,
                         const duckdb::named_parameter_map_t& options) {
  auto db_id = conn_ctx.GetDatabaseId();

  auto& catalog = catalog::GetCatalog();
  // Authorize BEFORE connecting the remote: RunAttach below creates an
  // instance-global DuckDB attachment, and the in-catalog gate throws (not a
  // Result), so a denied CREATE that attached first would orphan that
  // attachment past the cleanup branch. Checking here also avoids connecting a
  // remote on behalf of a caller who lacks the privilege.
  catalog.RequireCreateForeignServer(catalog::ActingAs(conn_ctx.GetRoleId()),
                                     db_id);
  if (catalog.GetCatalogSnapshot()->GetForeignServer(db_id, name)) {
    if (if_not_exists) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("server \"", name, "\" already exists"));
  }

  // Owner = the creating role; the default ACL then gives the owner USAGE and
  // the public nothing (auth::ClassPrivs/PublicDefaultPrivs).
  auto server = std::make_shared<catalog::ForeignServer>(
    catalog::Permissions{conn_ctx.GetRoleId()}, ObjectId{}, ObjectId{}, name,
    std::string{fdw_name}, MakeCatalogOptions(options));

  // Validate + attach first; only persist if the connection works, so a failed
  // CREATE SERVER leaves nothing behind. Any persist failure (duplicate name,
  // schema unresolved, write error) throws out of the catalog -- detach before
  // it surfaces so the failed CREATE leaves no live attachment either.
  RunAttach(*server);

  try {
    if (!catalog.CreateForeignServer(catalog::ActingAs(conn_ctx.GetRoleId()),
                                     db_id, server, if_not_exists)) {
      // A concurrent CREATE SERVER won the race past the pre-check above.
      RunDetach(name);
    }
  } catch (...) {
    RunDetach(name);
    throw;
  }
}

void DropForeignServer(ConnectionContext& conn_ctx, std::string_view name,
                       bool missing_ok, bool cascade) {
  auto& catalog = catalog::GetCatalog();
  // The catalog drops the server and, under CASCADE, its user mappings in one
  // atomic transaction (server<-mapping dependency). RESTRICT (the default)
  // throws DEPENDENT_OBJECTS_STILL_EXIST while any mapping still depends on
  // the server; absent + missing_ok returns false.
  if (!catalog.DropForeignServer(catalog::ActingAs(conn_ctx.GetRoleId()),
                                 conn_ctx.GetDatabase(), name, cascade,
                                 missing_ok)) {
    return;
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
  auto server_obj = snapshot->GetForeignServer(db_id, server);
  if (!server_obj) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("server \"", server, "\" does not exist"));
  }

  // A non-PUBLIC mapping is FOR an RBAC role, which must exist -- link it by id
  // so DROP ROLE is refused while the mapping references it (PG semantics).
  ObjectId role_id;
  if (role != "public") {
    auto role_obj = snapshot->GetRole(role);
    if (!role_obj) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("role \"", role, "\" does not exist"));
    }
    role_id = role_obj->GetId();
  }

  // The mapping's catalog name IS the mapped role; parent = the server (PG's
  // (umuser, umserver) identity).
  if (snapshot->GetUserMapping(server_obj->GetId(), role)) {
    if (if_not_exists) {
      return;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_DUPLICATE_OBJECT),
                    ERR_MSG("user mapping for \"", role, "\" on server \"",
                            server, "\" already exists"));
  }

  // A mapping's authority follows its server (PG): stamp the server's owner.
  auto mapping = std::make_shared<catalog::UserMapping>(
    catalog::Permissions{server_obj->GetOwner()}, server_obj->GetId(),
    ObjectId{}, role, std::string{server}, role, MakeCatalogOptions(options),
    server_obj->GetId(), role_id);

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

  if (!catalog.CreateUserMapping(catalog::ActingAs(conn_ctx.GetRoleId()), db_id,
                                 mapping, if_not_exists)) {
    // A concurrent CREATE USER MAPPING won the race past the pre-check above.
    return;
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

  auto& catalog = catalog::GetCatalog();
  if (!catalog.DropUserMapping(catalog::ActingAs(conn_ctx.GetRoleId()),
                               conn_ctx.GetDatabase(), server, role,
                               /*cascade=*/false)) {
    if (!missing_ok) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("user mapping for \"", role, "\" on server \"",
                              server, "\" does not exist"));
    }
    return;
  }
  // Dropping a PUBLIC mapping reverts the attachment to the server's own creds.
  if (role == "public") {
    ReattachServer(db_id, server, /*throw_on_error=*/false);
  }
}

}  // namespace sdb::pg
