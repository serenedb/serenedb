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

#include "catalog/entry/foreign_server.h"

#include <absl/algorithm/container.h>
#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/str_join.h>
#include <absl/strings/str_replace.h>
#include <absl/strings/strip.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/parsed_data/attach_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <span>
#include <string>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "catalog/boot.h"

namespace sdb::catalog {
namespace {

constexpr auto kClickHouseOptions = std::to_array<std::string_view>({
  "host",
  "hostname",
  "port",
  "user",
  "username",
  "password",
  "passwd",
  "database",
  "dbname",
  "db",
  "secure",
  "ssl",
  "compression",
});

constexpr auto kPostgresOptions = std::to_array<std::string_view>({
  "host",
  "hostname",
  "hostaddr",
  "port",
  "dbname",
  "database",
  "user",
  "username",
  "password",
  "connect_timeout",
  "client_encoding",
  "options",
  "application_name",
  "keepalives",
  "keepalives_idle",
  "keepalives_interval",
  "keepalives_count",
  "tcp_user_timeout",
  "sslmode",
  "sslnegotiation",
  "sslcompression",
  "sslsni",
  "ssl_min_protocol_version",
  "ssl_max_protocol_version",
  "min_protocol_version",
  "max_protocol_version",
  "target_session_attrs",
  "load_balance_hosts",
  "channel_binding",
  "schema",
  "isolation_level",
  "use_text_protocol",
});

std::string ConnectionString(const ServerOptions& options) {
  return absl::StrJoin(options, " ", [](std::string* out, const auto& option) {
    absl::StrAppend(
      out, option.first, "='",
      absl::StrReplaceAll(option.second, {{"\\", "\\\\"}, {"'", "\\'"}}), "'");
  });
}

bool OwnedBySuperuser(duckdb::ClientContext& context, duckdb::idx_t owner) {
  return owner == 0 || auth::ClosureFor(&context, owner)->is_superuser;
}

}  // namespace

void ForeignServerCatalogEntry::CheckOwner(duckdb::ClientContext& context,
                                           duckdb::idx_t owner) const {
  if (OwnedBySuperuser(context, owner)) {
    return;
  }
  const std::string_view type = absl::StripSuffix(_fdw_name, "_fdw");
  std::span<const std::string_view> allowed;
  if (type == "postgres") {
    allowed = kPostgresOptions;
  } else if (type == "clickhouse") {
    allowed = kClickHouseOptions;
  } else {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
      ERR_MSG("permission denied for foreign-data wrapper ", _fdw_name),
      ERR_DETAIL("Only roles with the SUPERUSER attribute may "
                 "own a ",
                 _fdw_name, " server."));
  }
  bool password = false;
  for (const auto& [key, value] : _options) {
    if (!absl::c_linear_search(allowed, key)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INSUFFICIENT_PRIVILEGE),
                      ERR_MSG("permission denied to use option \"", key, "\""),
                      ERR_DETAIL("Only roles with the SUPERUSER attribute may "
                                 "own a server that sets it."));
    }
    password |= (key == "password" || key == "passwd") && !value.empty();
  }
  if (!password) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_S_R_E_PROHIBITED_SQL_STATEMENT_ATTEMPTED),
      ERR_MSG("password is required"),
      ERR_DETAIL("A server owned by a role without the SUPERUSER attribute "
                 "must set a password in its options."));
  }
}

void ForeignServerCatalogEntry::Attach(duckdb::ClientContext& context) const {
  CheckOwner(context, permissions.owner);
  const bool restricted = !OwnedBySuperuser(context, permissions.owner);
  const std::string_view type = absl::StripSuffix(_fdw_name, "_fdw");
  duckdb::AttachInfo info;
  info.name = name;
  auto access_mode = duckdb::AccessMode::READ_WRITE;
  if (type == "clickhouse") {
    info.path = ConnectionString(_options);
  } else {
    for (const auto& [key, value] : _options) {
      info.options.emplace(key, duckdb::Value{value});
    }
    if (type == "iceberg") {
      auto warehouse = info.options.extract("warehouse");
      info.path = warehouse ? warehouse.mapped().ToString() : "";
    } else if (type == "duckdb" || type == "serenedb") {
      auto path = info.options.extract("path");
      if (!path) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
                        ERR_MSG("option \"path\" is required for ", _fdw_name));
      }
      info.path = path.mapped().ToString();
      access_mode = type == "serenedb" ? duckdb::AccessMode::READ_ONLY
                                       : duckdb::AccessMode::AUTOMATIC;
    } else if (type == "postgres" && restricted) {
      info.options.insert_or_assign(
        "require_auth", duckdb::Value{"password,md5,scram-sha-256"});
    }
  }
  absl::Cleanup restore_secrets =
    [&context, hidden = std::exchange(context.hide_secrets,
                                      context.hide_secrets || restricted)] {
      context.hide_secrets = hidden;
    };
  catalog::Attach(context, info, type, access_mode,
                  duckdb::AttachVisibility::SHOWN);
}

void ForeignServerCatalogEntry::OnDrop() {
  if (catalog.GetAttached().IsReadOnly()) {
    return;
  }
  duckdb::DatabaseManager::Get(catalog.GetDatabase()).DetachInternal(name);
}

ForeignServerCatalogEntry::ForeignServerCatalogEntry(
  duckdb::Catalog& catalog, duckdb::CreateForeignServerInfo& info)
  : duckdb::InCatalogEntry{duckdb::CatalogType::FOREIGN_SERVER_ENTRY, catalog,
                           info.GetQualifiedName().Name(), info.oid},
    _server_type{info.server_type},
    _version{info.version},
    _fdw_name{info.fdw_name},
    _options{info.options} {
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> ForeignServerCatalogEntry::GetInfo()
  const {
  auto info = duckdb::make_uniq<duckdb::CreateForeignServerInfo>();
  info->SetName(name);
  info->server_type = _server_type;
  info->version = _version;
  info->fdw_name = _fdw_name;
  info->options = _options;
  SDB_IF_FAILURE("foreign_server_without_database_option") {
    info->options.erase("database");
  }
  SDB_IF_FAILURE("foreign_server_with_unknown_option") {
    info->options.emplace("option_from_a_newer_release", "on");
  }
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> ForeignServerCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<ForeignServerCatalogEntry>(
    catalog, info->Cast<duckdb::CreateForeignServerInfo>());
}

}  // namespace sdb::catalog
