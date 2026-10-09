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

#include "catalog/entry/role.h"

#include <algorithm>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/common/exception.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <string_view>
#include <utility>

#include "auth/role_closure.h"
#include "connector/duckdb_client_state.h"
#include "network/credentials.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"

namespace sdb::catalog {

void RequireUnreservedRoleName(const duckdb::Identifier& name) {
  if (name.GetIdentifierName().starts_with("pg_")) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_RESERVED_NAME),
      ERR_MSG("role name \"", name.GetIdentifierName(), "\" is reserved"),
      ERR_DETAIL("Role names starting with \"pg_\" are reserved."));
  }
}

std::string StoredPassword(std::string_view password) {
  if (network::IsScramVerifier(password) || network::IsMd5Verifier(password)) {
    return std::string{password};
  }
  auto verifier = network::BuildScramVerifierString(password);
  if (!verifier) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("could not hash the password"));
  }
  return *verifier;
}

duckdb::idx_t GrantorOfMembership(duckdb::ClientContext& context) {
  auto* session = connector::GetSereneDBContextPtr(context);
  if (!session ||
      auth::ClosureFor(&context, session->GetRoleId())->is_superuser) {
    return pg::kRootUser;
  }
  return session->GetRoleId();
}

RoleCatalogEntry::RoleCatalogEntry(duckdb::Catalog& catalog,
                                   duckdb::CreateRoleInfo& info)
  : duckdb::InCatalogEntry{duckdb::CatalogType::ROLE_ENTRY, catalog,
                           info.GetQualifiedName().Name(), info.oid},
    _options{info.options},
    _conn_limit{info.conn_limit},
    _valid_until{info.valid_until},
    _password{info.password},
    _member_of{info.member_of},
    _config{info.config} {
  comment = info.comment;
  tags = info.tags;
  permissions = info.permissions;
}

duckdb::unique_ptr<duckdb::CreateInfo> RoleCatalogEntry::GetInfo() const {
  auto info = duckdb::make_uniq<duckdb::CreateRoleInfo>();
  info->SetName(name);
  info->options = _options;
  info->conn_limit = _conn_limit;
  info->valid_until = _valid_until;
  info->password = _password;
  info->member_of = _member_of;
  info->config = _config;
  info->comment = comment;
  info->tags = tags;
  return std::move(info);
}

duckdb::unique_ptr<duckdb::CatalogEntry> RoleCatalogEntry::Copy(
  duckdb::ClientContext& context) const {
  auto info = GetInfo();
  return duckdb::make_uniq<RoleCatalogEntry>(
    catalog, info->Cast<duckdb::CreateRoleInfo>());
}

namespace {

std::string_view ConfigKey(std::string_view entry) {
  return entry.substr(0, entry.find('='));
}

}  // namespace

duckdb::unique_ptr<duckdb::CatalogEntry> RoleCatalogEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::ALTER_ROLE) {
    return duckdb::InCatalogEntry::AlterEntry(context, info);
  }
  auto& alter = info.Cast<duckdb::AlterRoleInfo>();
  if (!alter.new_name.empty()) {
    RequireUnreservedRoleName(alter.new_name);
    auto* session = connector::GetSereneDBContextPtr(context);
    if (session &&
        (oid == session->GetRoleId() || oid == session->GetSessionRoleId())) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                      ERR_MSG("session user cannot be renamed"));
    }
    if (oid == pg::kRootUser) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_OBJECT_IN_USE),
        ERR_MSG("cannot rename role \"", name.GetIdentifierName(),
                "\" because it is required by the database system"));
    }
  }
  if (alter.set_password) {
    alter.password =
      alter.null_password ? std::string{} : StoredPassword(alter.password);
  }
  if (!alter.grant_role.empty() && alter.grant_role_id == 0) {
    const duckdb::Identifier granted{alter.grant_role};
    auto target = catalog.Cast<duckdb::DuckCatalog>()
                    .GetCatalogSet(duckdb::CatalogType::ROLE_ENTRY)
                    .GetEntry(catalog.GetCatalogTransaction(context), granted);
    if (!target) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
        ERR_MSG("role \"", granted.GetIdentifierName(), "\" does not exist"));
    }
    if (!alter.revoke &&
        (oid == target->oid ||
         auth::ComputeRoleClosure(*auth::RolesOf(&context), target->oid)
           .IsMember(oid))) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_GRANT_OPERATION),
        ERR_MSG("role \"", target->name.GetIdentifierName(),
                "\" is a member of role \"", name.GetIdentifierName(), "\""));
    }
    alter.grant_role_id = target->oid;
    if (!alter.grantor_id) {
      alter.grantor_id = GrantorOfMembership(context);
    }
  }
  auto copy = GetInfo();
  auto& next = copy->Cast<duckdb::CreateRoleInfo>();
  next.options = (next.options | alter.set_options) & ~alter.clear_options;
  if (alter.set_password) {
    next.password = alter.password;
  }
  if (alter.set_conn_limit) {
    next.conn_limit = alter.conn_limit;
  }
  if (alter.set_valid_until) {
    next.valid_until = alter.valid_until;
  }
  if (!alter.new_name.empty()) {
    next.SetName(alter.new_name);
  }
  if (alter.reset_all_config) {
    next.config.clear();
  }
  for (const auto& key : alter.reset_config) {
    std::erase_if(next.config, [&](std::string_view entry) {
      return ConfigKey(entry) == key;
    });
  }
  for (const auto& entry : alter.set_config) {
    const auto key = ConfigKey(entry);
    auto it = std::ranges::find_if(
      next.config, [&](std::string_view e) { return ConfigKey(e) == key; });
    if (it == next.config.end()) {
      next.config.emplace_back(entry);
    } else {
      *it = entry;
    }
  }
  if (alter.grant_role_id != 0) {
    auto it = std::ranges::find(next.member_of, alter.grant_role_id,
                                &duckdb::Membership::role);
    if (alter.revoke && !alter.option_only) {
      if (it != next.member_of.end()) {
        next.member_of.erase(it);
      }
    } else if (!alter.revoke || it != next.member_of.end()) {
      auto edge =
        it != next.member_of.end()
          ? *it
          : duckdb::Membership{
              .role = alter.grant_role_id,
              .grantor = alter.grantor_id,
              .admin_option = false,
              .inherit_option = HasOption(next.options, RoleOption::Inherit),
              .set_option = true,
            };
      if (alter.admin_option != -1) {
        edge.admin_option = alter.admin_option == 1;
      }
      if (alter.inherit_option != -1) {
        edge.inherit_option = alter.inherit_option == 1;
      }
      if (alter.set_option != -1) {
        edge.set_option = alter.set_option == 1;
      }
      if (it == next.member_of.end()) {
        next.member_of.emplace_back(edge);
      } else {
        *it = edge;
      }
    }
  }
  return duckdb::make_uniq<RoleCatalogEntry>(catalog, next);
}

}  // namespace sdb::catalog
