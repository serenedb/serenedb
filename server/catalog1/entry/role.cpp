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

#include "catalog1/entry/role.h"

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/alter_table_info.hpp>
#include <utility>

namespace sdb::catalog {

duckdb::unique_ptr<duckdb::CreateInfo> CreateRoleInfo::Copy() const {
  auto result = duckdb::make_uniq<CreateRoleInfo>();
  CopyProperties(*result);
  result->options = options;
  result->conn_limit = conn_limit;
  result->valid_until = valid_until;
  result->password = password;
  result->member_of = member_of;
  result->config = config;
  return std::move(result);
}

std::string CreateRoleInfo::ToString() const {
  return absl::StrCat("CREATE ROLE ",
                      duckdb::KeywordHelper::WriteOptionallyQuoted(
                        qualified_name.Name().GetIdentifierName()),
                      ";");
}

RoleCatalogEntry::RoleCatalogEntry(duckdb::Catalog& catalog,
                                   CreateRoleInfo& info)
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
  auto info = duckdb::make_uniq<CreateRoleInfo>();
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
  return duckdb::make_uniq<RoleCatalogEntry>(catalog,
                                             info->Cast<CreateRoleInfo>());
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
  const auto& alter = info.Cast<duckdb::AlterRoleInfo>();
  auto copy = GetInfo();
  auto& next = copy->Cast<CreateRoleInfo>();
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
    std::erase_if(next.config, [&](const std::string& entry) {
      return ConfigKey(entry) == key;
    });
  }
  for (const auto& entry : alter.set_config) {
    const auto key = ConfigKey(entry);
    auto it = std::ranges::find_if(
      next.config, [&](const std::string& e) { return ConfigKey(e) == key; });
    if (it == next.config.end()) {
      next.config.push_back(entry);
    } else {
      *it = entry;
    }
  }
  if (alter.grant_role_id != 0) {
    auto it =
      std::ranges::find(next.member_of, alter.grant_role_id, &Membership::role);
    if (alter.revoke && !alter.option_only) {
      if (it != next.member_of.end()) {
        next.member_of.erase(it);
      }
    } else if (!alter.revoke || it != next.member_of.end()) {
      auto edge =
        it != next.member_of.end()
          ? *it
          : Membership{
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
        next.member_of.push_back(edge);
      } else {
        *it = edge;
      }
    }
  }
  return duckdb::make_uniq<RoleCatalogEntry>(catalog, next);
}

std::string RoleCatalogEntry::ToSQL() const { return GetInfo()->ToString(); }

}  // namespace sdb::catalog
