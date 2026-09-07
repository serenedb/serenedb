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
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <utility>

namespace sdb::catalog {

duckdb::unique_ptr<duckdb::CreateInfo> CreateRoleInfo::Copy() const {
  auto result = duckdb::make_uniq<CreateRoleInfo>();
  CopyProperties(*result);
  result->oid = oid;
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
                           info.GetQualifiedName().Name()},
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

AlterRoleInfo::AlterRoleInfo(duckdb::Identifier name)
  : duckdb::AlterInfo{
      duckdb::AlterType::ALTER_ROLE,
      duckdb::QualifiedName{duckdb::Identifier{}, duckdb::Identifier{},
                            std::move(name)},
      duckdb::OnEntryNotFound::THROW_EXCEPTION} {}

duckdb::unique_ptr<duckdb::AlterInfo> AlterRoleInfo::Copy() const {
  auto result = duckdb::make_uniq<AlterRoleInfo>(qualified_name.Name());
  result->set_options = set_options;
  result->clear_options = clear_options;
  result->password = password;
  result->conn_limit = conn_limit;
  result->valid_until = valid_until;
  result->new_name = new_name;
  result->reset_all_config = reset_all_config;
  result->reset_config = reset_config;
  result->set_config = set_config;
  result->upsert_member_of = upsert_member_of;
  result->remove_member_of = remove_member_of;
  return std::move(result);
}

std::string AlterRoleInfo::ToString() const {
  return absl::StrCat("ALTER ROLE ",
                      duckdb::KeywordHelper::WriteOptionallyQuoted(
                        qualified_name.Name().GetIdentifierName()),
                      ";");
}

void AlterRoleInfo::Serialize(duckdb::Serializer& serializer) const {
  duckdb::AlterInfo::Serialize(serializer);
}

duckdb::unique_ptr<duckdb::CatalogEntry> RoleCatalogEntry::AlterEntry(
  duckdb::ClientContext& context, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::ALTER_ROLE) {
    return duckdb::InCatalogEntry::AlterEntry(context, info);
  }
  const auto& alter = static_cast<const AlterRoleInfo&>(info);
  auto copy = GetInfo();
  auto& next = copy->Cast<CreateRoleInfo>();
  next.options = (next.options | alter.set_options) & ~alter.clear_options;
  if (alter.password) {
    next.password = *alter.password;
  }
  if (alter.conn_limit) {
    next.conn_limit = *alter.conn_limit;
  }
  if (alter.valid_until) {
    next.valid_until = *alter.valid_until;
  }
  if (alter.new_name) {
    next.SetName(*alter.new_name);
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
  for (const auto& edge : alter.upsert_member_of) {
    auto it = std::ranges::find(next.member_of, edge.role, &Membership::role);
    if (it == next.member_of.end()) {
      next.member_of.push_back(edge);
    } else {
      *it = edge;
    }
  }
  for (const auto role : alter.remove_member_of) {
    std::erase_if(next.member_of,
                  [&](const Membership& edge) { return edge.role == role; });
  }
  return duckdb::make_uniq<RoleCatalogEntry>(catalog, next);
}

std::string RoleCatalogEntry::ToSQL() const { return GetInfo()->ToString(); }

}  // namespace sdb::catalog
