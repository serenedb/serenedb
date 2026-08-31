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

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <duckdb/parser/parsed_data/alter_info.hpp>
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
                           info.GetQualifiedName().Name()},
    _options{info.options},
    _conn_limit{info.conn_limit},
    _valid_until{info.valid_until},
    _password{info.password},
    _member_of{info.member_of},
    _config{info.config} {
  comment = info.comment;
  tags = info.tags;
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

// ALTER ROLE produces a new version through the same CatalogSet machinery a
// table ALTER uses, so a rolled back ALTER leaves the previous version rooted
// and a concurrent writer gets a write-write conflict.
duckdb::unique_ptr<duckdb::CatalogEntry> RoleCatalogEntry::AlterEntry(
  duckdb::CatalogTransaction transaction, duckdb::AlterInfo& info) {
  if (info.type != duckdb::AlterType::SET_COMMENT) {
    throw duckdb::NotImplementedException(
      "Unsupported ALTER for a role: %s",
      duckdb::EnumUtil::ToString(info.type));
  }
  auto create_info = GetInfo();
  auto& role_info = create_info->Cast<CreateRoleInfo>();
  role_info.comment = info.Cast<duckdb::SetCommentInfo>().comment_value;
  return duckdb::make_uniq<RoleCatalogEntry>(catalog, role_info);
}

std::string RoleCatalogEntry::ToSQL() const { return GetInfo()->ToString(); }

}  // namespace sdb::catalog
