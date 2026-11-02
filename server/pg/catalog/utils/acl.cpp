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

#include "pg/catalog/utils/acl.h"

#include <absl/algorithm/container.h>
#include <absl/strings/ascii.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_cat.h>
#include <absl/strings/strip.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <ranges>
#include <tuple>

#include "auth/role_closure.h"

namespace sdb::pg {
namespace {

constexpr auto kAclModes = [] {
  std::array<char, kAclPrivileges.size()> modes{};
  for (size_t i = 0; i < modes.size(); ++i) {
    modes[i] = kAclPrivileges[i].chr;
  }
  return modes;
}();

bool IsAclNameChar(char c) {
  return absl::ascii_isalnum(static_cast<unsigned char>(c)) || c == '_' ||
         static_cast<unsigned char>(c) >= 0x80;
}

std::string ReadAclName(std::string_view& text) {
  std::string name;
  text = absl::StripLeadingAsciiWhitespace(text);
  bool quoted = false;
  size_t i = 0;
  for (;
       i < text.size() && (quoted || text[i] == '"' || IsAclNameChar(text[i]));
       ++i) {
    if (text[i] == '"') {
      if (!quoted) {
        quoted = true;
        continue;
      }
      if (i + 1 == text.size() || text[i + 1] != '"') {
        quoted = false;
        continue;
      }
      ++i;
    }
    name.push_back(text[i]);
  }
  text = absl::StripLeadingAsciiWhitespace(text.substr(i));
  return name;
}

irs::pg::SqlErrorData AclSyntaxError(std::string message,
                                     std::string_view hint = {}) {
  return SQL_ERROR_DATA(ERR_CODE(ERRCODE_INVALID_TEXT_REPRESENTATION),
                        ERR_MSG(std::move(message)), ERR_HINT(hint));
}

std::optional<duckdb::idx_t> AclRole(
  const auth::RoleGraph& roles, std::string_view name, bool oids,
  std::optional<irs::pg::SqlErrorData>& error) {
  if (const auto* role = roles.FindByName(name)) {
    return role->first;
  }
  if (duckdb::idx_t oid = 0; oids && absl::SimpleAtoi(name, &oid)) {
    return oid;
  }
  error = SQL_ERROR_DATA(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                         ERR_MSG("role \"", name, "\" does not exist"));
  return std::nullopt;
}

void PutId(std::string& out, std::string_view name) {
  const bool safe = absl::c_all_of(
    name, [](unsigned char c) { return absl::ascii_isalnum(c) || c == '_'; });
  if (safe) {
    out.append(name);
    return;
  }
  out.append(duckdb::KeywordHelper::WriteQuotedAndEscaped(name, '"'));
}

void PutRole(std::string& out, duckdb::idx_t role,
             const auth::RoleGraph& roles) {
  if (const auto name = roles.NameOf(role); !name.empty()) {
    PutId(out, name);
    return;
  }
  absl::StrAppend(&out, role);
}

}  // namespace

void AppendAcl(std::string& out, const duckdb::AclItem& item,
               const auth::RoleGraph& roles) {
  if (item.grantee != kPublicGrantee) {
    PutRole(out, item.grantee, roles);
  }
  out.push_back('=');
  for (const auto& p : kAclPrivileges) {
    if ((item.privs & p.mode) != duckdb::AclMode::NoRights) {
      out.push_back(p.chr);
      if ((item.grant_option & p.mode) != duckdb::AclMode::NoRights) {
        out.push_back('*');
      }
    }
  }
  out.push_back('/');
  PutRole(out, item.grantor, roles);
}

std::optional<irs::pg::SqlErrorData> ParseAcl(std::string_view text,
                                              const auth::RoleGraph& roles,
                                              bool oids,
                                              duckdb::AclItem& item) {
  auto grantee = ReadAclName(text);
  if (!text.starts_with('=')) {
    if (grantee != "group" && grantee != "user") {
      return AclSyntaxError(
        absl::StrCat("unrecognized key word: \"", grantee, "\""),
        "ACL key word must be \"group\" or \"user\".");
    }
    grantee = ReadAclName(text);
    if (grantee.empty()) {
      return AclSyntaxError(
        "missing name",
        "A name must follow the \"group\" or \"user\" key word.");
    }
    if (!text.starts_with('=')) {
      return AclSyntaxError("missing \"=\" sign");
    }
  }
  text.remove_prefix(1);
  item.privs = duckdb::AclMode::NoRights;
  item.grant_option = duckdb::AclMode::NoRights;
  auto read = duckdb::AclMode::NoRights;
  for (; !text.empty() &&
         (absl::ascii_isalpha(static_cast<unsigned char>(text.front())) ||
          text.front() == '*');
       text.remove_prefix(1)) {
    if (text.front() == '*') {
      item.grant_option |= read;
      continue;
    }
    const auto it = absl::c_find_if(kAclPrivileges, [&](const auto& privilege) {
      return privilege.chr == text.front();
    });
    if (it == kAclPrivileges.end()) {
      return AclSyntaxError(absl::StrCat(
        "invalid mode character: must be one of \"",
        std::string_view{kAclModes.data(), kAclModes.size()}, "\""));
    }
    read = it->mode;
    item.privs |= read;
  }
  std::optional<irs::pg::SqlErrorData> error;
  if (grantee.empty()) {
    item.grantee = kPublicGrantee;
  } else if (const auto role = AclRole(roles, grantee, oids, error)) {
    item.grantee = *role;
  } else {
    return error;
  }
  item.grantor = kRootUser;
  if (text.starts_with('/')) {
    text.remove_prefix(1);
    const auto grantor = ReadAclName(text);
    if (grantor.empty()) {
      return AclSyntaxError("a name must follow the \"/\" sign");
    }
    const auto role = AclRole(roles, grantor, oids, error);
    if (!role) {
      return error;
    }
    item.grantor = *role;
  }
  if (!absl::StripLeadingAsciiWhitespace(text).empty()) {
    return AclSyntaxError("extra garbage at the end of the ACL specification");
  }
  return std::nullopt;
}

void VisitDefaultAcls(
  duckdb::ClientContext& context, const duckdb::CatalogEntry& database,
  absl::FunctionRef<void(duckdb::idx_t oid, const duckdb::DefaultAcl& row)>
    visitor) {
  auto attached =
    duckdb::DatabaseManager::Get(context).GetDatabase(context, database.name);
  if (!attached || attached->oid != database.oid) {
    return;
  }
  auto& catalog = attached->GetCatalog().Cast<duckdb::DuckCatalog>();
  const auto view = catalog.GetCatalogTransaction(context).view;
  duckdb::idx_t oid = 0;
  for (const auto& row : database.permissions.defaults) {
    ++oid;
    if (row.scope == kInvalidOid ||
        catalog.GetOidIndex().GetVisible(row.scope, view)) {
      visitor(oid, row);
    }
  }
}

}  // namespace sdb::pg
