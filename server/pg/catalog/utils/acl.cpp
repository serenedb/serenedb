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
#include <absl/strings/str_cat.h>

#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/duck_catalog.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <ranges>
#include <tuple>

#include "auth/role_closure.h"

namespace sdb::pg {
namespace {

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
  for (const auto& [oid, row] :
       std::views::zip(std::views::iota(duckdb::idx_t{1}),
                       database.permissions.defaults) |
         std::views::filter([&](const auto& item) {
           const auto scope = std::get<1>(item).scope;
           return scope == kInvalidOid ||
                  catalog.GetOidIndex().GetVisible(scope, view);
         })) {
    visitor(oid, row);
  }
}

}  // namespace sdb::pg
