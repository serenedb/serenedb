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

#include <array>
#include <duckdb/catalog/catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/main/attached_database.hpp>
#include <duckdb/main/database_manager.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <ranges>
#include <span>
#include <tuple>

#include "catalog/base_catalog.h"
#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "catalog/schema.h"
#include "pg/catalog/functions/pg_catalog/objectaddress.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"

namespace sdb::pg {
namespace {

constexpr auto kAclModes = [] {
  std::array<char, std::size(duckdb::ACL_PRIVILEGES)> modes{};
  for (size_t i = 0; i < modes.size(); ++i) {
    modes[i] = duckdb::ACL_PRIVILEGES[i].letter;
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
  const duckdb::SecuritySnapshot& roles, std::string_view name, bool oids,
  std::optional<irs::pg::SqlErrorData>& error) {
  if (const auto role = roles.FindByName(name); role.IsValid()) {
    return role.GetIndex();
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
             const duckdb::SecuritySnapshot& roles) {
  if (const auto name = roles.NameOf(role); !name.empty()) {
    PutId(out, name);
    return;
  }
  absl::StrAppend(&out, role);
}

std::string_view DefaultAclObjects(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return "sequences";
    case duckdb::CatalogType::MACRO_ENTRY:
      return "functions";
    case duckdb::CatalogType::TYPE_ENTRY:
      return "types";
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return "schemas";
    default:
      return "relations";
  }
}

}  // namespace

void AppendAcl(std::string& out, const duckdb::AclItem& item,
               const duckdb::SecuritySnapshot& roles) {
  if (item.grantee != kPublicGrantee) {
    PutRole(out, item.grantee, roles);
  }
  out.push_back('=');
  for (const auto& p : duckdb::ACL_PRIVILEGES) {
    if ((item.privs & p.mode) != duckdb::AclMode::NoRights) {
      out.push_back(p.letter);
      if ((item.grant_option & p.mode) != duckdb::AclMode::NoRights) {
        out.push_back('*');
      }
    }
  }
  out.push_back('/');
  PutRole(out, item.grantor, roles);
}

std::optional<irs::pg::SqlErrorData> ParseAcl(
  std::string_view text, const duckdb::SecuritySnapshot& roles, bool oids,
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
    const auto it = absl::c_find_if(
      duckdb::ACL_PRIVILEGES,
      [&](const auto& privilege) { return privilege.letter == text.front(); });
    if (it == std::end(duckdb::ACL_PRIVILEGES)) {
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
  auto& catalog = attached->GetCatalog().Cast<catalog::BaseCatalog>();
  const auto transaction = catalog.GetCatalogTransaction(context);
  duckdb::idx_t oid = 0;
  for (const auto& row : database.permissions.defaults) {
    ++oid;
    if (row.scope == kInvalidOid || catalog.LookupOid(transaction, row.scope)) {
      visitor(oid, row);
    }
  }
}

void VisitRoleReferences(
  duckdb::ClientContext& context, duckdb::idx_t role,
  absl::FunctionRef<void(const RoleReference& reference,
                         absl::FunctionRef<std::string()> describe)>
    visitor) {
  using enum duckdb::CatalogType;
  const auto names = [&](std::span<const duckdb::AclItem> acl) {
    return absl::c_any_of(acl, [&](const duckdb::AclItem& item) {
      return item.grantee == role || item.grantor == role;
    });
  };
  const auto use = [&](const duckdb::Permissions& permissions) -> char {
    if (permissions.owner == role) {
      return 'o';
    }
    return names(permissions.acl) ? 'a' : '\0';
  };
  auto& cluster = catalog::ClusterOf(context);
  const auto cluster_transaction = cluster.GetCatalogTransaction(context);
  for (const auto& db :
       catalog::SereneDBDatabases(duckdb::DatabaseManager::Get(context))) {
    auto& database = db->GetCatalog().Cast<catalog::SereneDBCatalog>();
    const auto transaction = database.GetCatalogTransaction(context);
    const auto entry = [&](duckdb::CatalogEntry& object) {
      if (object.type == INDEX_ENTRY || object.type == ROLE_ENTRY) {
        return;
      }
      if (object.type == MACRO_ENTRY || object.type == TABLE_MACRO_ENTRY) {
        const auto& macro = object.Cast<duckdb::MacroCatalogEntry>();
        for (const auto& overload : macro.macros) {
          if (const auto deptype = use(macro.OverloadPermissions(*overload))) {
            const auto oid = macro.OverloadOid(*overload);
            visitor({db->oid, kPgProcTable, oid, 0, deptype}, [&] {
              return DescribeObject(context, kPgProcTable, oid, 0);
            });
          }
        }
        return;
      }
      const auto classid = duckdb::ObjectKindOf(object.type).pg_class;
      if (const auto deptype = use(object.permissions)) {
        visitor({db->oid, classid, object.oid, 0, deptype},
                [&] { return DescribeObject(context, object); });
        if (deptype == 'o') {
          return;
        }
      }
      const auto column = [&](int32_t attnum) {
        visitor({db->oid, classid, object.oid, attnum, 'a'}, [&] {
          return DescribeObject(context, classid, object.oid, attnum);
        });
      };
      if (object.type == VIEW_ENTRY) {
        auto& view = object.Cast<duckdb::ViewCatalogEntry>();
        for (const auto& acl : view.column_acls) {
          if (!names(acl.acl)) {
            continue;
          }
          if (const auto attnum = ViewAttnum(context, view, acl.column)) {
            column(attnum);
          }
        }
      } else if (object.type == TABLE_ENTRY) {
        for (const auto& definition :
             object.Cast<duckdb::TableCatalogEntry>().GetColumns().Logical()) {
          if (names(definition.Acl())) {
            column(Attnum(definition));
          }
        }
      }
    };
    database.ScanEntries(transaction, entry);
    const auto record =
      cluster.Set(DATABASE_ENTRY).Get(cluster_transaction, db->GetName());
    if (!record) {
      continue;
    }
    VisitDefaultAcls(
      context, *record, [&](duckdb::idx_t oid, const duckdb::DefaultAcl& row) {
        const char deptype = row.role == role ? 'o' : names(row.acl) ? 'a' : 0;
        if (!deptype) {
          return;
        }
        visitor({record->oid, kPgDefaultAclTable, oid, 0, deptype}, [&] {
          auto object =
            absl::StrCat("default privileges on new ",
                         DefaultAclObjects(row.objtype), " belonging to role ",
                         catalog::RolesOf(&context)->NameOf(row.role));
          if (row.scope != kInvalidOid) {
            absl::StrAppend(&object, " in schema ",
                            database.LookupOid(transaction, row.scope)
                              ->name.GetIdentifierName());
          }
          return object;
        });
      });
    for (const auto& row : record->permissions.Rows().rows) {
      if (!names(row.acl)) {
        continue;
      }
      const auto classid = duckdb::ObjectKindOf(row.kind).pg_class;
      visitor({record->oid, classid, row.oid, 0, 'a'},
              [&] { return DescribeObject(context, classid, row.oid, 0); });
    }
  }
  cluster.Set(DATABASE_ENTRY)
    .Scan(cluster_transaction, [&](duckdb::CatalogEntry& database) {
      if (const auto deptype = use(database.permissions)) {
        visitor({kInvalidOid, kPgDatabaseTable, database.oid, 0, deptype}, [&] {
          return absl::StrCat("database ", database.name.GetIdentifierName());
        });
      }
    });
  cluster.Set(TABLESPACE_ENTRY)
    .Scan(cluster_transaction, [&](duckdb::CatalogEntry& tablespace) {
      if (const auto deptype = use(tablespace.permissions)) {
        visitor({kInvalidOid, kPgTablespaceTable, tablespace.oid, 0, deptype},
                [&] {
                  return absl::StrCat("tablespace ",
                                      tablespace.name.GetIdentifierName());
                });
      }
    });
  cluster.Set(PARAMETER_ENTRY)
    .Scan(cluster_transaction, [&](duckdb::CatalogEntry& parameter) {
      if (names(parameter.permissions.acl)) {
        visitor({kInvalidOid, kPgParameterAclTable, parameter.oid, 0, 'a'},
                [&] {
                  return absl::StrCat("parameter ",
                                      parameter.name.GetIdentifierName());
                });
      }
    });
}

}  // namespace sdb::pg
