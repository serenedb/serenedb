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
#include <absl/container/inlined_vector.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <array>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/macro_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/planner/expression/bound_function_expression.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/shared.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "catalog/catalog.h"
#include "catalog/cluster.h"
#include "connector/common.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/oids.h"
#include "pg/catalog/utils/columns.h"
#include "pg/catalog/utils/lookup.h"
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

using connector::AsView;
using duckdb::AclMode;
using duckdb::CatalogType;
using duckdb::Identifier;

[[noreturn]] void ThrowUndefinedObject(std::string_view kind,
                                       std::string_view name) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG(kind, " \"", name, "\" does not exist"));
}

struct DefaultAcl {
  char objtype;
  AclMode world;
  AclMode owner;
};

#include "pg/catalog/generated/acl_defaults.gen.inc"

const DefaultAcl* FindDefaultAcl(std::string_view type) {
  const auto it = absl::c_find_if(kDefaultAcls, [&](const DefaultAcl& acl) {
    return !type.empty() && type[0] == acl.objtype;
  });
  return it == std::end(kDefaultAcls) ? nullptr : &*it;
}

duckdb::unique_ptr<duckdb::FunctionData> AclDefaultBind(
  duckdb::BindScalarFunctionInput& input) {
  const auto objtype = input.TryGetConstant(0);
  if (!objtype || (!objtype->IsNull() &&
                   !FindDefaultAcl(duckdb::StringValue::Get(*objtype)))) {
    input.GetBoundFunction().SetFallible();
  }
  return nullptr;
}

void WriteAclTexts(
  duckdb::VectorWriter<duckdb::VectorListType<duckdb::string_t>>& writer,
  std::span<const duckdb::AclItem> items, const duckdb::SecuritySnapshot& roles,
  std::string& text) {
  auto item = items.begin();
  for (auto& child : writer.WriteList(items.size())) {
    text.clear();
    AppendAcl(text, *item++, roles);
    child.WriteValue(duckdb::string_t{text});
  }
}

void AclDefaultFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto roles = catalog::RolesOf(&state.GetContext());
  const auto count = args.size();
  const auto objtypes = args.data[0].Values<duckdb::string_t>();
  const auto owners = args.data[1].Values<int64_t>();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(result,
                                                                         count);
  std::string text;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto objtype = objtypes[row];
    const auto owner = owners[row];
    if (!objtype.IsValid() || !owner.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto type = AsView(objtype.GetValue());
    const auto* it = FindDefaultAcl(type);
    if (!it) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized object type abbreviation: ", type));
    }
    const auto owner_id = static_cast<duckdb::idx_t>(owner.GetValue());
    absl::InlinedVector<duckdb::AclItem, 2> items;
    if (it->world != AclMode::NoRights) {
      items.emplace_back(duckdb::AclItem{.grantee = duckdb::ACL_ID_PUBLIC,
                                         .grantor = owner_id,
                                         .privs = it->world});
    }
    if (it->owner != AclMode::NoRights) {
      items.emplace_back(duckdb::AclItem{
        .grantee = owner_id, .grantor = owner_id, .privs = it->owner});
    }
    WriteAclTexts(writer, items, *roles, text);
  }
}

struct ExplodedAcl {
  int64_t grantor;
  int64_t grantee;
  std::string_view privilege;
  bool grantable;
};

void ExplodeAcl(const duckdb::SecuritySnapshot& roles, std::string_view text,
                std::vector<ExplodedAcl>& rows) {
  duckdb::AclItem item{};
  if (auto error = ParseAcl(text, roles, true, item)) {
    THROW_SQL_ERROR_FROM_DATA(std::move(*error));
  }
  for (const auto& privilege : duckdb::ACL_PRIVILEGES) {
    if ((item.privs & privilege.mode) != AclMode::NoRights) {
      rows.emplace_back(ExplodedAcl{
        .grantor = static_cast<int64_t>(item.grantor),
        .grantee = static_cast<int64_t>(item.grantee),
        .privilege = privilege.keyword,
        .grantable =
          (item.grant_option & privilege.mode) != AclMode::NoRights});
    }
  }
}

duckdb::unique_ptr<duckdb::FunctionData> AclExplodeBind(
  duckdb::ClientContext&, duckdb::TableFunctionBindInput&,
  duckdb::vector<duckdb::LogicalType>& types,
  duckdb::vector<Identifier>& names) {
  names.emplace_back("grantor");
  types.emplace_back(OID());
  names.emplace_back("grantee");
  types.emplace_back(OID());
  names.emplace_back("privilege_type");
  types.emplace_back(duckdb::LogicalType::VARCHAR);
  names.emplace_back("is_grantable");
  types.emplace_back(duckdb::LogicalType::BOOLEAN);
  return duckdb::make_uniq<duckdb::TableFunctionData>();
}

void LoadAcls(duckdb::ClientContext& context, duckdb::DataChunk& input,
              std::vector<ExplodedAcl>& rows) {
  const auto roles = catalog::RolesOf(&context);
  auto lists = input.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
  for (const auto list : lists) {
    if (!list.IsValid()) {
      continue;
    }
    for (const auto item : list.GetChildValues()) {
      if (!item.IsValid()) {
        THROW_SQL_ERROR(ERR_CODE(ERRCODE_NULL_VALUE_NOT_ALLOWED),
                        ERR_MSG("ACL arrays must not contain null values"));
      }
      ExplodeAcl(*roles, AsView(item.GetValue()), rows);
    }
  }
}

void WriteAcls(std::span<const ExplodedAcl> rows, duckdb::DataChunk& output) {
  const auto count = rows.size();
  auto grantors = duckdb::FlatVector::Writer<int64_t>(output.data[0], count);
  auto grantees = duckdb::FlatVector::Writer<int64_t>(output.data[1], count);
  auto privileges =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[2], count);
  auto grantables = duckdb::FlatVector::Writer<bool>(output.data[3], count);
  for (const auto& row : rows) {
    grantors.WriteValue(row.grantor);
    grantees.WriteValue(row.grantee);
    privileges.WriteValue(duckdb::string_t{
      row.privilege.data(), static_cast<uint32_t>(row.privilege.size())});
    grantables.WriteValue(row.grantable);
  }
}

struct PrivilegeModes {
  AclMode privs = AclMode::NoRights;
  AclMode grant_options = AclMode::NoRights;
};

[[noreturn]] void ThrowUnrecognizedPrivilege(std::string_view privilege) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG("unrecognized privilege type: \"", privilege, "\""));
}

PrivilegeModes ParsePrivileges(std::string_view text, AclMode allowed) {
  constexpr std::string_view kGrantOption = " WITH GRANT OPTION";
  PrivilegeModes modes;
  for (const std::string_view token : absl::StrSplit(text, ',')) {
    const auto chunk = absl::StripAsciiWhitespace(token);
    auto keyword = chunk;
    auto* held = &modes.privs;
    if (keyword.size() > kGrantOption.size() &&
        absl::EqualsIgnoreCase(
          keyword.substr(keyword.size() - kGrantOption.size()), kGrantOption)) {
      keyword.remove_suffix(kGrantOption.size());
      held = &modes.grant_options;
    }
    const auto mode = duckdb::FindPrivilege(keyword);
    if (mode == AclMode::NoRights || (mode & allowed) != mode) {
      ThrowUnrecognizedPrivilege(chunk);
    }
    *held |= mode;
  }
  return modes;
}

void MakeAclItemFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto roles = catalog::RolesOf(&state.GetContext());
  std::string text;
  duckdb::VariadicExecutor::Execute<duckdb::string_t, int64_t, int64_t,
                                    duckdb::string_t, bool>(
    args, result,
    [&](int64_t grantee, int64_t grantor, duckdb::string_t privileges,
        bool grant_option) {
      auto privs = AclMode::NoRights;
      for (const std::string_view token :
           absl::StrSplit(AsView(privileges), ',')) {
        const auto chunk = absl::StripAsciiWhitespace(token);
        const auto mode = duckdb::FindPrivilege(chunk);
        if (mode == AclMode::NoRights) {
          ThrowUnrecognizedPrivilege(chunk);
        }
        privs |= mode;
      }
      text.clear();
      AppendAcl(text,
                duckdb::AclItem{
                  .grantee = static_cast<duckdb::idx_t>(grantee),
                  .grantor = static_cast<duckdb::idx_t>(grantor),
                  .privs = privs,
                  .grant_option = grant_option ? privs : AclMode::NoRights,
                },
                *roles);
      return duckdb::StringVector::AddString(result, text);
    });
}

struct ObjectTarget {
  const duckdb::Permissions* permissions;
  absl::InlinedVector<std::span<const duckdb::AclItem>, 1> columns;
  bool owner_only;
  const duckdb::ColumnList* any_column;
  AclMode rights = ~AclMode::NoRights;
  duckdb::RoleCapability capability = duckdb::RoleCapability::None;
};

bool Holds(const duckdb::RoleAccess& role, CatalogType type,
           const ObjectTarget& target, const PrivilegeModes& requested) {
  const auto& permissions = *target.permissions;
  if (target.owner_only) {
    return role.MemberOf(permissions.owner);
  }
  const auto privileges = requested.privs & target.rights;
  if (privileges != AclMode::NoRights &&
      target.capability != duckdb::RoleCapability::None &&
      role.Has(target.capability)) {
    return true;
  }
  return HasPrivilege(role, type, permissions, privileges,
                      requested.grant_options & target.rights, target.columns,
                      target.any_column);
}

const duckdb::Permissions kBuiltinPermissions{.owner = kRootUser};

CatalogType RelationType(const Relation& relation) {
  return relation.entry ? relation.entry->type : CatalogType::INVALID;
}

bool OwnerOnly(const Relation& relation) {
  return relation.key || RelationType(relation) == CatalogType::INDEX_ENTRY ||
         RelationType(relation) == CatalogType::TYPE_ENTRY;
}

AclMode RelationRights(const Relation& relation) {
  return RelationType(relation) == CatalogType::SEQUENCE_ENTRY
           ? duckdb::ObjectKindOf(CatalogType::SEQUENCE_ENTRY).privileges
           : ~AclMode::NoRights;
}

duckdb::idx_t RoleIdByName(const duckdb::SecuritySnapshot& roles,
                           std::string_view name) {
  if (const auto role = roles.FindByName(name); role.IsValid()) {
    return role.GetIndex();
  }
  ThrowUndefinedObject("role", name);
}

using Column = std::variant<std::string_view, int16_t>;

std::optional<ObjectTarget> TableTarget(const Session&,
                                        const RelationAccess& relation,
                                        const Column*) {
  return ObjectTarget{relation.permissions,
                      {},
                      OwnerOnly(relation),
                      nullptr,
                      RelationRights(relation)};
}

constexpr AclMode kColumnModes = duckdb::ACL_COLUMN_PRIVILEGES;

std::optional<ObjectTarget> AnyColumnTarget(const Session&,
                                            const RelationAccess& relation,
                                            const Column*) {
  ObjectTarget target{relation.permissions,
                      {},
                      OwnerOnly(relation),
                      nullptr,
                      RelationRights(relation)};
  if (!target.owner_only) {
    target.any_column = TableColumns(relation);
    if (RelationType(relation) == CatalogType::VIEW_ENTRY) {
      for (const auto& column :
           relation.entry->Cast<duckdb::ViewCatalogEntry>().column_acls) {
        target.columns.emplace_back(column.acl);
      }
    }
  }
  return target;
}

std::optional<ObjectTarget> NamedColumnTarget(const Session& session,
                                              const RelationAccess& relation,
                                              std::string_view column) {
  const Identifier name{column};
  if (OwnerOnly(relation)) {
    if (absl::c_linear_search(RelationAttributeNames(session, relation),
                              column)) {
      return ObjectTarget{relation.permissions, {}, true};
    }
  } else if (const auto* columns = TableColumns(relation)) {
    if (columns->ColumnExists(name)) {
      return ObjectTarget{relation.permissions,
                          {columns->GetColumn(name).Acl()}};
    }
  } else if (RelationType(relation) == CatalogType::SEQUENCE_ENTRY) {
    if (absl::c_linear_search(kSequenceColumns, column)) {
      return ObjectTarget{relation.permissions};
    }
  } else if (RelationType(relation) == CatalogType::VIEW_ENTRY) {
    auto& view = relation.entry.get_mutable()->Cast<duckdb::ViewCatalogEntry>();
    if (const auto attnum = ViewAttnum(*session.context, view, name)) {
      return ObjectTarget{relation.permissions, {view.ColumnAcl(attnum - 1)}};
    }
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
    ERR_MSG("column \"", column, "\" of relation \"",
            NameOf(*session.transaction, relation).name, "\" does not exist"));
}

std::optional<ObjectTarget> NumberedColumnTarget(const Session& session,
                                                 const RelationAccess& relation,
                                                 int16_t attnum) {
  if (attnum < 1) {
    return std::nullopt;
  }
  if (OwnerOnly(relation)) {
    if (static_cast<duckdb::idx_t>(attnum) >
        RelationAttributeNames(session, relation).size()) {
      return std::nullopt;
    }
    return ObjectTarget{relation.permissions, {}, true};
  }
  if (const auto* columns = TableColumns(relation)) {
    if (static_cast<duckdb::idx_t>(attnum) > columns->LogicalColumnCount()) {
      return std::nullopt;
    }
    return ObjectTarget{
      relation.permissions,
      {columns->GetColumn(duckdb::LogicalIndex(attnum - 1)).Acl()}};
  }
  duckdb::idx_t count = 0;
  if (RelationType(relation) == CatalogType::SEQUENCE_ENTRY) {
    count = kSequenceColumns.size();
  } else if (RelationType(relation) == CatalogType::VIEW_ENTRY) {
    auto& view = relation.entry.get_mutable()->Cast<duckdb::ViewCatalogEntry>();
    const auto info = ViewColumns(*session.context, view);
    SDB_ASSERT(info);
    if (static_cast<duckdb::idx_t>(attnum) > info->types.size()) {
      return std::nullopt;
    }
    return ObjectTarget{relation.permissions, {view.ColumnAcl(attnum - 1)}};
  }
  if (static_cast<duckdb::idx_t>(attnum) > count) {
    return std::nullopt;
  }
  return ObjectTarget{relation.permissions};
}

std::optional<ObjectTarget> ColumnTarget(const Session& session,
                                         const RelationAccess& relation,
                                         const Column* column) {
  if (const auto* name = std::get_if<std::string_view>(column)) {
    return NamedColumnTarget(session, relation, *name);
  }
  return NumberedColumnTarget(session, relation, std::get<int16_t>(*column));
}

std::optional<ObjectTarget> SequenceTarget(const Session& session,
                                           const RelationAccess& relation,
                                           const Column*) {
  if (RelationType(relation) != CatalogType::SEQUENCE_ENTRY) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                    ERR_MSG("\"", NameOf(*session.transaction, relation).name,
                            "\" is not a sequence"));
  }
  return ObjectTarget{relation.permissions};
}

uint64_t ResolveSchema(const Session& session, std::string_view name) {
  return *SchemaOid(session, name, nullptr);
}

std::optional<ObjectTarget> SchemaTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  if (FindSystemObject(kSystemNamespaces, oid)) {
    return ObjectTarget{&SchemaPermissions()};
  }
  if (entry) {
    if (entry->type != CatalogType::SCHEMA_ENTRY) {
      return std::nullopt;
    }
    return ObjectTarget{&duckdb::PermissionsOf(*entry)};
  }
  if (const auto schema = SchemaByOid(session, oid)) {
    return ObjectTarget{&duckdb::PermissionsOf(*schema)};
  }
  return std::nullopt;
}

uint64_t ResolveFunction(const Session& session, std::string_view name) {
  return *RegIn(session, RegKind::Procedure, name, nullptr);
}

std::optional<ObjectTarget> FunctionTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  MacroOverload found;
  if (!entry) {
    if (oid >= kMaxSystem) {
      found = FindMacro(session, oid);
    }
  } else if (duckdb::ObjectKindOf(entry->type).pg_class == kPgProcTable) {
    const auto& macro = entry->Cast<duckdb::MacroCatalogEntry>();
    found = {.entry = &macro, .macro = macro.FindOverload(oid).get()};
  }
  if (found.macro) {
    return ObjectTarget{&duckdb::PermissionsOf(*found.entry, found.macro)};
  }
  if (found.entry) {
    return std::nullopt;
  }
  const auto builtins = GetBuiltinFunctions(*session.context);
  const auto* builtin = builtins->Find(oid);
  if (!builtin) {
    return std::nullopt;
  }
  return ObjectTarget{.permissions = &BuiltinFunctionPermissions(*builtin),
                      .capability = builtin->capability};
}

uint64_t ResolveType(const Session& session, std::string_view name) {
  return *RegIn(session, RegKind::Type, name, nullptr);
}

std::optional<ObjectTarget> TypeTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry> entry, bool element_of_array) {
  if (entry) {
    if (entry->type != CatalogType::TYPE_ENTRY) {
      return std::nullopt;
    }
    return ObjectTarget{&duckdb::PermissionsOf(*entry)};
  }
  if (oid & kRowTypeOidBit) {
    const auto relation = EntryByOid(session, RowTypeRelation(oid));
    if (!relation ||
        duckdb::ObjectKindOf(relation->type).pg_class != kPgClassTable) {
      return std::nullopt;
    }
    return ObjectTarget{&kBuiltinPermissions};
  }
  if (oid >= kMaxSystem) {
    const auto element = ArrayElementOid(session, oid);
    if (element == kInvalidOid) {
      return std::nullopt;
    }
    if (!element_of_array) {
      return ObjectTarget{&kBuiltinPermissions};
    }
    return TypeTarget(session, element, EntryByOid(session, element), true);
  }
  if (!FindBuiltinType(static_cast<int32_t>(oid))) {
    return std::nullopt;
  }
  return ObjectTarget{&kBuiltinPermissions};
}

std::optional<ObjectTarget> TypePrivilegeTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  return TypeTarget(session, oid, entry, true);
}

uint64_t ResolveDatabase(const Session& session, std::string_view name) {
  if (auto database = FindDatabase(*session.context, name);
      database && Shown(session, *database)) {
    return database->oid;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                  ERR_MSG("database \"", name, "\" does not exist"));
}

std::optional<ObjectTarget> DatabaseTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry>) {
  auto database = FindDatabaseById(*session.context, oid);
  if (!database || !Shown(session, *database)) {
    return std::nullopt;
  }
  return ObjectTarget{&duckdb::PermissionsOf(*database)};
}

uint64_t ResolveServer(const Session& session, std::string_view name) {
  SDB_ASSERT(session.database);
  if (auto entry =
        session.database->Servers().Get(*session.transaction, Identifier{name});
      entry && Shown(session, *entry)) {
    return entry->oid;
  }
  ThrowUndefinedObject("server", name);
}

std::optional<ObjectTarget> ServerTarget(
  const Session&, uint64_t, duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  if (!entry || entry->type != CatalogType::FOREIGN_SERVER_ENTRY) {
    return std::nullopt;
  }
  return ObjectTarget{&duckdb::PermissionsOf(*entry)};
}

uint64_t ResolveWrapper(const Session&, std::string_view name) {
  if (const auto* wrapper = FindSystemObject(kForeignDataWrappers, name)) {
    return wrapper->oid;
  }
  ThrowUndefinedObject("foreign-data wrapper", name);
}

std::optional<ObjectTarget> WrapperTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry>) {
  const auto* wrapper = FindSystemObject(kForeignDataWrappers, oid);
  if (!wrapper) {
    return std::nullopt;
  }
  if (session.database) {
    duckdb::Catalog& catalog = *session.database;
    if (const auto entry = catalog.LookupEntry(
          *session.transaction, CatalogType::FOREIGN_DATA_WRAPPER_ENTRY,
          Identifier{wrapper->name})) {
      return ObjectTarget{&entry->permissions};
    }
  }
  return ObjectTarget{&kBuiltinPermissions};
}

uint64_t ResolveTablespace(const Session& session, std::string_view name) {
  auto& cluster = catalog::ClusterOf(*session.context);
  if (const auto tablespace =
        cluster.Set(CatalogType::TABLESPACE_ENTRY)
          .Get(cluster.GetCatalogTransaction(*session.context),
               duckdb::Identifier{name})) {
    return tablespace->oid;
  }
  ThrowUndefinedObject("tablespace", name);
}

std::optional<ObjectTarget> TablespaceTarget(
  const Session& session, uint64_t oid,
  duckdb::optional_ptr<duckdb::CatalogEntry>) {
  auto& cluster = catalog::ClusterOf(*session.context);
  const auto tablespace = cluster.FindIn<catalog::TablespaceCatalogEntry>(
    cluster.GetCatalogTransaction(*session.context), oid);
  if (!tablespace) {
    return std::nullopt;
  }
  return ObjectTarget{&duckdb::PermissionsOf(*tablespace)};
}

uint64_t ResolveLanguage(const Session&, std::string_view name) {
  if (const auto* language = FindSystemObject(kSystemLanguages, name)) {
    return language->oid;
  }
  ThrowUndefinedObject("language", name);
}

std::optional<ObjectTarget> LanguageTarget(
  const Session&, uint64_t oid, duckdb::optional_ptr<duckdb::CatalogEntry>) {
  if (!FindSystemObject(kSystemLanguages, oid)) {
    return std::nullopt;
  }
  return ObjectTarget{&kBuiltinPermissions};
}

std::optional<ObjectTarget> ParameterTarget(const Session&, std::string_view) {
  return ObjectTarget{&kBuiltinPermissions};
}

std::optional<ObjectTarget> LargeObjectTarget(
  const Session&, uint64_t, duckdb::optional_ptr<duckdb::CatalogEntry>) {
  return std::nullopt;
}

struct PrivilegeKind {
  std::string_view name;
  CatalogType type;
  AclMode modes;
  bool superuser_first;
  bool column;
  std::optional<ObjectTarget> (*relation)(const Session&, const RelationAccess&,
                                          const Column*);
  uint64_t (*resolve)(const Session&, std::string_view);
  std::optional<ObjectTarget> (*find)(
    const Session&, uint64_t, duckdb::optional_ptr<duckdb::CatalogEntry>);
  std::optional<ObjectTarget> (*find_named)(const Session&, std::string_view);
};

constexpr AclMode PrivilegesOf(CatalogType type) {
  return duckdb::ObjectKindOf(type).privileges;
}

constexpr PrivilegeKind kTablePrivileges{
  .name = "has_table_privilege",
  .type = CatalogType::TABLE_ENTRY,
  .modes = PrivilegesOf(CatalogType::TABLE_ENTRY),
  .relation = TableTarget,
};

constexpr PrivilegeKind kAnyColumnPrivileges{
  .name = "has_any_column_privilege",
  .type = CatalogType::TABLE_ENTRY,
  .modes = kColumnModes,
  .relation = AnyColumnTarget,
};

constexpr PrivilegeKind kColumnPrivileges{
  .name = "has_column_privilege",
  .type = CatalogType::TABLE_ENTRY,
  .modes = kColumnModes,
  .column = true,
  .relation = ColumnTarget,
};

constexpr PrivilegeKind kSequencePrivileges{
  .name = "has_sequence_privilege",
  .type = CatalogType::SEQUENCE_ENTRY,
  .modes = PrivilegesOf(CatalogType::SEQUENCE_ENTRY),
  .relation = SequenceTarget,
};

constexpr PrivilegeKind kSchemaPrivileges{
  .name = "has_schema_privilege",
  .type = CatalogType::SCHEMA_ENTRY,
  .modes = PrivilegesOf(CatalogType::SCHEMA_ENTRY),
  .superuser_first = true,
  .resolve = ResolveSchema,
  .find = SchemaTarget,
};

constexpr PrivilegeKind kFunctionPrivileges{
  .name = "has_function_privilege",
  .type = CatalogType::MACRO_ENTRY,
  .modes = PrivilegesOf(CatalogType::MACRO_ENTRY),
  .superuser_first = true,
  .resolve = ResolveFunction,
  .find = FunctionTarget,
};

constexpr PrivilegeKind kTypePrivileges{
  .name = "has_type_privilege",
  .type = CatalogType::TYPE_ENTRY,
  .modes = PrivilegesOf(CatalogType::TYPE_ENTRY),
  .superuser_first = true,
  .resolve = ResolveType,
  .find = TypePrivilegeTarget,
};

constexpr PrivilegeKind kDatabasePrivileges{
  .name = "has_database_privilege",
  .type = CatalogType::DATABASE_ENTRY,
  .modes = PrivilegesOf(CatalogType::DATABASE_ENTRY),
  .superuser_first = true,
  .resolve = ResolveDatabase,
  .find = DatabaseTarget,
};

constexpr PrivilegeKind kServerPrivileges{
  .name = "has_server_privilege",
  .type = CatalogType::FOREIGN_SERVER_ENTRY,
  .modes = PrivilegesOf(CatalogType::FOREIGN_SERVER_ENTRY),
  .superuser_first = true,
  .resolve = ResolveServer,
  .find = ServerTarget,
};

constexpr PrivilegeKind kWrapperPrivileges{
  .name = "has_foreign_data_wrapper_privilege",
  .type = CatalogType::FOREIGN_DATA_WRAPPER_ENTRY,
  .modes = PrivilegesOf(CatalogType::FOREIGN_DATA_WRAPPER_ENTRY),
  .superuser_first = true,
  .resolve = ResolveWrapper,
  .find = WrapperTarget,
};

constexpr PrivilegeKind kTablespacePrivileges{
  .name = "has_tablespace_privilege",
  .type = CatalogType::TABLESPACE_ENTRY,
  .modes = AclMode::Create,
  .superuser_first = true,
  .resolve = ResolveTablespace,
  .find = TablespaceTarget,
};

constexpr PrivilegeKind kLanguagePrivileges{
  .name = "has_language_privilege",
  .type = CatalogType::TYPE_ENTRY,
  .modes = AclMode::Usage,
  .superuser_first = true,
  .resolve = ResolveLanguage,
  .find = LanguageTarget,
};

constexpr PrivilegeKind kParameterPrivileges{
  .name = "has_parameter_privilege",
  .type = CatalogType::INVALID,
  .modes = AclMode::Set | AclMode::AlterSystem,
  .find_named = ParameterTarget,
};

constexpr PrivilegeKind kLargeObjectPrivileges{
  .name = "has_largeobject_privilege",
  .type = CatalogType::INVALID,
  .modes = AclMode::Select | AclMode::Update,
  .find = LargeObjectTarget,
};

constexpr PrivilegeKind kRolePrivileges{
  .name = "pg_has_role",
  .type = CatalogType::ROLE_ENTRY,
};

#include "pg/catalog/generated/object_classes.gen.inc"

class ObjectAcls final : public duckdb::FunctionLocalState {
 public:
  ObjectAcls(duckdb::ClientContext& context, duckdb::optional_idx caller)
    : _session{MakeSession(&context, caller)},
      _roles{catalog::RolesOf(&context)} {}

  std::optional<std::span<const duckdb::AclItem>> Of(uint64_t classid,
                                                     uint64_t objid,
                                                     int32_t objsubid) const {
    const bool column = classid == kPgClassTable && objsubid != 0;
    const auto target = Find(classid, objid, column ? objsubid : 0);
    if (!target || target->owner_only) {
      return std::nullopt;
    }
    if (!column) {
      return AclOf(*target->permissions);
    }
    return target->columns.empty() ? std::nullopt
                                   : AclOf(target->columns.front());
  }

  const duckdb::SecuritySnapshot& Roles() const { return *_roles; }

 private:
  std::optional<ObjectTarget> Find(uint64_t classid, uint64_t objid,
                                   int32_t objsubid) const {
    auto entry = EntryByOid(_session, objid);
    if (classid == kPgClassTable) {
      if (objsubid < 0 || objsubid > std::numeric_limits<int16_t>::max()) {
        return std::nullopt;
      }
      const auto relation = RelationByOid(_session, objid, entry);
      if (!relation) {
        return std::nullopt;
      }
      if (objsubid == 0) {
        return TableTarget(_session, *relation, nullptr);
      }
      return NumberedColumnTarget(_session, *relation,
                                  static_cast<int16_t>(objsubid));
    }
    if (classid == kPgProcTable) {
      return FunctionTarget(_session, objid, entry);
    }
    if (classid == kPgTypeTable) {
      return TypeTarget(_session, objid, entry, false);
    }
    if (classid == kPgNamespaceTable) {
      return SchemaTarget(_session, objid, entry);
    }
    if (classid == kPgDatabaseTable) {
      return DatabaseTarget(_session, objid, entry);
    }
    if (classid == kPgForeignServerTable) {
      return ServerTarget(_session, objid, entry);
    }
    if (classid == kPgForeignDataWrapperTable) {
      return WrapperTarget(_session, objid, entry);
    }
    if (classid == kPgLanguageTable) {
      return LanguageTarget(_session, objid, entry);
    }
    if (classid == kPgTablespaceTable) {
      return TablespaceTarget(_session, objid, entry);
    }
    return std::nullopt;
  }

  Session _session;
  duckdb::shared_ptr<const duckdb::SecuritySnapshot> _roles;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitObjectAcls(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<ObjectAcls>(state.GetContext(), CallerRole(expr));
}

void PgGetAclFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                      duckdb::Vector& result) {
  const auto& acls =
    duckdb::ExecuteFunctionState::GetFunctionState(state)->Cast<ObjectAcls>();
  const auto classids = args.data[0].Values<int64_t>();
  const auto objids = args.data[1].Values<int64_t>();
  const auto objsubids = args.data[2].Values<int32_t>();
  const auto count = args.size();
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(result,
                                                                         count);
  std::string text;
  for (duckdb::idx_t row = 0; row < count; ++row) {
    const auto classid = classids[row];
    const auto objid = objids[row];
    const auto objsubid = objsubids[row];
    if (!classid.IsValid() || !objid.IsValid() || !objsubid.IsValid()) {
      writer.WriteNull();
      continue;
    }
    const auto oid = static_cast<uint64_t>(classid.GetValue());
    if (!absl::c_linear_search(kAclClasses, oid)) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                      ERR_MSG("unrecognized class ID: ", oid));
    }
    const auto acl = acls.Of(oid, static_cast<uint64_t>(objid.GetValue()),
                             objsubid.GetValue());
    if (!acl) {
      writer.WriteNull();
      continue;
    }
    WriteAclTexts(writer, *acl, acls.Roles(), text);
  }
}

struct RoleModes {
  bool usage = false;
  bool member = false;
  bool set = false;
  bool admin = false;
};

RoleModes ParseRoleModes(std::string_view text) {
  RoleModes mask;
  for (std::string_view tok : absl::StrSplit(text, ',')) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    auto privilege = stripped;
    const bool option =
      absl::c_any_of(std::array<std::string_view, 2>{" WITH GRANT OPTION",
                                                     " WITH ADMIN OPTION"},
                     [&](std::string_view suffix) {
                       if (!absl::EndsWithIgnoreCase(privilege, suffix)) {
                         return false;
                       }
                       privilege.remove_suffix(suffix.size());
                       return true;
                     });
    bool* held = absl::EqualsIgnoreCase(privilege, "USAGE")    ? &mask.usage
                 : absl::EqualsIgnoreCase(privilege, "MEMBER") ? &mask.member
                 : absl::EqualsIgnoreCase(privilege, "SET")    ? &mask.set
                                                               : nullptr;
    if (!held) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized privilege type: \"", stripped, "\""));
    }
    *(option ? &mask.admin : held) = true;
  }
  return mask;
}

struct PrivilegeKindInfo final : duckdb::ScalarFunctionInfo {
  explicit PrivilegeKindInfo(const PrivilegeKind& kind) : kind{kind} {}

  const PrivilegeKind& kind;
};

bool ByName(const duckdb::UnifiedVectorFormat& argument) {
  return argument.physical_type == duckdb::PhysicalType::VARCHAR;
}

const duckdb::string_t& NameAt(const duckdb::UnifiedVectorFormat& argument,
                               duckdb::idx_t index) {
  return duckdb::UnifiedVectorFormat::GetDataUnsafe<duckdb::string_t>(
    argument)[index];
}

int64_t OidAt(const duckdb::UnifiedVectorFormat& argument,
              duckdb::idx_t index) {
  return duckdb::UnifiedVectorFormat::GetDataUnsafe<int64_t>(argument)[index];
}

Column ColumnAt(const duckdb::UnifiedVectorFormat& argument,
                duckdb::idx_t index) {
  if (ByName(argument)) {
    return AsView(NameAt(argument, index));
  }
  return duckdb::UnifiedVectorFormat::GetDataUnsafe<int16_t>(argument)[index];
}

class PrivilegeCheck final : public duckdb::FunctionLocalState {
 public:
  PrivilegeCheck(duckdb::ClientContext& context,
                 const duckdb::BoundFunctionExpression& expr)
    : _session{MakeSession(&context, CallerRole(expr))},
      _roles{catalog::RolesOf(&context)},
      _kind{
        expr.Function().GetExtraFunctionInfo().Cast<PrivilegeKindInfo>().kind} {
  }

  void Execute(duckdb::DataChunk& args, duckdb::Vector& result) {
    _named.reset();
    _modes.reset();
    _role_modes.reset();
    std::array<duckdb::UnifiedVectorFormat, 4> formats;
    const auto width = args.ColumnCount();
    for (duckdb::idx_t i = 0; i < width; ++i) {
      args.data[i].ToUnifiedFormat(formats[i]);
    }
    const bool role_given = width == (_kind.column ? 4 : 3);
    const duckdb::idx_t object = role_given ? 1 : 0;
    const Arguments arguments{
      .role = role_given ? &formats[0] : nullptr,
      .object = formats[object],
      .column = _kind.column ? &formats[object + 1] : nullptr,
      .privileges = formats[width - 1],
      .nullable =
        absl::c_any_of(std::span{formats.data(), width},
                       [](const duckdb::UnifiedVectorFormat& argument) {
                         return !argument.validity.AllValid();
                       }),
    };
    const auto count = args.size();
    if (absl::c_all_of(args.data, [](const duckdb::Vector& vector) {
          return vector.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR;
        })) {
      if (const auto answer = Row(arguments, 0)) {
        result.Reference(duckdb::Value::BOOLEAN(*answer),
                         duckdb::count_t(count));
      } else {
        duckdb::ConstantVector::SetNull(result, duckdb::count_t(count));
      }
      return;
    }
    auto writer = duckdb::FlatVector::Writer<bool>(result, count);
    for (duckdb::idx_t row = 0; row < count; ++row) {
      if (const auto answer = Row(arguments, row)) {
        writer.WriteValue(*answer);
      } else {
        writer.WriteNull();
      }
    }
  }

 private:
  struct Named {
    duckdb::string_t name;
    duckdb::idx_t role;
  };

  struct ParsedModes {
    duckdb::string_t text;
    PrivilegeModes modes;
  };

  struct ParsedRoleModes {
    duckdb::string_t text;
    RoleModes modes;
  };

  struct Arguments {
    const duckdb::UnifiedVectorFormat* role;
    const duckdb::UnifiedVectorFormat& object;
    const duckdb::UnifiedVectorFormat* column;
    const duckdb::UnifiedVectorFormat& privileges;
    bool nullable;
  };

  IRS_FORCE_INLINE duckdb::optional<bool> Row(const Arguments& arguments,
                                              duckdb::idx_t row) {
    const auto object = arguments.object.sel->get_index(row);
    const auto privileges = arguments.privileges.sel->get_index(row);
    const auto role = arguments.role ? arguments.role->sel->get_index(row) : 0;
    const auto column =
      arguments.column ? arguments.column->sel->get_index(row) : 0;
    if (arguments.nullable &&
        (!arguments.object.validity.RowIsValid(object) ||
         !arguments.privileges.validity.RowIsValid(privileges) ||
         (arguments.role && !arguments.role->validity.RowIsValid(role)) ||
         (arguments.column &&
          !arguments.column->validity.RowIsValid(column)))) {
      return duckdb::nullopt;
    }
    const auto member =
      arguments.role ? Role(*arguments.role, role) : _session.role.role;
    const auto& text = NameAt(arguments.privileges, privileges);
    if (_kind.type == CatalogType::ROLE_ENTRY) {
      return DecideRole(
        member, text,
        ByName(arguments.object)
          ? RoleIdByName(*_roles, AsView(NameAt(arguments.object, object)))
          : static_cast<duckdb::idx_t>(OidAt(arguments.object, object)));
    }
    if (!arguments.column) {
      return Decide(member, text, Find(arguments.object, object, nullptr));
    }
    const auto target = ColumnAt(*arguments.column, column);
    return Decide(member, text, Find(arguments.object, object, &target));
  }

  IRS_FORCE_INLINE duckdb::idx_t Role(const duckdb::UnifiedVectorFormat& role,
                                      duckdb::idx_t index) {
    if (!ByName(role)) {
      return static_cast<duckdb::idx_t>(OidAt(role, index));
    }
    const auto& name = NameAt(role, index);
    if (!_named || name != _named->name) {
      const auto text = AsView(name);
      _named = Named{.name = name,
                     .role = _kind.type != CatalogType::ROLE_ENTRY &&
                                 text == irs::StaticStrings::kPublic
                               ? duckdb::ACL_ID_PUBLIC
                               : RoleIdByName(*_roles, text)};
    }
    return _named->role;
  }

  const duckdb::RoleAccess& Closure(duckdb::idx_t role) {
    if (!_closure || role != _closure->role) {
      _closure = _roles->AccessOf(role);
    }
    return *_closure;
  }

  IRS_FORCE_INLINE std::optional<ObjectTarget> Find(
    const duckdb::UnifiedVectorFormat& object, duckdb::idx_t index,
    const Column* column) {
    if (ByName(object)) {
      return FindNamed(AsView(NameAt(object, index)), column);
    }
    return FindByOid(static_cast<uint64_t>(OidAt(object, index)), column);
  }

  std::optional<ObjectTarget> FindNamed(std::string_view name,
                                        const Column* column) {
    if (_kind.relation) {
      if (const auto entry = AttachedRelation(_session, name)) {
        return _kind.relation(_session,
                              RelationAccess{{entry, nullptr, nullptr, nullptr},
                                             &duckdb::PermissionsOf(*entry)},
                              column);
      }
      const auto oid = *RegIn(_session, RegKind::Class, name, nullptr);
      return FindRelation(oid, InternalEntryByOid(_session, oid), column);
    }
    if (_kind.find_named) {
      return _kind.find_named(_session, name);
    }
    const auto oid = _kind.resolve(_session, name);
    return _kind.find(_session, oid, InternalEntryByOid(_session, oid));
  }

  std::optional<ObjectTarget> FindByOid(uint64_t oid, const Column* column) {
    auto entry = EntryByOid(_session, oid);
    if (_kind.relation) {
      return FindRelation(oid, entry, column);
    }
    return _kind.find(_session, oid, entry);
  }

  std::optional<ObjectTarget> FindRelation(
    uint64_t oid, duckdb::optional_ptr<duckdb::CatalogEntry> entry,
    const Column* column) {
    const auto relation = RelationByOid(_session, oid, entry);
    if (!relation) {
      return std::nullopt;
    }
    return _kind.relation(_session, *relation, column);
  }

  duckdb::optional<bool> Decide(duckdb::idx_t role, duckdb::string_t text,
                                const std::optional<ObjectTarget>& target) {
    if (!_modes || text != _modes->text) {
      _modes = ParsedModes{.text = text,
                           .modes = ParsePrivileges(AsView(text), _kind.modes)};
    }
    if (target) {
      return Holds(Closure(role), _kind.type, *target, _modes->modes);
    }
    if (_kind.superuser_first && Closure(role).is_superuser) {
      return true;
    }
    return duckdb::nullopt;
  }

  bool DecideRole(duckdb::idx_t member, duckdb::string_t text,
                  duckdb::idx_t role) {
    if (!_role_modes || text != _role_modes->text) {
      _role_modes =
        ParsedRoleModes{.text = text, .modes = ParseRoleModes(AsView(text))};
    }
    const auto& mask = _role_modes->modes;
    const auto& closure = Closure(member);
    if (closure.is_superuser) {
      return true;
    }
    if (member == role) {
      return mask.usage || mask.member || mask.set;
    }
    if (mask.usage && closure.MemberOf(role)) {
      return true;
    }
    if (mask.set && closure.CanSet(role)) {
      return true;
    }
    if (mask.admin && closure.AdminOf(role)) {
      return true;
    }
    if (!mask.member) {
      return false;
    }
    if (_member != member) {
      _memberships = _roles->MembershipsOf(member);
      _member = member;
    }
    return _memberships.IsMember(role);
  }

  Session _session;
  duckdb::shared_ptr<const duckdb::SecuritySnapshot> _roles;
  const PrivilegeKind& _kind;
  std::optional<Named> _named;
  std::optional<duckdb::RoleAccess> _closure;
  std::optional<ParsedModes> _modes;
  std::optional<ParsedRoleModes> _role_modes;
  duckdb::idx_t _member = kInvalidOid;
  duckdb::RoleMemberships _memberships;
};

void ExecutePrivilegeCheck(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  duckdb::ExecuteFunctionState::GetFunctionState(state)
    ->Cast<PrivilegeCheck>()
    .Execute(args, result);
}

duckdb::unique_ptr<duckdb::FunctionLocalState> InitPrivilegeCheck(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<PrivilegeCheck>(state.GetContext(), expr);
}

void RegisterPrivilegeFunctions(
  duckdb::ExtensionLoader& loader, const PrivilegeKind& kind,
  std::span<const duckdb::vector<duckdb::LogicalType>> shapes) {
  const auto info = duckdb::make_shared_ptr<PrivilegeKindInfo>(kind);
  const std::array<duckdb::vector<duckdb::LogicalType>, 3> roles{
    {{duckdb::LogicalType::VARCHAR}, {OID()}, {}}};
  duckdb::ScalarFunctionSet set{Identifier{kind.name}};
  for (const auto& objects : shapes) {
    for (const auto& role : roles) {
      auto arguments = role;
      arguments.insert(arguments.end(), objects.begin(), objects.end());
      arguments.emplace_back(duckdb::LogicalType::VARCHAR);
      duckdb::ScalarFunction function{std::move(arguments),
                                      duckdb::LogicalType::BOOLEAN,
                                      ExecutePrivilegeCheck,
                                      BindCaller,
                                      nullptr,
                                      InitPrivilegeCheck};
      function.SetExtraFunctionInfo(info);
      set.AddFunction(std::move(function));
    }
  }
  set.SetFallible();
  Register(loader, irs::StaticStrings::kPgCatalogSchema,
           duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY, std::move(set));
}

}  // namespace

void RegisterAclFunctions(duckdb::ExtensionLoader& loader) {
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  RegisterScalar(loader, pg_catalog, "acldefault",
                 duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY, nullptr,
                 {duckdb::ScalarFunction{{CHAR(), OID()},
                                         duckdb::LogicalType::LIST(ACLITEM()),
                                         AclDefaultFunction,
                                         AclDefaultBind}});
  duckdb::TableFunction function{
    "aclexplode", {duckdb::LogicalType::LIST(ACLITEM())},
    nullptr,      AclExplodeBind,
    nullptr,      InitRows<ExplodedAcl>};
  function.in_out_function = RowsInOut<ExplodedAcl, &LoadAcls, &WriteAcls>;
  RegisterTable(loader, pg_catalog, std::move(function));

  const auto name = duckdb::LogicalType::VARCHAR;
  const auto attnum = duckdb::LogicalType::SMALLINT;
  const std::array<duckdb::vector<duckdb::LogicalType>, 2> objects{
    {{name}, {OID()}}};
  for (const auto* kind :
       {&kTablePrivileges, &kAnyColumnPrivileges, &kSequencePrivileges,
        &kSchemaPrivileges, &kFunctionPrivileges, &kTypePrivileges,
        &kDatabasePrivileges, &kServerPrivileges, &kWrapperPrivileges,
        &kTablespacePrivileges, &kLanguagePrivileges, &kRolePrivileges}) {
    RegisterPrivilegeFunctions(loader, *kind, objects);
  }
  RegisterPrivilegeFunctions(
    loader, kColumnPrivileges,
    std::array<duckdb::vector<duckdb::LogicalType>, 4>{
      {{name, name}, {name, attnum}, {OID(), name}, {OID(), attnum}}});
  RegisterPrivilegeFunctions(loader, kParameterPrivileges,
                             std::span{objects}.first(1));
  RegisterPrivilegeFunctions(loader, kLargeObjectPrivileges,
                             std::span{objects}.last(1));
  duckdb::ScalarFunction make_aclitem{
    {OID(), OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BOOLEAN},
    ACLITEM(),
    MakeAclItemFunction};
  make_aclitem.SetFallible();
  RegisterScalar(loader, pg_catalog, "makeaclitem",
                 duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY, nullptr,
                 {make_aclitem});
  RegisterScalar(
    loader, pg_catalog, "pg_get_acl",
    duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY, InitObjectAcls,
    {duckdb::ScalarFunction{{OID(), OID(), duckdb::LogicalType::INTEGER},
                            duckdb::LogicalType::LIST(ACLITEM()),
                            PgGetAclFunction,
                            BindCaller}});
}

}  // namespace sdb::pg
