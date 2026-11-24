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
#include <iresearch/utils/static_strings.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
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
#include "pg/connection_context.h"
#include "pg/types/char.h"
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
  const auto lists =
    input.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
  for (duckdb::idx_t row = 0; row < input.size(); ++row) {
    const auto list = lists[row];
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

bool Holds(const duckdb::RoleAccess& closure, CatalogType type,
           const ObjectTarget& target, const PrivilegeModes& requested) {
  const auto& permissions = *target.permissions;
  if (permissions.owner == kInvalidOid) {
    return true;
  }
  if (target.owner_only) {
    return closure.Owns(permissions.owner);
  }
  const PrivilegeModes modes{
    .privs = requested.privs & target.rights,
    .grant_options = requested.grant_options & target.rights};
  if (modes.privs == AclMode::NoRights &&
      modes.grant_options == AclMode::NoRights) {
    return closure.is_superuser;
  }
  if (modes.privs != AclMode::NoRights &&
      ((target.capability != duckdb::RoleCapability::None &&
        closure.Has(target.capability)) ||
       closure.CanAny(type, permissions, modes.privs))) {
    return true;
  }
  if (modes.privs != AclMode::NoRights) {
    const auto grants = [&](std::span<const duckdb::AclItem> acl) {
      return (closure.HeldModes(acl) & modes.privs) != AclMode::NoRights;
    };
    if (absl::c_any_of(target.columns, grants) ||
        (target.any_column &&
         absl::c_any_of(target.any_column->Logical(),
                        [&](const duckdb::ColumnDefinition& column) {
                          return grants(column.Acl());
                        }))) {
      return true;
    }
  }
  if (modes.grant_options == AclMode::NoRights) {
    return false;
  }
  if (closure.Owns(permissions.owner)) {
    return true;
  }
  auto held = closure.GrantableModes(permissions.acl);
  for (const auto acl : target.columns) {
    held |= closure.GrantableModes(acl);
  }
  if (target.any_column) {
    for (const auto& column : target.any_column->Logical()) {
      held |= closure.GrantableModes(column.Acl());
    }
  }
  return (held & modes.grant_options) != AclMode::NoRights;
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
  duckdb::optional_ptr<duckdb::CatalogEntry>) {
  if (FindSystemObject(kSystemNamespaces, oid)) {
    return ObjectTarget{&SchemaPermissions()};
  }
  if (const auto schema = SchemaByOid(session, oid)) {
    return ObjectTarget{&PrivilegePermissions(*schema)};
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
    return ObjectTarget{duckdb::IsUnowned(*found.entry)
                          ? &PrivilegePermissions(*found.entry)
                          : &found.entry->OverloadPermissions(*found.macro)};
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
    return ObjectTarget{&PrivilegePermissions(*entry)};
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
  return ObjectTarget{&PrivilegePermissions(*database)};
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
  return ObjectTarget{&PrivilegePermissions(*entry)};
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
    if (const auto entry = catalog.LookupTopLevelEntry(
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
  return ObjectTarget{&PrivilegePermissions(*tablespace)};
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

std::optional<ObjectTarget> ParameterTarget(const Session& session,
                                            std::string_view name) {
  auto& cluster = catalog::ClusterOf(*session.context);
  const auto parameter = cluster.Set(CatalogType::PARAMETER_ENTRY)
                           .Get(cluster.GetCatalogTransaction(*session.context),
                                duckdb::Identifier{name});
  return ObjectTarget{parameter ? &PrivilegePermissions(*parameter)
                                : &kBuiltinPermissions};
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
  .type = CatalogType::LANGUAGE_ENTRY,
  .modes = PrivilegesOf(CatalogType::LANGUAGE_ENTRY),
  .superuser_first = true,
  .resolve = ResolveLanguage,
  .find = LanguageTarget,
};

constexpr PrivilegeKind kParameterPrivileges{
  .name = "has_parameter_privilege",
  .type = CatalogType::PARAMETER_ENTRY,
  .modes = PrivilegesOf(CatalogType::PARAMETER_ENTRY),
  .find_named = ParameterTarget,
};

constexpr PrivilegeKind kLargeObjectPrivileges{
  .name = "has_largeobject_privilege",
  .type = CatalogType::INVALID,
  .modes = AclMode::Select | AclMode::Update,
  .find = LargeObjectTarget,
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
    if (entry && !Shown(_session, *entry)) {
      return std::nullopt;
    }
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
  auto writer =
    duckdb::FlatVector::Writer<duckdb::VectorListType<duckdb::string_t>>(
      result, args.size());
  std::string text;
  for (duckdb::idx_t row = 0; row < args.size(); ++row) {
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

template<typename T>
duckdb::LogicalType PrivilegeArgument() {
  if constexpr (std::is_same_v<T, duckdb::string_t>) {
    return duckdb::LogicalType::VARCHAR;
  } else if constexpr (std::is_same_v<T, int16_t>) {
    return duckdb::LogicalType::SMALLINT;
  } else {
    return OID();
  }
}

struct PrivilegeKindInfo final : duckdb::ScalarFunctionInfo {
  explicit PrivilegeKindInfo(const PrivilegeKind& kind) : kind{kind} {}

  const PrivilegeKind& kind;
};

class PrivilegeState : public duckdb::FunctionLocalState {
 public:
  explicit PrivilegeState(duckdb::ClientContext& context)
    : _roles{catalog::RolesOf(&context)} {}

  duckdb::idx_t Role(int64_t oid) { return static_cast<duckdb::idx_t>(oid); }

 protected:
  const duckdb::SecuritySnapshot& Roles() const { return *_roles; }

  duckdb::idx_t NamedRole(duckdb::string_t name, bool with_public) {
    if (!_named || name != _named->name) {
      const auto text = AsView(name);
      _named = Named{.name = name,
                     .role = with_public && text == irs::StaticStrings::kPublic
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

  void ForgetNames() { _named.reset(); }

 private:
  struct Named {
    duckdb::string_t name;
    duckdb::idx_t role;
  };

  duckdb::shared_ptr<const duckdb::SecuritySnapshot> _roles;
  std::optional<Named> _named;
  std::optional<duckdb::RoleAccess> _closure;
};

Column ColumnOf(const duckdb::string_t& name) { return AsView(name); }

Column ColumnOf(int16_t attnum) { return attnum; }

class PrivilegeCheck final : public PrivilegeState {
 public:
  PrivilegeCheck(duckdb::ClientContext& context,
                 const duckdb::BoundFunctionExpression& expr)
    : PrivilegeState{context},
      _session{MakeSession(&context, CallerRole(expr))},
      _kind{
        expr.Function().GetExtraFunctionInfo().Cast<PrivilegeKindInfo>().kind} {
  }

  PrivilegeCheck& ForChunk() {
    ForgetNames();
    _modes.reset();
    return *this;
  }

  using PrivilegeState::Role;

  duckdb::idx_t Role(duckdb::string_t name) { return NamedRole(name, true); }

  template<typename Object>
  duckdb::optional<bool> Answer(duckdb::idx_t role, duckdb::string_t text,
                                Object object) {
    return Decide(role, text, Find(object, nullptr));
  }

  template<typename Object, typename Attribute>
  duckdb::optional<bool> Answer(duckdb::idx_t role, duckdb::string_t text,
                                Object object, Attribute attribute) {
    const auto column = ColumnOf(attribute);
    return Decide(role, text, Find(object, &column));
  }

 private:
  struct ParsedModes {
    duckdb::string_t text;
    PrivilegeModes modes;
  };

  std::optional<ObjectTarget> Find(duckdb::string_t object,
                                   const Column* column) {
    const auto name = AsView(object);
    if (_kind.relation) {
      if (const auto entry = AttachedRelation(_session, name)) {
        return _kind.relation(_session,
                              RelationAccess{{entry, nullptr, nullptr, nullptr},
                                             &PrivilegePermissions(*entry)},
                              column);
      }
      const auto oid = *RegIn(_session, RegKind::Class, name, nullptr);
      return FindRelation(oid, EntryByOid(_session, oid), column);
    }
    if (_kind.find_named) {
      return _kind.find_named(_session, name);
    }
    const auto oid = _kind.resolve(_session, name);
    return _kind.find(_session, oid, EntryByOid(_session, oid));
  }

  std::optional<ObjectTarget> Find(int64_t object, const Column* column) {
    const auto oid = static_cast<uint64_t>(object);
    auto entry = EntryByOid(_session, oid);
    if (entry && !Shown(_session, *entry)) {
      return std::nullopt;
    }
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

  Session _session;
  const PrivilegeKind& _kind;
  std::optional<ParsedModes> _modes;
};

class RoleCheck final : public PrivilegeState {
 public:
  RoleCheck(duckdb::ClientContext& context,
            const duckdb::BoundFunctionExpression&)
    : PrivilegeState{context} {}

  RoleCheck& ForChunk() {
    ForgetNames();
    _modes.reset();
    return *this;
  }

  using PrivilegeState::Role;

  duckdb::idx_t Role(duckdb::string_t name) { return NamedRole(name, false); }

  duckdb::optional<bool> Answer(duckdb::idx_t member, duckdb::string_t text,
                                duckdb::string_t role) {
    return Decide(member, text, RoleIdByName(Roles(), AsView(role)));
  }

  duckdb::optional<bool> Answer(duckdb::idx_t member, duckdb::string_t text,
                                int64_t role) {
    return Decide(member, text, static_cast<duckdb::idx_t>(role));
  }

 private:
  struct ParsedModes {
    duckdb::string_t text;
    RoleModes modes;
  };

  bool Decide(duckdb::idx_t member, duckdb::string_t text, duckdb::idx_t role) {
    if (!_modes || text != _modes->text) {
      _modes = ParsedModes{.text = text, .modes = ParseRoleModes(AsView(text))};
    }
    const auto& mask = _modes->modes;
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
    if (!mask.member && !mask.admin) {
      return false;
    }
    if (_member != member) {
      _memberships = Roles().MembershipsOf(member);
      _member = member;
    }
    return (mask.member && _memberships.IsMember(role)) ||
           (mask.admin && _memberships.IsAdminOf(role));
  }

  std::optional<ParsedModes> _modes;
  duckdb::idx_t _member = kInvalidOid;
  duckdb::RoleMemberships _memberships;
};

template<typename State>
duckdb::unique_ptr<duckdb::FunctionLocalState> InitCheck(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<State>(state.GetContext(), expr);
}

template<typename State, typename Role, typename... Object>
duckdb::ScalarFunction PrivilegeFunction() {
  duckdb::vector<duckdb::LogicalType> arguments;
  if constexpr (!std::is_void_v<Role>) {
    arguments.emplace_back(PrivilegeArgument<Role>());
  }
  (arguments.emplace_back(PrivilegeArgument<Object>()), ...);
  arguments.emplace_back(duckdb::LogicalType::VARCHAR);
  return duckdb::ScalarFunction{
    std::move(arguments),
    duckdb::LogicalType::BOOLEAN,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& check = duckdb::ExecuteFunctionState::GetFunctionState(state)
                      ->Cast<State>()
                      .ForChunk();
      if constexpr (std::is_void_v<Role>) {
        const auto caller =
          CallerRole(state.expr.Cast<duckdb::BoundFunctionExpression>());
        const auto role =
          caller.IsValid()
            ? caller.GetIndex()
            : ConnectionContext::Get(state.GetContext()).GetRoleId();
        duckdb::VariadicExecutor::Execute<bool, Object..., duckdb::string_t>(
          args, result, [&](Object... object, duckdb::string_t text) {
            return check.Answer(role, text, object...);
          });
      } else {
        duckdb::VariadicExecutor::Execute<bool, Role, Object...,
                                          duckdb::string_t>(
          args, result,
          [&](Role role, Object... object, duckdb::string_t text) {
            return check.Answer(check.Role(role), text, object...);
          });
      }
    },
    BindCaller,
    nullptr,
    InitCheck<State>};
}

template<typename State, typename... Object>
void AddPrivilegeForms(duckdb::ScalarFunctionSet& set,
                       std::type_identity<std::tuple<Object...>>) {
  set.AddFunction(PrivilegeFunction<State, duckdb::string_t, Object...>());
  set.AddFunction(PrivilegeFunction<State, int64_t, Object...>());
  set.AddFunction(PrivilegeFunction<State, void, Object...>());
}

template<typename State, typename... Objects>
void RegisterPrivilegeFunctions(
  duckdb::ExtensionLoader& loader, std::string_view name,
  const duckdb::shared_ptr<duckdb::ScalarFunctionInfo>& info) {
  duckdb::ScalarFunctionSet set{Identifier{name}};
  (AddPrivilegeForms<State>(set, std::type_identity<Objects>{}), ...);
  set.ApplyToFunctions([&](duckdb::ScalarFunction& function) {
    function.SetExtraFunctionInfo(info);
  });
  set.SetFallible();
  Register(loader, irs::StaticStrings::kPgCatalogSchema,
           duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY, std::move(set));
}

template<typename... Objects>
void RegisterKind(duckdb::ExtensionLoader& loader, const PrivilegeKind& kind) {
  RegisterPrivilegeFunctions<PrivilegeCheck, Objects...>(
    loader, kind.name, duckdb::make_shared_ptr<PrivilegeKindInfo>(kind));
}

}  // namespace

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

  using ByName = std::tuple<duckdb::string_t>;
  using ByOid = std::tuple<int64_t>;
  for (const auto* kind :
       {&kTablePrivileges, &kAnyColumnPrivileges, &kSequencePrivileges,
        &kSchemaPrivileges, &kFunctionPrivileges, &kTypePrivileges,
        &kDatabasePrivileges, &kServerPrivileges, &kWrapperPrivileges,
        &kTablespacePrivileges, &kLanguagePrivileges}) {
    RegisterKind<ByName, ByOid>(loader, *kind);
  }
  RegisterKind<std::tuple<duckdb::string_t, duckdb::string_t>,
               std::tuple<duckdb::string_t, int16_t>,
               std::tuple<int64_t, duckdb::string_t>,
               std::tuple<int64_t, int16_t>>(loader, kColumnPrivileges);
  RegisterKind<ByName>(loader, kParameterPrivileges);
  RegisterKind<ByOid>(loader, kLargeObjectPrivileges);
  RegisterPrivilegeFunctions<RoleCheck, ByName, ByOid>(loader, "pg_has_role",
                                                       nullptr);
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
