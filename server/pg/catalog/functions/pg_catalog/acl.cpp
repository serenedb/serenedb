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
#include <duckdb/main/database_manager.hpp>
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
      items.emplace_back(duckdb::AclItem{
        .grantee = kPublicGrantee, .grantor = owner_id, .privs = it->world});
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

struct ObjectScope : Session {
  duckdb::shared_ptr<const duckdb::SecuritySnapshot> roles;
};

template<typename F>
struct ObjectPrivileges {
  using Scope = ObjectScope;

  static ObjectScope Begin(duckdb::ClientContext& context,
                           duckdb::optional_idx caller) {
    return {MakeSession(&context, caller), catalog::RolesOf(&context)};
  }

  static duckdb::idx_t Role(const ObjectScope& scope, duckdb::string_t name) {
    const auto text = AsView(name);
    if (text == irs::StaticStrings::kPublic) {
      return kPublicGrantee;
    }
    return RoleIdByName(*scope.roles, text);
  }

  using Modes = PrivilegeModes;

  static PrivilegeModes Parse(std::string_view text) {
    return ParsePrivileges(text, F::kModes);
  }

  static bool Check(const ObjectScope&, const duckdb::RoleAccess& closure,
                    duckdb::idx_t, const ObjectTarget& target,
                    const PrivilegeModes& modes) {
    return Holds(closure, F::kType, target, modes);
  }
};

template<typename F>
struct RelationPrivileges : ObjectPrivileges<F> {
  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn(session, RegKind::Class, name, nullptr);
  }

  template<typename... Rest>
  static std::optional<ObjectTarget> Find(
    const Session& session, uint64_t oid,
    duckdb::optional_ptr<duckdb::CatalogEntry> entry, Rest... rest) {
    const auto relation = RelationByOid(session, oid, entry);
    if (!relation) {
      return std::nullopt;
    }
    return F::Target(session, *relation, rest...);
  }

  template<typename... Rest>
  static std::optional<ObjectTarget> FindNamed(const Session& session,
                                               std::string_view name,
                                               Rest... rest) {
    if (const auto entry = AttachedRelation(session, name)) {
      return F::Target(session,
                       RelationAccess{{entry, nullptr, nullptr, nullptr},
                                      &PrivilegePermissions(*entry)},
                       rest...);
    }
    const auto oid = Resolve(session, name);
    return Find(session, oid, EntryByOid(session, oid), rest...);
  }
};

struct TablePrivileges : RelationPrivileges<TablePrivileges> {
  static constexpr std::string_view kName = "has_table_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> Target(const Session&,
                                            const RelationAccess& relation) {
    return ObjectTarget{relation.permissions,
                        {},
                        OwnerOnly(relation),
                        nullptr,
                        RelationRights(relation)};
  }
};

constexpr AclMode kColumnModes = duckdb::ACL_COLUMN_PRIVILEGES;

struct AnyColumnPrivileges : RelationPrivileges<AnyColumnPrivileges> {
  static constexpr std::string_view kName = "has_any_column_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = kColumnModes;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> Target(const Session&,
                                            const RelationAccess& relation) {
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
};

struct ColumnPrivileges : RelationPrivileges<ColumnPrivileges> {
  static constexpr std::string_view kName = "has_column_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = kColumnModes;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> Target(const Session& session,
                                            const RelationAccess& relation,
                                            duckdb::string_t column) {
    const Identifier name{AsView(column)};
    if (OwnerOnly(relation)) {
      if (absl::c_linear_search(RelationAttributeNames(session, relation),
                                AsView(column))) {
        return ObjectTarget{relation.permissions, {}, true};
      }
    } else if (const auto* columns = TableColumns(relation)) {
      if (columns->ColumnExists(name)) {
        return ObjectTarget{relation.permissions,
                            {columns->GetColumn(name).Acl()}};
      }
    } else if (RelationType(relation) == CatalogType::SEQUENCE_ENTRY) {
      if (absl::c_linear_search(kSequenceColumns, AsView(column))) {
        return ObjectTarget{relation.permissions};
      }
    } else if (RelationType(relation) == CatalogType::VIEW_ENTRY) {
      auto& view =
        relation.entry.get_mutable()->Cast<duckdb::ViewCatalogEntry>();
      if (const auto attnum = ViewAttnum(*session.context, view, name)) {
        return ObjectTarget{relation.permissions, {view.ColumnAcl(attnum - 1)}};
      }
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", AsView(column), "\" of relation \"",
                            NameOf(*session.transaction, relation).name,
                            "\" does not exist"));
  }

  static std::optional<ObjectTarget> Target(const Session& session,
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
      auto& view =
        relation.entry.get_mutable()->Cast<duckdb::ViewCatalogEntry>();
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
};

struct SchemaPrivileges : ObjectPrivileges<SchemaPrivileges> {
  static constexpr std::string_view kName = "has_schema_privilege";
  static constexpr CatalogType kType = CatalogType::SCHEMA_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *SchemaOid(session, name, nullptr);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    if (FindSystemObject(kSystemNamespaces, oid)) {
      return ObjectTarget{&SchemaPermissions()};
    }
    if (const auto schema = SchemaByOid(session, oid)) {
      return ObjectTarget{&PrivilegePermissions(*schema)};
    }
    return std::nullopt;
  }
};

struct SequencePrivileges : RelationPrivileges<SequencePrivileges> {
  static constexpr std::string_view kName = "has_sequence_privilege";
  static constexpr CatalogType kType = CatalogType::SEQUENCE_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> Target(const Session& session,
                                            const RelationAccess& relation) {
    if (RelationType(relation) != CatalogType::SEQUENCE_ENTRY) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", NameOf(*session.transaction, relation).name,
                              "\" is not a sequence"));
    }
    return ObjectTarget{relation.permissions};
  }
};

struct FunctionPrivileges : ObjectPrivileges<FunctionPrivileges> {
  static constexpr std::string_view kName = "has_function_privilege";
  static constexpr CatalogType kType = CatalogType::MACRO_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn(session, RegKind::Procedure, name, nullptr);
  }

  static std::optional<ObjectTarget> Find(
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
};

struct TypePrivileges : ObjectPrivileges<TypePrivileges> {
  static constexpr std::string_view kName = "has_type_privilege";
  static constexpr CatalogType kType = CatalogType::TYPE_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn(session, RegKind::Type, name, nullptr);
  }

  static std::optional<ObjectTarget> Find(
    const Session& session, uint64_t oid,
    duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
    return Type(session, oid, entry, true);
  }

  static std::optional<ObjectTarget> Row(
    const Session& session, uint64_t oid,
    duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
    return Type(session, oid, entry, false);
  }

  static std::optional<ObjectTarget> Type(
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
      return Type(session, element, EntryByOid(session, element), true);
    }
    if (!FindBuiltinType(static_cast<int32_t>(oid))) {
      return std::nullopt;
    }
    return ObjectTarget{&kBuiltinPermissions};
  }
};

struct DatabasePrivileges : ObjectPrivileges<DatabasePrivileges> {
  static constexpr std::string_view kName = "has_database_privilege";
  static constexpr CatalogType kType = CatalogType::DATABASE_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    if (const auto database = FindDatabase(*session.context, name)) {
      return database->oid;
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_DATABASE),
                    ERR_MSG("database \"", name, "\" does not exist"));
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    const auto database = FindDatabaseById(*session.context, oid);
    if (!database) {
      return std::nullopt;
    }
    return ObjectTarget{&PrivilegePermissions(*database)};
  }
};

struct ServerPrivileges : ObjectPrivileges<ServerPrivileges> {
  static constexpr std::string_view kName = "has_server_privilege";
  static constexpr CatalogType kType = CatalogType::FOREIGN_SERVER_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    SDB_ASSERT(session.database);
    if (const auto entry = session.database->Servers().Get(*session.transaction,
                                                           Identifier{name})) {
      return entry->oid;
    }
    ThrowUndefinedObject("server", name);
  }

  static std::optional<ObjectTarget> Find(
    const Session& session, uint64_t,
    duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
    if (!entry || entry->type != CatalogType::FOREIGN_SERVER_ENTRY) {
      return std::nullopt;
    }
    return ObjectTarget{&PrivilegePermissions(*entry)};
  }
};

struct ForeignDataWrapperPrivileges
  : ObjectPrivileges<ForeignDataWrapperPrivileges> {
  static constexpr std::string_view kName =
    "has_foreign_data_wrapper_privilege";
  static constexpr CatalogType kType = CatalogType::FOREIGN_DATA_WRAPPER_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  struct Scope : ObjectScope {
    std::array<const duckdb::Permissions*, kForeignDataWrappers.size()>
      wrappers;
  };

  static Scope Begin(duckdb::ClientContext& context,
                     duckdb::optional_idx caller) {
    Scope scope{ObjectPrivileges::Begin(context, caller)};
    auto catalog = duckdb::Catalog::GetCatalogEntry(
      context, duckdb::DatabaseManager::GetDefaultDatabase(context));
    for (size_t i = 0; i < kForeignDataWrappers.size(); ++i) {
      scope.wrappers[i] = &kBuiltinPermissions;
      if (!catalog) {
        continue;
      }
      if (const auto entry = catalog->LookupTopLevelEntry(
            catalog->GetCatalogTransaction(context),
            duckdb::CatalogType::FOREIGN_DATA_WRAPPER_ENTRY,
            duckdb::Identifier{kForeignDataWrappers[i].name})) {
        scope.wrappers[i] = &entry->permissions;
      }
    }
    return scope;
  }

  static uint64_t Resolve(const Session&, std::string_view name) {
    if (const auto* wrapper = FindSystemObject(kForeignDataWrappers, name)) {
      return wrapper->oid;
    }
    ThrowUndefinedObject("foreign-data wrapper", name);
  }

  static std::optional<ObjectTarget> Find(const Scope& scope, uint64_t oid) {
    const auto* wrapper = FindSystemObject(kForeignDataWrappers, oid);
    if (!wrapper) {
      return std::nullopt;
    }
    return ObjectTarget{scope.wrappers[wrapper - kForeignDataWrappers.data()]};
  }
};

struct TablespacePrivileges : ObjectPrivileges<TablespacePrivileges> {
  static constexpr std::string_view kName = "has_tablespace_privilege";
  static constexpr CatalogType kType = CatalogType::TABLESPACE_ENTRY;
  static constexpr AclMode kModes = AclMode::Create;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    auto& cluster = catalog::ClusterOf(*session.context);
    if (const auto tablespace = cluster.Set(kType).Get(
          cluster.GetCatalogTransaction(*session.context),
          duckdb::Identifier{name})) {
      return tablespace->oid;
    }
    ThrowUndefinedObject("tablespace", name);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    auto& cluster = catalog::ClusterOf(*session.context);
    const auto tablespace = cluster.FindIn<catalog::TablespaceCatalogEntry>(
      cluster.GetCatalogTransaction(*session.context), oid);
    if (!tablespace) {
      return std::nullopt;
    }
    return ObjectTarget{&PrivilegePermissions(*tablespace)};
  }
};

struct LanguagePrivileges : ObjectPrivileges<LanguagePrivileges> {
  static constexpr std::string_view kName = "has_language_privilege";
  static constexpr CatalogType kType = CatalogType::LANGUAGE_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session&, std::string_view name) {
    if (const auto* language = FindSystemObject(kSystemLanguages, name)) {
      return language->oid;
    }
    ThrowUndefinedObject("language", name);
  }

  static std::optional<ObjectTarget> Find(const Session&, uint64_t oid) {
    if (!FindSystemObject(kSystemLanguages, oid)) {
      return std::nullopt;
    }
    return ObjectTarget{&kBuiltinPermissions};
  }
};

struct ParameterPrivileges : ObjectPrivileges<ParameterPrivileges> {
  static constexpr std::string_view kName = "has_parameter_privilege";
  static constexpr CatalogType kType = CatalogType::PARAMETER_ENTRY;
  static constexpr AclMode kModes = duckdb::ObjectKindOf(kType).privileges;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> FindNamed(const Session& session,
                                               std::string_view name) {
    auto& cluster = catalog::ClusterOf(*session.context);
    const auto parameter =
      cluster.Set(kType).Get(cluster.GetCatalogTransaction(*session.context),
                             duckdb::Identifier{name});
    return ObjectTarget{parameter ? &PrivilegePermissions(*parameter)
                                  : &kBuiltinPermissions};
  }
};

struct LargeObjectPrivileges : ObjectPrivileges<LargeObjectPrivileges> {
  static constexpr std::string_view kName = "has_largeobject_privilege";
  static constexpr AclMode kModes = AclMode::Select | AclMode::Update;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> Find(const Session&, uint64_t) {
    return std::nullopt;
  }

  static bool Check(const ObjectScope&, const duckdb::RoleAccess&,
                    duckdb::idx_t, const ObjectTarget&, const PrivilegeModes&) {
    SDB_UNREACHABLE();
  }
};

#include "pg/catalog/generated/object_classes.gen.inc"

class ObjectAcls final : public duckdb::FunctionLocalState {
 public:
  ObjectAcls(duckdb::ClientContext& context, duckdb::optional_idx caller)
    : _wrappers{ForeignDataWrapperPrivileges::Begin(context, caller)} {}

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

  const duckdb::SecuritySnapshot& Roles() const { return *_wrappers.roles; }

 private:
  std::optional<ObjectTarget> Find(uint64_t classid, uint64_t objid,
                                   int32_t objsubid) const {
    const Session& session = _wrappers;
    if (classid == kPgClassTable) {
      if (objsubid == 0) {
        return TablePrivileges::Find(session, objid,
                                     EntryByOid(session, objid));
      }
      if (objsubid < 1 || objsubid > std::numeric_limits<int16_t>::max()) {
        return std::nullopt;
      }
      return ColumnPrivileges::Find(session, objid, EntryByOid(session, objid),
                                    static_cast<int16_t>(objsubid));
    }
    if (classid == kPgProcTable) {
      return FunctionPrivileges::Find(session, objid,
                                      EntryByOid(session, objid));
    }
    if (classid == kPgTypeTable) {
      return TypePrivileges::Row(session, objid, EntryByOid(session, objid));
    }
    if (classid == kPgNamespaceTable) {
      return SchemaPrivileges::Find(session, objid);
    }
    if (classid == kPgDatabaseTable) {
      return DatabasePrivileges::Find(session, objid);
    }
    if (classid == kPgForeignServerTable) {
      return ServerPrivileges::Find(session, objid, EntryByOid(session, objid));
    }
    if (classid == kPgForeignDataWrapperTable) {
      return ForeignDataWrapperPrivileges::Find(_wrappers, objid);
    }
    if (classid == kPgLanguageTable) {
      return LanguagePrivileges::Find(session, objid);
    }
    if (classid == kPgTablespaceTable) {
      return TablespacePrivileges::Find(session, objid);
    }
    return std::nullopt;
  }

  ForeignDataWrapperPrivileges::Scope _wrappers;
};

duckdb::unique_ptr<duckdb::FunctionLocalState> InitObjectAcls(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<ObjectAcls>(state.GetContext(), expr.CallerRole());
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

struct RolePrivMask {
  bool usage = false;
  bool member = false;
  bool set = false;
  bool admin = false;
};

struct RolePrivileges {
  static constexpr std::string_view kName = "pg_has_role";
  static constexpr bool kSuperuserFirst = false;

  struct Scope {
    duckdb::shared_ptr<const duckdb::SecuritySnapshot> roles;
    mutable duckdb::idx_t member = kInvalidOid;
    mutable duckdb::RoleMemberships memberships;
  };

  static Scope Begin(duckdb::ClientContext& context, duckdb::optional_idx) {
    return {.roles = catalog::RolesOf(&context)};
  }

  static const duckdb::RoleMemberships& MembershipsOf(const Scope& scope,
                                                      duckdb::idx_t member) {
    if (scope.member != member) {
      scope.memberships = scope.roles->MembershipsOf(member);
      scope.member = member;
    }
    return scope.memberships;
  }

  static duckdb::idx_t Role(const Scope& scope, duckdb::string_t name) {
    return RoleIdByName(*scope.roles, AsView(name));
  }

  static uint64_t Resolve(const Scope& scope, std::string_view name) {
    return RoleIdByName(*scope.roles, name);
  }

  static std::optional<duckdb::idx_t> Find(const Scope&, uint64_t oid) {
    return oid;
  }

  using Modes = RolePrivMask;

  static RolePrivMask Parse(std::string_view text) {
    RolePrivMask mask;
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

  static bool Check(const Scope& scope, const duckdb::RoleAccess& closure,
                    duckdb::idx_t member, duckdb::idx_t role,
                    const RolePrivMask& mask) {
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
    const auto& memberships = MembershipsOf(scope, member);
    return (mask.member && memberships.IsMember(role)) ||
           (mask.admin && memberships.IsAdminOf(role));
  }
};

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

template<typename F>
class PrivilegeCheck final : public duckdb::FunctionLocalState {
 public:
  PrivilegeCheck(duckdb::ClientContext& context, duckdb::optional_idx caller)
    : _scope{F::Begin(context, caller)} {}

  PrivilegeCheck& ForChunk() {
    _named.reset();
    _modes.reset();
    return *this;
  }

  void Prefetch(duckdb::Vector& oids) {
    if constexpr (kFindsEntries) {
      _oids.clear();
      _next = 0;
      if (oids.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
        return;
      }
      for (const auto oid : oids.ValidValues<int64_t>()) {
        _oids.push_back(static_cast<duckdb::idx_t>(oid.GetValue()));
      }
      _entries.resize(_oids.size());
      EntriesByOid(_scope, _oids, _entries);
    }
  }

  duckdb::idx_t Role(duckdb::string_t name) {
    if (!_named || name != _named->name) {
      _named = NamedRole{.name = name, .role = F::Role(_scope, name)};
    }
    return _named->role;
  }

  duckdb::idx_t Role(int64_t oid) { return static_cast<duckdb::idx_t>(oid); }

  template<typename Object, typename... Rest>
  duckdb::optional<bool> Answer(duckdb::idx_t role, duckdb::string_t text,
                                Object object, Rest... rest) {
    const auto target = FindObject(object, rest...);
    const auto& modes = Modes(text);
    if (target) {
      return F::Check(_scope, Closure(role), role, *target, modes);
    }
    if constexpr (F::kSuperuserFirst) {
      if (Closure(role).is_superuser) {
        return true;
      }
    }
    return duckdb::nullopt;
  }

 private:
  static constexpr bool kFindsEntries =
    requires(const typename F::Scope& scope, uint64_t oid,
             duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
      F::Find(scope, oid, entry);
    };

  struct NamedRole {
    duckdb::string_t name;
    duckdb::idx_t role;
  };

  struct ParsedModes {
    duckdb::string_t text;
    typename F::Modes modes;
  };

  template<typename... Rest>
  auto FindObject(duckdb::string_t name, Rest... rest) {
    if constexpr (requires { F::FindNamed(_scope, AsView(name), rest...); }) {
      return F::FindNamed(_scope, AsView(name), rest...);
    } else {
      return FindObject(F::Resolve(_scope, AsView(name)), rest...);
    }
  }

  template<typename... Rest>
  auto FindObject(uint64_t oid, Rest... rest) {
    if constexpr (kFindsEntries) {
      return F::Find(_scope, oid, Entry(oid), rest...);
    } else {
      return F::Find(_scope, oid, rest...);
    }
  }

  duckdb::optional_ptr<duckdb::CatalogEntry> Entry(uint64_t oid) {
    for (auto next = _next; next < _oids.size(); ++next) {
      if (_oids[next] == oid) {
        _next = next + 1;
        return _entries[next];
      }
    }
    return EntryByOid(_scope, oid);
  }

  const typename F::Modes& Modes(duckdb::string_t text) {
    if (!_modes || text != _modes->text) {
      _modes = ParsedModes{.text = text, .modes = F::Parse(AsView(text))};
    }
    return _modes->modes;
  }

  const duckdb::RoleAccess& Closure(duckdb::idx_t role) {
    if (!_closure || role != _closure->role) {
      _closure = _scope.roles->AccessOf(role);
    }
    return *_closure;
  }

  typename F::Scope _scope;
  std::optional<NamedRole> _named;
  std::optional<ParsedModes> _modes;
  std::optional<duckdb::RoleAccess> _closure;
  std::vector<duckdb::idx_t> _oids;
  std::vector<duckdb::optional_ptr<duckdb::CatalogEntry>> _entries;
  size_t _next = 0;
};

template<typename F>
duckdb::unique_ptr<duckdb::FunctionLocalState> InitPrivilegeCheck(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression& expr,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<PrivilegeCheck<F>>(state.GetContext(),
                                              expr.CallerRole());
}

template<typename F, typename Role, typename... Object>
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
                      ->Cast<PrivilegeCheck<F>>()
                      .ForChunk();
      if constexpr (std::is_same_v<
                      std::tuple_element_t<0, std::tuple<Object...>>,
                      int64_t>) {
        check.Prefetch(args.data[std::is_void_v<Role> ? 0 : 1]);
      }
      if constexpr (std::is_void_v<Role>) {
        const auto caller =
          state.expr.Cast<duckdb::BoundFunctionExpression>().CallerRole();
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
    nullptr,
    nullptr,
    InitPrivilegeCheck<F>};
}

template<typename F, typename... Object>
void AddPrivilegeForms(duckdb::ScalarFunctionSet& set,
                       std::type_identity<std::tuple<Object...>>) {
  set.AddFunction(PrivilegeFunction<F, duckdb::string_t, Object...>());
  set.AddFunction(PrivilegeFunction<F, int64_t, Object...>());
  set.AddFunction(PrivilegeFunction<F, void, Object...>());
}

template<typename F, typename... Objects>
void RegisterPrivilegeFunctions(duckdb::ExtensionLoader& loader) {
  duckdb::ScalarFunctionSet set{Identifier{F::kName}};
  (AddPrivilegeForms<F>(set, std::type_identity<Objects>{}), ...);
  set.SetFallible();
  Register(loader, irs::StaticStrings::kPgCatalogSchema,
           duckdb::FunctionStability::CONSISTENT_WITHIN_QUERY, std::move(set));
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
  RegisterPrivilegeFunctions<TablePrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<AnyColumnPrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<
    ColumnPrivileges, std::tuple<duckdb::string_t, duckdb::string_t>,
    std::tuple<duckdb::string_t, int16_t>,
    std::tuple<int64_t, duckdb::string_t>, std::tuple<int64_t, int16_t>>(
    loader);
  RegisterPrivilegeFunctions<SchemaPrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<SequencePrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<FunctionPrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<TypePrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<DatabasePrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<ServerPrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<ForeignDataWrapperPrivileges, ByName, ByOid>(
    loader);
  RegisterPrivilegeFunctions<TablespacePrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<LanguagePrivileges, ByName, ByOid>(loader);
  RegisterPrivilegeFunctions<ParameterPrivileges, ByName>(loader);
  RegisterPrivilegeFunctions<LargeObjectPrivileges, ByOid>(loader);
  RegisterPrivilegeFunctions<RolePrivileges, ByName, ByOid>(loader);
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
                            PgGetAclFunction}});
}

}  // namespace sdb::pg
