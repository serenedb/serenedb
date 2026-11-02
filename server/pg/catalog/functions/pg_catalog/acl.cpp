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
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/function/table_function.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <iresearch/utils/static_strings.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
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
#include "pg/types/logical.h"

namespace sdb::pg {
namespace {

using connector::AsView;
using connector::GetSereneDBContext;
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

DefaultAcl ClassAcl(char objtype, CatalogType type) {
  return {objtype, duckdb::Permissions::PublicPrivileges(type),
          duckdb::Permissions::AllPrivileges(type)};
}

const DefaultAcl kDefaultAcls[] = {
  {'c', AclMode::NoRights, AclMode::NoRights},
  ClassAcl('r', CatalogType::TABLE_ENTRY),
  ClassAcl('s', CatalogType::SEQUENCE_ENTRY),
  ClassAcl('d', CatalogType::DATABASE_ENTRY),
  ClassAcl('f', CatalogType::MACRO_ENTRY),
  {'l', AclMode::Usage, AclMode::Usage},
  {'L', AclMode::NoRights, AclMode::Select | AclMode::Update},
  ClassAcl('n', CatalogType::SCHEMA_ENTRY),
  {'t', AclMode::NoRights, AclMode::Create},
  {'F', AclMode::NoRights, AclMode::Usage},
  ClassAcl('S', CatalogType::FOREIGN_SERVER_ENTRY),
  ClassAcl('T', CatalogType::TYPE_ENTRY),
  {'p', AclMode::NoRights, AclMode::Set | AclMode::AlterSystem},
};

void AclDefaultFunction(duckdb::DataChunk& args, duckdb::ExpressionState& state,
                        duckdb::Vector& result) {
  const auto roles = auth::RolesOf(&state.GetContext());
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
    const auto it = absl::c_find_if(kDefaultAcls, [&](const DefaultAcl& acl) {
      return type.size() == 1 && type[0] == acl.objtype;
    });
    if (it == std::end(kDefaultAcls)) {
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

void ExplodeAcl(const auth::RoleGraph& roles, std::string_view text,
                std::vector<ExplodedAcl>& rows) {
  duckdb::AclItem item{};
  if (auto error = ParseAcl(text, roles, true, item)) {
    THROW_SQL_ERROR_FROM_DATA(std::move(*error));
  }
  for (const auto& privilege : kAclPrivileges) {
    if ((item.privs & privilege.mode) != AclMode::NoRights) {
      rows.emplace_back(ExplodedAcl{
        .grantor = static_cast<int64_t>(item.grantor),
        .grantee = static_cast<int64_t>(item.grantee),
        .privilege = privilege.name,
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
  const auto roles = auth::RolesOf(&context);
  const auto lists =
    input.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
  for (duckdb::idx_t row = 0; row < input.size(); ++row) {
    const auto list = lists[row];
    if (!list.IsValid()) {
      continue;
    }
    for (const auto item : list.GetChildValues()) {
      if (item.IsValid()) {
        ExplodeAcl(*roles, AsView(item.GetValue()), rows);
      }
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

std::optional<AclMode> FindPrivilege(std::string_view keyword) {
  if (absl::EqualsIgnoreCase(keyword, "TEMP")) {
    return AclMode::CreateTemp;
  }
  const auto it =
    absl::c_find_if(kAclPrivileges, [&](const AclPrivilege& privilege) {
      return absl::EqualsIgnoreCase(privilege.name, keyword);
    });
  if (it == kAclPrivileges.end()) {
    return std::nullopt;
  }
  return it->mode;
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
    const auto mode = FindPrivilege(keyword);
    if (!mode || (*mode & allowed) != *mode) {
      ThrowUnrecognizedPrivilege(chunk);
    }
    *held |= *mode;
  }
  return modes;
}

void MakeAclItemFunction(duckdb::DataChunk& args,
                         duckdb::ExpressionState& state,
                         duckdb::Vector& result) {
  const auto roles = auth::RolesOf(&state.GetContext());
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
        const auto mode = FindPrivilege(chunk);
        if (!mode) {
          ThrowUnrecognizedPrivilege(chunk);
        }
        privs |= *mode;
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
};

bool Holds(const auth::RoleClosure& closure, CatalogType type,
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
      closure.CanAny(type, permissions, modes.privs)) {
    return true;
  }
  auto columns = target.columns;
  if (target.any_column) {
    for (const auto& column : target.any_column->Logical()) {
      if (!column.Acl().empty()) {
        columns.emplace_back(column.Acl());
      }
    }
  }
  if (modes.privs != AclMode::NoRights && !columns.empty()) {
    for (auto bits = std::to_underlying(modes.privs); bits != 0;
         bits &= bits - 1) {
      const auto privilege = static_cast<AclMode>(bits & (~bits + 1));
      if (closure.CanAnyColumn(permissions, privilege, columns)) {
        return true;
      }
    }
  }
  if (modes.grant_options == AclMode::NoRights) {
    return false;
  }
  if (closure.Owns(permissions.owner)) {
    return true;
  }
  auto held = closure.GrantableModes(permissions.acl);
  for (const auto acl : columns) {
    held |= closure.GrantableModes(acl);
  }
  return (held & modes.grant_options) != AclMode::NoRights;
}

const duckdb::Permissions kBuiltinPermissions{.owner = kRootUser};

const duckdb::Permissions kForeignDataWrapperPermissions{
  .owner = kRootUser,
  .acl = {kForeignDataWrapperAcl.begin(), kForeignDataWrapperAcl.end()}};

CatalogType RelationType(const Relation& relation) {
  return relation.entry ? relation.entry->type : CatalogType::INVALID;
}

bool OwnerOnly(const Relation& relation) {
  return relation.key || RelationType(relation) == CatalogType::INDEX_ENTRY ||
         RelationType(relation) == CatalogType::TYPE_ENTRY;
}

AclMode RelationRights(const Relation& relation) {
  return RelationType(relation) == CatalogType::SEQUENCE_ENTRY
           ? duckdb::Permissions::AllPrivileges(CatalogType::SEQUENCE_ENTRY)
           : ~AclMode::NoRights;
}

duckdb::idx_t RoleIdByName(const auth::RoleGraph& roles,
                           std::string_view name) {
  if (const auto* role = roles.FindByName(name)) {
    return role->first;
  }
  ThrowUndefinedObject("role", name);
}

struct ObjectScope : Session {
  std::shared_ptr<const auth::RoleGraph> roles;
};

template<typename F>
struct ObjectPrivileges {
  using Scope = ObjectScope;

  static ObjectScope Begin(duckdb::ClientContext& context) {
    return {MakeSession(&context), auth::RolesOf(&context)};
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

  static bool Check(const auth::RoleClosure& closure, duckdb::idx_t,
                    const ObjectTarget& target, const PrivilegeModes& modes) {
    return Holds(closure, F::kType, target, modes);
  }
};

template<typename F>
struct RelationPrivileges : ObjectPrivileges<F> {
  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn(session, RegKind::Class, name, nullptr);
  }

  template<typename... Rest>
  static std::optional<ObjectTarget> Find(const Session& session, uint64_t oid,
                                          Rest... rest) {
    const auto relation = RelationByOid(session, oid);
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
    return Find(session, Resolve(session, name), rest...);
  }
};

struct TablePrivileges : RelationPrivileges<TablePrivileges> {
  static constexpr std::string_view kName = "has_table_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = AclMode::Select | AclMode::Insert |
                                    AclMode::Update | AclMode::Delete |
                                    AclMode::Truncate | AclMode::References |
                                    AclMode::Trigger | AclMode::Maintain;
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

constexpr AclMode kColumnModes =
  AclMode::Select | AclMode::Insert | AclMode::Update | AclMode::References;

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
    } else if (RelationType(relation) == CatalogType::VIEW_ENTRY &&
               ViewAttnum(
                 *session.context,
                 relation.entry.get_mutable()->Cast<duckdb::ViewCatalogEntry>(),
                 name) != 0) {
      return ObjectTarget{relation.permissions};
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
      const auto info = ViewColumns(
        *session.context,
        relation.entry.get_mutable()->Cast<duckdb::ViewCatalogEntry>());
      SDB_ASSERT(info);
      count = info->types.size();
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
  static constexpr AclMode kModes = AclMode::Usage | AclMode::Create;
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
  static constexpr AclMode kModes =
    AclMode::Usage | AclMode::Select | AclMode::Update;
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
  static constexpr AclMode kModes = AclMode::Execute;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn(session, RegKind::Procedure, name, nullptr);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    if (const auto* macro = FindMacro(session, oid)) {
      return ObjectTarget{&PrivilegePermissions(*macro)};
    }
    if (!GetBuiltinFunctions(*session.context)->Find(oid)) {
      return std::nullopt;
    }
    return ObjectTarget{&kBuiltinPermissions};
  }
};

struct TypePrivileges : ObjectPrivileges<TypePrivileges> {
  static constexpr std::string_view kName = "has_type_privilege";
  static constexpr CatalogType kType = CatalogType::TYPE_ENTRY;
  static constexpr AclMode kModes = AclMode::Usage;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn(session, RegKind::Type, name, nullptr);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    if (const auto entry = EntryByOid(session, oid)) {
      if (entry->type != CatalogType::TYPE_ENTRY) {
        return std::nullopt;
      }
      return ObjectTarget{&PrivilegePermissions(*entry)};
    }
    if (oid >= kMaxSystem || !FindBuiltinType(static_cast<int32_t>(oid))) {
      return std::nullopt;
    }
    return ObjectTarget{&kBuiltinPermissions};
  }
};

struct DatabasePrivileges : ObjectPrivileges<DatabasePrivileges> {
  static constexpr std::string_view kName = "has_database_privilege";
  static constexpr CatalogType kType = CatalogType::DATABASE_ENTRY;
  static constexpr AclMode kModes =
    AclMode::Create | AclMode::CreateTemp | AclMode::Connect;
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
  static constexpr AclMode kModes = AclMode::Usage;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    SDB_ASSERT(session.database);
    if (const auto entry =
          session.database->GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
            .GetEntry(*session.transaction, Identifier{name})) {
      return entry->oid;
    }
    ThrowUndefinedObject("server", name);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    const auto entry = EntryByOid(session, oid);
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
  static constexpr CatalogType kType = CatalogType::FOREIGN_SERVER_ENTRY;
  static constexpr AclMode kModes = AclMode::Usage;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session&, std::string_view name) {
    if (const auto* wrapper = FindSystemObject(kForeignDataWrappers, name)) {
      return wrapper->oid;
    }
    ThrowUndefinedObject("foreign-data wrapper", name);
  }

  static std::optional<ObjectTarget> Find(const Session&, uint64_t oid) {
    if (!FindSystemObject(kForeignDataWrappers, oid)) {
      return std::nullopt;
    }
    return ObjectTarget{&kForeignDataWrapperPermissions};
  }
};

struct TablespacePrivileges : ObjectPrivileges<TablespacePrivileges> {
  static constexpr std::string_view kName = "has_tablespace_privilege";
  static constexpr CatalogType kType = CatalogType::DATABASE_ENTRY;
  static constexpr AclMode kModes = AclMode::Create;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session&, std::string_view name) {
    if (const auto* tablespace = FindSystemObject(kSystemTablespaces, name)) {
      return tablespace->oid;
    }
    ThrowUndefinedObject("tablespace", name);
  }

  static std::optional<ObjectTarget> Find(const Session&, uint64_t oid) {
    if (!FindSystemObject(kSystemTablespaces, oid)) {
      return std::nullopt;
    }
    return ObjectTarget{&kBuiltinPermissions};
  }
};

struct LanguagePrivileges : ObjectPrivileges<LanguagePrivileges> {
  static constexpr std::string_view kName = "has_language_privilege";
  static constexpr CatalogType kType = CatalogType::TYPE_ENTRY;
  static constexpr AclMode kModes = AclMode::Usage;
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
  static constexpr AclMode kModes = AclMode::Set | AclMode::AlterSystem;
  static constexpr bool kSuperuserFirst = false;

  static uint64_t Resolve(const Session&, std::string_view) {
    return kInvalidOid;
  }

  static std::optional<ObjectTarget> Find(const Session&, uint64_t) {
    return ObjectTarget{&kBuiltinPermissions};
  }

  static bool Check(const auth::RoleClosure& closure, duckdb::idx_t,
                    const ObjectTarget&, const PrivilegeModes&) {
    return closure.is_superuser;
  }
};

struct LargeObjectPrivileges : ObjectPrivileges<LargeObjectPrivileges> {
  static constexpr std::string_view kName = "has_largeobject_privilege";
  static constexpr AclMode kModes = AclMode::Select | AclMode::Update;
  static constexpr bool kSuperuserFirst = false;

  static std::optional<ObjectTarget> Find(const Session&, uint64_t) {
    return std::nullopt;
  }

  static bool Check(const auth::RoleClosure&, duckdb::idx_t,
                    const ObjectTarget&, const PrivilegeModes&) {
    SDB_UNREACHABLE();
  }
};

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
    std::shared_ptr<const auth::RoleGraph> roles;
  };

  static Scope Begin(duckdb::ClientContext& context) {
    return {auth::RolesOf(&context)};
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

  static bool Check(const auth::RoleClosure& closure, duckdb::idx_t member,
                    duckdb::idx_t role, const RolePrivMask& mask) {
    if (closure.is_superuser) {
      return true;
    }
    if (member == role) {
      return mask.usage || mask.member || mask.set;
    }
    return (mask.usage && closure.MemberOf(role)) ||
           (mask.member && closure.IsMember(role)) ||
           (mask.set && closure.CanSet(role)) ||
           (mask.admin && closure.IsAdminOf(role));
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

template<typename F, typename... Rest>
auto FindObject(const typename F::Scope& scope, duckdb::string_t name,
                Rest... rest) {
  if constexpr (requires { F::FindNamed(scope, AsView(name), rest...); }) {
    return F::FindNamed(scope, AsView(name), rest...);
  } else {
    return F::Find(scope, F::Resolve(scope, AsView(name)), rest...);
  }
}

template<typename F, typename... Rest>
auto FindObject(const typename F::Scope& scope, int64_t oid, Rest... rest) {
  return F::Find(scope, static_cast<uint64_t>(oid), rest...);
}

template<typename F>
class PrivilegeCheck final : public duckdb::FunctionLocalState {
 public:
  explicit PrivilegeCheck(duckdb::ClientContext& context)
    : _context{context}, _scope{F::Begin(context)} {}

  PrivilegeCheck& ForChunk() {
    _named.reset();
    _modes.reset();
    return *this;
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
    const auto target = FindObject<F>(_scope, object, rest...);
    const auto& modes = Modes(text);
    if (target) {
      return F::Check(Closure(role), role, *target, modes);
    }
    if constexpr (F::kSuperuserFirst) {
      if (Closure(role).is_superuser) {
        return true;
      }
    }
    return duckdb::nullopt;
  }

 private:
  struct NamedRole {
    duckdb::string_t name;
    duckdb::idx_t role;
  };

  struct ParsedModes {
    duckdb::string_t text;
    typename F::Modes modes;
  };

  struct RoleClosureOf {
    duckdb::idx_t role;
    std::shared_ptr<const auth::RoleClosure> closure;
  };

  const typename F::Modes& Modes(duckdb::string_t text) {
    if (!_modes || text != _modes->text) {
      _modes = ParsedModes{.text = text, .modes = F::Parse(AsView(text))};
    }
    return _modes->modes;
  }

  const auth::RoleClosure& Closure(duckdb::idx_t role) {
    if (!_closure || role != _closure->role) {
      _closure = RoleClosureOf{.role = role,
                               .closure = auth::ClosureFor(&_context, role)};
    }
    return *_closure->closure;
  }

  duckdb::ClientContext& _context;
  typename F::Scope _scope;
  std::optional<NamedRole> _named;
  std::optional<ParsedModes> _modes;
  std::optional<RoleClosureOf> _closure;
};

template<typename F>
duckdb::unique_ptr<duckdb::FunctionLocalState> InitPrivilegeCheck(
  duckdb::ExpressionState& state, const duckdb::BoundFunctionExpression&,
  duckdb::FunctionData*) {
  return duckdb::make_uniq<PrivilegeCheck<F>>(state.GetContext());
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
      if constexpr (std::is_void_v<Role>) {
        const auto role = GetSereneDBContext(state.GetContext()).GetRoleId();
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
  std::span<const duckdb::AclItem> items, const auth::RoleGraph& roles,
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
                 {duckdb::ScalarFunction{{duckdb::LogicalType::VARCHAR, OID()},
                                         duckdb::LogicalType::LIST(ACLITEM()),
                                         AclDefaultFunction}});
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
}

}  // namespace sdb::pg
