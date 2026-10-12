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

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/index_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/type_catalog_entry.hpp>
#include <duckdb/catalog/catalog_entry/view_catalog_entry.hpp>
#include <duckdb/catalog/entry_lookup_info.hpp>
#include <duckdb/common/vector_operations/variadic_executor.hpp>
#include <duckdb/function/table_function.hpp>
#include <duckdb/parser/parsed_data/create_table_function_info.hpp>
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
#include "connector/pg_logical_types.h"
#include "pg/catalog/builtin/builtin.h"
#include "pg/catalog/engine/builtin_functions.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/functions/pg_catalog/regproc.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"

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
  std::vector<std::string> items;
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
    items.clear();
    if (it->world != AclMode::NoRights) {
      AppendAcl(
        items.emplace_back(),
        duckdb::AclItem{
          .grantee = kPublicGrantee, .grantor = owner_id, .privs = it->world},
        *roles);
    }
    if (it->owner != AclMode::NoRights) {
      AppendAcl(items.emplace_back(),
                duckdb::AclItem{
                  .grantee = owner_id, .grantor = owner_id, .privs = it->owner},
                *roles);
    }
    size_t i = 0;
    for (auto& child : writer.WriteList(items.size())) {
      child.WriteValue(duckdb::string_t{
        items[i].data(), static_cast<uint32_t>(items[i].size())});
      ++i;
    }
  }
}

struct ExplodedAcl {
  int64_t grantor;
  int64_t grantee;
  std::string_view privilege;
  bool grantable;
};

class AclReader {
 public:
  explicit AclReader(std::string_view text) : _text{text} {}

  std::string_view Role() {
    if (!_text.empty() && _text.front() == '"') {
      _buffer.clear();
      size_t pos = 1;
      while (pos < _text.size()) {
        if (_text[pos] == '"') {
          if (pos + 1 < _text.size() && _text[pos + 1] == '"') {
            _buffer.push_back('"');
            pos += 2;
            continue;
          }
          ++pos;
          break;
        }
        _buffer.push_back(_text[pos]);
        ++pos;
      }
      _text.remove_prefix(pos);
      return _buffer;
    }
    const auto end = _text.find_first_of("=/");
    const auto role = _text.substr(0, end);
    _text.remove_prefix(role.size());
    return role;
  }

  bool Skip(char c) {
    if (_text.empty() || _text.front() != c) {
      return false;
    }
    _text.remove_prefix(1);
    return true;
  }

  std::string_view Privileges() {
    const auto privileges = _text.substr(0, _text.find('/'));
    _text.remove_prefix(privileges.size());
    return privileges;
  }

 private:
  std::string_view _text;
  std::string _buffer;
};

int64_t RoleIdOf(duckdb::ClientContext& context, std::string_view name) {
  if (name.empty()) {
    return static_cast<int64_t>(kPublicGrantee);
  }
  if (const auto* role = FindRole(context, name)) {
    return static_cast<int64_t>(role->oid);
  }
  int64_t oid = 0;
  if (absl::SimpleAtoi(name, &oid)) {
    return oid;
  }
  return 0;
}

void ExplodeAcl(duckdb::ClientContext& context, std::string_view text,
                std::vector<ExplodedAcl>& rows) {
  AclReader reader{text};
  const auto grantee = RoleIdOf(context, reader.Role());
  if (!reader.Skip('=')) {
    return;
  }
  const auto privileges = reader.Privileges();
  int64_t grantor = 0;
  if (reader.Skip('/')) {
    grantor = RoleIdOf(context, reader.Role());
  }
  AclMode held = AclMode::NoRights;
  AclMode grantable = AclMode::NoRights;
  for (size_t i = 0; i < privileges.size(); ++i) {
    const auto it = absl::c_find_if(kAclPrivileges, [&](const AclPrivilege& p) {
      return p.chr == privileges[i];
    });
    if (it == kAclPrivileges.end()) {
      continue;
    }
    held |= it->mode;
    if (i + 1 < privileges.size() && privileges[i + 1] == '*') {
      grantable |= it->mode;
      ++i;
    }
  }
  for (const auto& privilege : kAclPrivileges) {
    if ((held & privilege.mode) != AclMode::NoRights) {
      rows.emplace_back(ExplodedAcl{
        .grantor = grantor,
        .grantee = grantee,
        .privilege = privilege.name,
        .grantable = (grantable & privilege.mode) != AclMode::NoRights});
    }
  }
}

struct AclExplodeState final : duckdb::LocalTableFunctionState {
  std::vector<ExplodedAcl> rows;
  size_t next = 0;
  bool loaded = false;
};

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

duckdb::unique_ptr<duckdb::LocalTableFunctionState> AclExplodeInit(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState*) {
  return duckdb::make_uniq<AclExplodeState>();
}

duckdb::OperatorResultType AclExplodeFunction(duckdb::ExecutionContext& context,
                                              duckdb::TableFunctionInput& data,
                                              duckdb::DataChunk& input,
                                              duckdb::DataChunk& output) {
  auto& state = data.local_state->Cast<AclExplodeState>();
  if (!state.loaded) {
    state.rows.clear();
    state.next = 0;
    state.loaded = true;
    const auto lists =
      input.data[0].Values<duckdb::VectorListType<duckdb::string_t>>();
    for (duckdb::idx_t row = 0; row < input.size(); ++row) {
      const auto list = lists[row];
      if (!list.IsValid()) {
        continue;
      }
      for (const auto item : list.GetChildValues()) {
        if (item.IsValid()) {
          ExplodeAcl(context.client, AsView(item.GetValue()), state.rows);
        }
      }
    }
  }
  if (state.next == state.rows.size()) {
    state.loaded = false;
    output.SetCardinality(0);
    return duckdb::OperatorResultType::NEED_MORE_INPUT;
  }
  const auto count =
    std::min<size_t>(state.rows.size() - state.next, STANDARD_VECTOR_SIZE);
  auto grantors = duckdb::FlatVector::Writer<int64_t>(output.data[0], count);
  auto grantees = duckdb::FlatVector::Writer<int64_t>(output.data[1], count);
  auto privileges =
    duckdb::FlatVector::Writer<duckdb::string_t>(output.data[2], count);
  auto grantables = duckdb::FlatVector::Writer<bool>(output.data[3], count);
  for (size_t i = 0; i < count; ++i) {
    const auto& row = state.rows[state.next + i];
    grantors.WriteValue(row.grantor);
    grantees.WriteValue(row.grantee);
    privileges.WriteValue(duckdb::string_t{
      row.privilege.data(), static_cast<uint32_t>(row.privilege.size())});
    grantables.WriteValue(row.grantable);
  }
  state.next += count;
  output.SetCardinality(count);
  return duckdb::OperatorResultType::HAVE_MORE_OUTPUT;
}

struct PrivilegeModes {
  AclMode privs = AclMode::NoRights;
  AclMode grant_options = AclMode::NoRights;
};

PrivilegeModes ParsePrivileges(std::string_view text, CatalogType type,
                               AclMode allowed) {
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
    auto mode = AclMode::NoRights;
    if (absl::EqualsIgnoreCase(keyword, "ALL") ||
        !duckdb::Permissions::TryParsePrivilege(keyword, type, mode) ||
        (mode & allowed) != mode) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                      ERR_MSG("unrecognized privilege type: \"", chunk, "\""));
    }
    *held |= mode;
  }
  return modes;
}

struct ObjectTarget {
  const duckdb::Permissions* permissions;
  absl::InlinedVector<std::span<const duckdb::AclItem>, 1> columns;
  bool owner_only;
  const duckdb::ColumnList* any_column;
};

bool Holds(duckdb::ClientContext& context, duckdb::idx_t role, CatalogType type,
           AclMode allowed, const ObjectTarget& target, std::string_view text) {
  const auto modes = ParsePrivileges(text, type, allowed);
  const auto closure = auth::ClosureFor(&context, role);
  const auto& permissions = *target.permissions;
  if (target.owner_only) {
    return closure->Owns(permissions.owner);
  }
  if (modes.privs != AclMode::NoRights &&
      closure->CanAny(type, permissions, modes.privs)) {
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
      if (closure->CanAnyColumn(permissions, privilege, columns)) {
        return true;
      }
    }
  }
  if (modes.grant_options == AclMode::NoRights) {
    return false;
  }
  if (closure->Owns(permissions.owner)) {
    return true;
  }
  auto held = closure->GrantableModes(permissions.acl);
  for (const auto acl : columns) {
    held |= closure->GrantableModes(acl);
  }
  return (held & modes.grant_options) != AclMode::NoRights;
}

const duckdb::Permissions kBuiltinPermissions{.owner = kRootUser};

const duckdb::Permissions kSystemSchemaPermissions{
  .owner = kRootUser,
  .acl = {duckdb::AclItem{
    .grantee = kPublicGrantee, .grantor = kRootUser, .privs = AclMode::Usage}}};

const duckdb::Permissions kForeignDataWrapperPermissions{
  .owner = kRootUser,
  .acl = {kForeignDataWrapperAcl.begin(), kForeignDataWrapperAcl.end()}};

const duckdb::Permissions& SystemTablePermissions(const SystemTable& table) {
  static const auto kPublic = SystemPermissions(false);
  static const auto kSuperuser = SystemPermissions(true);
  return table.Sql().superuser_only ? kSuperuser : kPublic;
}

struct Relation {
  const duckdb::Permissions* permissions;
  std::string_view name;
  duckdb::optional_ptr<duckdb::CatalogEntry> entry;
  const SystemTable* system;
  const duckdb::UniqueConstraint* key;
};

bool IsComposite(const duckdb::CatalogEntry& entry) {
  return entry.type == CatalogType::TYPE_ENTRY &&
         duckdb::StructType::IsStruct(
           entry.Cast<duckdb::TypeCatalogEntry>().user_type);
}

std::optional<Relation> RelationByOid(const Session& session, uint64_t oid) {
  if (const auto entry = EntryByOid(session, oid)) {
    if (entry->type == CatalogType::INDEX_ENTRY) {
      const auto owner =
        EntryByOid(session, entry->Cast<duckdb::IndexCatalogEntry>().table_oid);
      return Relation{&(owner ? owner : entry)->permissions,
                      entry->name.GetIdentifierName(), entry, nullptr, nullptr};
    }
    if (entry->type != CatalogType::TABLE_ENTRY &&
        entry->type != CatalogType::VIEW_ENTRY &&
        entry->type != CatalogType::SEQUENCE_ENTRY && !IsComposite(*entry)) {
      return std::nullopt;
    }
    return Relation{&entry->permissions, entry->name.GetIdentifierName(), entry,
                    nullptr, nullptr};
  }
  if (const auto* system = FindSystemTable(oid)) {
    return Relation{&SystemTablePermissions(*system), system->Sql().name,
                    nullptr, system, nullptr};
  }
  if (const auto* view = FindSystemView(oid)) {
    if (const auto entry = duckdb::Catalog::GetEntry(
          *session.context,
          duckdb::EntryLookupInfo{
            CatalogType::VIEW_ENTRY,
            duckdb::QualifiedName{{Identifier{view->schema}},
                                  Identifier{view->name}}},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      return Relation{&entry->permissions, view->name, entry, nullptr, nullptr};
    }
  }
  if (const auto key = FindKeyIndex(session, oid); key.table) {
    return Relation{&key.table->permissions, key.key_index->constraint_name,
                    key.table, nullptr, key.key_index};
  }
  return std::nullopt;
}

CatalogType RelationType(const Relation& relation) {
  return relation.entry ? relation.entry->type : CatalogType::INVALID;
}

bool OwnerOnly(const Relation& relation) {
  return relation.key || RelationType(relation) == CatalogType::INDEX_ENTRY ||
         RelationType(relation) == CatalogType::TYPE_ENTRY;
}

std::vector<Identifier> OwnerOnlyColumns(const Session& session,
                                         const Relation& relation) {
  std::vector<Identifier> names;
  if (relation.key) {
    for (const auto attnum : KeyAttnums(
           relation.entry->Cast<duckdb::TableCatalogEntry>().GetColumns(),
           *relation.key)) {
      names.emplace_back(IndexColumnAt(*relation.entry, nullptr, attnum).name);
    }
    return names;
  }
  if (relation.entry->type == CatalogType::TYPE_ENTRY) {
    const auto& type =
      relation.entry->Cast<duckdb::TypeCatalogEntry>().user_type;
    const bool tuple = type.id() == duckdb::LogicalTypeId::TUPLE;
    for (duckdb::idx_t i = 0; i < duckdb::StructType::GetChildCount(type);
         ++i) {
      names.emplace_back(tuple ? Identifier{duckdb::TupleType::GetChildName(i)}
                               : duckdb::StructType::GetChildName(type, i));
    }
    return names;
  }
  const auto& index = relation.entry->Cast<duckdb::IndexCatalogEntry>();
  auto owner = EntryByOid(session, index.table_oid);
  if (!owner) {
    return names;
  }
  const auto columns =
    owner->type == CatalogType::VIEW_ENTRY
      ? ViewColumns(*session.context, owner->Cast<duckdb::ViewCatalogEntry>())
      : nullptr;
  for (const auto attnum : IndexAttnums(*session.context, index, *owner)) {
    names.emplace_back(IndexColumnAt(*owner, columns.get(), attnum).name);
  }
  return names;
}

template<typename F>
struct ObjectPrivileges {
  using Scope = Session;

  static Session Begin(duckdb::ClientContext& context) {
    return MakeSession(&context);
  }

  static duckdb::idx_t Role(const Session& session, duckdb::string_t name) {
    const auto text = AsView(name);
    if (const auto role = RoleOrPublic(*session.context, text)) {
      return *role;
    }
    ThrowUndefinedObject("role", text);
  }

  static duckdb::idx_t Role(const Session&, int64_t oid) {
    return static_cast<duckdb::idx_t>(oid);
  }

  static void Validate(std::string_view text) {
    ParsePrivileges(text, F::kType, F::kModes);
  }

  static bool Check(const Session& session, duckdb::idx_t role,
                    const ObjectTarget& target, std::string_view text) {
    return Holds(*session.context, role, F::kType, F::kModes, target, text);
  }
};

struct TablePrivileges : ObjectPrivileges<TablePrivileges> {
  static constexpr std::string_view kName = "has_table_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = AclMode::Select | AclMode::Insert |
                                    AclMode::Update | AclMode::Delete |
                                    AclMode::Truncate | AclMode::References |
                                    AclMode::Trigger | AclMode::Maintain;
  static constexpr bool kSuperuserFirst = false;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn<RegKind::Class>(session, name, false);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    const auto relation = RelationByOid(session, oid);
    if (!relation) {
      return std::nullopt;
    }
    return ObjectTarget{relation->permissions, {}, OwnerOnly(*relation)};
  }
};

constexpr AclMode kColumnModes =
  AclMode::Select | AclMode::Insert | AclMode::Update | AclMode::References;

struct AnyColumnPrivileges : ObjectPrivileges<AnyColumnPrivileges> {
  static constexpr std::string_view kName = "has_any_column_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = kColumnModes;
  static constexpr bool kSuperuserFirst = false;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return TablePrivileges::Resolve(session, name);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    const auto relation = RelationByOid(session, oid);
    if (!relation) {
      return std::nullopt;
    }
    ObjectTarget target{relation->permissions, {}, OwnerOnly(*relation)};
    if (!target.owner_only &&
        RelationType(*relation) == CatalogType::TABLE_ENTRY) {
      target.any_column =
        &relation->entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
    }
    return target;
  }
};

struct ColumnPrivileges : ObjectPrivileges<ColumnPrivileges> {
  static constexpr std::string_view kName = "has_column_privilege";
  static constexpr CatalogType kType = CatalogType::TABLE_ENTRY;
  static constexpr AclMode kModes = kColumnModes;
  static constexpr bool kSuperuserFirst = false;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return TablePrivileges::Resolve(session, name);
  }

  static std::optional<ObjectTarget> Find(const Session& session, uint64_t oid,
                                          duckdb::string_t column) {
    auto relation = RelationByOid(session, oid);
    if (!relation) {
      return std::nullopt;
    }
    const Identifier name{AsView(column)};
    const auto type = RelationType(*relation);
    if (OwnerOnly(*relation)) {
      if (absl::c_linear_search(OwnerOnlyColumns(session, *relation), name)) {
        return ObjectTarget{relation->permissions, {}, true};
      }
    } else if (type == CatalogType::TABLE_ENTRY) {
      const auto& columns =
        relation->entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
      if (columns.ColumnExists(name)) {
        return ObjectTarget{relation->permissions,
                            {columns.GetColumn(name).Acl()}};
      }
    } else if (type == CatalogType::VIEW_ENTRY
                 ? ViewAttnum(*session.context,
                              relation->entry->Cast<duckdb::ViewCatalogEntry>(),
                              name) != 0
                 : relation->system &&
                     relation->system->Columns().ColumnExists(name)) {
      return ObjectTarget{relation->permissions};
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", AsView(column), "\" of relation \"",
                            relation->name, "\" does not exist"));
  }

  static std::optional<ObjectTarget> Find(const Session& session, uint64_t oid,
                                          int16_t attnum) {
    auto relation = RelationByOid(session, oid);
    if (!relation || attnum < 1) {
      return std::nullopt;
    }
    if (OwnerOnly(*relation)) {
      if (static_cast<duckdb::idx_t>(attnum) >
          OwnerOnlyColumns(session, *relation).size()) {
        return std::nullopt;
      }
      return ObjectTarget{relation->permissions, {}, true};
    }
    const auto type = RelationType(*relation);
    if (type == CatalogType::TABLE_ENTRY) {
      const auto& columns =
        relation->entry->Cast<duckdb::TableCatalogEntry>().GetColumns();
      if (static_cast<duckdb::idx_t>(attnum) > columns.LogicalColumnCount()) {
        return std::nullopt;
      }
      return ObjectTarget{
        relation->permissions,
        {columns.GetColumn(duckdb::LogicalIndex(attnum - 1)).Acl()}};
    }
    duckdb::idx_t count = 0;
    if (type == CatalogType::VIEW_ENTRY) {
      if (const auto info =
            ViewColumns(*session.context,
                        relation->entry->Cast<duckdb::ViewCatalogEntry>())) {
        count = info->types.size();
      }
    } else if (relation->system) {
      count = relation->system->Columns().LogicalColumnCount();
    }
    if (static_cast<duckdb::idx_t>(attnum) > count) {
      return std::nullopt;
    }
    return ObjectTarget{relation->permissions};
  }
};

struct SchemaPrivileges : ObjectPrivileges<SchemaPrivileges> {
  static constexpr std::string_view kName = "has_schema_privilege";
  static constexpr CatalogType kType = CatalogType::SCHEMA_ENTRY;
  static constexpr AclMode kModes = AclMode::Usage | AclMode::Create;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn<RegKind::Namespace>(session, name, false);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    if (FindSystemObject(kSystemNamespaces, oid)) {
      return ObjectTarget{&kSystemSchemaPermissions};
    }
    if (session.database) {
      if (const auto schema =
            session.database->FindSchemaById(*session.transaction, oid)) {
        return ObjectTarget{&schema->permissions};
      }
    }
    return std::nullopt;
  }
};

struct SequencePrivileges : ObjectPrivileges<SequencePrivileges> {
  static constexpr std::string_view kName = "has_sequence_privilege";
  static constexpr CatalogType kType = CatalogType::SEQUENCE_ENTRY;
  static constexpr AclMode kModes =
    AclMode::Usage | AclMode::Select | AclMode::Update;
  static constexpr bool kSuperuserFirst = false;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return TablePrivileges::Resolve(session, name);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    const auto relation = RelationByOid(session, oid);
    if (!relation) {
      return std::nullopt;
    }
    if (RelationType(*relation) != CatalogType::SEQUENCE_ENTRY) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_WRONG_OBJECT_TYPE),
                      ERR_MSG("\"", relation->name, "\" is not a sequence"));
    }
    return ObjectTarget{relation->permissions};
  }
};

struct FunctionPrivileges : ObjectPrivileges<FunctionPrivileges> {
  static constexpr std::string_view kName = "has_function_privilege";
  static constexpr CatalogType kType = CatalogType::MACRO_ENTRY;
  static constexpr AclMode kModes = AclMode::Execute;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    return *RegIn<RegKind::Procedure>(session, name, false);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    if (const auto entry = EntryByOid(session, oid)) {
      if (CatalogClassOid(entry->type) != kPgProcTable) {
        return std::nullopt;
      }
      return ObjectTarget{&entry->permissions};
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
    return *RegIn<RegKind::Type>(session, name, false);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    if (const auto entry = EntryByOid(session, oid)) {
      if (entry->type != CatalogType::TYPE_ENTRY) {
        return std::nullopt;
      }
      return ObjectTarget{&entry->permissions};
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
    return ObjectTarget{&database->permissions};
  }
};

struct ServerPrivileges : ObjectPrivileges<ServerPrivileges> {
  static constexpr std::string_view kName = "has_server_privilege";
  static constexpr CatalogType kType = CatalogType::FOREIGN_SERVER_ENTRY;
  static constexpr AclMode kModes = AclMode::Usage;
  static constexpr bool kSuperuserFirst = true;

  static uint64_t Resolve(const Session& session, std::string_view name) {
    if (session.database) {
      if (const auto entry =
            session.database->GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
              .GetEntry(*session.transaction, Identifier{name})) {
        return entry->oid;
      }
    }
    ThrowUndefinedObject("server", name);
  }

  static std::optional<ObjectTarget> Find(const Session& session,
                                          uint64_t oid) {
    const auto entry = EntryByOid(session, oid);
    if (!entry || entry->type != CatalogType::FOREIGN_SERVER_ENTRY) {
      return std::nullopt;
    }
    return ObjectTarget{&entry->permissions};
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

struct RolePrivMask {
  bool usage = false;
  bool member = false;
  bool set = false;
  bool admin = false;
};

RolePrivMask ParseRolePrivs(std::string_view priv_text) {
  RolePrivMask mask;
  for (std::string_view tok : absl::StrSplit(priv_text, ',')) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    if (absl::EqualsIgnoreCase(stripped, "USAGE")) {
      mask.usage = true;
    } else if (absl::EqualsIgnoreCase(stripped, "MEMBER")) {
      mask.member = true;
    } else if (absl::EqualsIgnoreCase(stripped, "SET")) {
      mask.set = true;
    } else if (absl::EqualsIgnoreCase(stripped, "USAGE WITH GRANT OPTION") ||
               absl::EqualsIgnoreCase(stripped, "USAGE WITH ADMIN OPTION") ||
               absl::EqualsIgnoreCase(stripped, "MEMBER WITH GRANT OPTION") ||
               absl::EqualsIgnoreCase(stripped, "MEMBER WITH ADMIN OPTION") ||
               absl::EqualsIgnoreCase(stripped, "SET WITH GRANT OPTION") ||
               absl::EqualsIgnoreCase(stripped, "SET WITH ADMIN OPTION")) {
      mask.admin = true;
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized privilege type: \"", stripped, "\""));
    }
  }
  return mask;
}

bool PgHasRoleImpl(const auth::RoleGraph& roles, duckdb::idx_t member,
                   duckdb::idx_t target, std::string_view priv_text) {
  const auto mask = ParseRolePrivs(priv_text);
  const auto* role = roles.Find(member);
  if (role && role->is_superuser) {
    return mask.usage || mask.member || mask.set || mask.admin;
  }
  if (member == target) {
    return mask.usage || mask.member || mask.set;
  }
  const auto closure = auth::ComputeRoleClosure(roles, member);
  return (mask.usage && closure.MemberOf(target)) ||
         (mask.member && closure.IsMember(target)) ||
         (mask.set && closure.CanSet(target)) ||
         (mask.admin && closure.IsAdminOf(target));
}

duckdb::idx_t RoleIdByName(const auth::RoleGraph& roles,
                           std::string_view name) {
  if (const auto* role = roles.FindByName(name)) {
    return role->first;
  }
  ThrowUndefinedObject("role", name);
}

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

  static duckdb::idx_t Role(const Scope&, int64_t oid) {
    return static_cast<duckdb::idx_t>(oid);
  }

  static uint64_t Resolve(const Scope& scope, std::string_view name) {
    return RoleIdByName(*scope.roles, name);
  }

  static std::optional<duckdb::idx_t> Find(const Scope&, uint64_t oid) {
    return oid;
  }

  static void Validate(std::string_view text) { ParseRolePrivs(text); }

  static bool Check(const Scope& scope, duckdb::idx_t member,
                    duckdb::idx_t role, std::string_view text) {
    return PgHasRoleImpl(*scope.roles, member, role, text);
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
uint64_t ObjectOid(const typename F::Scope& scope, duckdb::string_t name) {
  return F::Resolve(scope, AsView(name));
}

template<typename F>
uint64_t ObjectOid(const typename F::Scope&, int64_t oid) {
  return static_cast<uint64_t>(oid);
}

template<typename F, typename Object, typename... Rest>
duckdb::optional<bool> Answer(duckdb::ClientContext& context,
                              const typename F::Scope& scope,
                              duckdb::idx_t role, duckdb::string_t text,
                              Object object, Rest... rest) {
  const auto target = F::Find(scope, ObjectOid<F>(scope, object), rest...);
  if (target) {
    return F::Check(scope, role, *target, AsView(text));
  }
  F::Validate(AsView(text));
  if (F::kSuperuserFirst && auth::ClosureFor(&context, role)->is_superuser) {
    return true;
  }
  return duckdb::nullopt;
}

template<typename F, typename Role, typename... Object>
duckdb::ScalarFunction PrivilegeFunction() {
  duckdb::vector<duckdb::LogicalType> arguments;
  if constexpr (!std::is_void_v<Role>) {
    arguments.emplace_back(PrivilegeArgument<Role>());
  }
  (arguments.emplace_back(PrivilegeArgument<Object>()), ...);
  arguments.emplace_back(duckdb::LogicalType::VARCHAR);
  return WithinQuery(duckdb::ScalarFunction{
    std::move(arguments), duckdb::LogicalType::BOOLEAN,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto scope = F::Begin(context);
      if constexpr (std::is_void_v<Role>) {
        const auto role = GetSereneDBContext(context).GetRoleId();
        duckdb::VariadicExecutor::Execute<bool, Object..., duckdb::string_t>(
          args, result, [&](Object... object, duckdb::string_t text) {
            return Answer<F>(context, scope, role, text, object...);
          });
      } else {
        duckdb::VariadicExecutor::Execute<bool, Role, Object...,
                                          duckdb::string_t>(
          args, result,
          [&](Role role, Object... object, duckdb::string_t text) {
            return Answer<F>(context, scope, F::Role(scope, role), text,
                             object...);
          });
      }
    }});
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
  Register(loader, irs::StaticStrings::kPgCatalogSchema, std::move(set));
}

}  // namespace

void RegisterAclFunctions(duckdb::ExtensionLoader& loader) {
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  RegisterScalar(
    loader, pg_catalog, "acldefault",
    {WithinQuery(duckdb::ScalarFunction{{duckdb::LogicalType::VARCHAR, OID()},
                                        duckdb::LogicalType::LIST(ACLITEM()),
                                        AclDefaultFunction})});
  duckdb::TableFunction function{
    "aclexplode", {duckdb::LogicalType::LIST(ACLITEM())},
    nullptr,      AclExplodeBind,
    nullptr,      AclExplodeInit};
  function.in_out_function = AclExplodeFunction;
  duckdb::CreateTableFunctionInfo info{std::move(function)};
  info.SetSchema(Identifier{pg_catalog});
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));

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
  RegisterPrivilegeFunctions<RolePrivileges, ByName, ByOid>(loader);
}

}  // namespace sdb::pg
