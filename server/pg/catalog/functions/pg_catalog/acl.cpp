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
#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>

#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
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
#include <vector>

#include "auth/role_closure.h"
#include "catalog/catalog.h"
#include "connector/common.h"
#include "connector/duckdb_client_state.h"
#include "connector/pg_logical_types.h"
#include "pg/catalog/engine/registry.h"
#include "pg/catalog/engine/system_table.h"
#include "pg/catalog/functions/functions.h"
#include "pg/catalog/lookup.h"
#include "pg/catalog/oids.h"
#include "pg/connection_context.h"

namespace sdb::pg {
namespace {

using connector::AsView;
using connector::GetSereneDBContext;
using duckdb::CatalogType;
using duckdb::Identifier;

duckdb::idx_t RoleByName(duckdb::ClientContext& context,
                         std::string_view name) {
  if (const auto role = RoleOrPublic(context, name)) {
    return *role;
  }
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("role \"", name, "\" does not exist"));
}

duckdb::idx_t CurrentRole(duckdb::ClientContext& context) {
  return RoleByName(context, GetSereneDBContext(context).EffectiveUserName());
}

enum class PrivilegeObject : uint8_t {
  ForeignDataWrapper,
  ForeignServer,
  Tablespace,
};

struct PrivilegeRequest {
  bool plain = false;
  bool grant = false;
};

PrivilegeRequest ParsePrivileges(PrivilegeObject object,
                                 std::string_view text) {
  const bool tablespace = object == PrivilegeObject::Tablespace;
  const std::string_view keyword = tablespace ? "CREATE" : "USAGE";
  const std::string_view with_grant =
    tablespace ? "CREATE WITH GRANT OPTION" : "USAGE WITH GRANT OPTION";
  PrivilegeRequest request;
  for (const std::string_view token : absl::StrSplit(text, ',')) {
    const auto stripped = absl::StripAsciiWhitespace(token);
    if (absl::EqualsIgnoreCase(stripped, keyword)) {
      request.plain = true;
    } else if (absl::EqualsIgnoreCase(stripped, with_grant)) {
      request.grant = true;
    } else {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
        ERR_MSG("unrecognized privilege type: \"", stripped, "\""));
    }
  }
  return request;
}

duckdb::Permissions TablespacePermissions() {
  return duckdb::Permissions{.owner = kRootUser};
}

duckdb::Permissions ForeignDataWrapperPermissions() {
  return duckdb::Permissions{
    .owner = kRootUser,
    .acl = {kForeignDataWrapperAcl.begin(), kForeignDataWrapperAcl.end()}};
}

std::optional<duckdb::Permissions> PrivilegeTargetByName(
  duckdb::ClientContext& context, PrivilegeObject object,
  std::string_view name) {
  if (object == PrivilegeObject::ForeignServer) {
    if (auto database = SessionCatalog(context)) {
      if (auto entry =
            database->GetCatalogSet(CatalogType::FOREIGN_SERVER_ENTRY)
              .GetEntry(database->GetCatalogTransaction(context),
                        Identifier{name})) {
        return entry->permissions;
      }
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("server \"", name, "\" does not exist"));
  }
  if (object == PrivilegeObject::Tablespace) {
    if (absl::c_any_of(kSystemTablespaces, [&](const SystemTablespace& space) {
          return space.name == name;
        })) {
      return TablespacePermissions();
    }
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                    ERR_MSG("tablespace \"", name, "\" does not exist"));
  }
  if (FindForeignDataWrapper(name)) {
    return ForeignDataWrapperPermissions();
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG("foreign-data wrapper \"", name, "\" does not exist"));
}

std::optional<duckdb::Permissions> PrivilegeTargetByOid(const Session& session,
                                                        PrivilegeObject object,
                                                        int64_t oid) {
  if (object == PrivilegeObject::ForeignServer) {
    if (auto entry = EntryByOid(session, oid);
        entry && entry->type == CatalogType::FOREIGN_SERVER_ENTRY) {
      return entry->permissions;
    }
    return std::nullopt;
  }
  if (object == PrivilegeObject::Tablespace &&
      absl::c_any_of(kSystemTablespaces, [&](const SystemTablespace& space) {
        return static_cast<int64_t>(space.oid) == oid;
      })) {
    return TablespacePermissions();
  }
  if (object == PrivilegeObject::ForeignDataWrapper &&
      absl::c_any_of(kForeignDataWrappers, [&](const auto& wrapper) {
        return static_cast<int64_t>(wrapper.oid) == oid;
      })) {
    return ForeignDataWrapperPermissions();
  }
  return std::nullopt;
}

duckdb::optional<bool> HasPrivilege(
  duckdb::ClientContext& context, PrivilegeObject object, duckdb::idx_t role,
  const std::optional<duckdb::Permissions>& target, std::string_view text) {
  const auto request = ParsePrivileges(object, text);
  const auto closure = auth::ClosureFor(&context, role);
  if (closure->is_superuser) {
    return true;
  }
  if (!target) {
    return duckdb::nullopt;
  }
  const auto mode = object == PrivilegeObject::Tablespace
                      ? duckdb::AclMode::Create
                      : duckdb::AclMode::Usage;
  if (request.plain) {
    const bool held =
      object == PrivilegeObject::ForeignServer
        ? closure->CanAny(CatalogType::FOREIGN_SERVER_ENTRY, *target, mode)
        : closure->Owns(target->owner) || (closure->HeldModes(target->acl) &
                                           mode) != duckdb::AclMode::NoRights;
    if (held) {
      return true;
    }
  }
  if (request.grant) {
    return closure->Owns(target->owner) ||
           (closure->GrantableModes(target->acl) & mode) !=
             duckdb::AclMode::NoRights;
  }
  return false;
}

template<PrivilegeObject Object>
void RegisterPrivilegeFunctions(duckdb::ExtensionLoader& loader,
                                std::string_view name) {
  const auto text = duckdb::LogicalType::VARCHAR;
  const auto oid = OID();
  const auto boolean = duckdb::LogicalType::BOOLEAN;
  duckdb::ScalarFunctionSet set{Identifier{name}};
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {text, text, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::VariadicExecutor::Execute<bool, duckdb::string_t,
                                        duckdb::string_t, duckdb::string_t>(
        args, result,
        [&](duckdb::string_t role, duckdb::string_t target,
            duckdb::string_t priv) {
          const auto role_id = RoleByName(context, AsView(role));
          return HasPrivilege(
            context, Object, role_id,
            PrivilegeTargetByName(context, Object, AsView(target)),
            AsView(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {text, oid, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto session = MakeSession(&context);
      duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, int64_t,
                                        duckdb::string_t>(
        args, result,
        [&](duckdb::string_t role, int64_t target, duckdb::string_t priv) {
          const auto role_id = RoleByName(context, AsView(role));
          return HasPrivilege(context, Object, role_id,
                              PrivilegeTargetByOid(session, Object, target),
                              AsView(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {oid, text, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                        duckdb::string_t>(
        args, result,
        [&](int64_t role, duckdb::string_t target, duckdb::string_t priv) {
          return HasPrivilege(
            context, Object, static_cast<duckdb::idx_t>(role),
            PrivilegeTargetByName(context, Object, AsView(target)),
            AsView(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {oid, oid, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto session = MakeSession(&context);
      duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t,
                                        duckdb::string_t>(
        args, result, [&](int64_t role, int64_t target, duckdb::string_t priv) {
          return HasPrivilege(context, Object, static_cast<duckdb::idx_t>(role),
                              PrivilegeTargetByOid(session, Object, target),
                              AsView(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {text, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto role_id = CurrentRole(context);
      duckdb::VariadicExecutor::Execute<bool, duckdb::string_t,
                                        duckdb::string_t>(
        args, result, [&](duckdb::string_t target, duckdb::string_t priv) {
          return HasPrivilege(
            context, Object, role_id,
            PrivilegeTargetByName(context, Object, AsView(target)),
            AsView(priv));
        });
    }}));
  set.AddFunction(WithinQuery(duckdb::ScalarFunction{
    {oid, text},
    boolean,
    [](duckdb::DataChunk& args, duckdb::ExpressionState& state,
       duckdb::Vector& result) {
      auto& context = state.GetContext();
      const auto role_id = CurrentRole(context);
      const auto session = MakeSession(&context);
      duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
        args, result, [&](int64_t target, duckdb::string_t priv) {
          return HasPrivilege(context, Object, role_id,
                              PrivilegeTargetByOid(session, Object, target),
                              AsView(priv));
        });
    }}));
  Register(loader, irs::StaticStrings::kPgCatalogSchema, std::move(set));
}

struct DefaultAcl {
  char objtype;
  duckdb::AclMode world;
  duckdb::AclMode owner;
};

DefaultAcl ClassAcl(char objtype, CatalogType type) {
  return {objtype, duckdb::Permissions::PublicPrivileges(type),
          duckdb::Permissions::AllPrivileges(type)};
}

const DefaultAcl kDefaultAcls[] = {
  {'c', duckdb::AclMode::NoRights, duckdb::AclMode::NoRights},
  ClassAcl('r', CatalogType::TABLE_ENTRY),
  ClassAcl('s', CatalogType::SEQUENCE_ENTRY),
  ClassAcl('d', CatalogType::DATABASE_ENTRY),
  ClassAcl('f', CatalogType::MACRO_ENTRY),
  {'l', duckdb::AclMode::Usage, duckdb::AclMode::Usage},
  {'L', duckdb::AclMode::NoRights,
   duckdb::AclMode::Select | duckdb::AclMode::Update},
  ClassAcl('n', CatalogType::SCHEMA_ENTRY),
  {'t', duckdb::AclMode::NoRights, duckdb::AclMode::Create},
  {'F', duckdb::AclMode::NoRights, duckdb::AclMode::Usage},
  ClassAcl('S', CatalogType::FOREIGN_SERVER_ENTRY),
  ClassAcl('T', CatalogType::TYPE_ENTRY),
  {'p', duckdb::AclMode::NoRights,
   duckdb::AclMode::Set | duckdb::AclMode::AlterSystem},
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
    if (it->world != duckdb::AclMode::NoRights) {
      AppendAcl(
        items.emplace_back(),
        duckdb::AclItem{
          .grantee = kPublicGrantee, .grantor = owner_id, .privs = it->world},
        *roles);
    }
    if (it->owner != duckdb::AclMode::NoRights) {
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

struct PrivilegeName {
  duckdb::AclMode mode;
  std::string_view name;
};

constexpr PrivilegeName kPrivilegeNames[] = {
  {duckdb::AclMode::Insert, "INSERT"},
  {duckdb::AclMode::Select, "SELECT"},
  {duckdb::AclMode::Update, "UPDATE"},
  {duckdb::AclMode::Delete, "DELETE"},
  {duckdb::AclMode::Truncate, "TRUNCATE"},
  {duckdb::AclMode::References, "REFERENCES"},
  {duckdb::AclMode::Trigger, "TRIGGER"},
  {duckdb::AclMode::Execute, "EXECUTE"},
  {duckdb::AclMode::Usage, "USAGE"},
  {duckdb::AclMode::Create, "CREATE"},
  {duckdb::AclMode::CreateTemp, "TEMPORARY"},
  {duckdb::AclMode::Connect, "CONNECT"},
  {duckdb::AclMode::Set, "SET"},
  {duckdb::AclMode::AlterSystem, "ALTER SYSTEM"},
  {duckdb::AclMode::Maintain, "MAINTAIN"},
};

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
  duckdb::AclMode held = duckdb::AclMode::NoRights;
  duckdb::AclMode grantable = duckdb::AclMode::NoRights;
  for (size_t i = 0; i < privileges.size(); ++i) {
    const auto it = absl::c_find_if(
      kPrivChars, [&](const PrivChar& p) { return p.chr == privileges[i]; });
    if (it == kPrivChars.end()) {
      continue;
    }
    held |= it->mode;
    if (i + 1 < privileges.size() && privileges[i + 1] == '*') {
      grantable |= it->mode;
      ++i;
    }
  }
  for (const auto& privilege : kPrivilegeNames) {
    if ((held & privilege.mode) != duckdb::AclMode::NoRights) {
      rows.emplace_back(ExplodedAcl{.grantor = grantor,
                                    .grantee = grantee,
                                    .privilege = privilege.name,
                                    .grantable = (grantable & privilege.mode) !=
                                                 duckdb::AclMode::NoRights});
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

[[noreturn]] void ThrowRoleNotFound(std::string_view role) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                  ERR_MSG("role \"", role, "\" does not exist"));
}

[[noreturn]] void ThrowRelationNotFound(std::string_view rel) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_TABLE),
                  ERR_MSG("relation \"", rel, "\" does not exist"));
}

[[noreturn]] void ThrowInvalidPrivilege(const irs::SqlException& e) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                  ERR_MSG(e.message()));
}

const duckdb::TableCatalogEntry* TableByOid(const pg::Session& session,
                                            int64_t oid) {
  auto entry = pg::EntryByOid(session, static_cast<uint64_t>(oid));
  return entry && entry->type == duckdb::CatalogType::TABLE_ENTRY
           ? &entry->Cast<duckdb::TableCatalogEntry>()
           : nullptr;
}

// A view answers the relation privilege functions the way a table does -- pg
// spells both with the relation keywords -- so a relation name takes either.
duckdb::optional_ptr<duckdb::CatalogEntry> FindRelation(
  duckdb::ClientContext& context, const duckdb::QualifiedName& name) {
  for (const auto type :
       {duckdb::CatalogType::TABLE_ENTRY, duckdb::CatalogType::VIEW_ENTRY}) {
    if (auto entry = duckdb::Catalog::GetEntry(
          context, duckdb::EntryLookupInfo{type, name},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      return entry;
    }
  }
  return nullptr;
}

duckdb::ViewCatalogEntry* ViewEntry(
  duckdb::optional_ptr<duckdb::CatalogEntry> entry) {
  return entry && entry->type == duckdb::CatalogType::VIEW_ENTRY
           ? &entry->Cast<duckdb::ViewCatalogEntry>()
           : nullptr;
}

struct PrivCheckModes {
  duckdb::AclMode privs = duckdb::AclMode::NoRights;
  duckdb::AclMode grant_options = duckdb::AclMode::NoRights;
};

duckdb::AclMode PrivCheckKeyword(std::string_view keyword,
                                 duckdb::CatalogType type) {
  duckdb::AclMode mode;
  if (!duckdb::Permissions::TryParsePrivilege(keyword, type, mode)) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INVALID_PARAMETER_VALUE),
                    ERR_MSG("unrecognized privilege type: \"", keyword, "\""));
  }
  return mode;
}

PrivCheckModes ParsePrivCheckText(std::string_view priv_text,
                                  duckdb::CatalogType type) {
  constexpr std::string_view kSuffix = " WITH GRANT OPTION";
  PrivCheckModes out;
  for (std::string_view tok :
       absl::StrSplit(priv_text, ',', absl::SkipEmpty())) {
    const auto stripped = absl::StripAsciiWhitespace(tok);
    if (stripped.size() > kSuffix.size() &&
        absl::EqualsIgnoreCase(
          stripped.substr(stripped.size() - kSuffix.size()), kSuffix)) {
      out.grant_options |= PrivCheckKeyword(
        stripped.substr(0, stripped.size() - kSuffix.size()), type);
    } else {
      out.privs |= PrivCheckKeyword(stripped, type);
    }
  }
  return out;
}

bool HasAnyPermissionsPrivilegeText(duckdb::ClientContext& context,
                                    duckdb::idx_t role_id,
                                    const duckdb::Permissions& perm,
                                    duckdb::CatalogType type,
                                    std::string_view priv_text) {
  const auto modes = ParsePrivCheckText(priv_text, type);
  // The cached inherit-closure (superuser bit + sorted role ids): one hash
  // lookup unless a role DDL has happened since the last check.
  const auto closure = auth::ClosureFor(&context, role_id);
  if (modes.privs != duckdb::AclMode::NoRights &&
      closure->CanAny(type, perm, modes.privs)) {
    return true;
  }
  if (modes.grant_options == duckdb::AclMode::NoRights) {
    return false;
  }
  if (closure->Owns(perm.owner)) {
    return true;
  }
  const duckdb::AclMode held = closure->GrantableModes(perm.acl);
  return (held & modes.grant_options) != duckdb::AclMode::NoRights;
}

bool HasAnyTablePrivilegeText(duckdb::ClientContext& context,
                              duckdb::idx_t role_id,
                              const duckdb::Permissions& perm,
                              std::string_view priv_text) {
  return HasAnyPermissionsPrivilegeText(
    context, role_id, perm, duckdb::CatalogType::TABLE_ENTRY, priv_text);
}

// The grants `entry` answers a relation privilege check with. Postgres gives a
// table and a view the same vocabulary, so both come back here; anything else
// under the relation namespace -- the index-as-table wrapper -- is not one, and
// a null makes the caller answer NULL rather than false.
const duckdb::Permissions* RelationPermissions(
  const duckdb::CatalogEntry* entry) {
  if (!entry) {
    return nullptr;
  }
  if (entry->type != duckdb::CatalogType::TABLE_ENTRY &&
      entry->type != duckdb::CatalogType::VIEW_ENTRY) {
    return nullptr;
  }
  return &entry->permissions;
}

const pg::SystemTable* ResolveSystemRelation(
  const duckdb::QualifiedName& name) {
  const auto& schema = name.Schema();
  const auto& relation = name.Name().GetIdentifierName();
  if (schema == irs::StaticStrings::kPgCatalogSchema ||
      schema == irs::StaticStrings::kInformationSchema) {
    return pg::GetSystemTable(schema.GetIdentifierName(), relation);
  }
  if (schema.empty()) {
    return pg::GetSystemTable(irs::StaticStrings::kPgCatalogSchema, relation);
  }
  return nullptr;
}

bool HasTablePrivilegeImpl(ConnectionContext& conn_ctx,
                           std::string_view role_name,
                           std::string_view table_name,
                           std::string_view priv_text) {
  auto role_id = pg::RoleOrPublic(conn_ctx.GetClientContext(), role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  const auto name = duckdb::QualifiedName::Parse(table_name);
  auto entry = FindRelation(conn_ctx.GetClientContext(), name);
  try {
    if (const auto* perm = RelationPermissions(entry.get())) {
      return HasAnyPermissionsPrivilegeText(
        conn_ctx.GetClientContext(), *role_id, *perm,
        duckdb::CatalogType::TABLE_ENTRY, priv_text);
    }
    if (const auto* sys = ResolveSystemRelation(name)) {
      return HasAnyPermissionsPrivilegeText(
        conn_ctx.GetClientContext(), *role_id,
        pg::SystemPermissions(sys->Sql().superuser_only),
        duckdb::CatalogType::TABLE_ENTRY, priv_text);
    }
    ThrowRelationNotFound(name.Name().GetIdentifierName());
  } catch (const irs::SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
}

void HasTablePrivilege3Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t table,
        duckdb::string_t priv) -> bool {
      return HasTablePrivilegeImpl(conn_ctx, {role.GetData(), role.GetSize()},
                                   {table.GetData(), table.GetSize()},
                                   {priv.GetData(), priv.GetSize()});
    });
}

void HasTablePrivilege2Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const std::string_view current = conn_ctx.user();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t table, duckdb::string_t priv) -> bool {
      return HasTablePrivilegeImpl(conn_ctx, current,
                                   {table.GetData(), table.GetSize()},
                                   {priv.GetData(), priv.GetSize()});
    });
}

bool HasTablePrivilegeByOidImpl(const pg::Session& session,
                                duckdb::idx_t role_id, duckdb::idx_t table_id,
                                std::string_view priv_text, bool& is_null) {
  is_null = false;
  const auto* perm =
    RelationPermissions(pg::EntryByOid(session, table_id).get());
  std::optional<duckdb::Permissions> system_perm;
  if (!perm) {
    if (const auto* table = pg::FindSystemTable(table_id)) {
      perm = &system_perm.emplace(
        pg::SystemPermissions(table->Sql().superuser_only));
    }
  }
  if (!perm) {
    is_null = true;
    return false;
  }
  try {
    return HasAnyPermissionsPrivilegeText(*session.context, role_id, *perm,
                                          duckdb::CatalogType::TABLE_ENTRY,
                                          priv_text);
  } catch (const irs::SqlException& e) {
    ThrowInvalidPrivilege(e);
  }
}

void HasTablePrivilegeOid2Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, duckdb::string_t priv) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        session, current->oid, static_cast<uint64_t>(toid),
        {priv.GetData(), priv.GetSize()}, is_null);
      return is_null ? duckdb::nullopt : duckdb::optional<bool>{r};
    });
}

void HasTablePrivilegeOid3Function(duckdb::DataChunk& args,
                                   duckdb::ExpressionState& state,
                                   duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t roid, int64_t toid,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        session, static_cast<uint64_t>(roid), static_cast<uint64_t>(toid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

void HasTablePrivilegeOidName3Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t tname,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      const auto name = duckdb::QualifiedName::Parse(tname.GetString());
      const auto* perm =
        RelationPermissions(FindRelation(state.GetContext(), name).get());
      const auto role = static_cast<duckdb::idx_t>(roid);
      const std::string_view priv_text{priv.GetData(), priv.GetSize()};
      try {
        if (perm) {
          return HasAnyPermissionsPrivilegeText(
            state.GetContext(), role, *perm, duckdb::CatalogType::TABLE_ENTRY,
            priv_text);
        } else if (const auto* sys = ResolveSystemRelation(name)) {
          return HasAnyPermissionsPrivilegeText(
            state.GetContext(), role,
            pg::SystemPermissions(sys->Sql().superuser_only),
            duckdb::CatalogType::TABLE_ENTRY, priv_text);
        } else {
          ThrowRelationNotFound(name.Name().GetIdentifierName());
        }
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasTablePrivilegeNameOid3Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, int64_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t rname, int64_t toid,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      auto role_id = pg::RoleOrPublic(state.GetContext(),
                                      {rname.GetData(), rname.GetSize()});
      if (!role_id) {
        ThrowRoleNotFound({rname.GetData(), rname.GetSize()});
      }
      bool is_null = false;
      bool r = HasTablePrivilegeByOidImpl(
        session, *role_id, static_cast<uint64_t>(toid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

const char* ObjectClassWord(duckdb::CatalogType type) {
  switch (type) {
    case duckdb::CatalogType::SCHEMA_ENTRY:
      return "schema";
    case duckdb::CatalogType::SEQUENCE_ENTRY:
      return "relation";
    case duckdb::CatalogType::MACRO_ENTRY:
      return "function";
    case duckdb::CatalogType::DATABASE_ENTRY:
      return "database";
    default:
      return "object";
  }
}

// No snapshot: every kind this answers for has its entry as the object.
bool HasObjectPrivilegeByName(duckdb::ClientContext& context,
                              duckdb::CatalogType type, duckdb::idx_t role_id,
                              std::string_view obj_name,
                              std::string_view priv_text) {
  // A database is not in the snapshot -- its entry is the object -- so its
  // owner and ACL come off the entry the cluster-global set holds.
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto database = FindDatabase(context, obj_name);
    if (!database) {
      THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
                      ERR_MSG("database \"", obj_name, "\" does not exist"));
    }
    try {
      return HasAnyPermissionsPrivilegeText(
        context, role_id, database->permissions, type, priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  // Nor is a schema, for the same reason. pg_catalog and information_schema
  // are the two names that resolve to nothing and are still answered for, just
  // below.
  auto database = SessionCatalog(context);
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    if (auto schema =
          database->GetSchema(context, duckdb::Identifier{obj_name},
                              duckdb::OnEntryNotFound::RETURN_NULL)) {
      try {
        return HasAnyPermissionsPrivilegeText(
          context, role_id, schema->permissions, type, priv_text);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Nor is a sequence, whose name is in the relation namespace and whose
  // permissions come off the entry the schema's set holds.
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    if (auto sequence = duckdb::Catalog::GetEntry(
          context,
          duckdb::EntryLookupInfo{type, duckdb::QualifiedName::Parse(obj_name)},
          duckdb::OnEntryNotFound::RETURN_NULL)) {
      try {
        return HasAnyPermissionsPrivilegeText(
          context, role_id, sequence->permissions, type, priv_text);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Nor is a type or a function. An unresolved name still answers below, the
  // way a built-in one does.
  if (type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::MACRO_ENTRY) {
    // A function name carries its argument list; the entry is keyed on the bare
    // name, as duckdb's macro sets are.
    const auto bare = type == duckdb::CatalogType::MACRO_ENTRY
                        ? obj_name.substr(0, obj_name.find('('))
                        : obj_name;
    const auto name =
      duckdb::QualifiedName::Parse(absl::StripAsciiWhitespace(bare));
    auto entry =
      duckdb::Catalog::GetEntry(context, duckdb::EntryLookupInfo{type, name},
                                duckdb::OnEntryNotFound::RETURN_NULL);
    if (!entry && type == duckdb::CatalogType::MACRO_ENTRY) {
      entry = duckdb::Catalog::GetEntry(
        context,
        duckdb::EntryLookupInfo{duckdb::CatalogType::TABLE_MACRO_ENTRY, name},
        duckdb::OnEntryNotFound::RETURN_NULL);
    }
    if (entry) {
      try {
        return HasAnyPermissionsPrivilegeText(
          context, role_id, entry->permissions, type, priv_text);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    }
  }
  // Every kind this function is registered for is handled above. What is left
  // is a name none of them holds.
  //
  // Functions, types and sequences include built-ins / objects not tracked as
  // catalog entries (version(), integer, ...). PG grants EXECUTE on functions,
  // USAGE on types and (for the owner/PUBLIC defaults) sequences, so serenedb
  // reports an unresolved object of these classes as held rather than erroring
  // on a name it cannot resolve.
  if (type == duckdb::CatalogType::MACRO_ENTRY ||
      type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::SEQUENCE_ENTRY) {
    return true;
  }
  // pg_catalog / information_schema are virtual schemas absent from the
  // schema store. PG grants PUBLIC USAGE on both and restricts CREATE to
  // superusers.
  if (type == duckdb::CatalogType::SCHEMA_ENTRY &&
      (obj_name == irs::StaticStrings::kPgCatalogSchema ||
       obj_name == irs::StaticStrings::kInformationSchema)) {
    const auto modes =
      ParsePrivCheckText(priv_text, duckdb::CatalogType::SCHEMA_ENTRY);
    if ((modes.privs & duckdb::AclMode::Create) != duckdb::AclMode::NoRights ||
        (modes.grant_options & duckdb::AclMode::Create) !=
          duckdb::AclMode::NoRights) {
      return auth::ClosureFor(&context, role_id)->is_superuser;
    }
    return true;
  }
  THROW_SQL_ERROR(
    ERR_CODE(ERRCODE_UNDEFINED_OBJECT),
    ERR_MSG(ObjectClassWord(type), " \"", obj_name, "\" does not exist"));
}

bool HasObjectPrivilegeImpl(ConnectionContext& conn_ctx,
                            duckdb::CatalogType type,
                            std::string_view role_name,
                            std::string_view obj_name,
                            std::string_view priv_text) {
  auto role_id = pg::RoleOrPublic(conn_ctx.GetClientContext(), role_name);
  if (!role_id) {
    ThrowRoleNotFound(role_name);
  }
  return HasObjectPrivilegeByName(conn_ctx.GetClientContext(), type, *role_id,
                                  obj_name, priv_text);
}

template<duckdb::CatalogType kType>
void HasObjectPrivilege3Function(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t obj,
        duckdb::string_t priv) -> bool {
      return HasObjectPrivilegeImpl(
        conn_ctx, kType, {role.GetData(), role.GetSize()},
        {obj.GetData(), obj.GetSize()}, {priv.GetData(), priv.GetSize()});
    });
}

template<duckdb::CatalogType kType>
void HasObjectPrivilege2Function(duckdb::DataChunk& args,
                                 duckdb::ExpressionState& state,
                                 duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  const std::string_view current = conn_ctx.user();
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t obj, duckdb::string_t priv) -> bool {
      return HasObjectPrivilegeImpl(conn_ctx, kType, current,
                                    {obj.GetData(), obj.GetSize()},
                                    {priv.GetData(), priv.GetSize()});
    });
}

bool HasObjectPrivilegeByOidImpl(const pg::Session& session,
                                 duckdb::CatalogType type,
                                 duckdb::idx_t role_id, duckdb::idx_t obj_id,
                                 std::string_view priv_text, bool& is_null) {
  auto& context = *session.context;
  is_null = false;
  if (type == duckdb::CatalogType::DATABASE_ENTRY) {
    auto database = FindDatabaseById(context, obj_id);
    if (!database) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(
        context, role_id, database->permissions, type, priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::SCHEMA_ENTRY) {
    auto schema = session.database ? session.database->FindSchemaById(
                                       *session.transaction, obj_id)
                                   : nullptr;
    if (!schema) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(
        context, role_id, schema->permissions, type, priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  if (type == duckdb::CatalogType::SEQUENCE_ENTRY ||
      type == duckdb::CatalogType::TYPE_ENTRY ||
      type == duckdb::CatalogType::MACRO_ENTRY) {
    auto entry = pg::EntryByOid(session, obj_id);
    if (!entry || !(type == duckdb::CatalogType::MACRO_ENTRY
                      ? pg::CatalogClassOid(entry->type) == pg::kPgProcTable
                      : entry->type == type)) {
      is_null = true;
      return false;
    }
    try {
      return HasAnyPermissionsPrivilegeText(
        context, role_id, entry->permissions, type, priv_text);
    } catch (const irs::SqlException& e) {
      ThrowInvalidPrivilege(e);
    }
  }
  // As above: every kind this answers for has its entry as the object, so an
  // oid none of the sets holds names nothing.
  is_null = true;
  return false;
}

template<duckdb::CatalogType kType>
void HasObjectPrivilegeOid2Function(duckdb::DataChunk& args,
                                    duckdb::ExpressionState& state,
                                    duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t ooid, duckdb::string_t priv) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      bool is_null = false;
      bool r = HasObjectPrivilegeByOidImpl(
        session, kType, current->oid, static_cast<uint64_t>(ooid),
        {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

template<duckdb::CatalogType kType>
void HasObjectPrivilegeOid3Function(duckdb::DataChunk& args,
                                    duckdb::ExpressionState& state,
                                    duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t roid, int64_t ooid,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      bool is_null = false;
      bool r = HasObjectPrivilegeByOidImpl(
        session, kType, static_cast<uint64_t>(roid),
        static_cast<uint64_t>(ooid), {priv.GetData(), priv.GetSize()}, is_null);
      if (is_null) {
        return duckdb::nullopt;
      } else {
        return r;
      }
    });
}

template<duckdb::CatalogType kType>
void HasObjectPrivilegeOidName3Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t obj,
        duckdb::string_t priv) -> duckdb::optional<bool> {
      return HasObjectPrivilegeByName(
        state.GetContext(), kType, static_cast<uint64_t>(roid),
        {obj.GetData(), obj.GetSize()}, {priv.GetData(), priv.GetSize()});
    });
}

struct RolePrivMask {
  bool usage = false;
  bool member = false;
  bool set = false;
  bool admin = false;
};

RolePrivMask ParseRolePrivs(std::string_view priv_text) {
  RolePrivMask mask;
  for (std::string_view tok :
       absl::StrSplit(priv_text, ',', absl::SkipEmpty())) {
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
  ThrowRoleNotFound(name);
}

void PgHasRoleNameName3Function(duckdb::DataChunk& args,
                                duckdb::ExpressionState& state,
                                duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, duckdb::string_t,
                                   duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t user, duckdb::string_t role,
        duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, RoleIdByName(*roles, {user.GetData(), user.GetSize()}),
        RoleIdByName(*roles, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleNameOid3Function(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<duckdb::string_t, int64_t, duckdb::string_t,
                                   bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](duckdb::string_t user, int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, RoleIdByName(*roles, {user.GetData(), user.GetSize()}),
        static_cast<uint64_t>(role), {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOidName3Function(duckdb::DataChunk& args,
                               duckdb::ExpressionState& state,
                               duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<int64_t, duckdb::string_t, duckdb::string_t,
                                   bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](int64_t user, duckdb::string_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, static_cast<uint64_t>(user),
        RoleIdByName(*roles, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOidOid3Function(duckdb::DataChunk& args,
                              duckdb::ExpressionState& state,
                              duckdb::Vector& result) {
  auto roles = auth::RolesOf(&state.GetContext());
  duckdb::TernaryExecutor::Execute<int64_t, int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], args.data[2], result, args.size(),
    [&](int64_t user, int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*roles, static_cast<uint64_t>(user),
                           static_cast<uint64_t>(role),
                           {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleName2Function(duckdb::DataChunk& args,
                            duckdb::ExpressionState& state,
                            duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto roles = auth::RolesOf(&state.GetContext());
  const duckdb::idx_t member = RoleIdByName(*roles, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<duckdb::string_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](duckdb::string_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(
        *roles, member, RoleIdByName(*roles, {role.GetData(), role.GetSize()}),
        {priv.GetData(), priv.GetSize()});
    });
}

void PgHasRoleOid2Function(duckdb::DataChunk& args,
                           duckdb::ExpressionState& state,
                           duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto roles = auth::RolesOf(&state.GetContext());
  const duckdb::idx_t member = RoleIdByName(*roles, conn_ctx.user());
  duckdb::BinaryExecutor::Execute<int64_t, duckdb::string_t, bool>(
    args.data[0], args.data[1], result, args.size(),
    [&](int64_t role, duckdb::string_t priv) -> bool {
      return PgHasRoleImpl(*roles, member, static_cast<uint64_t>(role),
                           {priv.GetData(), priv.GetSize()});
    });
}

bool AttnumExists(const duckdb::TableCatalogEntry& table, int64_t attnum) {
  return attnum >= 1 && attnum <= static_cast<int64_t>(
                                    table.GetColumns().LogicalColumnCount());
}

// Whether `role_id` holds `priv` on `column` of `table`. PG resolves a column
// privilege via a table-level grant OR a per-column grant
// (pg_attribute.attacl); the optional "WITH GRANT OPTION" suffix additionally
// requires the grant-option bit on the column (or table).
bool ColumnPrivHeld(duckdb::ClientContext& context, duckdb::idx_t role_id,
                    const duckdb::TableCatalogEntry& table,
                    const duckdb::ColumnDefinition& column,
                    std::string_view priv) {
  const auto modes = ParsePrivCheckText(priv, duckdb::CatalogType::TABLE_ENTRY);
  const std::span<const duckdb::AclItem> column_acl = column.Acl();
  const auto closure = auth::ClosureFor(&context, role_id);
  if (modes.privs != duckdb::AclMode::NoRights &&
      closure->CanColumns(table.permissions, modes.privs, {&column_acl, 1})) {
    return true;
  }
  if (modes.grant_options == duckdb::AclMode::NoRights) {
    return false;
  }
  const auto& rc = *closure;
  if (rc.Owns(table.permissions.owner)) {
    return true;
  }
  const auto held =
    rc.GrantableModes(table.permissions.acl) | rc.GrantableModes(column_acl);
  return (held & modes.grant_options) != duckdb::AclMode::NoRights;
}

bool HasColumnPrivByName(duckdb::ClientContext& context, duckdb::idx_t role_id,
                         const duckdb::TableCatalogEntry& table,
                         std::string_view col, std::string_view priv) {
  const auto& columns = table.GetColumns();
  const duckdb::Identifier key{col};
  if (!columns.ColumnExists(key)) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", col, "\" of relation \"",
              table.name.GetIdentifierName(), "\" does not exist"));
  }
  return ColumnPrivHeld(context, role_id, table, columns.GetColumn(key), priv);
}

// has_column_privilege(role, oid, attnum, priv): attnum is 1-based over the
// table's columns, the same numbering pg_attribute reports.
bool HasColumnPrivByAttnum(duckdb::ClientContext& context,
                           duckdb::idx_t role_id,
                           const duckdb::TableCatalogEntry& table,
                           int64_t attnum, std::string_view priv) {
  return ColumnPrivHeld(
    context, role_id, table,
    table.GetColumns().GetColumn(duckdb::LogicalIndex(attnum - 1)), priv);
}

bool ViewColumnPrivByName(duckdb::ClientContext& context, duckdb::idx_t role_id,
                          duckdb::ViewCatalogEntry& view, std::string_view col,
                          std::string_view priv) {
  if (pg::ViewAttnum(context, view, duckdb::Identifier{col}) == 0) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
      ERR_MSG("column \"", col, "\" of relation \"",
              view.name.GetIdentifierName(), "\" does not exist"));
  }
  return HasAnyTablePrivilegeText(context, role_id, view.permissions, priv);
}

std::optional<bool> ViewColumnPrivByAttnum(duckdb::ClientContext& context,
                                           duckdb::idx_t role_id,
                                           duckdb::ViewCatalogEntry& view,
                                           int64_t attnum,
                                           std::string_view priv) {
  const auto columns = pg::ViewColumns(context, view);
  if (!columns || attnum < 1 ||
      attnum > static_cast<int64_t>(columns->types.size())) {
    return std::nullopt;
  }
  return HasAnyTablePrivilegeText(context, role_id, view.permissions, priv);
}

// System relations have no per-column ACLs: the column privilege reduces to the
// relation-level privilege once the column is known to exist.
bool SystemRelationColumnPriv(ConnectionContext& conn_ctx,
                              duckdb::idx_t role_id,
                              const duckdb::QualifiedName& name,
                              std::string_view col, std::string_view priv) {
  const auto* sys = ResolveSystemRelation(name);
  if (!sys) {
    ThrowRelationNotFound(name.Name().GetIdentifierName());
  }
  if (!sys->Columns().ColumnExists(duckdb::Identifier{col})) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_UNDEFINED_COLUMN),
                    ERR_MSG("column \"", col, "\" of relation \"",
                            sys->Sql().name, "\" does not exist"));
  }
  return HasAnyPermissionsPrivilegeText(
    conn_ctx.GetClientContext(), role_id,
    pg::SystemPermissions(sys->Sql().superuser_only),
    duckdb::CatalogType::TABLE_ENTRY, priv);
}

bool ColumnPrivByName(ConnectionContext& conn_ctx, duckdb::idx_t role_id,
                      const duckdb::QualifiedName& name, std::string_view col,
                      std::string_view priv) {
  auto& context = conn_ctx.GetClientContext();
  const auto entry = FindRelation(context, name);
  if (entry && entry->type == duckdb::CatalogType::TABLE_ENTRY) {
    return HasColumnPrivByName(
      context, role_id, entry->Cast<duckdb::TableCatalogEntry>(), col, priv);
  }
  if (auto* view = ViewEntry(entry)) {
    return ViewColumnPrivByName(context, role_id, *view, col, priv);
  }
  return SystemRelationColumnPriv(conn_ctx, role_id, name, col, priv);
}

void HasColumnPrivilegeNameName4Function(duckdb::DataChunk& args,
                                         duckdb::ExpressionState& state,
                                         duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t, duckdb::string_t c,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto role = FindRole(state.GetContext(), {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      const auto name = duckdb::QualifiedName::Parse(t.GetString());
      const std::string_view col{c.GetData(), c.GetSize()};
      const std::string_view priv{p.GetData(), p.GetSize()};
      try {
        return ColumnPrivByName(conn_ctx, role->oid, name, col, priv);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                     duckdb::ExpressionState& state,
                                     duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t t, duckdb::string_t c,
        duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto name = duckdb::QualifiedName::Parse(t.GetString());
      const std::string_view col{c.GetData(), c.GetSize()};
      const std::string_view priv{p.GetData(), p.GetSize()};
      try {
        return ColumnPrivByName(conn_ctx, current->oid, name, col, priv);
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeOidAttnum3Function(duckdb::DataChunk& args,
                                          duckdb::ExpressionState& state,
                                          duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int32_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, int32_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto entry = pg::EntryByOid(session, static_cast<uint64_t>(toid));
      try {
        if (auto* view = ViewEntry(entry)) {
          const auto held =
            ViewColumnPrivByAttnum(state.GetContext(), current->oid, *view,
                                   attnum, {p.GetData(), p.GetSize()});
          if (held) {
            return *held;
          }
          return duckdb::nullopt;
        }
        const auto* table = TableByOid(session, toid);
        if (!table || !AttnumExists(*table, attnum)) {
          return duckdb::nullopt;
        }
        return HasColumnPrivByAttnum(state.GetContext(), current->oid, *table,
                                     attnum, {p.GetData(), p.GetSize()});
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

// Column privilege by (role, table-by-name, attnum, priv), with the same
// system-relation fallback as the name-column path. Returns a tri-state: an
// unset optional means the attnum is out of range (SQL NULL, matching PG).
std::optional<bool> ColumnPrivByNameTableAttnum(ConnectionContext& conn_ctx,
                                                duckdb::idx_t role_id,
                                                std::string_view table_name,
                                                int64_t attnum,
                                                std::string_view priv) {
  const auto name = duckdb::QualifiedName::Parse(table_name);
  const auto entry = FindRelation(conn_ctx.GetClientContext(), name);
  if (entry && entry->type == duckdb::CatalogType::TABLE_ENTRY) {
    const auto& table = entry->Cast<duckdb::TableCatalogEntry>();
    if (!AttnumExists(table, attnum)) {
      return std::nullopt;
    }
    return HasColumnPrivByAttnum(conn_ctx.GetClientContext(), role_id, table,
                                 attnum, priv);
  }
  if (auto* view = ViewEntry(entry)) {
    return ViewColumnPrivByAttnum(conn_ctx.GetClientContext(), role_id, *view,
                                  attnum, priv);
  }
  const auto* sys = ResolveSystemRelation(name);
  if (!sys) {
    ThrowRelationNotFound(name.Name().GetIdentifierName());
  }
  // System relations carry no per-column ACL; a valid attnum reduces to the
  // relation-level privilege.
  if (attnum < 1 ||
      attnum > static_cast<int64_t>(sys->Columns().LogicalColumnCount())) {
    return std::nullopt;
  }
  return HasAnyPermissionsPrivilegeText(
    conn_ctx.GetClientContext(), role_id,
    pg::SystemPermissions(sys->Sql().superuser_only),
    duckdb::CatalogType::TABLE_ENTRY, priv);
}

void HasColumnPrivilegeNameAttnum4Function(duckdb::DataChunk& args,
                                           duckdb::ExpressionState& state,
                                           duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    int16_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto role = FindRole(state.GetContext(), {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      try {
        auto r = ColumnPrivByNameTableAttnum(conn_ctx, role->oid,
                                             {t.GetData(), t.GetSize()}, attnum,
                                             {p.GetData(), p.GetSize()});
        if (r) {
          return *r;
        } else {
          return duckdb::nullopt;
        }
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeOidNameAttnum4Function(duckdb::DataChunk& args,
                                              duckdb::ExpressionState& state,
                                              duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t, int16_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, duckdb::string_t t, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      try {
        auto r = ColumnPrivByNameTableAttnum(
          conn_ctx, static_cast<uint64_t>(roid), {t.GetData(), t.GetSize()},
          attnum, {p.GetData(), p.GetSize()});
        if (r) {
          return *r;
        } else {
          return duckdb::nullopt;
        }
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasColumnPrivilegeOidOidAttnum4Function(duckdb::DataChunk& args,
                                             duckdb::ExpressionState& state,
                                             duckdb::Vector& result) {
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, int64_t, int16_t,
                                    duckdb::string_t>(
    args, result,
    [&](int64_t roid, int64_t toid, int16_t attnum,
        duckdb::string_t p) -> duckdb::optional<bool> {
      const auto entry = pg::EntryByOid(session, static_cast<uint64_t>(toid));
      try {
        if (auto* view = ViewEntry(entry)) {
          const auto held = ViewColumnPrivByAttnum(
            state.GetContext(), static_cast<uint64_t>(roid), *view, attnum,
            {p.GetData(), p.GetSize()});
          if (held) {
            return *held;
          }
          return duckdb::nullopt;
        }
        const auto* table = TableByOid(session, toid);
        if (!table || !AttnumExists(*table, attnum)) {
          return duckdb::nullopt;
        }
        return HasColumnPrivByAttnum(state.GetContext(),
                                     static_cast<uint64_t>(roid), *table,
                                     attnum, {p.GetData(), p.GetSize()});
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasAnyColumnPrivilegeName3Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t,
                                    duckdb::string_t>(
    args, result,
    [&](duckdb::string_t u, duckdb::string_t t,
        duckdb::string_t p) -> duckdb::optional<bool> {
      auto role = FindRole(state.GetContext(), {u.GetData(), u.GetSize()});
      if (!role) {
        ThrowRoleNotFound({u.GetData(), u.GetSize()});
      }
      const auto name = duckdb::QualifiedName::Parse(t.GetString());
      const auto* perm =
        RelationPermissions(FindRelation(state.GetContext(), name).get());
      try {
        if (perm) {
          return HasAnyTablePrivilegeText(state.GetContext(), role->oid, *perm,
                                          {p.GetData(), p.GetSize()});
        } else if (const auto* sys = ResolveSystemRelation(name)) {
          return HasAnyTablePrivilegeText(
            state.GetContext(), role->oid,
            pg::SystemPermissions(sys->Sql().superuser_only),
            {p.GetData(), p.GetSize()});
        } else {
          ThrowRelationNotFound(name.Name().GetIdentifierName());
        }
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasAnyColumnPrivilegeOid2Function(duckdb::DataChunk& args,
                                       duckdb::ExpressionState& state,
                                       duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  const auto session = pg::MakeSession(&state.GetContext());
  duckdb::VariadicExecutor::Execute<bool, int64_t, duckdb::string_t>(
    args, result,
    [&](int64_t toid, duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto* perm = RelationPermissions(
        pg::EntryByOid(session, static_cast<uint64_t>(toid)).get());
      if (!perm) {
        return duckdb::nullopt;
      }
      try {
        return HasAnyTablePrivilegeText(state.GetContext(), current->oid, *perm,
                                        {p.GetData(), p.GetSize()});
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

void HasAnyColumnPrivilegeName2Function(duckdb::DataChunk& args,
                                        duckdb::ExpressionState& state,
                                        duckdb::Vector& result) {
  auto& conn_ctx = GetSereneDBContext(state.GetContext());
  auto current = FindRole(state.GetContext(), conn_ctx.user());
  duckdb::VariadicExecutor::Execute<bool, duckdb::string_t, duckdb::string_t>(
    args, result,
    [&](duckdb::string_t t, duckdb::string_t p) -> duckdb::optional<bool> {
      if (!current) {
        return duckdb::nullopt;
      }
      const auto name = duckdb::QualifiedName::Parse(t.GetString());
      const auto* perm =
        RelationPermissions(FindRelation(state.GetContext(), name).get());
      try {
        if (perm) {
          return HasAnyTablePrivilegeText(state.GetContext(), current->oid,
                                          *perm, {p.GetData(), p.GetSize()});
        } else if (const auto* sys = ResolveSystemRelation(name)) {
          return HasAnyTablePrivilegeText(
            state.GetContext(), current->oid,
            pg::SystemPermissions(sys->Sql().superuser_only),
            {p.GetData(), p.GetSize()});
        } else {
          ThrowRelationNotFound(name.Name().GetIdentifierName());
        }
      } catch (const irs::SqlException& e) {
        ThrowInvalidPrivilege(e);
      }
    });
}

}  // namespace

void RegisterAclFunctions(duckdb::ExtensionLoader& loader) {
  const std::string_view pg_catalog = irs::StaticStrings::kPgCatalogSchema;
  RegisterScalar(
    loader, pg_catalog, "acldefault",
    {WithinQuery(duckdb::ScalarFunction{{duckdb::LogicalType::VARCHAR, OID()},
                                        duckdb::LogicalType::LIST(ACLITEM()),
                                        AclDefaultFunction})});
  RegisterPrivilegeFunctions<PrivilegeObject::ForeignDataWrapper>(
    loader, "has_foreign_data_wrapper_privilege");
  RegisterPrivilegeFunctions<PrivilegeObject::ForeignServer>(
    loader, "has_server_privilege");
  RegisterPrivilegeFunctions<PrivilegeObject::Tablespace>(
    loader, "has_tablespace_privilege");
  duckdb::TableFunction function{
    "aclexplode", {duckdb::LogicalType::LIST(ACLITEM())},
    nullptr,      AclExplodeBind,
    nullptr,      AclExplodeInit};
  function.in_out_function = AclExplodeFunction;
  duckdb::CreateTableFunctionInfo info{std::move(function)};
  info.SetSchema(Identifier{pg_catalog});
  info.on_conflict = duckdb::OnCreateConflict::REPLACE_ON_CONFLICT;
  loader.RegisterFunction(std::move(info));

  loader.RegisterFunction(duckdb::ScalarFunction{
    "has_table_privilege",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    HasTablePrivilege3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "has_table_privilege",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    HasTablePrivilege2Function});
  {
    duckdb::ScalarFunction func{"has_table_privilege",
                                {pg::OID(), duckdb::LogicalType::VARCHAR},
                                duckdb::LogicalType::BOOLEAN,
                                HasTablePrivilegeOid2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeOid3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeOidName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_table_privilege",
      {duckdb::LogicalType::VARCHAR, pg::OID(), duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasTablePrivilegeNameOid3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  const auto register_object_priv = [&loader]<duckdb::CatalogType kType>(
                                      std::string_view name) {
    loader.RegisterFunction(duckdb::ScalarFunction{
      duckdb::Identifier{name},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasObjectPrivilege3Function<kType>});
    loader.RegisterFunction(duckdb::ScalarFunction{
      duckdb::Identifier{name},
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasObjectPrivilege2Function<kType>});
    {
      duckdb::ScalarFunction func{duckdb::Identifier{name},
                                  {pg::OID(), duckdb::LogicalType::VARCHAR},
                                  duckdb::LogicalType::BOOLEAN,
                                  HasObjectPrivilegeOid2Function<kType>};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      loader.RegisterFunction(func);
    }
    {
      duckdb::ScalarFunction func{
        duckdb::Identifier{name},
        {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilegeOidName3Function<kType>};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      loader.RegisterFunction(func);
    }
    {
      duckdb::ScalarFunction func{
        duckdb::Identifier{name},
        {pg::OID(), pg::OID(), duckdb::LogicalType::VARCHAR},
        duckdb::LogicalType::BOOLEAN,
        HasObjectPrivilegeOid3Function<kType>};
      func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
      loader.RegisterFunction(func);
    }
  };
  register_object_priv.operator()<duckdb::CatalogType::SCHEMA_ENTRY>(
    "has_schema_privilege");
  register_object_priv.operator()<duckdb::CatalogType::SEQUENCE_ENTRY>(
    "has_sequence_privilege");
  register_object_priv.operator()<duckdb::CatalogType::MACRO_ENTRY>(
    "has_function_privilege");
  register_object_priv.operator()<duckdb::CatalogType::DATABASE_ENTRY>(
    "has_database_privilege");
  register_object_priv.operator()<duckdb::CatalogType::TYPE_ENTRY>(
    "has_type_privilege");

  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleNameName3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleNameOid3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOidName3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::BIGINT,
     duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOidOid3Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleName2Function});
  loader.RegisterFunction(duckdb::ScalarFunction{
    "pg_has_role",
    {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
    duckdb::LogicalType::BOOLEAN,
    PgHasRoleOid2Function});

  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeNameName4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::INTEGER,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidAttnum3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::SMALLINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeNameAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {pg::OID(), duckdb::LogicalType::VARCHAR, duckdb::LogicalType::SMALLINT,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidNameAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_column_privilege",
      {pg::OID(), pg::REGCLASS(), duckdb::LogicalType::SMALLINT,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasColumnPrivilegeOidOidAttnum4Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }

  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR,
       duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeName3Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::VARCHAR, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeName2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
  {
    duckdb::ScalarFunction func{
      "has_any_column_privilege",
      {duckdb::LogicalType::BIGINT, duckdb::LogicalType::VARCHAR},
      duckdb::LogicalType::BOOLEAN,
      HasAnyColumnPrivilegeOid2Function};
    func.SetNullHandling(duckdb::FunctionNullHandling::SPECIAL_HANDLING);
    loader.RegisterFunction(func);
  }
}

}  // namespace sdb::pg
