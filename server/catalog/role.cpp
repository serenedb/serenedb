////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
////////////////////////////////////////////////////////////////////////////////

#include "catalog/role.h"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/parser/keyword_helper.hpp>
#include <map>
#include <ranges>
#include <string_view>

#include "auth/acl.h"
#include "basics/serializer.h"
#include "basics/simdjson_sink.h"
#include "basics/static_strings.h"
#include "catalog/entry.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/persistence/role.h"

namespace sdb::catalog {

Role::Role(ObjectId id, persistence::RoleData data)
  : _options{static_cast<RoleOption>(data.options)},
    _member_of{std::move(data.member_of)},
    _conn_limit{data.conn_limit},
    _valid_until{std::move(data.valid_until)},
    _config{std::move(data.config)},
    _default_acls{std::move(data.default_acls)},
    _password{std::move(data.password)} {
  SetId(id);
  SetRoleName(data.name);
  if (data.name == StaticStrings::kDefaultUser) {
    _options |= RoleOption::Superuser;
  }
}

persistence::RoleData Role::ToData() const {
  return persistence::RoleData{
    .name = std::string{GetName()},
    .options = static_cast<uint32_t>(_options),
    .member_of = _member_of,
    .conn_limit = _conn_limit,
    .valid_until = _valid_until,
    .config = _config,
    .default_acls = _default_acls,
    .password = _password,
  };
}

std::string Role::ToString() const {
  std::string out = absl::StrCat(
    "CREATE ROLE ",
    duckdb::KeywordHelper::WriteOptionallyQuoted(std::string{GetName()}));
  if ((_options & RoleOption::Login) != RoleOption{}) {
    absl::StrAppend(&out, " LOGIN");
  }
  if ((_options & RoleOption::Superuser) != RoleOption{}) {
    absl::StrAppend(&out, " SUPERUSER");
  }
  if (_conn_limit >= 0) {
    absl::StrAppend(&out, " CONNECTION LIMIT ", _conn_limit);
  }
  return absl::StrCat(out, ";");
}

void Role::SerializePayload(duckdb::Serializer& sink) const {
  sink.WritePropertyWithDefault<duckdb::Identifier>(200, "name",
                                                    duckdb::Identifier{_name});
  sink.WritePropertyWithDefault(201, "options",
                                static_cast<uint32_t>(_options));
  sink.WritePropertyWithDefault(202, "conn_limit", _conn_limit);
  sink.WritePropertyWithDefault(203, "valid_until", _valid_until);
  sink.WritePropertyWithDefault(204, "password", _password);
  // Session config, membership edges and default ACLs are std::vector of our
  // own types: the basics framework is the only serializer they have, so they
  // ride inside one property.
  sink.OnPropertyBegin(205, "grants");
  basics::WriteTuple(sink, std::tie(_config, _member_of, _default_acls));
  sink.OnPropertyEnd();
  // The role's own identity, so the record states everything the object is
  // built from: duckdb's base carries the same one on the record around it, but
  // reads it back after the payload is already a finished object.
  sink.WritePropertyWithDefault<uint64_t>(206, "sdb_id", _id.id());
}

duckdb::unique_ptr<duckdb::CreateInfo> Role::Deserialize(
  duckdb::Deserializer& src) {
  auto role = std::make_shared<Role>();
  role->_name = src.ReadPropertyWithDefault<duckdb::Identifier>(200, "name")
                  .GetIdentifierName();
  role->_options = static_cast<RoleOption>(
    src.ReadPropertyWithDefault<uint32_t>(201, "options"));
  src.ReadPropertyWithDefault(202, "conn_limit", role->_conn_limit);
  src.ReadPropertyWithDefault(203, "valid_until", role->_valid_until);
  src.ReadPropertyWithDefault(204, "password", role->_password);
  src.OnPropertyBegin(205, "grants");
  auto refs = std::tie(role->_config, role->_member_of, role->_default_acls);
  basics::ReadTuple(src, refs);
  src.OnPropertyEnd();
  role->_id = ObjectId{src.ReadPropertyWithDefault<uint64_t>(206, "sdb_id")};
  return duckdb::make_uniq<CreateRoleInfo>(std::move(role));
}

void Role::AddMembership(const Membership& edge) {
  if (edge.role == GetId()) {
    return;
  }
  auto it = std::ranges::find(_member_of, edge.role, &Membership::role);
  if (it == _member_of.end()) {
    _member_of.push_back(edge);
  } else {
    *it = edge;
  }
}

void Role::RemoveMembership(ObjectId role) {
  if (auto it = std::ranges::find(_member_of, role, &Membership::role);
      it != _member_of.end()) {
    _member_of.erase(it);
  }
}

namespace {

std::string_view ConfigKey(std::string_view entry) {
  return entry.substr(0, entry.find('='));
}

}  // namespace

void Role::SetConfig(std::string_view guc, std::string_view value) {
  auto entry = absl::StrCat(guc, "=", value);
  auto it = std::ranges::find_if(
    _config, [&](const std::string& e) { return ConfigKey(e) == guc; });
  if (it != _config.end()) {
    *it = std::move(entry);
  } else {
    _config.push_back(std::move(entry));
  }
}

void Role::ResetConfig(std::string_view guc) {
  std::erase_if(_config,
                [&](const std::string& e) { return ConfigKey(e) == guc; });
}

void Role::ChangeDefaultAcl(ObjectId schema, char objtype,
                            duckdb::CatalogType type,
                            absl::FunctionRef<void(Acl&)> mutate) {
  const auto matches = [&](const DefaultAcl& d) {
    return d.schema == schema && d.objtype == objtype;
  };
  auto it = std::ranges::find_if(_default_acls, matches);
  if (it == _default_acls.end()) {
    it = _default_acls.insert(_default_acls.end(),
                              DefaultAcl{.schema = schema, .objtype = objtype});
  }
  if (it->acl.empty()) {
    it->acl = auth::AclDefault(type, GetId());
  }
  mutate(it->acl);
  const auto seed = auth::AclDefault(type, GetId());
  const auto is_default =
    it->acl.size() == seed.size() &&
    absl::c_all_of(seed, [&](const AclItem& def) {
      return absl::c_any_of(it->acl, [&](const AclItem& item) {
        return item.grantee == def.grantee && item.grantor == def.grantor &&
               item.privs == def.privs && item.grant_option == def.grant_option;
      });
    });
  if (is_default) {
    _default_acls.erase(it);
  }
}

duckdb::unique_ptr<Role> Role::Clone() const {
  return duckdb::make_uniq<Role>(GetId(), ToData());
}

CreateRoleInfo::CreateRoleInfo(std::shared_ptr<const Role> role)
  : duckdb::CreateInfo{duckdb::CatalogType::ROLE_ENTRY},
    _role{std::move(role)} {
  oid = _role->GetId().id();
  SetName(duckdb::Identifier{std::string{_role->GetName()}});
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateRoleInfo::Copy() const {
  // The role itself is shared: every version of it that a copy of this record
  // reaches is the same object.
  auto result = duckdb::make_uniq<CreateRoleInfo>(_role);
  CopyProperties(*result);
  return std::move(result);
}

void CreateRoleInfo::Serialize(duckdb::Serializer& sink) const {
  duckdb::CreateInfo::Serialize(sink);
  _role->SerializePayload(sink);
}

std::string CreateRoleInfo::ToString() const { return _role->ToString(); }

}  // namespace sdb::catalog
