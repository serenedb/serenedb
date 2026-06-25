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

#include <absl/strings/str_cat.h>

#include <algorithm>
#include <map>
#include <ranges>
#include <string_view>

#include "auth/acl.h"
#include "basics/serializer.h"
#include "basics/static_strings.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

namespace sdb::catalog {

Role::Role(RoleData data)
  : catalog::Object{Permissions{},
                    {},
                    data.id,
                    std::move(data.name),
                    ObjectType::Role},
    _active{data.active},
    _options{static_cast<RoleOption>(data.options)},
    _member_of{std::move(data.member_of)},
    _conn_limit{data.conn_limit},
    _valid_until{std::move(data.valid_until)},
    _config{std::move(data.config)},
    _default_acls{std::move(data.default_acls)},
    _password_verifier{std::move(data.password_verifier)} {
  if (_name == StaticStrings::kDefaultUser) {
    _options |= RoleOption::Superuser;
  }
}

RoleData Role::BuildData() const {
  RoleData data{
    .id = GetId(),
    .name = std::string{GetName()},
    .active = _active,
    .options = static_cast<uint32_t>(_options),
    .member_of = _member_of,
    .conn_limit = _conn_limit,
    .valid_until = _valid_until,
    .config = _config,
    .default_acls = _default_acls,
    .password_verifier = _password_verifier,
  };
  return data;
}

void catalog::Role::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, BuildData());
}

std::shared_ptr<Role> Role::Deserialize(duckdb::Deserializer& src,
                                        ReadContext) {
  RoleData data;
  basics::ReadTuple(src, data);
  return std::make_shared<catalog::Role>(std::move(data));
}

void catalog::Role::AddMembership(const Membership& edge) {
  if (edge.role == GetId()) {
    return;
  }
  auto it = std::ranges::find(_member_of, edge.role, &Membership::role);
  if (it == _member_of.end()) {
    _member_of.push_back(edge);
  } else {
    // Re-GRANT updates the existing edge's options (PG merges, never dups).
    *it = edge;
  }
}

void catalog::Role::RemoveMembership(ObjectId role) {
  if (auto it = std::ranges::find(_member_of, role, &Membership::role);
      it != _member_of.end()) {
    _member_of.erase(it);
  }
}

namespace {

// The GUC name portion of a "guc=value" setconfig entry.
std::string_view ConfigKey(std::string_view entry) {
  return entry.substr(0, entry.find('='));
}

}  // namespace

void catalog::Role::SetConfig(std::string_view guc, std::string_view value) {
  auto entry = absl::StrCat(guc, "=", value);
  auto it = std::ranges::find_if(
    _config, [&](const std::string& e) { return ConfigKey(e) == guc; });
  if (it != _config.end()) {
    *it = std::move(entry);
  } else {
    _config.push_back(std::move(entry));
  }
}

void catalog::Role::ResetConfig(std::string_view guc) {
  std::erase_if(_config,
                [&](const std::string& e) { return ConfigKey(e) == guc; });
}

void catalog::Role::ChangeDefaultAcl(ObjectId schema, char objtype,
                                     ObjectType type,
                                     absl::FunctionRef<void(Acl&)> mutate) {
  const auto matches = [&](const DefaultAclData& d) {
    return d.schema == schema && d.objtype == objtype;
  };
  auto it = std::ranges::find_if(_default_acls, matches);
  if (it == _default_acls.end()) {
    it = _default_acls.insert(
      _default_acls.end(),
      DefaultAclData{.schema = schema, .objtype = objtype});
  }
  if (it->acl.empty()) {
    it->acl = auth::AclDefault(type, GetId());
  }
  mutate(it->acl);
  const bool only_owner =
    std::ranges::all_of(it->acl, [id = GetId()](const AclItem& item) {
      return item.grantee == id && item.grantor == id;
    });
  if (only_owner) {
    _default_acls.erase(it);
  }
}

std::shared_ptr<Object> Role::Clone() const {
  return std::make_shared<Role>(BuildData());
}

}  // namespace sdb::catalog
