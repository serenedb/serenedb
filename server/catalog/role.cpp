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

#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <map>
#include <ranges>
#include <string_view>

#include "app/app_server.h"
#include "basics/log.h"
#include "basics/random/uniform_character.h"
#include "basics/serializer.h"
#include "basics/ssl/ssl_interface.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "general_server/general_server_feature.h"
#include "general_server/state.h"

namespace sdb::catalog {
namespace {

// private hash function
ErrorCode HexHashFromData(std::string_view hash_method, std::string_view str,
                          std::string& out_hash) {
#ifdef SDB_DEV
  if (hash_method == "noop") {
    out_hash = str;
    return ERROR_OK;
  }
#endif
  // maximum length is 64 bytes for SHA512
  char buffer[64];
  size_t crypted_length;

  if (hash_method == "sha1") {
    rest::ssl_interface::SslShA1(str.data(), str.size(), &buffer[0]);
    crypted_length = 20;
  } else if (hash_method == "sha512") {
    rest::ssl_interface::SslShA512(str.data(), str.size(), &buffer[0]);
    crypted_length = 64;
  } else if (hash_method == "sha384") {
    rest::ssl_interface::SslShA384(str.data(), str.size(), &buffer[0]);
    crypted_length = 48;
  } else if (hash_method == "sha256") {
    rest::ssl_interface::SslShA256(str.data(), str.size(), &buffer[0]);
    crypted_length = 32;
  } else if (hash_method == "sha224") {
    rest::ssl_interface::SslShA224(str.data(), str.size(), &buffer[0]);
    crypted_length = 28;
  } else if (hash_method == "md5") {
    rest::ssl_interface::SslMD5(str.data(), str.size(), &buffer[0]);
    crypted_length = 16;
  } else {
    // invalid algorithm...
    SDB_DEBUG(GENERAL, "invalid algorithm for hexHashFromData: ", hash_method);
    return ERROR_BAD_PARAMETER;
  }

  SDB_ASSERT(crypted_length > 0);

  out_hash = basics::string_utils::EncodeHex(&buffer[0], crypted_length);

  return ERROR_OK;
}

}  // namespace

Role::Role(PrivateTag, ObjectId id, std::string_view name)
  : catalog::Role{id, name} {}

Role::Role(ObjectId id, std::string_view name)
  : catalog::Object{
      Permissions{}, {}, id, std::string{name}, ObjectType::Role} {}

RoleData Role::ToData() const {
  RoleData data{
    .id = GetId(),
    .name = std::string{GetName()},
    .active = _active,
    .password_method = _password_method,
    .password_salt = _password_salt,
    .password_hash = _password_hash,
    .options = static_cast<uint32_t>(_options),
    .member_of = _member_of,
    .conn_limit = _conn_limit,
    .valid_until = _valid_until,
    .config = _config,
    .default_acls = _default_acls,
    .builtin_type_acls = _builtin_type_acls,
  };
  for (const auto& [db_name, context] : _db_access) {
    data.db_access.emplace(db_name, context.database_auth_level);
  }
  return data;
}

void catalog::Role::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, ToData());
}

std::shared_ptr<catalog::Role> catalog::Role::NewUser(std::string_view name,
                                                      std::string_view password,
                                                      ObjectId id) {
  auto role = std::make_shared<catalog::Role>(PrivateTag{}, id, name);
  role->_active = true;
  role->_options = RoleOption::Login | RoleOption::Inherit;

  role->_password_method = "sha256";

  auto salt = random::UniformCharacter("0123456789abcdef").random(8);
  std::string hash;
  auto r =
    HexHashFromData(role->_password_method, absl::StrCat(salt, password), hash);
  if (r != ERROR_OK) {
    SDB_THROW(r, "Could not calculate hex-hash from data");
  }

  role->_password_salt = salt;
  role->_password_hash = hash;

  // build authentication entry
  return role;
}

std::shared_ptr<Role> Role::FromData(RoleData data) {
  auto role =
    std::make_shared<catalog::Role>(Role::PrivateTag{}, data.id, data.name);
  role->UpdateActive(data.active);
  role->_password_method = std::move(data.password_method);
  role->_password_salt = std::move(data.password_salt);
  role->_password_hash = std::move(data.password_hash);
  role->_options = static_cast<RoleOption>(data.options);
  role->_member_of = std::move(data.member_of);
  role->_conn_limit = data.conn_limit;
  role->_valid_until = std::move(data.valid_until);
  role->_config = std::move(data.config);
  role->_default_acls = std::move(data.default_acls);
  role->_builtin_type_acls = std::move(data.builtin_type_acls);
  for (const auto& [db_name, level] : data.db_access) {
    try {
      role->GrantDatabase(db_name, level);
    } catch (const basics::Exception& e) {
      SDB_DEBUG(GENERAL, e.message());
    }
  }
  // The default user always retains RW on the default database and is a
  // superuser -- enforced at load time so a tampered or downgraded grant can't
  // lock it out.
  if (data.name == StaticStrings::kDefaultUser) {
    role->GrantDatabase(StaticStrings::kDefaultDatabase, auth::Level::RW);
    role->_options |= RoleOption::Superuser;
  }
  return role;
}

std::shared_ptr<Role> Role::Deserialize(duckdb::Deserializer& src,
                                        ReadContext) {
  RoleData data;
  basics::ReadTuple(src, data);
  return FromData(std::move(data));
}

bool catalog::Role::CheckPassword(std::string_view password) const {
  std::string hash;
  auto res = HexHashFromData(_password_method,
                             absl::StrCat(_password_salt, password), hash);
  if (res != ERROR_OK) {
    SDB_THROW(res, "Could not calculate hex-hash from input");
  }
  return _password_hash == hash;
}

void catalog::Role::UpdatePassword(std::string_view password) {
  std::string hash;
  auto res = HexHashFromData(_password_method,
                             absl::StrCat(_password_salt, password), hash);
  if (res != ERROR_OK) {
    SDB_THROW(res, "Could not calculate hex-hash from input");
  }
  _password_hash = hash;
}

void catalog::Role::GrantDatabase(std::string_view database,
                                  auth::Level level) {
  if (database.empty() || level == auth::Level::Undefined) {
    SDB_THROW(ERROR_BAD_PARAMETER, "Cannot set rights for empty db name");
  }
  if (_name == StaticStrings::kDefaultUser &&
      database == StaticStrings::kDefaultDatabase && level != auth::Level::RW) {
    SDB_THROW(ERROR_FORBIDDEN, "Cannot lower access level of '",
              StaticStrings::kDefaultUser, "' to ",
              StaticStrings::kDefaultDatabase);
  }
  SDB_DEBUG(GENERAL, _name, ": Granting ", ConvertFromAuthLevel(level), " on ",
            database);

  auto it = _db_access.find(database);
  if (it != _db_access.end()) {
    it->second.database_auth_level = level;
  } else {
    // GrantDatabase is not supposed to change any rights on the
    // collection level code which relies on the old behavior
    // will need to be adjusted
    _db_access.try_emplace(database, DBAuthContext(level));
  }
}

/// Removes the entry, returns true if entry existed
bool catalog::Role::RemoveDatabase(std::string_view database) {
  if (database.empty()) {
    SDB_THROW(ERROR_BAD_PARAMETER, "Cannot remove rights for empty db name");
  }
  if (_name == StaticStrings::kDefaultUser &&
      database == StaticStrings::kDefaultDatabase) {
    SDB_THROW(ERROR_FORBIDDEN, "Cannot remove access level of '",
              StaticStrings::kDefaultUser, "' to ",
              StaticStrings::kDefaultDatabase);
  }
  SDB_DEBUG(GENERAL, _name, ": Removing grant on ", database);
  return _db_access.erase(database) > 0;
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

bool catalog::Role::RemoveMembership(ObjectId role) {
  auto it = std::ranges::find(_member_of, role, &Membership::role);
  if (it == _member_of.end()) {
    return false;
  }
  _member_of.erase(it);
  return true;
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

catalog::Role::DefaultAclData& catalog::Role::MutableDefaultAcl(ObjectId schema,
                                                                char objtype) {
  auto it = std::ranges::find_if(_default_acls, [&](const DefaultAclData& d) {
    return d.schema == schema && d.objtype == objtype;
  });
  if (it != _default_acls.end()) {
    return *it;
  }
  return _default_acls.emplace_back(
    DefaultAclData{.schema = schema, .objtype = objtype});
}

void catalog::Role::RemoveDefaultAcl(ObjectId schema, char objtype) {
  std::erase_if(_default_acls, [&](const DefaultAclData& d) {
    return d.schema == schema && d.objtype == objtype;
  });
}

catalog::AclView catalog::Role::BuiltinTypeAcl(
  uint64_t type_oid) const noexcept {
  auto it =
    std::ranges::find(_builtin_type_acls, type_oid, &TypeAclData::type_oid);
  return it != _builtin_type_acls.end() ? AclView{it->acl} : AclView{};
}

catalog::Acl& catalog::Role::MutableBuiltinTypeAcl(uint64_t type_oid) {
  auto it =
    std::ranges::find(_builtin_type_acls, type_oid, &TypeAclData::type_oid);
  if (it != _builtin_type_acls.end()) {
    return it->acl;
  }
  return _builtin_type_acls.emplace_back(TypeAclData{.type_oid = type_oid}).acl;
}

void catalog::Role::RemoveBuiltinTypeAcl(uint64_t type_oid) {
  std::erase_if(_builtin_type_acls,
                [&](const TypeAclData& t) { return t.type_oid == type_oid; });
}

// Resolve the access level for this database.
auth::Level catalog::Role::ConfiguredDBAuthLevel(
  std::string_view database) const {
  auto it = _db_access.find(database);
  if (it != _db_access.end()) {  // found specific grant
    return it->second.database_auth_level;
  }
  return auth::Level::Undefined;
}

auth::Level catalog::Role::DatabaseAuthLevel(std::string_view database) const {
  auto lvl = ConfiguredDBAuthLevel(database);
  if (lvl == auth::Level::Undefined && database != "*") {
    // take best from wildcard or _system
    auto it = _db_access.find("*");
    if (it != _db_access.end()) {
      lvl = std::max(it->second.database_auth_level, lvl);
    }
    if (database != StaticStrings::kDefaultDatabase) {
      it = _db_access.find(StaticStrings::kDefaultDatabase);
      if (it != _db_access.end()) {
        lvl = std::max(it->second.database_auth_level, lvl);
      }
    }
  }

  return std::max(lvl, auth::Level::None);
}

std::shared_ptr<Object> Role::Clone() const {
  duckdb::MemoryStream stream;
  return catalog::DeserializeObject<Role>(
    catalog::SerializeObject(*this, stream), {});
}

}  // namespace sdb::catalog
