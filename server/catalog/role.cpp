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

#include <duckdb/common/serializer/memory_stream.hpp>
#include <map>
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
  : catalog::Object{{}, id, std::string{name}, ObjectType::Role} {}

RoleData Role::ToData() const {
  RoleData data{
    .id = GetId(),
    .name = std::string{GetName()},
    .active = _active,
    .password_method = _password_method,
    .password_salt = _password_salt,
    .password_hash = _password_hash,
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
  role->updateActive(data.active);
  role->_password_method = std::move(data.password_method);
  role->_password_salt = std::move(data.password_salt);
  role->_password_hash = std::move(data.password_hash);
  for (const auto& [db_name, level] : data.db_access) {
    try {
      role->grantDatabase(db_name, level);
    } catch (const basics::Exception& e) {
      SDB_DEBUG(GENERAL, e.message());
    }
  }
  // The default user always retains RW on the default database -- enforced
  // at load time so a tampered or downgraded grant can't lock it out.
  if (data.name == StaticStrings::kDefaultUser) {
    role->grantDatabase(StaticStrings::kDefaultDatabase, auth::Level::RW);
  }
  return role;
}

std::shared_ptr<Role> Role::Deserialize(duckdb::Deserializer& src,
                                        ReadContext) {
  RoleData data;
  basics::ReadTuple(src, data);
  return FromData(std::move(data));
}

bool catalog::Role::checkPassword(std::string_view password) const {
  std::string hash;
  auto res = HexHashFromData(_password_method,
                             absl::StrCat(_password_salt, password), hash);
  if (res != ERROR_OK) {
    SDB_THROW(res, "Could not calculate hex-hash from input");
  }
  return _password_hash == hash;
}

void catalog::Role::updatePassword(std::string_view password) {
  std::string hash;
  auto res = HexHashFromData(_password_method,
                             absl::StrCat(_password_salt, password), hash);
  if (res != ERROR_OK) {
    SDB_THROW(res, "Could not calculate hex-hash from input");
  }
  _password_hash = hash;
}

void catalog::Role::grantDatabase(std::string_view database,
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
    // grantDatabase is not supposed to change any rights on the
    // collection level code which relies on the old behavior
    // will need to be adjusted
    _db_access.try_emplace(database, DBAuthContext(level));
  }
}

/// Removes the entry, returns true if entry existed
bool catalog::Role::removeDatabase(std::string_view database) {
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

// Resolve the access level for this database.
auth::Level catalog::Role::configuredDBAuthLevel(
  std::string_view database) const {
  auto it = _db_access.find(database);
  if (it != _db_access.end()) {  // found specific grant
    return it->second.database_auth_level;
  }
  return auth::Level::Undefined;
}

auth::Level catalog::Role::databaseAuthLevel(std::string_view database) const {
  auto lvl = configuredDBAuthLevel(database);
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
