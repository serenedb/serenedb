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
#include <vpack/iterator.h>

#include <string_view>

#include "app/app_server.h"
#include "auth/role_utils.h"
#include "basics/logger/logger.h"
#include "basics/random/uniform_character.h"
#include "basics/read_locker.h"
#include "basics/ssl/ssl_interface.h"
#include "basics/static_strings.h"
#include "basics/string_utils.h"
#include "basics/system-functions.h"
#include "basics/write_locker.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"
#include "general_server/general_server_feature.h"
#include "general_server/state.h"
#include "vpack/vpack_helper.h"

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
    SDB_DEBUG("xxxxx", Logger::AUTHENTICATION,
              "invalid algorithm for hexHashFromData: ", hash_method);
    return ERROR_BAD_PARAMETER;
  }

  SDB_ASSERT(crypted_length > 0);

  out_hash = basics::string_utils::EncodeHex(&buffer[0], crypted_length);

  return ERROR_OK;
}

void AddAuthLevel(vpack::Builder& build, auth::Level lvl) {
  if (lvl == auth::Level::RW) {
    build.add("read", true);
    build.add("write", true);
  } else if (lvl == auth::Level::RO) {
    build.add("read", true);
    build.add("write", false);
  } else if (lvl == auth::Level::None) {
    build.add("read", false);
    build.add("write", false);
  } else if (lvl == auth::Level::Undefined) {
    build.add("undefined", true);
  }
}

auth::Level AuthLevelFromSlice(vpack::Slice slice) {
  SDB_ASSERT(slice.isObject());
  vpack::Slice v = slice.get("write");
  if (v.isBool() && v.isTrue()) {
    return auth::Level::RW;
  }
  v = slice.get("read");
  if (v.isBool() && v.isTrue()) {
    return auth::Level::RO;
  }
  v = slice.get("undefined");
  if (v.isBool() && v.isTrue()) {
    return auth::Level::Undefined;
  }
  return auth::Level::None;
}

}  // namespace

Role::Role(PrivateTag, ObjectId id, std::string_view name)
  : catalog::Role{id, name} {}

Role::Role(ObjectId id, std::string_view name)
  : catalog::Object{{}, id, std::string{name}, ObjectType::Role} {}

void catalog::Role::WriteInternal(vpack::Builder& build) const {
  WriteProperties(build);
}

void catalog::Role::WriteProperties(vpack::Builder& build) const {
  SDB_ASSERT(build.isOpenObject());
  build.add("id", GetId().id());
  build.add("name", GetName());
  {
    vpack::ObjectBuilder auth_guard{&build, "authData", true};
    build.add("active", _active);
    {
      vpack::ObjectBuilder password_guard{&build, "simple", true};
      build.add("hash", _password_hash);
      build.add("salt", _password_salt);
      build.add("method", _password_method);
    }
  }
  {
    vpack::ObjectBuilder databases_guard{&build, "databases", true};
    for (const auto& [name, context] : _db_access) {
      vpack::ObjectBuilder database_guard{&build, name, true};
      {
        vpack::ObjectBuilder permissions_guard{&build, "permissions", true};
        auto lvl = context.database_auth_level;
        AddAuthLevel(build, lvl);
      }
    }
  }
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

void catalog::Role::fromDocumentDatabases(catalog::Role& role,
                                          vpack::Slice databases_slice) {
  for (const auto& [obj_key, obj_value] :
       vpack::ObjectIterator{databases_slice}) {
    const auto db_name = obj_key.stringView();

    if (obj_value.isObject()) {
      auto database_auth = auth::Level::None;

      const auto permissions_slice = obj_value.get("permissions");

      if (permissions_slice.isObject()) {
        database_auth = AuthLevelFromSlice(permissions_slice);
      }

      try {
        role.grantDatabase(db_name, database_auth);
      } catch (const basics::Exception& e) {
        SDB_DEBUG("xxxxx", Logger::AUTHENTICATION, e.message());
      }
    } else {
      auto value = obj_value.stringView();
      if (absl::EqualsIgnoreCase(value, "rw")) {
        role.grantDatabase(db_name, auth::Level::RW);
      } else if (absl::EqualsIgnoreCase(value, "ro")) {
        role.grantDatabase(db_name, auth::Level::RO);
      }
    }
  }
}

Result catalog::Role::Instantiate(std::shared_ptr<catalog::Role>& role,
                                  vpack::Slice slice, bool is_user_request) {
  if (!slice.isObject()) {
    return {ERROR_BAD_PARAMETER, "role should be object"};
  }

  const auto name_slice = slice.get("name");
  if (!name_slice.isString()) {
    return {ERROR_BAD_PARAMETER, "cannot extract name from role"};
  }

  const auto auth_data_slice = slice.get("authData");
  if (!auth_data_slice.isObject()) {
    return {ERROR_BAD_PARAMETER, "cannot extract authData from role"};
  }

  const auto simple_slice = auth_data_slice.get("simple");
  if (!simple_slice.isObject()) {
    return {ERROR_BAD_PARAMETER, "cannot extract simple from role"};
  }

  const auto method_slice = simple_slice.get("method");
  const auto salt_slice = simple_slice.get("salt");
  const auto hash_slice = simple_slice.get("hash");

  if (!method_slice.isString() || !salt_slice.isString() ||
      !hash_slice.isString()) {
    return {ERROR_BAD_PARAMETER, "cannot extract password internals from role"};
  }

  const auto active_slice = auth_data_slice.get("active");
  if (!active_slice.isBool()) {
    return {ERROR_BAD_PARAMETER, "cannot extract active flag from role"};
  }

  const ObjectId id{
    is_user_request ? 0 : basics::VPackHelper::extractIdValue(slice)};
  const auto name = name_slice.stringView();
  auto tmp = std::make_shared<catalog::Role>(PrivateTag{}, id, name);

  tmp->_active = active_slice.getBool();
  tmp->_password_method = method_slice.stringView();
  tmp->_password_salt = salt_slice.stringView();
  tmp->_password_hash = hash_slice.stringView();

  const auto databases_slice = slice.get("databases");
  if (databases_slice.isObject()) {
    fromDocumentDatabases(*tmp, databases_slice);
  }
  // TODO(mbkkt) remove it, probably only gtest needed it
  // ensure the root user always has the right to change permissions
  if (tmp->_name == auth::kRootUserName) {
    tmp->grantDatabase(StaticStrings::kSystemDatabase, auth::Level::RW);
  }

  role = std::move(tmp);
  return {};
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
  if (_name == auth::kRootUserName &&
      database == StaticStrings::kSystemDatabase && level != auth::Level::RW) {
    SDB_THROW(ERROR_FORBIDDEN,
              "Cannot lower access level of 'root' to _system");
  }
  SDB_DEBUG("xxxxx", Logger::AUTHENTICATION, _name, ": Granting ",
            ConvertFromAuthLevel(level), " on ", database);

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
  if (_name == auth::kRootUserName &&
      database == StaticStrings::kSystemDatabase) {
    SDB_THROW(ERROR_FORBIDDEN,
              "Cannot remove access level of 'root' to _system");
  }
  SDB_DEBUG("xxxxx", Logger::AUTHENTICATION, _name, ": Removing grant on ",
            database);
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
    if (database != StaticStrings::kSystemDatabase) {
      it = _db_access.find(StaticStrings::kSystemDatabase);
      if (it != _db_access.end()) {
        lvl = std::max(it->second.database_auth_level, lvl);
      }
    }
  }

  return std::max(lvl, auth::Level::None);
}

}  // namespace sdb::catalog
