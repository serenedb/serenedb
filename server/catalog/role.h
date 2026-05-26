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

#pragma once

#include <absl/container/node_hash_map.h>

#include <set>
#include <string>

#include "auth/common.h"
#include "basics/containers/node_hash_map.h"
#include "catalog/identifiers/object_id.h"
#include "catalog/object.h"

namespace sdb::catalog {

// Persistent on-disk catalog format.
struct RoleData {
  ObjectId id;
  std::string name;
  bool active = true;
  std::string password_method;
  std::string password_salt;
  std::string password_hash;
  absl::node_hash_map<std::string, std::underlying_type_t<auth::Level>>
    db_access;
};

class Role final : public catalog::Object {
 public:
  struct PrivateTag {
    explicit PrivateTag() = default;
  };

  explicit Role(PrivateTag, ObjectId id, std::string_view name);

  // Capture the persistent state into a flat RoleData (used by the
  // reflection-based Serialize path).
  RoleData ToData() const;
  // Construct a Role from RoleData (read-side counterpart). Static helper
  // so Deserialize has a shared implementation.
  static std::shared_ptr<Role> FromData(RoleData data);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  static std::shared_ptr<catalog::Role> NewUser(std::string_view name,
                                                std::string_view password,
                                                ObjectId id = {});
  static std::shared_ptr<Role> Deserialize(duckdb::Deserializer& src,
                                           ReadContext ctx);

  std::string_view username() const { return GetName(); }
  std::string_view passwordMethod() const { return _password_method; }
  std::string_view passwordSalt() const { return _password_salt; }
  std::string_view passwordHash() const { return _password_hash; }
  bool isActive() const { return _active; }

  bool checkPassword(std::string_view password) const;

  // Resolve the access level for this database.
  auth::Level configuredDBAuthLevel(std::string_view database) const;

  // Resolve the access level for this database. Might fall back to
  // the special '*' entry if the specific database is not found
  auth::Level databaseAuthLevel(std::string_view database) const;

  void updateId(Identifier id) { _id = id; }
  void updateName(std::string_view name) { _name = name; }
  void updatePassword(std::string_view password);
  void updateActive(bool active) { _active = active; }

  /// Grant specific access rights for db.
  /// The default "*" is also a valid database name
  void grantDatabase(std::string_view database, auth::Level level);

  /// Removes the entry, returns true if entry existed
  bool removeDatabase(std::string_view database);

 private:
  Role(ObjectId id, std::string_view name);

  bool _active = true;
  std::string _password_method;
  std::string _password_salt;
  std::string _password_hash;
  struct DBAuthContext {
    auth::Level database_auth_level = auth::Level::Undefined;
  };
  containers::NodeHashMap<std::string, DBAuthContext> _db_access;
};

}  // namespace sdb::catalog
