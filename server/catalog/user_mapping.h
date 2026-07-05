////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "catalog/object.h"

namespace duckdb {

class Serializer;
class Deserializer;

}  // namespace duckdb
namespace sdb::catalog {

class UserMapping : public Object {
 public:
  static std::shared_ptr<UserMapping> Deserialize(duckdb::Deserializer& src,
                                                  ReadContext ctx);

  void Serialize(duckdb::Serializer& sink) const final;
  std::shared_ptr<Object> Clone() const final;

  UserMapping(ObjectId schema_id, ObjectId id, std::string_view name,
              std::string server_name, std::string user_name,
              std::vector<std::string> option_keys,
              std::vector<std::string> option_values,
              ObjectId server_id = {}, ObjectId role_id = {});

  std::string_view GetServerName() const noexcept { return _server_name; }
  std::string_view GetUserName() const noexcept { return _user_name; }

  // The owning FOREIGN SERVER's id (drives the server->mapping drop cascade).
  ObjectId GetServerId() const noexcept { return _server_id; }
  // The RBAC role this mapping is FOR; unset for a PUBLIC mapping (registers a
  // role->mapping dependency so DROP ROLE is aware of it).
  ObjectId GetRoleId() const noexcept { return _role_id; }

  const std::vector<std::string>& GetOptionKeys() const noexcept {
    return _option_keys;
  }
  const std::vector<std::string>& GetOptionValues() const noexcept {
    return _option_values;
  }

 private:
  std::string _server_name;
  std::string _user_name;
  std::vector<std::string> _option_keys;
  std::vector<std::string> _option_values;
  ObjectId _server_id;
  ObjectId _role_id;
};

// Schema-unique synthetic name for a user mapping:
// "<len(user)>:<user>@<server>". A PUBLIC mapping uses user "public". The
// length prefix makes the (user, server)
// -> name mapping injective even when a user or server name contains '@' (which
// a plain "<user>@<server>" could not disambiguate, e.g. ("a","b@c") vs
// ("a@b","c")).
inline std::string MakeUserMappingName(std::string_view user_name,
                                       std::string_view server_name) {
  std::string out = std::to_string(user_name.size());
  out += ':';
  out += user_name;
  out += '@';
  out += server_name;
  return out;
}

}  // namespace sdb::catalog
