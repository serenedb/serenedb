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
#include <string>
#include <string_view>

#include "catalog/object.h"
#include "catalog/options.h"

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

  UserMapping(Permissions perm, ObjectId schema_id, ObjectId id,
              std::string_view name, std::string server_name,
              std::string user_name, Options options, ObjectId server_id = {},
              ObjectId role_id = {});

  std::string_view GetServerName() const noexcept { return _server_name; }

  // The owning FOREIGN SERVER's id (drives the server->mapping drop cascade).
  ObjectId GetServerId() const noexcept { return _server_id; }
  // The RBAC role this mapping is FOR; unset for a PUBLIC mapping (registers a
  // role->mapping dependency so DROP ROLE is aware of it).
  ObjectId GetRoleId() const noexcept { return _role_id; }

  const Options& GetOptions() const noexcept { return _options; }

 private:
  std::string _server_name;
  std::string _user_name;
  Options _options;
  ObjectId _server_id;
  ObjectId _role_id;
};

}  // namespace sdb::catalog
