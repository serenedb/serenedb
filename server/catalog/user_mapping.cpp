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

#include "catalog/user_mapping.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/memory_stream.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <string>
#include <string_view>
#include <utility>

#include "basics/serializer.h"
#include "catalog/persistence/user_mapping.h"

namespace sdb::catalog {
namespace {

using persistence::UserMappingData;

}  // namespace

UserMapping::UserMapping(Permissions perm, ObjectId schema_id, ObjectId id,
                         std::string_view name, std::string server_name,
                         std::string user_name,
                         std::vector<std::string> option_keys,
                         std::vector<std::string> option_values,
                         ObjectId server_id, ObjectId role_id)
  : Object{std::move(perm), schema_id, id, name, ObjectType::UserMapping},
    _server_name{std::move(server_name)},
    _user_name{std::move(user_name)},
    _option_keys{std::move(option_keys)},
    _option_values{std::move(option_values)},
    _server_id{server_id},
    _role_id{role_id} {}

std::shared_ptr<UserMapping> UserMapping::Deserialize(duckdb::Deserializer& src,
                                                      ReadContext ctx) {
  UserMappingData data;
  basics::ReadTuple(src, data);

  return std::make_shared<UserMapping>(
    std::move(data.perm), ctx.schema_id, ctx.id, data.name,
    std::move(data.server_name), std::move(data.user_name),
    std::move(data.option_keys), std::move(data.option_values), data.server_id,
    data.role_id);
}

void UserMapping::Serialize(duckdb::Serializer& sink) const {
  UserMappingData data{
    .perm = GetPermissions(),
    .name = std::string{GetName()},
    .server_name = _server_name,
    .user_name = _user_name,
    .option_keys = _option_keys,
    .option_values = _option_values,
    .server_id = _server_id,
    .role_id = _role_id,
  };
  basics::WriteTuple(sink, data);
}

std::shared_ptr<Object> UserMapping::Clone() const {
  duckdb::MemoryStream stream;
  return DeserializeObject<UserMapping>(
    SerializeObject(*this, stream),
    {.id = GetId(), .schema_id = GetParentId()});
}

}  // namespace sdb::catalog
