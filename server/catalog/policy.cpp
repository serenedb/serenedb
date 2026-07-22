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

#include "catalog/policy.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>

#include "basics/serializer.h"

namespace sdb::catalog {

Policy::Policy(ObjectId database_id, ObjectId schema_id, ObjectId id,
               ObjectId relation_id, persistence::PolicyData data)
  : Object{Permissions{}, schema_id, id, data.name, ObjectType::Policy},
    _database_id{database_id},
    _relation_id{relation_id},
    _data{std::move(data)} {}

std::shared_ptr<Policy> Policy::Deserialize(duckdb::Deserializer& src,
                                            ReadContext ctx) {
  persistence::PolicyData data;
  basics::ReadTuple(src, data);
  return std::make_shared<Policy>(ctx.database_id, ctx.schema_id, ctx.id,
                                  ctx.relation_id, std::move(data));
}

void Policy::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, _data);
}

std::shared_ptr<Object> Policy::Clone() const {
  return std::make_shared<Policy>(_database_id, GetParentId(), GetId(),
                                  _relation_id, _data);
}

RowSecurity::RowSecurity(ObjectId database_id, ObjectId schema_id, ObjectId id,
                         ObjectId relation_id, persistence::RowSecurityData data)
  : Object{Permissions{}, schema_id, id, "row_security",
           ObjectType::RowSecurity},
    _database_id{database_id},
    _relation_id{relation_id},
    _data{data} {}

std::shared_ptr<RowSecurity> RowSecurity::Deserialize(duckdb::Deserializer& src,
                                                      ReadContext ctx) {
  persistence::RowSecurityData data;
  basics::ReadTuple(src, data);
  return std::make_shared<RowSecurity>(ctx.database_id, ctx.schema_id, ctx.id,
                                       ctx.relation_id, data);
}

void RowSecurity::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, _data);
}

std::shared_ptr<Object> RowSecurity::Clone() const {
  return std::make_shared<RowSecurity>(_database_id, GetParentId(), GetId(),
                                       _relation_id, _data);
}

}  // namespace sdb::catalog
