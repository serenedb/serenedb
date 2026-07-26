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

#include "catalog/schema.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>

#include "basics/serializer.h"
#include "basics/simdjson_sink.h"

namespace sdb::catalog {

CreateSchemaInfo::CreateSchemaInfo(ObjectId id, ObjectId database_id,
                                   std::string_view name) {
  SetId(id);
  SetDatabaseId(database_id);
  SetSchemaName(name);
}

persistence::SchemaOptions CreateSchemaInfo::ToData() const {
  return persistence::SchemaOptions{.name = std::string{GetName()}};
}

void CreateSchemaInfo::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, ToData());
}

void CreateSchemaInfo::WriteJson(basics::JsonSink& sink) const {
  basics::WriteObject(sink, ToData());
}

std::shared_ptr<CreateSchemaInfo> CreateSchemaInfo::Deserialize(
  duckdb::Deserializer& src, ObjectId id, ObjectId database_id) {
  persistence::SchemaOptions data;
  basics::ReadTuple(src, data);
  return std::make_shared<CreateSchemaInfo>(id, database_id, data.name);
}

std::shared_ptr<CreateSchemaInfo> CreateSchemaInfo::CloneSchema() const {
  return std::make_shared<CreateSchemaInfo>(GetId(), GetDatabaseId(),
                                            GetName());
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateSchemaInfo::Copy() const {
  return duckdb::make_uniq<CreateSchemaInfo>(GetId(), GetDatabaseId(),
                                             GetName());
}

}  // namespace sdb::catalog
