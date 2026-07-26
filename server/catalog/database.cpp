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

#include "catalog/database.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>

#include "basics/serializer.h"
#include "basics/simdjson_sink.h"

namespace sdb::catalog {

CreateDatabaseInfo::CreateDatabaseInfo(ObjectId id, std::string_view name)
  : duckdb::CreateInfo{duckdb::CatalogType::DATABASE_ENTRY} {
  SetId(id);
  SetDatabaseName(name);
}

persistence::DatabaseOptions CreateDatabaseInfo::ToData() const {
  return persistence::DatabaseOptions{.name = std::string{GetName()}};
}

void CreateDatabaseInfo::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, ToData());
}

void CreateDatabaseInfo::WriteJson(basics::JsonSink& sink) const {
  basics::WriteObject(sink, ToData());
}

std::shared_ptr<CreateDatabaseInfo> CreateDatabaseInfo::Deserialize(
  duckdb::Deserializer& src, ObjectId id) {
  persistence::DatabaseOptions data;
  basics::ReadTuple(src, data);
  return std::make_shared<CreateDatabaseInfo>(id, data.name);
}

std::shared_ptr<CreateDatabaseInfo> CreateDatabaseInfo::CloneDatabase() const {
  return std::make_shared<CreateDatabaseInfo>(GetId(), GetName());
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateDatabaseInfo::Copy() const {
  return duckdb::make_uniq<CreateDatabaseInfo>(GetId(), GetName());
}

}  // namespace sdb::catalog
