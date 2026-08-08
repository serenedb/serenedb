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

#include <absl/strings/str_cat.h>

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>
#include <duckdb/parser/keyword_helper.hpp>
#include <string>

namespace sdb::catalog {

CreateDatabaseInfo::CreateDatabaseInfo(ObjectId id, std::string_view name)
  : duckdb::CreateInfo{duckdb::CatalogType::DATABASE_ENTRY} {
  SetId(id);
  SetDatabaseName(name);
}

std::string CreateDatabaseInfo::ToString() const {
  return absl::StrCat(
    "CREATE DATABASE ",
    duckdb::KeywordHelper::WriteOptionallyQuoted(std::string{GetName()}), ";");
}

void CreateDatabaseInfo::Serialize(duckdb::Serializer& sink) const {
  duckdb::CreateInfo::Serialize(sink);
  sink.WritePropertyWithDefault<duckdb::Identifier>(200, "name",
                                                    qualified_name.Name());
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateDatabaseInfo::Deserialize(
  duckdb::Deserializer& src) {
  auto result = duckdb::make_uniq<CreateDatabaseInfo>();
  result->SetName(src.ReadPropertyWithDefault<duckdb::Identifier>(200, "name"));
  return std::move(result);
}

std::shared_ptr<CreateDatabaseInfo> CreateDatabaseInfo::CloneDatabase() const {
  return std::make_shared<CreateDatabaseInfo>(GetId(), GetName());
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateDatabaseInfo::Copy() const {
  return duckdb::make_uniq<CreateDatabaseInfo>(GetId(), GetName());
}

}  // namespace sdb::catalog
