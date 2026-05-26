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

namespace sdb::catalog {

Schema::Schema(ObjectId database_id, SchemaOptions options)
  : Object{database_id, options.id, std::move(options.name),
           ObjectType::Schema} {}

std::shared_ptr<Schema> Schema::Deserialize(duckdb::Deserializer& src,
                                            ReadContext ctx) {
  SchemaOptions options;
  basics::ReadTuple(src, options);
  return std::make_shared<Schema>(ctx.database_id, std::move(options));
}

void Schema::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, SchemaOptions{.id = GetId(), .name = _name});
}

std::shared_ptr<Object> Schema::Clone() const {
  return std::make_shared<Schema>(
    GetParentId(), SchemaOptions{.id = GetId(), .name = _name});
}

}  // namespace sdb::catalog
