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

namespace sdb::catalog {

Database::Database(ObjectId id, DatabaseOptions opts)
  : Object{{}, id, opts.name, ObjectType::Database} {}

std::shared_ptr<Database> Database::Deserialize(duckdb::Deserializer& src,
                                                ReadContext ctx) {
  DatabaseOptions opts;
  basics::ReadTuple(src, opts);
  return std::make_shared<Database>(ctx.id, std::move(opts));
}

void Database::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, DatabaseOptions{std::string{GetName()}});
}

std::shared_ptr<Object> Database::Clone() const {
  return std::make_shared<Database>(GetId(),
                                    DatabaseOptions{std::string{GetName()}});
}

}  // namespace sdb::catalog
