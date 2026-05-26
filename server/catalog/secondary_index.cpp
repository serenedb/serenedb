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

#include "catalog/secondary_index.h"

#include <duckdb/common/serializer/deserializer.hpp>
#include <duckdb/common/serializer/serializer.hpp>

#include "basics/serializer.h"

namespace sdb::catalog {
namespace {

// Persistent on-disk catalog format.
struct SecondaryIndexData {
  std::string name;
  std::vector<Column::Id> column_ids;
  bool unique = false;
};

std::shared_ptr<SecondaryIndex> FromData(SecondaryIndexData data,
                                         ReadContext ctx) {
  return std::make_shared<SecondaryIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::move(data.name), std::move(data.column_ids), data.unique);
}

}  // namespace

SecondaryIndex::SecondaryIndex(ObjectId database_id, ObjectId schema_id,
                               ObjectId id, ObjectId relation_id,
                               std::string name,
                               std::vector<Column::Id> column_ids, bool unique)
  : Index(database_id, schema_id, id, relation_id, std::move(name),
          std::move(column_ids), ObjectType::SecondaryIndex),
    _unique{unique} {}

std::shared_ptr<SecondaryIndex> SecondaryIndex::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  SecondaryIndexData data;
  basics::ReadTuple(src, data);
  return FromData(std::move(data), ctx);
}

void SecondaryIndex::Serialize(duckdb::Serializer& sink) const {
  auto ids = GetColumnIds();
  basics::WriteTuple(sink, SecondaryIndexData{
                             .name = std::string{GetName()},
                             .column_ids = {ids.begin(), ids.end()},
                             .unique = _unique,
                           });
}

std::shared_ptr<Object> SecondaryIndex::Clone() const {
  auto ids = GetColumnIds();
  return std::make_shared<SecondaryIndex>(
    GetDatabaseId(), GetParentId(), GetId(), GetRelationId(),
    std::string{GetName()}, std::vector<Column::Id>{ids.begin(), ids.end()},
    _unique);
}

}  // namespace sdb::catalog
