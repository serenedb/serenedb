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

#include "basics/containers/flat_hash_set.h"
#include "basics/serializer.h"
#include "catalog/persistence/secondary_index.h"

namespace sdb::catalog {

SecondaryIndex::SecondaryIndex(ObjectId database_id, ObjectId schema_id,
                               ObjectId id, ObjectId relation_id,
                               std::string name,
                               std::vector<Column::Id> columns,
                               std::vector<ExpressionData> expressions,
                               bool unique)
  : Index(database_id, schema_id, id, relation_id, std::move(name),
          DeriveIds(columns, expressions), ObjectType::SecondaryIndex),
    _columns{std::move(columns)},
    _expressions{std::move(expressions)},
    _unique{unique} {}

std::shared_ptr<SecondaryIndex> SecondaryIndex::Deserialize(
  duckdb::Deserializer& src, ReadContext ctx) {
  persistence::SecondaryIndexData data;
  basics::ReadTuple(src, data);
  return std::make_shared<SecondaryIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::move(data.name), std::move(data.columns), std::move(data.expressions),
    data.unique);
}

void SecondaryIndex::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, persistence::SecondaryIndexData{
                             .name = GetName(),
                             .unique = _unique,
                             .columns = Columns(),
                             .expressions = Expressions(),
                           });
}

std::shared_ptr<Object> SecondaryIndex::Clone() const {
  return std::make_shared<SecondaryIndex>(GetDatabaseId(), GetParentId(),
                                          GetId(), GetRelationId(), GetName(),
                                          Columns(), Expressions(), _unique);
}

}  // namespace sdb::catalog
