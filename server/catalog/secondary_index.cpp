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
#include "basics/simdjson_sink.h"
#include "catalog/persistence/secondary_index.h"

namespace sdb::catalog {

CreateSecondaryIndexInfo::CreateSecondaryIndexInfo(
  ObjectId schema_id, ObjectId id, ObjectId relation_id,
  persistence::SecondaryIndexData data)
  : CreateIndexInfoBase{schema_id,
                        id,
                        relation_id,
                        data.name,
                        std::move(data.comment),
                        DeriveIds(data.columns, data.expressions, {}),
                        /*inverted=*/false},
    _key_columns{std::move(data.columns)},
    _expressions{std::move(data.expressions)},
    _unique{data.unique} {
  // duckdb's own half, so upstream machinery (duckdb_indexes, ToString) reads
  // the same facts our payload carries.
  constraint_type = _unique ? duckdb::IndexConstraintType::UNIQUE
                            : duckdb::IndexConstraintType::NONE;
}

persistence::SecondaryIndexData CreateSecondaryIndexInfo::ToData() const {
  return persistence::SecondaryIndexData{
    .name = std::string{GetName()},
    .unique = _unique,
    .columns = _key_columns,
    .expressions = _expressions,
    .comment = std::string{Comment()},
  };
}

void CreateSecondaryIndexInfo::Serialize(duckdb::Serializer& sink) const {
  basics::WriteTuple(sink, ToData());
}

void CreateSecondaryIndexInfo::WriteJson(basics::JsonSink& sink) const {
  basics::WriteObject(sink, ToData());
}

duckdb::unique_ptr<duckdb::CreateInfo> CreateSecondaryIndexInfo::Copy() const {
  auto copy = duckdb::make_uniq<CreateSecondaryIndexInfo>(
    GetSchemaId(), GetId(), GetRelationId(), ToData());
  CopyProperties(*copy);
  return copy;
}

std::shared_ptr<CreateSecondaryIndexInfo> CreateSecondaryIndexInfo::Deserialize(
  duckdb::Deserializer& src, ObjectId schema_id, ObjectId id,
  ObjectId relation_id) {
  persistence::SecondaryIndexData data;
  basics::ReadTuple(src, data);
  return std::make_shared<CreateSecondaryIndexInfo>(schema_id, id, relation_id,
                                                    std::move(data));
}

}  // namespace sdb::catalog
