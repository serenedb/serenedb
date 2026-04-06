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

#include <vpack/serializer.h>

namespace sdb::catalog {

SecondaryIndex::SecondaryIndex(ObjectId database_id, ObjectId schema_id,
                               ObjectId id, ObjectId relation_id,
                               std::string name,
                               std::vector<Column::Id> column_ids, bool unique)
  : Index(database_id, schema_id, id, relation_id, std::move(name),
          std::move(column_ids), ObjectType::SecondaryIndex),
    _unique{unique} {}

std::shared_ptr<SecondaryIndex> SecondaryIndex::ReadInternal(vpack::Slice slice,
                                                             ReadContext ctx) {
  auto name_slice = slice.get("name");
  if (!name_slice.isString()) {
    return nullptr;
  }

  std::vector<Column::Id> column_ids;
  if (auto r = vpack::ReadTupleNothrow(slice.get("column_ids"), column_ids);
      !r.ok()) {
    return nullptr;
  }

  auto unique = slice.get("unique");

  return std::make_shared<SecondaryIndex>(
    ctx.database_id, ctx.schema_id, ctx.id, ctx.relation_id,
    std::string{name_slice.stringView()}, std::move(column_ids),
    unique.getBool());
}

void SecondaryIndex::WriteInternal(vpack::Builder& b) const {
  b.openObject();
  WriteObject(b, [&](vpack::Builder& b) {
    b.add("unique", _unique);
    b.add("column_ids");
    vpack::WriteTuple(b, _column_ids);
  });
  b.close();
}

std::shared_ptr<Object> SecondaryIndex::Clone() const {
  vpack::Builder b;
  WriteInternal(b);
  return ReadInternal(b.slice(), {.id = GetId(),
                                  .database_id = GetDatabaseId(),
                                  .schema_id = GetSchemaId(),
                                  .relation_id = GetRelationId()});
}

}  // namespace sdb::catalog
