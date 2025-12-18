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

#include "catalog/index.h"

#include "vpack/serializer.h"

namespace sdb::catalog {

Index::Index(IndexOptions options, ObjectId database_id)
  : SchemaObject{{},
                 database_id,
                 {},
                 options.id,
                 std::move(options.name),
                 ObjectType::Index},
    _relation_id{options.relation_id},
    _type(options.type) {}

void Index::WriteInternal(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());
  vpack::WriteTuple(b, IndexOptions{
                         .id = GetId(),
                         .relation_id = GetRelationId(),
                         .name = std::string{GetName()},
                         .type = GetIndexType(),
                         .options = vpack::Slice{},
                       });
}

void Index::WriteProperties(vpack::Builder& b) const {
  SDB_ASSERT(b.isOpenObject());
  vpack::WriteObject(b, IndexOptions{
                          .id = GetId(),
                          .relation_id = GetRelationId(),
                          .name = std::string{GetName()},
                          .type = GetIndexType(),
                          .options = vpack::Slice{},
                        });
}

}  // namespace sdb::catalog
