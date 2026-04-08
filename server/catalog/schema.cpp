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

#include <vpack/serializer.h>

namespace sdb::catalog {

Schema::Schema(ObjectId database_id, SchemaOptions options)
  : DatabaseObject{options.owner_id, database_id, options.id,
                   std::move(options.name), ObjectType::Schema} {}

std::shared_ptr<Schema> Schema::ReadInternal(vpack::Slice slice,
                                             ReadContext ctx) {
  SchemaOptions options;
  if (auto r = vpack::ReadTupleNothrow(slice, options); !r.ok()) {
    return nullptr;
  }
  return std::make_shared<Schema>(ctx.database_id, std::move(options));
}

void Schema::WriteInternal(vpack::Builder& b) const {
  vpack::WriteTuple(b, SchemaOptions{
                         .owner_id = GetOwnerId(),
                         .id = GetId(),
                         .name = _name,
                       });
}

std::shared_ptr<Object> Schema::Clone() const {
  vpack::Builder b;
  WriteInternal(b);
  return ReadInternal(b.slice(), {.database_id = GetDatabaseId()});
}

}  // namespace sdb::catalog
