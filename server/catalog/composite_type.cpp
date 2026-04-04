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

#include "catalog/composite_type.h"

#include <vpack/serializer.h>

#include "basics/assert.h"
#include "utils/velox_vpack.h"

namespace sdb::catalog {

CompositeType::CompositeType(ObjectId id, Options options)
  : SchemaObject{{}, {}, {}, id, options.name, ObjectType::CompositeType},
    _row_type{std::move(options.row_type)} {}

void CompositeType::WriteInternal(vpack::Builder& b) const {
  vpack::WriteTuple(b, Options{
                         .name = GetName(),
                         .row_type = _row_type,
                       });
}

std::shared_ptr<CompositeType> CompositeType::FromVPack(ObjectId id,
                                                        vpack::Slice slice) {
  Options options;
  auto r = vpack::ReadTupleNothrow(slice, options);
  SDB_ASSERT(r.ok());

  return std::make_shared<CompositeType>(id, std::move(options));
}

}  // namespace sdb::catalog
