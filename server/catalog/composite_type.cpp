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

#include "basics/errors.h"
#include "utils/velox_vpack.h"

namespace sdb::catalog {
namespace {

struct VPackData {
  std::string_view name;
  velox::RowTypePtr row_type;
};

}  // namespace

CompositeType::CompositeType(std::string_view name, velox::RowTypePtr row_type)
  : SchemaObject{{}, {}, {}, {}, name, ObjectType::CompositeType},
    _row_type{std::move(row_type)} {}

CompositeType::CompositeType(ObjectId id, std::string_view name,
                             velox::RowTypePtr row_type)
  : SchemaObject{{}, {}, {}, id, name, ObjectType::CompositeType},
    _row_type{std::move(row_type)} {}

void CompositeType::WriteInternal(vpack::Builder& b) const {
  vpack::WriteTuple(b, VPackData{
                         .name = GetName(),
                         .row_type = _row_type,
                       });
}

Result CompositeType::Instantiate(std::shared_ptr<CompositeType>& result,
                                  ObjectId id, vpack::Slice slice) {
  VPackData data;
  auto r = vpack::ReadTupleNothrow(slice, data);
  if (!r.ok()) {
    return {ERROR_BAD_PARAMETER,
            "invalid composite type definition: ", r.errorMessage()};
  }

  result =
    std::make_shared<CompositeType>(id, data.name, std::move(data.row_type));
  return {};
}

}  // namespace sdb::catalog
