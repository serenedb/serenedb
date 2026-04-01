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

#include <velox/type/Type.h>
#include <velox/type/parser/TypeParser.h>

#include "basics/assert.h"

namespace sdb::catalog {

CompositeType::CompositeType(ObjectId id, std::string_view name,
                             velox::RowTypePtr row_type)
  : SchemaObject{{}, {}, {}, id, name, ObjectType::CompositeType},
    _row_type{std::move(row_type)} {}

void CompositeType::WriteInternal(vpack::Builder& b) const {
  b.add("name", GetName());
  b.add("row_type", _row_type->toString());
}

std::shared_ptr<CompositeType> CompositeType::FromVPack(ObjectId id,
                                                        vpack::Slice slice) {
  auto name = slice.get("name");
  SDB_ASSERT(name.isString());

  auto row_type_str = slice.get("row_type");
  SDB_ASSERT(row_type_str.isString());
  auto row_type = std::dynamic_pointer_cast<const velox::RowType>(
    velox::parseType(std::string{row_type_str.stringView()}));
  SDB_ASSERT(row_type);

  return std::make_shared<CompositeType>(id, name.stringView(),
                                         std::move(row_type));
}

}  // namespace sdb::catalog
