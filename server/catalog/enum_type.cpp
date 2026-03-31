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

#include "catalog/enum_type.h"

#include <vpack/iterator.h>
#include <vpack/value.h>
#include <vpack/value_type.h>

#include <algorithm>

#include "basics/assert.h"

namespace sdb::catalog {

EnumType::EnumType(ObjectId id, std::string_view name,
                   std::vector<std::string> labels)
  : SchemaObject{{}, {}, {}, id, name, ObjectType::EnumType},
    _labels{std::move(labels)} {}

void EnumType::WriteInternal(vpack::Builder& b) const {
  b.add("name", GetName());
  b.add("labels", vpack::Value{vpack::ValueType::Array});
  for (const auto& label : _labels) {
    b.add(label);
  }
  b.close();
}

std::shared_ptr<EnumType> EnumType::FromVPack(ObjectId id, vpack::Slice slice) {
  auto name = slice.get("name");
  SDB_ASSERT(name.isString());

  std::vector<std::string> labels;
  auto labels_slice = slice.get("labels");
  if (labels_slice.isArray()) {
    for (auto entry : vpack::ArrayIterator(labels_slice)) {
      labels.emplace_back(entry.stringView());
    }
  }

  return std::make_shared<EnumType>(id, name.stringView(), std::move(labels));
}

}  // namespace sdb::catalog
