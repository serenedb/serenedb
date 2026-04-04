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

#include "basics/errors.h"

namespace sdb::catalog {

EnumType::EnumType(std::string_view name, std::vector<std::string> labels)
  : SchemaObject{{}, {}, {}, {}, name, ObjectType::EnumType} {
  _entries.reserve(labels.size());
  for (uint64_t i = 0; i < labels.size(); ++i) {
    _entries.emplace_back(i + 1, std::move(labels[i]));
  }
}

EnumType::EnumType(ObjectId id, std::string_view name,
                   std::vector<EnumLabel> entries)
  : SchemaObject{{}, {}, {}, id, name, ObjectType::EnumType},
    _entries{std::move(entries)} {}

void EnumType::WriteInternal(vpack::Builder& b) const {
  b.add("name", GetName());
  b.add("labels", vpack::Value{vpack::ValueType::Array});
  for (const auto& entry : _entries) {
    b.openObject();
    b.add("sortorder", entry.sortorder);
    b.add("label", entry.label);
    b.close();
  }
  b.close();
}

Result EnumType::Instantiate(std::shared_ptr<EnumType>& result, ObjectId id,
                             vpack::Slice slice) {
  auto name = slice.get("name");
  if (!name.isString()) {
    return {ERROR_BAD_PARAMETER, "invalid enum type definition: missing name"};
  }

  std::vector<EnumLabel> entries;
  auto labels_slice = slice.get("labels");
  if (labels_slice.isArray()) {
    for (auto it : vpack::ArrayIterator(labels_slice)) {
      entries.emplace_back(it.get("sortorder").getUInt(),
                           std::string{it.get("label").stringView()});
    }
  }

  result =
    std::make_shared<EnumType>(id, name.stringView(), std::move(entries));
  return {};
}

}  // namespace sdb::catalog
