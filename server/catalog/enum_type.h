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

#pragma once

#include <vpack/builder.h>
#include <vpack/slice.h>

#include <string>
#include <vector>

#include "catalog/object.h"

namespace sdb::catalog {

struct EnumLabel {
  float sortorder;
  std::string label;
};

class EnumType : public SchemaObject {
 public:
  EnumType(ObjectId id, std::string_view name, std::vector<EnumLabel> entries);

  const auto& GetEntries() const noexcept { return _entries; }

  void WriteInternal(vpack::Builder& b) const final;

  static std::shared_ptr<EnumType> FromVPack(ObjectId id, vpack::Slice slice);

 private:
  std::vector<EnumLabel> _entries;
};

}  // namespace sdb::catalog
