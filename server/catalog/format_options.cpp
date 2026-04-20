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

#include "catalog/format_options.h"

#include <vpack/iterator.h>

namespace sdb {

// Serialised as a single vpack object whose keys are the WITH-option names
// and whose values are always strings (DuckDB's reader re-parses them).
void FormatOptions::toVPack(vpack::Builder& b) const {
  for (const auto& [k, v] : _pairs) {
    b.add(std::string_view{k}, std::string_view{v});
  }
}

std::shared_ptr<FormatOptions> FormatOptions::fromVPack(vpack::Slice slice) {
  if (!slice.isObject()) {
    return nullptr;
  }
  std::vector<std::pair<std::string, std::string>> pairs;
  for (auto entry : vpack::ObjectIterator{slice}) {
    auto value = entry.value();
    if (!value.isString()) {
      continue;
    }
    pairs.emplace_back(std::string{entry.key.stringView()},
                       std::string{value.stringView()});
  }
  return std::make_shared<FormatOptions>(std::move(pairs));
}

}  // namespace sdb
