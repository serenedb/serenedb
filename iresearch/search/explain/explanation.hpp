////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::explain {

using Value = std::variant<score_t, uint64_t>;

struct Subject {
  field_id field = field_limits::invalid();
  std::vector<bstring> terms;
  bool phrase = false;
};

struct Node {
  std::string_view key;
  Value value = score_t{0};
  std::string description;
  Subject subject;
  std::vector<Node> details;
};

inline bool HasSubject(const Node& node) noexcept {
  return field_limits::valid(node.subject.field);
}

inline Node& AddDetail(Node& parent, std::string_view key, Value value,
                       std::string description) {
  return parent.details.emplace_back(Node{
    .key = key,
    .value = value,
    .description = std::move(description),
  });
}

}  // namespace irs::explain
