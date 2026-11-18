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
#include <optional>
#include <string>
#include <string_view>
#include <variant>
#include <vector>

#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::explain {

using Value = std::variant<score_t, uint64_t>;

struct PhrasePosition {
  uint32_t gap_min = 0;
  uint32_t gap_max = 0;
  std::vector<bstring> terms;
};

struct Subject {
  field_id field = field_limits::invalid();
  std::vector<bstring> terms;
  std::vector<PhrasePosition> phrase;
  uint32_t slop = 0;
};

struct Node {
  std::string_view key;
  Value value = score_t{0};
  std::string expression;
  std::string note;
  std::string scorer;
  Subject subject;
  std::vector<Node> details;
};

inline bool HasSubject(const Node& node) noexcept {
  return field_limits::valid(node.subject.field);
}

inline bool IsGroup(const Node& node) noexcept {
  return node.key == "sum" || node.key == "max";
}

inline Node& AddDetail(Node& parent, std::string_view key, Value value) {
  return parent.details.emplace_back(Node{.key = key, .value = value});
}

class Explainer {
 public:
  virtual ~Explainer() = default;

  virtual Node Explain(doc_id_t doc, std::optional<score_t> returned) = 0;
};

}  // namespace irs::explain
