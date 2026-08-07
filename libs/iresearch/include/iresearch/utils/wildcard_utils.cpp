////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "wildcard_utils.hpp"

#include "iresearch/utils/utf8_utils.hpp"

namespace irs {

WildcardType ComputeWildcardType(bytes_view pattern) noexcept {
  if (pattern.empty()) {
    return WildcardType::Term;
  }

  bool escaped = false;
  bool seen_escaped = false;
  size_t size_any_str = 0;
  size_t curr_any_str = 0;

  const auto* it = pattern.data();
  const auto* end = it + pattern.size();
  for (; it != end; it = utf8_utils::Next(it, end)) {
    auto prev_any_str = std::exchange(curr_any_str, 0);
    if (escaped) {
      escaped = false;
      continue;
    }
    switch (*it) {
      case WildcardMatch::kAnyStr:
        if (prev_any_str == size_any_str) {
          curr_any_str = ++size_any_str;
          break;
        }
        [[fallthrough]];
      case WildcardMatch::kAnyChr:
        return WildcardType::Wildcard;
      case WildcardMatch::kEscape:
        escaped = true;
        seen_escaped = true;
        break;
      default:
        break;
    }
  }
  if (size_any_str == 0) {
    return seen_escaped ? WildcardType::TermEscaped : WildcardType::Term;
  }
  if (size_any_str == curr_any_str) {
    return seen_escaped ? WildcardType::PrefixEscaped : WildcardType::Prefix;
  }
  return WildcardType::Wildcard;
}

}  // namespace irs
