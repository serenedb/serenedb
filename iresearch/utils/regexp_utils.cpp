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

#include "regexp_utils.hpp"

#include "iresearch/utils/assert.hpp"

namespace irs {

bytes_view UnescapeRegexp(bytes_view in, bstring& out) {
  out.clear();
  out.reserve(in.size());

  bool escaped = false;
  for (byte_type c : in) {
    if (escaped) {
      // Should only be called for patterns classified as
      // LiteralEscaped - only simple escapes allowed.
      SDB_ASSERT(IsSimpleEscape(c));
      out.push_back(c);
      escaped = false;
    } else if (c == AsByte(RegexpMeta::kEscape)) {
      escaped = true;
    } else {
      out.push_back(c);
    }
  }
  SDB_ASSERT(!escaped);

  return bytes_view{out.data(), out.size()};
}

RegexpType ComputeRegexpType(bytes_view pattern) noexcept {
  if (pattern.empty()) {
    return RegexpType::Literal;
  }

  bool has_escapes = false;
  bool escaped = false;
  for (size_t i = 0; i < pattern.size(); ++i) {
    if (escaped) {
      escaped = false;
      if (!IsSimpleEscape(pattern[i])) {
        return RegexpType::Complex;
      }
      has_escapes = true;
      continue;
    }
    if (pattern[i] == AsByte(RegexpMeta::kEscape)) {
      escaped = true;
      continue;
    }
    if (IsRegexpMeta(pattern[i])) {
      return RegexpType::Complex;
    }
  }

  if (escaped) {
    return RegexpType::Complex;
  }

  return has_escapes ? RegexpType::LiteralEscaped : RegexpType::Literal;
}

}  // namespace irs
