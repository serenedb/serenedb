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

#include <absl/strings/ascii.h>

#include <cstddef>
#include <string_view>

#include "basics/shared.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/utf8_character_utils.hpp"
#include "iresearch/utils/utf8_utils.hpp"

namespace irs::analysis::casing {

template<bool ToLower>
IRS_FORCE_INLINE inline void CaseConvertAscii(char* dst, const char* src,
                                              size_t n) noexcept {
  if constexpr (ToLower) {
    absl::ascii_internal::AsciiStrToLower(dst, src, n);
  } else {
    absl::ascii_internal::AsciiStrToUpper(dst, src, n);
  }
}

constexpr size_t CaseConvertUtf8Bound(size_t size) noexcept {
  return size + size / 2 + utf8_utils::kMaxCharSize;
}

// 1:1 simple case mapping of a whole UTF-8 value into `dst` (capacity at
// least CaseConvertUtf8Bound(in.size())); invalid bytes are copied verbatim.
template<bool ToLower>
size_t CaseConvertUtf8(std::string_view in, byte_type* dst) {
  static_assert(utf8_utils::kSimpleCaseMaxUtf8Growth <= 1);
  auto* out = dst;
  const auto* it = reinterpret_cast<const byte_type*>(in.data());
  const auto* end = it + in.size();
  while (it != end) {
    const auto* cp_start = it;
    uint32_t cp = utf8_utils::ToChar32(it, end);
    if (cp == utf8_utils::kInvalidChar32) [[unlikely]] {
      *out++ = *cp_start;
      continue;
    }
    if (cp < 0x80) {
      const auto c = static_cast<unsigned char>(cp);
      if constexpr (ToLower) {
        cp = absl::ascii_tolower(c);
      } else {
        cp = absl::ascii_toupper(c);
      }
    } else if constexpr (ToLower) {
      cp = utf8_utils::CharToLowerSimple(cp);
    } else {
      cp = utf8_utils::CharToUpperSimple(cp);
    }
    out += utf8_utils::FromChar32(cp, out);
  }
  return out - dst;
}

}  // namespace irs::analysis::casing
