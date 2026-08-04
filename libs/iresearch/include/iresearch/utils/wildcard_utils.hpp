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

#pragma once

#include <cstdint>
#include <cstring>

#include "string.hpp"

namespace irs {

enum class WildcardType {
  TermEscaped = 0,  // f\%o
  Term,             // foo
  PrefixEscaped,    // fo\%
  Prefix,           // foo%
  Wildcard,         // f_o%
};

WildcardType ComputeWildcardType(bytes_view pattern) noexcept;

enum WildcardMatch : uint8_t {
  kAnyStr = '%',   // match any number of arbitrary characters
  kAnyChr = '_',   // match a single arbitrary character
  kEscape = '\\',  // escape control symbol, e.g. "\%" issues literal "%"
};

// The lead-byte ranges `%` and `_` walk. Deliberately not strict UTF-8: the
// model leaves the continuation bytes of a lead unconstrained, so overlongs and
// surrogates are admitted while C0..C1 and F5..FF are not. A dictionary key is
// arbitrary bytes, which is why `%` does not match everything.
inline constexpr byte_type kUtf8ContinuationMin = 0x80;
inline constexpr byte_type kUtf8ContinuationMax = 0xBF;

// True when `key` is a run of code points that model admits -- the language of
// a bare `%`.
inline bool AcceptsAnyUtf8(bytes_view key) noexcept {
  const auto* p = key.data();
  const auto* end = p + key.size();
  while (p != end) {
    // An ASCII run self-validates a word at a time.
    while (static_cast<size_t>(end - p) >= sizeof(uint64_t)) {
      uint64_t word;
      std::memcpy(&word, p, sizeof(word));
      if ((word & 0x8080808080808080ULL) != 0) {
        break;
      }
      p += sizeof(word);
    }
    if (p == end) {
      break;
    }
    const uint32_t lead = *p++;
    if (lead <= 0x7F) {
      continue;
    }
    uint32_t extra;
    if (lead >= 0xC2 && lead <= 0xDF) {
      extra = 1;
    } else if (lead >= 0xE0 && lead <= 0xEF) {
      extra = 2;
    } else if (lead >= 0xF0 && lead <= 0xF4) {
      extra = 3;
    } else {
      return false;
    }
    if (static_cast<size_t>(end - p) < extra) {
      return false;
    }
    for (uint32_t i = 0; i != extra; ++i) {
      const uint32_t continuation = *p++;
      if (continuation < kUtf8ContinuationMin ||
          continuation > kUtf8ContinuationMax) {
        return false;
      }
    }
  }
  return true;
}

}  // namespace irs
