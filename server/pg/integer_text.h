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
#include <limits>
#include <string_view>
#include <type_traits>

namespace sdb::pg {

enum class IntegerText {
  Ok,
  Syntax,
  Range,
};

template<typename T>
IntegerText ParseIntegerText(std::string_view text, T& out) {
  static_assert(std::is_integral_v<T>);
  const auto space = [](char c) {
    return c == ' ' || (c >= '\t' && c <= '\r');
  };
  const char* ptr = text.data();
  const char* const end = ptr + text.size();
  while (ptr != end && space(*ptr)) {
    ++ptr;
  }
  bool negative = false;
  if (ptr != end && (*ptr == '-' || *ptr == '+')) {
    negative = *ptr == '-';
    ++ptr;
  }
  uint64_t base = 10;
  if (end - ptr >= 2 && ptr[0] == '0') {
    switch (ptr[1]) {
      case 'x':
      case 'X':
        base = 16;
        break;
      case 'o':
      case 'O':
        base = 8;
        break;
      case 'b':
      case 'B':
        base = 2;
        break;
      default:
        break;
    }
    if (base != 10) {
      ptr += 2;
    }
  }
  const auto digit = [base](char c) -> uint64_t {
    const auto lower = static_cast<unsigned char>(c | 0x20);
    const uint64_t value = c >= '0' && c <= '9' ? uint64_t(c - '0')
                           : lower >= 'a' && lower <= 'f'
                             ? uint64_t(lower - 'a' + 10)
                             : base;
    return value < base ? value : base;
  };
  uint64_t bound = std::numeric_limits<T>::max();
  if (negative) {
    bound = std::is_signed_v<T> ? bound + 1 : 0;
  }
  const char* const first = ptr;
  uint64_t magnitude = 0;
  while (ptr != end) {
    if (const auto value = digit(*ptr); value != base) {
      if (__builtin_mul_overflow(magnitude, base, &magnitude) ||
          __builtin_add_overflow(magnitude, value, &magnitude) ||
          magnitude > bound) {
        return IntegerText::Range;
      }
      ++ptr;
    } else if (*ptr == '_' && (base != 10 || ptr != first)) {
      ++ptr;
      if (ptr == end || digit(*ptr) == base) {
        return IntegerText::Syntax;
      }
    } else {
      break;
    }
  }
  if (ptr == first) {
    return IntegerText::Syntax;
  }
  while (ptr != end && space(*ptr)) {
    ++ptr;
  }
  if (ptr != end) {
    return IntegerText::Syntax;
  }
  out = static_cast<T>(negative ? uint64_t{0} - magnitude : magnitude);
  return IntegerText::Ok;
}

}  // namespace sdb::pg
