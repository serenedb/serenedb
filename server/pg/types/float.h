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

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/function/cast/cast_function_set.hpp>
#include <iresearch/utils/assert.hpp>
#include <limits>
#include <string_view>
#include <type_traits>

namespace sdb::pg {

inline constexpr size_t kNumberStrMaxLen = 24;

char* WriteShortestFloat(float value, char* buffer);
char* WriteShortestFloat(double value, char* buffer);

template<bool Precise, typename T>
size_t WriteFloat(T value, int8_t extra_float_digits, char* buffer) {
  static_assert(std::is_same_v<T, float> || std::is_same_v<T, double>);
  if (std::isnan(value)) [[unlikely]] {
    std::memcpy(buffer, "NaN", 3);
    return 3;
  }
  if (std::isinf(value)) [[unlikely]] {
    const std::string_view text =
      std::signbit(value) ? "-Infinity" : "Infinity";
    std::memcpy(buffer, text.data(), text.size());
    return text.size();
  }
  if constexpr (Precise) {
    return static_cast<size_t>(WriteShortestFloat(value, buffer) - buffer);
  } else {
    int digits = std::numeric_limits<T>::digits10 + extra_float_digits;
    if constexpr (std::is_same_v<float, T>) {
      digits = std::max(0, digits);
    } else {
      SDB_ASSERT(digits >= 0);
    }
    const auto result = std::to_chars(buffer, buffer + kNumberStrMaxLen, value,
                                      std::chars_format::general, digits);
    SDB_ASSERT(result);
    return static_cast<size_t>(result.ptr - buffer);
  }
}

void RegisterFloat(duckdb::CastFunctionSet& casts);

}  // namespace sdb::pg
