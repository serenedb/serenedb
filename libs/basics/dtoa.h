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

#include <absl/strings/numbers.h>

#include <cstring>

namespace sdb::basics {

// 22 is enough for int64_t plus trailing '\0'
inline constexpr size_t kIntStrMaxLen = 22;

// 24 is enough for double plus trailing '\0'
inline constexpr size_t kNumberStrMaxLen = 24;

template<size_t Len0, size_t Len1, size_t Len2>
struct DtoaLiterals {
  // at least trailing '\0' symbol
  static_assert(Len0 > 0 && Len1 > 0 && Len2 > 0);

  static constexpr auto kNanLen = Len0 - 1;
  static constexpr auto kNegInfLen = Len1 - 1;
  static constexpr auto kPosInfLen = Len2 - 1;

  const char nan[kNanLen + 1];
  const char neg_inf[kNegInfLen + 1];
  const char pos_inf[kPosInfLen + 1];
};

template<size_t Len0, size_t Len1, size_t Len2>
DtoaLiterals(const char (&)[Len0], const char (&)[Len1], const char (&)[Len2])
  -> DtoaLiterals<Len0, Len1, Len2>;

inline constexpr DtoaLiterals kPgDtoaLiterals{
  .nan = "NaN",
  .neg_inf = "-Infinity",
  .pos_inf = "Infinity",
};

template<DtoaLiterals Literals, typename Float>
char* dtoa_literals(Float d, char* buf) {  // NOLINT
  static_assert(std::is_floating_point_v<Float>);
  auto dtoa_impl = [&](const char* data, size_t len) {
    std::memcpy(buf, data, len);
    return buf + len;
  };
  if (std::isnan(d)) [[unlikely]] {
    return dtoa_impl(Literals.nan, Literals.kNanLen);
  }
  if (std::isinf(d)) [[unlikely]] {
    if (std::signbit(d)) {
      return dtoa_impl(Literals.neg_inf, Literals.kNegInfLen);
    } else {
      return dtoa_impl(Literals.pos_inf, Literals.kPosInfLen);
    }
  }
  return nullptr;
}

template<typename Float>
char* dtoa_fast(Float d, char* buf);  // NOLINT

}  // namespace sdb::basics
