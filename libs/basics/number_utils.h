////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2023 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstdint>
#include <limits>
#include <type_traits>

#include "basics/common.h"
#include "basics/system-compiler.h"

namespace sdb {
namespace number_utils {

template<typename From, typename To = double>
consteval To Min() {
  static_assert(std::is_integral_v<From>);
  if constexpr (std::is_integral_v<To>) {
    static_assert(std::is_signed_v<To>);
    static_assert(sizeof(From) <= sizeof(To));
    return static_cast<To>(std::numeric_limits<From>::min());
  } else if constexpr (std::is_signed_v<From>) {
    return static_cast<To>(std::numeric_limits<From>::min());
  } else {
    return {};
  }
}

template<typename From, typename To = double>
consteval double Max() {
  static_assert(std::is_integral_v<From>);
  if constexpr (std::is_integral_v<To>) {
    static_assert(sizeof(From) <= sizeof(To));
    if constexpr (sizeof(From) == sizeof(To) && std::is_signed_v<To>) {
      return std::numeric_limits<To>::max();
    } else {
      return static_cast<To>(std::numeric_limits<From>::max());
    }
  } else if constexpr (std::is_signed_v<From>) {
    return -static_cast<To>(std::numeric_limits<From>::min());
  } else if (sizeof(From) <= 4) {
    return static_cast<To>(std::numeric_limits<From>::max()) + 1.0;
  } else {
    return static_cast<To>(std::numeric_limits<From>::max());
  }
}

}  // namespace number_utils

constexpr uint32_t ZigZagEncode32(int32_t n) noexcept {
  // right shift must be arithmetic
  // left shift must be unsigned because of overflow
  return (static_cast<uint32_t>(n) << 1) ^ static_cast<uint32_t>(n >> 31);
}

constexpr int32_t ZigZagDecode32(uint32_t n) noexcept {
  // using unsigned types prevent undefined behavior
  return static_cast<int32_t>((n >> 1) ^ (~(n & 1) + 1));
}

constexpr uint64_t ZigZagEncode64(int64_t n) noexcept {
  // right shift must be arithmetic
  // left shift must be unsigned because of overflow
  return (static_cast<uint64_t>(n) << 1) ^ static_cast<uint64_t>(n >> 63);
}

constexpr int64_t ZigZagDecode64(uint64_t n) noexcept {
  // using unsigned types prevent undefined behavior
  return static_cast<int64_t>((n >> 1) ^ (~(n & 1) + 1));
}

static_assert(number_utils::Min<int8_t>() == -128.0);  // -2^7
static_assert(number_utils::Min<uint8_t>() == 0.0);    //  0
static_assert(number_utils::Max<int8_t>() == 128.0);   //  2^7
static_assert(number_utils::Max<uint8_t>() == 256.0);  //  2^8

static_assert(number_utils::Min<int16_t>() == -32768.0);  // -2^15
static_assert(number_utils::Min<uint16_t>() == 0.0);      //  0
static_assert(number_utils::Max<int16_t>() == 32768.0);   //  2^15
static_assert(number_utils::Max<uint16_t>() == 65536.0);  //  2^16

static_assert(number_utils::Min<int32_t>() == -2147483648.0);  // -2^31
static_assert(number_utils::Min<uint32_t>() == 0.0);           //  0
static_assert(number_utils::Max<int32_t>() == 2147483648.0);   //  2^31
static_assert(number_utils::Max<uint32_t>() == 4294967296.0);  //  2^32

static_assert(number_utils::Min<int64_t>() == -9223372036854775808.0);  // -2^63
static_assert(number_utils::Min<uint64_t>() == 0.0);                    //  0
static_assert(number_utils::Max<int64_t>() == 9223372036854775808.0);   //  2^63
static_assert(number_utils::Max<uint64_t>() ==
              18446744073709551616.0);  //  2^64

}  // namespace sdb
