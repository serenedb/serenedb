////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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

#include <optional>
#include <type_traits>

#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/math_utils.hpp"
#include "iresearch/utils/numeric_utils.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs {

template<typename T, size_t N = sizeof(T)>
struct bytes_io;

template<typename T>
struct bytes_io<T, sizeof(uint8_t)> {
  static constexpr T kMaxVSize = 1;
};

template<typename T>
struct bytes_io<T, sizeof(uint16_t)> {
  static constexpr T kMaxVSize = 2;
};

template<typename T>
struct bytes_io<T, sizeof(uint32_t)> {
  static constexpr T kMaxVSize = 5;
  static constexpr T kMask = 0x80;

  template<typename InputIterator>
  static T vread(InputIterator& in) {
    static_assert(
      std::is_same_v<typename std::iterator_traits<InputIterator>::value_type,
                     byte_type>);
    T out = *in;
    ++in;
    if (!(out & kMask)) [[likely]] {
      return out;
    }
    return vread_tail(in, out);
  }

  template<typename InputIterator>
  static T vread_tail(InputIterator& in, T out) {
    out -= kMask;
    T b = *in;
    ++in;
    out += b << 7;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 7;
    b = *in;
    ++in;
    out += b << 14;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 14;
    b = *in;
    ++in;
    out += b << 21;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 21;
    b = *in;
    ++in;
    out += b << 28;
    return out;
  }

  /// @returns number of bytes required to store value in variable length format
  static uint32_t vsize(uint32_t value) {
    // compute 0 == value ? 1 : 1 + floor(log2(value)) / 7

    // OR 0x1 since Log2Floor32 does not accept 0
    const uint32_t log2 = math::Log2Floor32(value | 0x1);

    // division within range [1;31]
    return (73 + 9 * log2) >> 6;
  }
};

template<typename T>
struct bytes_io<T, sizeof(uint64_t)> {
  static constexpr T kMaxVSize = 10;
  static constexpr T kMask = 0x80;

  template<typename InputIterator>
  static T vread(InputIterator& in) {
    static_assert(
      std::is_same_v<typename std::iterator_traits<InputIterator>::value_type,
                     byte_type>);
    T out = *in;
    ++in;
    if (!(out & kMask)) [[likely]] {
      return out;
    }
    return vread_tail(in, out);
  }

  template<typename InputIterator>
  static T vread_tail(InputIterator& in, T out) {
    out -= kMask;
    T b = *in;
    ++in;
    out += b << 7;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 7;
    b = *in;
    ++in;
    out += b << 14;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 14;
    b = *in;
    ++in;
    out += b << 21;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 21;
    b = *in;
    ++in;
    out += b << 28;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 28;
    b = *in;
    ++in;
    out += b << 35;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 35;
    b = *in;
    ++in;
    out += b << 42;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 42;
    b = *in;
    ++in;
    out += b << 49;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 49;
    b = *in;
    ++in;
    out += b << 56;
    if (!(b & kMask)) {
      return out;
    }

    out -= kMask << 56;
    b = *in;
    ++in;
    out += b << 63;
    return out;
  }

  /// @returns number of bytes required to store value in variable length format
  static uint64_t vsize(uint64_t value) {
    // compute 0 == value ? 1 : 1 + floor(log2(value)) / 7

    // OR 0x1 since Log2Floor64 does not accept 0
    const uint64_t log2 = math::Log2Floor64(value | 0x1);

    // division within range [1;63]
    return (73 + 9 * log2) >> 6;
  }
};

/// @brief read a variable-size encoded value of type T from 'in'
///        will increment 'in' to position after the end of the read value
///        variable-size encoding allows using less bytes for small values
template<typename T, typename Iterator>
inline T vread(Iterator& in) {
  return bytes_io<T, sizeof(T)>::vread(in);
}

template<typename T>
IRS_FORCE_INLINE std::optional<T> ReadVarint(const byte_type*& in,
                                             const byte_type* end) noexcept {
  static_assert(std::is_unsigned_v<T>);
  constexpr uint32_t kBits = 8 * sizeof(T);
  T value = 0;
  for (uint32_t shift = 0; in != end && shift < kBits; shift += 7) {
    const byte_type b = *in++;
    const T bits = b & 0x7Fu;
    if (kBits - shift < 7 && bits >> (kBits - shift) != 0) {
      return std::nullopt;
    }
    value |= bits << shift;
    if (b < 0x80) {
      return value;
    }
  }
  return std::nullopt;
}

template<typename N, std::invocable<byte_type> Assign>
IRS_FORCE_INLINE void WriteVarint(N n, Assign&& assign) {
  static_assert(std::is_unsigned_v<N>);
  static_assert(sizeof(N) >= 4,
                "Otherwise probably better to use fixed size format");
  static constexpr N kMax = 0x80;
  while (n >= kMax) [[unlikely]] {
    assign(static_cast<byte_type>(n | kMax));
    n >>= 7;
  }
  assign(static_cast<byte_type>(n));
}

template<typename N, std::output_iterator<byte_type> Out>
IRS_FORCE_INLINE void WriteVarint(N n, Out& out) {
  WriteVarint(n, [&](byte_type b) { *out++ = b; });
}

template<typename N, std::output_iterator<byte_type> Out>
IRS_FORCE_INLINE void WriteLE(N n, Out& out) {
  static_assert(std::is_unsigned_v<N>);
  for (size_t i = 0; i < sizeof(N); ++i) {
    *out++ = static_cast<byte_type>(n >> (i * 8));
  }
}

template<typename N, std::output_iterator<byte_type> Out>
IRS_FORCE_INLINE void WriteBE(N n, Out& out) {
  static_assert(std::is_unsigned_v<N>);
  for (size_t i = 0; i < sizeof(N); ++i) {
    *out++ = static_cast<byte_type>(n >> ((sizeof(N) - 1 - i) * 8));
  }
}

}  // namespace irs
