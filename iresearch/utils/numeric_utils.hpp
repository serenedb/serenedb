////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <type_traits>

#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::numeric_utils {

int32_t Ftoi32(float_t value);
float_t I32tof(int32_t value);
int64_t Dtoi64(double_t value);
double_t I64tod(int64_t value);

template<typename T>
T CanonicalFloat(T value) noexcept {
  static_assert(std::is_floating_point_v<T>);
  if (value == 0) {
    return 0;
  }
  return std::isnan(value) ? std::numeric_limits<T>::quiet_NaN() : value;
}

template<typename Key>
inline constexpr Key kSignBit = Key{1} << (sizeof(Key) * 8 - 1);

template<typename T>
struct NumericEncodeOf;

template<typename Signed>
struct SignedKeyOf {
  using Key = std::make_unsigned_t<Signed>;
  static constexpr Key ToKey(Signed value) noexcept {
    return static_cast<Key>(value) ^ kSignBit<Key>;
  }
  static constexpr Signed FromKey(Key key) noexcept {
    return static_cast<Signed>(key ^ kSignBit<Key>);
  }
};

template<typename Unsigned>
struct UnsignedKeyOf {
  using Key = Unsigned;
  static constexpr Key ToKey(Unsigned value) noexcept { return value; }
  static constexpr Unsigned FromKey(Key key) noexcept { return key; }
};

template<>
struct NumericEncodeOf<int8_t> : SignedKeyOf<int8_t> {};

template<>
struct NumericEncodeOf<int16_t> : SignedKeyOf<int16_t> {};

template<>
struct NumericEncodeOf<int32_t> : SignedKeyOf<int32_t> {};

template<>
struct NumericEncodeOf<int64_t> : SignedKeyOf<int64_t> {};

template<>
struct NumericEncodeOf<__int128> : SignedKeyOf<__int128> {};

template<>
struct NumericEncodeOf<uint64_t> : UnsignedKeyOf<uint64_t> {};

template<>
struct NumericEncodeOf<unsigned __int128> : UnsignedKeyOf<unsigned __int128> {};

#ifndef FLOAT_T_IS_DOUBLE_T
template<>
struct NumericEncodeOf<float_t> {
  using Key = uint32_t;
  static Key ToKey(float_t value) noexcept {
    return static_cast<Key>(Ftoi32(CanonicalFloat(value))) ^ kSignBit<Key>;
  }
  static float_t FromKey(Key key) noexcept {
    return I32tof(static_cast<int32_t>(key ^ kSignBit<Key>));
  }
};
#endif

template<>
struct NumericEncodeOf<double_t> {
  using Key = uint64_t;
  static Key ToKey(double_t value) noexcept {
    return static_cast<Key>(Dtoi64(CanonicalFloat(value))) ^ kSignBit<Key>;
  }
  static double_t FromKey(Key key) noexcept {
    return I64tod(static_cast<int64_t>(key ^ kSignBit<Key>));
  }
};

template<typename T>
using NumericKey = typename NumericEncodeOf<T>::Key;

template<typename T>
inline constexpr size_t kNumericTermSize = sizeof(NumericKey<T>);

inline constexpr size_t kNumericTermMaxSize = sizeof(unsigned __int128);

template<typename T>
bytes_view EncodeNumericTerm(byte_type* buf, T value) {
  static_assert(std::endian::native == std::endian::little);
  const auto key = std::byteswap(NumericEncodeOf<T>::ToKey(value));
  std::memcpy(buf, &key, sizeof(key));
  return {buf, sizeof(key)};
}

template<typename T>
T DecodeNumericTerm(bytes_view term) {
  static_assert(std::endian::native == std::endian::little);
  NumericKey<T> key;
  SDB_ASSERT(term.size() == sizeof(key));
  std::memcpy(&key, term.data(), sizeof(key));
  return NumericEncodeOf<T>::FromKey(std::byteswap(key));
}

}  // namespace irs::numeric_utils
