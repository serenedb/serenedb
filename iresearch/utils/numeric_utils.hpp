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

#include <absl/base/internal/endian.h>

#include <cmath>
#include <cstdint>
#include <limits>
#include <type_traits>

#include "iresearch/utils/assert.hpp"
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

template<typename Signed, typename Unsigned>
struct SignedKeyOf {
  using Key = Unsigned;
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
struct NumericEncodeOf<int32_t> : SignedKeyOf<int32_t, uint32_t> {};

template<>
struct NumericEncodeOf<int64_t> : SignedKeyOf<int64_t, uint64_t> {};

template<>
struct NumericEncodeOf<__int128> : SignedKeyOf<__int128, unsigned __int128> {};

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
  using Key = NumericKey<T>;
  const Key key = NumericEncodeOf<T>::ToKey(value);
  if constexpr (sizeof(Key) == sizeof(unsigned __int128)) {
    absl::big_endian::Store64(buf, static_cast<uint64_t>(key >> 64));
    absl::big_endian::Store64(buf + sizeof(uint64_t),
                              static_cast<uint64_t>(key));
  } else if constexpr (sizeof(Key) == sizeof(uint64_t)) {
    absl::big_endian::Store64(buf, key);
  } else {
    static_assert(sizeof(Key) == sizeof(uint32_t));
    absl::big_endian::Store32(buf, key);
  }
  return {buf, sizeof(Key)};
}

template<typename T>
T DecodeNumericTerm(bytes_view term) {
  using Key = NumericKey<T>;
  SDB_ASSERT(term.size() == sizeof(Key));
  const auto* in = term.data();
  Key key = 0;
  if constexpr (sizeof(Key) == sizeof(unsigned __int128)) {
    key = Key{absl::big_endian::Load64(in)} << 64 |
          absl::big_endian::Load64(in + sizeof(uint64_t));
  } else if constexpr (sizeof(Key) == sizeof(uint64_t)) {
    key = absl::big_endian::Load64(in);
  } else {
    static_assert(sizeof(Key) == sizeof(uint32_t));
    key = absl::big_endian::Load32(in);
  }
  return NumericEncodeOf<T>::FromKey(key);
}

}  // namespace irs::numeric_utils
