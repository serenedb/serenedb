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
#include <cstdint>
#include <cstring>
#include <duckdb/common/radix.hpp>
#include <type_traits>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::numeric_utils {

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

template<>
struct NumericEncodeOf<float> {
  using Key = uint32_t;
  static Key ToKey(float value) noexcept {
    return duckdb::Radix::EncodeFloat(value);
  }
  static float FromKey(Key key) noexcept {
    return duckdb::Radix::DecodeFloat(key);
  }
};

template<>
struct NumericEncodeOf<double> {
  using Key = uint64_t;
  static Key ToKey(double value) noexcept {
    return duckdb::Radix::EncodeDouble(value);
  }
  static double FromKey(Key key) noexcept {
    return duckdb::Radix::DecodeDouble(key);
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
