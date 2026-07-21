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

#include <cstring>
#include <limits>

#include "basics/bit_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::numeric_utils {

template<typename T>
struct encode_traits;

template<>
struct encode_traits<uint64_t> {
  typedef uint64_t type;
  static constexpr byte_type TYPE_MAGIC = 0x60;
};

template<>
struct encode_traits<uint32_t> {
  typedef uint32_t type;
  static constexpr byte_type TYPE_MAGIC = 0;
};

#ifndef FLOAT_T_IS_DOUBLE_T
template<>
struct encode_traits<float_t> : encode_traits<uint32_t> {
  static constexpr byte_type TYPE_MAGIC = 0x20;
};
#endif

template<>
struct encode_traits<double_t> : encode_traits<uint64_t> {
  static constexpr byte_type TYPE_MAGIC = 0xA0;
};

// returns number of bytes required to store
// value of type T with the specified offset
template<typename T>
constexpr size_t EncodedSize(size_t shift) noexcept {
  const size_t bits = BitsRequired<T>();
  return bits > shift ? 1 + (bits - 1 - shift) / 8 : 0;
}

template<typename T, typename EncodeTraits = encode_traits<T>>
size_t Encode(typename EncodeTraits::type value, byte_type* out, size_t shift) {
  typedef typename EncodeTraits::type Type;

  value ^= Type(1) << (BitsRequired<Type>() - 1);
  value &= std::numeric_limits<Type>::max() ^ ((Type(1) << shift) - 1);
  value = absl::big_endian::FromHost(value);

  const size_t size = EncodedSize<Type>(shift);
  *out = static_cast<byte_type>(shift) + EncodeTraits::TYPE_MAGIC;
  std::memcpy(out + 1, reinterpret_cast<const void*>(&value), size);
  return size + 1;
}

bytes_view Mini64();
bytes_view Maxi64();

uint64_t Decode64(const byte_type* out);
size_t Encode64(uint64_t value, byte_type* out, size_t shift = 0);
bytes_view Minu64();
bytes_view Maxu64();

bytes_view Mini32();
bytes_view Maxi32();

uint32_t Decode32(const byte_type* out);
size_t Encode32(uint32_t value, byte_type* out, size_t shift = 0);
bytes_view Minu32();
bytes_view Maxu32();

size_t Encodef32(uint32_t value, byte_type* out, size_t shift = 0);
uint32_t Decodef32(const byte_type* out);
int32_t Ftoi32(float_t value);
float_t I32tof(int32_t value);
bytes_view Minf32();
bytes_view Maxf32();
bytes_view Finf32();
bytes_view Nfinf32();

size_t Encoded64(uint64_t value, byte_type* out, size_t shift = 0);
uint64_t Decoded64(const byte_type* out);
int64_t Dtoi64(double_t value);
double_t I64tod(int64_t value);
bytes_view Mind64();
bytes_view Maxd64();
bytes_view Dinf64();
bytes_view Ndinf64();

template<typename T>
struct numeric_traits;

template<>
struct numeric_traits<int32_t> {
  typedef int32_t integral_t;
  static bytes_view min() { return Mini32(); }
  static bytes_view max() { return Maxi32(); }
  inline static integral_t integral(integral_t value) { return value; }
  constexpr static size_t size() { return sizeof(integral_t) + 1; }
  static size_t encode(integral_t value, byte_type* out, size_t offset = 0) {
    return Encode32(value, out, offset);
  }
  static integral_t decode(const byte_type* in) { return Decode32(in); }
};

template<>
struct numeric_traits<uint32_t> {
  typedef uint32_t integral_t;
  static integral_t decode(const byte_type* in) { return Decode32(in); }
  static size_t encode(integral_t value, byte_type* out, size_t offset = 0) {
    return Encode32(value, out, offset);
  }
  inline static integral_t integral(integral_t value) { return value; }
  static bytes_view min() { return Minu32(); }
  static bytes_view max() { return Maxu32(); }
  static bytes_view raw_ref(const integral_t& value) {
    return bytes_view(reinterpret_cast<const byte_type*>(&value),
                      sizeof(value));
  }
  constexpr static size_t size() { return sizeof(integral_t) + 1; }
};

template<>
struct numeric_traits<int64_t> {
  typedef int64_t integral_t;
  static bytes_view min() { return Mini64(); }
  static bytes_view max() { return Maxi64(); }
  inline static integral_t integral(integral_t value) { return value; }
  constexpr static size_t size() { return sizeof(integral_t) + 1; }
  static size_t encode(integral_t value, byte_type* out, size_t offset = 0) {
    return Encode64(value, out, offset);
  }
  static integral_t decode(const byte_type* in) { return Decode64(in); }
};

template<>
struct numeric_traits<uint64_t> {
  typedef uint64_t integral_t;
  static integral_t decode(const byte_type* in) { return Decode64(in); }
  static size_t encode(integral_t value, byte_type* out, size_t offset = 0) {
    return Encode64(value, out, offset);
  }
  inline static integral_t integral(integral_t value) { return value; }
  static bytes_view max() { return Maxu64(); }
  static bytes_view min() { return Minu64(); }
  static bytes_view raw_ref(const integral_t& value) {
    return bytes_view(reinterpret_cast<const byte_type*>(&value),
                      sizeof(value));
  }
  constexpr static size_t size() { return sizeof(integral_t) + 1; }
};

template<>
struct numeric_traits<float> {
  typedef int32_t integral_t;
  static bytes_view ninf() { return Nfinf32(); }
  static bytes_view min() { return Minf32(); }
  static bytes_view max() { return Maxf32(); }
  static bytes_view inf() { return Finf32(); }
  static float_t floating(integral_t value) { return I32tof(value); }
  static integral_t integral(float_t value) { return Ftoi32(value); }
  constexpr static size_t size() { return sizeof(integral_t) + 1; }
  static size_t encode(integral_t value, byte_type* out, size_t offset = 0) {
    return Encodef32(value, out, offset);
  }
  static float_t decode(const byte_type* in) { return floating(Decodef32(in)); }
};

template<>
struct numeric_traits<double> {
  typedef int64_t integral_t;
  static bytes_view ninf() { return Ndinf64(); }
  static bytes_view min() { return Mind64(); }
  static bytes_view max() { return Maxd64(); }
  static bytes_view inf() { return Dinf64(); }
  static double_t floating(integral_t value) { return I64tod(value); }
  static integral_t integral(double_t value) { return Dtoi64(value); }
  constexpr static size_t size() { return sizeof(integral_t) + 1; }
  static size_t encode(integral_t value, byte_type* out, size_t offset = 0) {
    return Encoded64(value, out, offset);
  }
  static double_t decode(const byte_type* in) {
    return floating(Decoded64(in));
  }
};

template<>
struct numeric_traits<long double> {};  // numeric_traits

inline constexpr uint32_t kPrecisionStepDef = 16;

template<typename T>
struct NumericEncodeOf;

template<>
struct NumericEncodeOf<int32_t> {
  using Traits = encode_traits<uint32_t>;
  static constexpr uint32_t Integral(int32_t value) noexcept {
    return static_cast<uint32_t>(value);
  }
};

template<>
struct NumericEncodeOf<int64_t> {
  using Traits = encode_traits<uint64_t>;
  static constexpr uint64_t Integral(int64_t value) noexcept {
    return static_cast<uint64_t>(value);
  }
};

#ifndef FLOAT_T_IS_DOUBLE_T
template<>
struct NumericEncodeOf<float_t> {
  using Traits = encode_traits<float_t>;
  static uint32_t Integral(float_t value) noexcept {
    return static_cast<uint32_t>(numeric_traits<float_t>::integral(value));
  }
};
#endif

template<>
struct NumericEncodeOf<double_t> {
  using Traits = encode_traits<double_t>;
  static uint64_t Integral(double_t value) noexcept {
    return static_cast<uint64_t>(numeric_traits<double_t>::integral(value));
  }
};

template<typename T>
constexpr uint32_t NumericTermCount(
  uint32_t step = kPrecisionStepDef) noexcept {
  using U = typename NumericEncodeOf<T>::Traits::type;
  return (BitsRequired<U>() + step - 1) / step;
}

inline constexpr size_t kNumericTermMaxSize = 1 + sizeof(uint64_t);

// Precision-step trie terms of `value`, highest precision (shift 0) first.
// `visit` receives a
// bytes_view into a scratch buffer valid only for the duration of the call.
template<typename T, typename Visit>
void ForEachNumericTerm(T value, Visit&& visit,
                        uint32_t step = kPrecisionStepDef) {
  using Enc = typename NumericEncodeOf<T>::Traits;
  using U = typename Enc::type;
  constexpr uint32_t kBits = BitsRequired<U>();
  const U payload = NumericEncodeOf<T>::Integral(value) ^ (U{1} << (kBits - 1));
  for (uint32_t shift = 0; shift < kBits; shift += step) {
    byte_type buf[kNumericTermMaxSize];
    buf[0] = static_cast<byte_type>(shift) + Enc::TYPE_MAGIC;
    const U be = absl::big_endian::FromHost(
      payload & (std::numeric_limits<U>::max() ^ ((U{1} << shift) - U{1})));
    std::memcpy(buf + 1, &be, sizeof(U));
    visit(bytes_view{buf, EncodedSize<U>(shift) + 1});
  }
}

// The full-precision (shift 0) term: what exact-match numeric filters seek.
template<typename T>
bytes_view EncodeNumericTerm(byte_type (&buf)[kNumericTermMaxSize], T value) {
  using Enc = typename NumericEncodeOf<T>::Traits;
  using U = typename Enc::type;
  return {buf, Encode<U, Enc>(NumericEncodeOf<T>::Integral(value), buf, 0)};
}

}  // namespace irs::numeric_utils
