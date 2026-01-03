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

#include "numeric_utils.hpp"

#include <cmath>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"

#if defined(_WIN32)
#include <Winsock2.h>
#pragma comment(lib, "Ws2_32.lib")
#elif defined(__APPLE__)
#include <arpa/inet.h>
#include <machine/endian.h>
#else
#include <arpa/inet.h>
#include <endian.h>

#define HTONLL htobe64
#define NTOHLL be64toh
#endif  // _WIN32

namespace {

enum class BufIdT {
  NInf,
  Min,
  Max,
  Inf,
};

template<typename T, BufIdT ID>
irs::bstring& StaticBuf() {
  static irs::bstring gBuf;
  return gBuf;
}

}  // namespace

namespace irs {
namespace numeric_utils {

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
size_t EncodedSize(size_t shift) {
  const size_t bits = BitsRequired<T>();  // number of bits required to store T
  return bits > shift ? 1 + (bits - 1 - shift) / 8 : 0;
}

template<typename T>
bstring& Encode(bstring& buf, T value, size_t offset = 0) {
  typedef numeric_traits<T> TraitsT;
  buf.resize(TraitsT::size());
  buf.resize(TraitsT::encode(TraitsT::integral(value), buf.data(), offset));
  return buf;
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

template<typename T, typename EncodeTraits = encode_traits<T>>
typename EncodeTraits::type Decode(const byte_type* in) {
  typedef typename EncodeTraits::type Type;
  Type value{};

  const size_t size = EncodedSize<Type>(*in - EncodeTraits::TYPE_MAGIC);
  if (size) {
    std::memcpy(reinterpret_cast<void*>(&value), in + 1, size);
    value = absl::big_endian::ToHost(value);
    value ^= Type(1) << (BitsRequired<Type>() - 1);
  }

  return value;
}

// static_assert that signed right shift works as expected
static_assert(static_cast<uint32_t>(INT32_C(-1) >> 31) == UINT32_C(0xFFFFFFFF));
static_assert(static_cast<uint64_t>(INT64_C(-1) >> 63) ==
              UINT64_C(0xFFFFFFFFFFFFFFFF));

constexpr int32_t MakeSortable32(int32_t value) noexcept {
  return value ^ ((value >> 31) & INT32_C(0x7FFFFFFF));
}

constexpr int64_t MakeSortable64(int64_t value) noexcept {
  return value ^ ((value >> 63) & INT64_C(0x7FFFFFFFFFFFFFFF));
}

size_t Encode64(uint64_t value, byte_type* out, size_t shift /* = 0 */) {
  return Encode<uint64_t>(value, out, shift);
}

uint64_t Decode64(const byte_type* in) { return Decode<uint64_t>(in); }

size_t Encode32(uint32_t value, byte_type* out, size_t shift /* = 0 */) {
  return Encode<uint32_t>(value, out, shift);
}

uint32_t Decode32(const byte_type* in) { return Decode<uint32_t>(in); }

size_t Encodef32(uint32_t value, byte_type* out, size_t shift /* = 0 */) {
  return Encode<float_t>(value, out, shift);
}

uint32_t Decodef32(const byte_type* in) { return Decode<float_t>(in); }

size_t Encoded64(uint64_t value, byte_type* out, size_t shift /* = 0 */) {
  return Encode<double_t>(value, out, shift);
}

uint64_t Decoded64(const byte_type* in) { return Decode<double_t>(in); }

bytes_view Mini32() {
  static bytes_view gData = Encode(StaticBuf<int32_t, BufIdT::Min>(),
                                   std::numeric_limits<int32_t>::min());
  return gData;
}

bytes_view Maxi32() {
  static bytes_view gData = Encode(StaticBuf<int32_t, BufIdT::Max>(),
                                   std::numeric_limits<int32_t>::max());
  return gData;
}

bytes_view Minu32() {
  static bytes_view gData = Encode(StaticBuf<uint32_t, BufIdT::Min>(),
                                   std::numeric_limits<uint32_t>::min());
  return gData;
}

bytes_view Maxu32() {
  static bytes_view gData = Encode(StaticBuf<uint32_t, BufIdT::Max>(),
                                   std::numeric_limits<uint32_t>::max());
  return gData;
}

bytes_view Mini64() {
  static bytes_view gData = Encode(StaticBuf<int64_t, BufIdT::Min>(),
                                   std::numeric_limits<int64_t>::min());
  return gData;
}

bytes_view Maxi64() {
  static bytes_view gData = Encode(StaticBuf<int64_t, BufIdT::Max>(),
                                   std::numeric_limits<int64_t>::max());
  return gData;
}

bytes_view Minu64() {
  static bytes_view gData = Encode(StaticBuf<uint64_t, BufIdT::Min>(),
                                   std::numeric_limits<uint64_t>::min());
  return gData;
}

bytes_view Maxu64() {
  static bytes_view gData = Encode(StaticBuf<uint64_t, BufIdT::Max>(),
                                   std::numeric_limits<uint64_t>::max());
  return gData;
}

int32_t Ftoi32(float_t value) {
  static_assert(std::numeric_limits<float_t>::is_iec559,
                "compiler does not support ieee754 (float)");

  union {
    float_t in;
    int32_t out;
  } conv;

  conv.in = value;
  return MakeSortable32(conv.out);
}

float_t I32tof(int32_t value) {
  static_assert(std::numeric_limits<float_t>::is_iec559,
                "compiler does not support ieee754 (float)");

  union {
    float_t out;
    int32_t in;
  } conv;

  conv.in = MakeSortable32(value);
  return conv.out;
}

int64_t Dtoi64(double_t value) {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (double)");

  union {
    double_t in;
    int64_t out;
  } conv;

  conv.in = value;
  return MakeSortable64(conv.out);
}

double_t I64tod(int64_t value) {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (double)");

  union {
    double_t out;
    int64_t in;
  } conv;

  conv.in = MakeSortable64(value);
  return conv.out;
}

bytes_view Finf32() {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (float)");
  static bytes_view gData = Encode(StaticBuf<float_t, BufIdT::Inf>(),
                                   std::numeric_limits<float_t>::infinity());
  return gData;
}

bytes_view Nfinf32() {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (float)");
  static bytes_view gData =
    Encode(StaticBuf<float_t, BufIdT::NInf>(),
           -1 * std::numeric_limits<float_t>::infinity());
  return gData;
}
bytes_view Minf32() {
  static bytes_view gData = Encode(StaticBuf<float_t, BufIdT::Min>(),
                                   (std::numeric_limits<float_t>::min)());
  return gData;
}

bytes_view Maxf32() {
  static bytes_view gData = Encode(StaticBuf<float_t, BufIdT::Max>(),
                                   (std::numeric_limits<float_t>::max)());
  return gData;
}

bytes_view Dinf64() {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (double)");
  static bytes_view gData = Encode(StaticBuf<double_t, BufIdT::Inf>(),
                                   std::numeric_limits<double_t>::infinity());
  return gData;
}

bytes_view Ndinf64() {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (double)");
  static bytes_view gData =
    Encode(StaticBuf<double_t, BufIdT::NInf>(),
           -1 * std::numeric_limits<double_t>::infinity());
  return gData;
}

bytes_view Mind64() {
  static bytes_view gData = Encode(StaticBuf<double_t, BufIdT::Min>(),
                                   (std::numeric_limits<double_t>::min)());
  return gData;
}

bytes_view Maxd64() {
  static bytes_view gData = Encode(StaticBuf<double_t, BufIdT::Max>(),
                                   (std::numeric_limits<double_t>::max)());
  return gData;
}

}  // namespace numeric_utils
}  // namespace irs
