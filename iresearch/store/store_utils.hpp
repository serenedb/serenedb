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

#include "data_input.hpp"
#include "data_output.hpp"
#include "iresearch/utils/attributes.hpp"
#include "iresearch/utils/bit_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/number_utils.hpp"
#include "iresearch/utils/numeric_utils.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/std.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

template<typename StringType,
         typename TraitsType = typename StringType::traits_type>
StringType ToString(const byte_type* begin) {
  typedef typename TraitsType::char_type char_type;

  const auto size = irs::vread<uint32_t>(begin);

  return StringType(reinterpret_cast<const char_type*>(begin), size);
}

IRS_FORCE_INLINE inline void WriteZV32(DataOutput& out, int32_t v) {
  out.WriteV32(irs::ZigZagEncode32(v));
}

inline int32_t ReadZV32(IndexInput& in) {
  return irs::ZigZagDecode32(in.ReadV32());
}

IRS_FORCE_INLINE inline void WriteZV64(DataOutput& out, int64_t v) {
  out.WriteV64(irs::ZigZagEncode64(v));
}

inline int64_t ReadZV64(IndexInput& in) {
  return irs::ZigZagDecode64(in.ReadV64());
}

IRS_FORCE_INLINE inline void WriteStr(DataOutput& out, const char* s,
                                      size_t len) {
  SDB_ASSERT(len < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(len));
  out.WriteData(reinterpret_cast<const byte_type*>(s), len);
}

IRS_FORCE_INLINE inline void WriteStr(DataOutput& out, const byte_type* s,
                                      size_t len) {
  SDB_ASSERT(len < std::numeric_limits<uint32_t>::max());
  out.WriteV32(static_cast<uint32_t>(len));
  out.WriteData(s, len);
}

template<typename StringType>
IRS_FORCE_INLINE inline void WriteStr(DataOutput& out, const StringType& str) {
  WriteStr(out, str.data(), str.size());
}

template<typename StringType>
inline StringType ReadString(IndexInput& in) {
  const size_t len = in.ReadV32();

  StringType str(len, 0);
  in.ReadData(reinterpret_cast<byte_type*>(str.data()), str.size());
  return str;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' array of data pointed by 'value' of length 'size'
/// @return bytes written
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename T>
size_t WriteBytes(OutputIterator& out, const T* value, size_t size) {
  auto* data = reinterpret_cast<const byte_type*>(value);

  size = sizeof(T) * size;

  // write data out byte-by-byte
  for (auto i = size; i; --i) {
    *out = *data;
    ++out;
    ++data;
  }

  return size;
}

////////////////////////////////////////////////////////////////////////////////
/// @brief write to 'out' raw byte representation of data in 'value'
/// @return bytes written
////////////////////////////////////////////////////////////////////////////////
template<typename OutputIterator, typename T>
size_t WriteBytes(OutputIterator& out, const T& value) {
  return WriteBytes(out, &value, 1);
}

IRS_FORCE_INLINE constexpr uint64_t ShiftPack64(uint64_t val, bool b) noexcept {
  SDB_ASSERT(val <= UINT64_C(0x7FFFFFFFFFFFFFFF));
  return (val << 1) | uint64_t(b);
}

IRS_FORCE_INLINE constexpr uint32_t ShiftPack32(uint32_t val, bool b) noexcept {
  SDB_ASSERT(val <= UINT32_C(0x7FFFFFFF));
  return (val << 1) | uint32_t(b);
}

template<typename T = bool, typename U = uint64_t>
IRS_FORCE_INLINE T ShiftUnpack64(uint64_t in, U& out) noexcept {
  out = static_cast<U>(in >> 1);
  return static_cast<T>(in & 1);
}

template<typename T = bool, typename U = uint32_t>
IRS_FORCE_INLINE T ShiftUnpack32(uint32_t in, U& out) noexcept {
  out = static_cast<U>(in >> 1);
  return static_cast<T>(in & 1);
}

namespace encode::delta {

template<typename Iterator>
inline void Decode(Iterator begin, Iterator end) {
  SDB_ASSERT(std::distance(begin, end) > 0);

  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  const auto second = begin + 1;

  std::transform(second, end, begin, second, std::plus<value_type>());
}

template<typename Iterator>
inline void Encode(Iterator begin, Iterator end) {
  SDB_ASSERT(std::distance(begin, end) > 0);

  typedef typename std::iterator_traits<Iterator>::value_type value_type;
  const auto rend = irstd::MakeReverseIterator(begin);
  const auto rbegin = irstd::MakeReverseIterator(end);

  std::transform(
    rbegin + 1, rend, rbegin, rbegin,
    [](const value_type& lhs, const value_type& rhs) { return rhs - lhs; });
}

}  // namespace encode::delta
}  // namespace irs
