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

#include <cmath>
#include <cstdint>
#include <iresearch/store/data_input.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/utils/bytes_utils.hpp>
#include <iresearch/utils/number_utils.hpp>

namespace irs {

template<typename StringType,
         typename TraitsType = typename StringType::traits_type>
StringType ToString(const byte_type* begin) {
  const auto size = irs::vread<uint32_t>(begin);
  return StringType(
    reinterpret_cast<const typename TraitsType::char_type*>(begin), size);
}

template<typename N>
N ReadVarintBytes(auto& in) {
  N value = 0;
  for (uint32_t shift = 0;; shift += 7) {
    const byte_type b = in.ReadByte();
    value |= static_cast<N>(b & 0x7F) << shift;
    if (b < 0x80) {
      return value;
    }
  }
}

uint32_t ReadV32(auto& in) { return ReadVarintBytes<uint32_t>(in); }

uint64_t ReadV64(auto& in) { return ReadVarintBytes<uint64_t>(in); }

inline void WriteZV32(DataOutput& out, int32_t v) {
  out.WriteV32(irs::ZigZagEncode32(v));
}

int32_t ReadZV32(auto& in) { return irs::ZigZagDecode32(ReadV32(in)); }

inline void WriteZV64(DataOutput& out, int64_t v) {
  out.WriteV64(irs::ZigZagEncode64(v));
}

template<typename StringType>
StringType ReadString(auto& in) {
  StringType str(ReadV32(in), 0);
  in.ReadData(reinterpret_cast<byte_type*>(str.data()), str.size());
  return str;
}

}  // namespace irs
namespace tests {

uint16_t ReadU16(irs::IndexInput& in);

void WriteZvfloat(irs::DataOutput& out, float_t v);

float_t ReadZvfloat(irs::IndexInput& in);

void WriteZvdouble(irs::DataOutput& out, double_t v);

double_t ReadZvdouble(irs::IndexInput& in);

}  // namespace tests
