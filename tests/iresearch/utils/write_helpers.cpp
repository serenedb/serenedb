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

#include "utils/write_helpers.hpp"

#include <bit>

namespace tests {
namespace {

int32_t Ftoi32(float v) {
  const auto bits = std::bit_cast<int32_t>(v);
  return bits ^ ((bits >> 31) & INT32_C(0x7FFFFFFF));
}

int64_t Dtoi64(double v) {
  const auto bits = std::bit_cast<int64_t>(v);
  return bits ^ ((bits >> 63) & INT64_C(0x7FFFFFFFFFFFFFFF));
}

}  // namespace

void WriteZvfloat(duckdb::WriteStream& out, float v) {
  const int32_t iv = Ftoi32(v);

  if (iv >= -1 && iv <= 125) {
    // small signed values between [-1 and 125]
    out.Write<irs::byte_type>(static_cast<irs::byte_type>(0x80 | (1 + iv)));
  } else if (!std::signbit(v)) {
    // positive value
    out.Write<uint32_t>(std::byteswap(iv));
  } else {
    // negative value
    out.Write<irs::byte_type>(0xFF);
    out.Write<uint32_t>(iv);
  }
};

void WriteZvdouble(duckdb::WriteStream& out, double v) {
  const int64_t lv = Dtoi64(v);

  if (lv > -1 && lv <= 124) {
    // small signed values between [-1 and 124]
    out.Write<irs::byte_type>(static_cast<irs::byte_type>(0x80 | (1 + lv)));
  } else {
    const float fv = static_cast<float>(v);

    if (static_cast<double>(fv) == v) {
      out.Write<irs::byte_type>(0xFE);
      out.Write<uint32_t>(Ftoi32(fv));
    } else if (!std::signbit(v)) {
      // positive value
      out.Write<uint64_t>(std::byteswap(lv));
    } else {
      // negative value
      out.Write<irs::byte_type>(0xFF);
      out.Write<uint64_t>(lv);
    }
  }
}

}  // namespace tests
