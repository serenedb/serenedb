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

#include "iresearch/utils/numeric_utils.hpp"

namespace tests {

void WriteZvfloat(irs::DataOutput& out, float_t v) {
  const int32_t iv = irs::numeric_utils::Ftoi32(v);

  if (iv >= -1 && iv <= 125) {
    // small signed values between [-1 and 125]
    out.WriteByte(static_cast<irs::byte_type>(0x80 | (1 + iv)));
  } else if (!std::signbit(v)) {
    // positive value
    out.WriteU32(iv);
  } else {
    // negative value
    out.WriteByte(0xFF);
    out.WriteU32(iv);
  }
};

float_t ReadZvfloat(irs::DataInput& in) {
  const uint32_t b = in.ReadByte();

  if (0xFF == b) {
    // negative value
    return irs::numeric_utils::I32tof(in.ReadI32());
  } else if (0 != (b & 0x80)) {
    // small signed value
    return float_t((b & 0x7F) - 1);
  }

  // positive float (ensure read order)
  const auto part = uint32_t(uint16_t(in.ReadI16())) << 8;

  return irs::numeric_utils::I32tof((b << 24) | part | uint32_t(in.ReadByte()));
}

void WriteZvdouble(irs::DataOutput& out, double_t v) {
  const int64_t lv = irs::numeric_utils::Dtoi64(v);

  if (lv > -1 && lv <= 124) {
    // small signed values between [-1 and 124]
    out.WriteByte(static_cast<irs::byte_type>(0x80 | (1 + lv)));
  } else {
    const float_t fv = static_cast<float_t>(v);

    if (static_cast<double_t>(fv) == v) {
      out.WriteByte(0xFE);
      out.WriteU32(irs::numeric_utils::Ftoi32(fv));
    } else if (!std::signbit(v)) {
      // positive value
      out.WriteU64(lv);
    } else {
      // negative value
      out.WriteByte(0xFF);
      out.WriteU64(lv);
    }
  }
}

double_t ReadZvdouble(irs::DataInput& in) {
  const uint64_t b = in.ReadByte();

  if (0xFF == b) {
    // negative value
    return irs::numeric_utils::I64tod(in.ReadI64());
  } else if (0xFE == b) {
    // float value
    return static_cast<double_t>(irs::numeric_utils::I32tof(in.ReadI32()));
  } else if (0 != (b & 0x80)) {
    // small signed value
    return double_t((b & 0x7F) - 1);
  }

  // positive double (ensure read order)
  const auto part1 = uint64_t(uint32_t(in.ReadI32())) << 24;
  const auto part2 = uint64_t(uint16_t(in.ReadI16())) << 8;

  return irs::numeric_utils::I64tod((b << 56) | part1 | part2 |
                                    uint64_t(in.ReadByte()));
}

}  // namespace tests
