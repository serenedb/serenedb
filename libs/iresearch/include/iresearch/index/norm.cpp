////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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

#include "norm.hpp"

#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

void NormHeader::Write(const NormHeader& hdr, DataOutput& out) {
  out.WriteU32(static_cast<uint32_t>(ByteSize()));
  out.WriteByte(static_cast<byte_type>(NormVersion::Min));
  out.WriteByte(static_cast<byte_type>(hdr._encoding));
  out.WriteU32(hdr._max);
  out.WriteU64(hdr._sum);
  out.WriteU64(hdr._non_zero_count);
}

std::optional<NormHeader> NormHeader::Read(bytes_view payload) noexcept {
  if (payload.size() != ByteSize()) [[unlikely]] {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Invalid 'norm' header size ", payload.size()));
    return std::nullopt;
  }

  const auto* p = payload.data();

  if (const byte_type ver = *p++;
      ver != static_cast<byte_type>(NormVersion::Min)) [[unlikely]] {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "'norm' header version mismatch, expected: ",
              static_cast<uint32_t>(NormVersion::Min),
              ", got: ", static_cast<uint32_t>(ver));
    return std::nullopt;
  }

  const byte_type num_bytes = *p++;
  if (!CheckNumBytes(num_bytes)) [[unlikely]] {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              "Malformed 'norm' header, invalid number of bytes: ",
              static_cast<uint32_t>(num_bytes));
    return std::nullopt;
  }

  NormHeader hdr{NormEncoding{num_bytes}};
  hdr._max = irs::read<decltype(_max)>(p);
  hdr._sum = irs::read<decltype(_sum)>(p);
  hdr._non_zero_count = irs::read<decltype(_non_zero_count)>(p);

  return hdr;
}

REGISTER_ATTRIBUTE(Norm);

}  // namespace irs
