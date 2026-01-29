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

#include "basics/shared.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

bool NormReaderContextBase::Reset(const ColumnProvider& reader,
                                  field_id column_id, const DocAttr& doc) {
  const auto* column = reader.column(column_id);

  if (column) {
    auto it = column->iterator(ColumnHint::Normal);
    if (it) [[likely]] {
      auto* payload = irs::get<PayAttr>(*it);
      if (payload) [[likely]] {
        this->header = column->payload();
        this->it = std::move(it);
        this->payload = payload;
        this->doc = &doc;
        return true;
      }
    }
  }

  return false;
}

bool NormReaderContext::Reset(const ColumnProvider& reader, field_id column_id,
                              const DocAttr& doc) {
  if (NormReaderContextBase::Reset(reader, column_id, doc)) {
    const auto hdr = NormHeader::Read(header);
    if (hdr.has_value()) {
      auto& value = hdr.value();
      num_bytes = static_cast<uint32_t>(value.NumBytes());
      max_num_bytes = static_cast<uint32_t>(value.MaxNumBytes());
      return true;
    }
  }

  return false;
}

void NormHeader::Reset(const NormHeader& hdr) noexcept {
  _min = std::min(hdr._min, _min);
  _max = std::max(hdr._max, _max);
  _encoding = std::max(hdr._encoding, _encoding);
}

void NormHeader::Write(const NormHeader& hdr, DataOutput& out) {
  out.WriteU32(static_cast<uint32_t>(ByteSize()));
  out.WriteByte(static_cast<byte_type>(NormVersion::Min));
  out.WriteByte(static_cast<byte_type>(hdr._encoding));
  out.WriteU32(hdr._min);
  out.WriteU32(hdr._max);
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
              absl::StrCat("'norm' header version mismatch, expected: ",
                           static_cast<uint32_t>(NormVersion::Min),
                           ", got: ", static_cast<uint32_t>(ver)));
    return std::nullopt;
  }

  const byte_type num_bytes = *p++;
  if (!CheckNumBytes(num_bytes)) [[unlikely]] {
    SDB_ERROR("xxxxx", sdb::Logger::IRESEARCH,
              absl::StrCat("Malformed 'norm' header, invalid number of bytes: ",
                           static_cast<uint32_t>(num_bytes)));
    return std::nullopt;
  }

  NormHeader hdr{NormEncoding{num_bytes}};
  hdr._min = irs::read<decltype(_min)>(p);
  hdr._max = irs::read<decltype(_max)>(p);

  return hdr;
}

FeatureWriter::ptr Norm::MakeWriter(std::span<const bytes_view> headers) {
  size_t max_bytes{sizeof(ValueType)};

  if (!headers.empty()) {
    NormHeader acc{NormEncoding::Byte};
    for (auto header : headers) {
      auto hdr = NormHeader::Read(header);
      if (!hdr.has_value()) {
        return nullptr;
      }
      acc.Reset(hdr.value());
    }
    max_bytes = acc.MaxNumBytes();
  }

  switch (max_bytes) {
    case sizeof(uint8_t):
      return memory::make_managed<NormWriter<uint8_t>>();
    case sizeof(uint16_t):
      return memory::make_managed<NormWriter<uint16_t>>();
    default:
      SDB_ASSERT(max_bytes == sizeof(uint32_t));
      return memory::make_managed<NormWriter<uint32_t>>();
  }
}

REGISTER_ATTRIBUTE(Norm);

}  // namespace irs
