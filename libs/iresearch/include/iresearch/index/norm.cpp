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

#include "iresearch/index/buffered_column_iterator.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {
namespace {

// TODO(mbkkt) Make it faster, with compile time known size of value.
template<NormEncoding Encoding>
class BufferedNormReader : public NormReader {
 public:
  explicit BufferedNormReader(uint64_t sum,
                              std::span<const BufferedValue> values,
                              bytes_view data) noexcept
    : _sum{sum}, _it{values, data} {}

  void Get(std::span<const doc_id_t> docs, std::span<uint32_t> values) final {
    GetImpl(docs, values);
  }

  uint32_t Get(doc_id_t doc) final {
    const auto r = _it.seek(doc);
    if (r != doc) [[unlikely]] {
      return {};
    }
    const auto payload = _it.GetPayload();
    return Norm::Read<Encoding>(payload);
  }

  score_t GetAvg() const noexcept final {
    return static_cast<double>(_sum) / _it.Size();
  }

 private:
  uint64_t _sum;
  BufferedColumnIterator _it;
};

}  // namespace

void NormHeader::Write(const NormHeader& hdr, DataOutput& out) {
  out.WriteU32(static_cast<uint32_t>(ByteSize()));
  out.WriteByte(static_cast<byte_type>(NormVersion::Min));
  out.WriteByte(static_cast<byte_type>(hdr._encoding));
  out.WriteU32(hdr._max);
  out.WriteU64(hdr._sum);
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

  return hdr;
}

FeatureWriter::ptr Norm::MakeWriter(std::span<const bytes_view> headers) {
  size_t max_bytes = sizeof(ValueType);
  if (!headers.empty()) {
    uint32_t max_value = 0;
    for (const auto header : headers) {
      const auto hdr = NormHeader::Read(header);
      if (!hdr) {
        return {};
      }
      max_value = std::max(max_value, hdr->Max());
    }
    max_bytes = NormHeader::MaxNumBytes(max_value);
  }
  SDB_ASSERT(NormHeader::CheckNumBytes(max_bytes));
  return ResolveNormEncoding(
    static_cast<NormEncoding>(max_bytes),
    []<NormEncoding Encoding> -> FeatureWriter::ptr {
      return memory::make_managed<NormWriter<Encoding>>();
    });
}

REGISTER_ATTRIBUTE(Norm);

NormReader::ptr MakeNormReader(bytes_view payload,
                               std::span<const BufferedValue> values,
                               bytes_view data) {
  auto header = NormHeader::Read(payload);
  if (!header) [[unlikely]] {
    return {};
  }
  return ResolveNormEncoding(
    header->Encoding(), [&]<NormEncoding Encoding> -> NormReader::ptr {
      return memory::make_managed<BufferedNormReader<Encoding>>(header->Sum(),
                                                                values, data);
    });
}

}  // namespace irs
