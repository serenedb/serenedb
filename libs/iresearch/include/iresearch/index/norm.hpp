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

#pragma once

#include <absl/base/internal/endian.h>

#include <cstdint>

#include "iresearch/formats/formats.hpp"
#include "iresearch/index/buffered_column.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/types.hpp"

namespace irs {

enum class NormVersion : uint8_t {
  Min = 0,
};

enum class NormEncoding : uint8_t {
  Byte = sizeof(uint8_t),
  Short = sizeof(uint16_t),
  Int = sizeof(uint32_t),
};

template<typename F>
IRS_FORCE_INLINE auto ResolveNormEncoding(NormEncoding encoding, F&& f) {
  switch (encoding) {
    case NormEncoding::Byte:
      return std::forward<F>(f).template operator()<NormEncoding::Byte>();
    case NormEncoding::Short:
      return std::forward<F>(f).template operator()<NormEncoding::Short>();
    case NormEncoding::Int:
      return std::forward<F>(f).template operator()<NormEncoding::Int>();
    default:
      SDB_UNREACHABLE();
  }
}

class NormHeader final {
 public:
  constexpr static size_t ByteSize() noexcept {
    return sizeof(NormVersion) + sizeof(_encoding) + sizeof(_max) +
           sizeof(_sum);
  }

  constexpr static bool CheckNumBytes(size_t num_bytes) noexcept {
    return num_bytes == std::to_underlying(NormEncoding::Byte) ||
           num_bytes == std::to_underlying(NormEncoding::Short) ||
           num_bytes == std::to_underlying(NormEncoding::Int);
  }

  explicit NormHeader(NormEncoding encoding) noexcept : _encoding{encoding} {
    SDB_ASSERT(CheckNumBytes(std::to_underlying(_encoding)));
  }

  void Reset(uint32_t value) noexcept {
    _max = std::max(_max, value);
    _sum += value;
  }

  NormEncoding Encoding() const noexcept { return _encoding; }
  uint32_t Max() const noexcept { return _max; }
  uint64_t Sum() const noexcept { return _sum; }

  static void Write(const NormHeader& hdr, DataOutput& out);
  static std::optional<NormHeader> Read(bytes_view payload) noexcept;

  static size_t MaxNumBytes(uint32_t value) noexcept {
    if (value <= std::numeric_limits<uint8_t>::max()) {
      return sizeof(uint8_t);
    } else if (value <= std::numeric_limits<uint16_t>::max()) {
      return sizeof(uint16_t);
    } else {
      return sizeof(uint32_t);
    }
  }

 private:
  NormEncoding _encoding;  // Number of bytes used for encoding
  uint32_t _max = 0;
  uint64_t _sum = 0;
};

class Norm : public Attribute {
 public:
  using ValueType = uint32_t;

  static constexpr std::string_view type_name() noexcept { return "norm"; }

  static FeatureWriter::ptr MakeWriter(std::span<const bytes_view> payload);

  template<NormEncoding Encoding>
  IRS_FORCE_INLINE static uint32_t ReadUnchecked(
    const byte_type* value) noexcept {
    if constexpr (Encoding == NormEncoding::Byte) {
      return *value;
    } else if constexpr (Encoding == NormEncoding::Short) {
      return absl::little_endian::Load16(value);
    } else if constexpr (Encoding == NormEncoding::Int) {
      return absl::little_endian::Load32(value);
    } else {
      static_assert(false);
    }
  }

  static uint32_t Read(bytes_view value) noexcept {
    SDB_ASSERT(NormHeader::CheckNumBytes(value.size()));
    return ResolveNormEncoding(static_cast<NormEncoding>(value.size()),
                               [&]<NormEncoding Encoding> {
                                 return ReadUnchecked<Encoding>(value.data());
                               });
  }

  template<NormEncoding Encoding>
  IRS_FORCE_INLINE static uint32_t Read(bytes_view value) noexcept {
    SDB_ASSERT(value.size() == std::to_underlying(Encoding));
    return ReadUnchecked<Encoding>(value.data());
  }

  ValueType value = 0;
  byte_type num_bytes = sizeof(ValueType);
};

static_assert(std::is_nothrow_move_constructible_v<Norm>);
static_assert(std::is_nothrow_move_assignable_v<Norm>);

template<NormEncoding Encoding>
class NormWriter : public FeatureWriter {
 public:
  explicit NormWriter() noexcept : _hdr{Encoding} {}

  void write(const FieldStats& stats, doc_id_t doc,
             ColumnOutput& writer) final {
    _hdr.Reset(stats.len);
    writer.Prepare(doc);
    WriteValue(writer, stats.len);
  }

  void write(DataOutput& out, bytes_view payload) final {
    const uint32_t value = Norm::Read(payload);
    _hdr.Reset(value);
    WriteValue(out, value);
  }

  void finish(DataOutput& out) final { NormHeader::Write(_hdr, out); }

 private:
  static void WriteValue(DataOutput& out, uint32_t value) {
    // TODO(mbkkt) We should use WriteU16, WriteU32
    // as soon as we will switch to little endian
    SDB_ASSERT(NormHeader::MaxNumBytes(value) <= std::to_underlying(Encoding));
    if constexpr (Encoding == NormEncoding::Byte) {
      const auto v = static_cast<uint8_t>(value);
      out.WriteByte(v);
    } else if constexpr (Encoding == NormEncoding::Short) {
      const auto v = absl::little_endian::FromHost16(value);
      out.WriteBytes(reinterpret_cast<const byte_type*>(&v), sizeof(uint16_t));
    } else if constexpr (Encoding == NormEncoding::Int) {
      const auto v = absl::little_endian::FromHost32(value);
      out.WriteBytes(reinterpret_cast<const byte_type*>(&v), sizeof(uint32_t));
    } else {
      static_assert(false);
    }
  }

  NormHeader _hdr;
};

NormReader::ptr MakeNormReader(bytes_view payload,
                               std::span<const BufferedValue> values,
                               bytes_view data);

}  // namespace irs
