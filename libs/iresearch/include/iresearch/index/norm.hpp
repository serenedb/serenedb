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
#include "iresearch/index/field_meta.hpp"
#include "iresearch/types.hpp"

namespace irs {

enum class NormVersion : uint8_t { Min = 0 };

enum class NormEncoding : uint8_t {
  Byte = sizeof(uint8_t),
  Short = sizeof(uint16_t),
  Int = sizeof(uint32_t)
};

class NormHeader final {
 public:
  constexpr static size_t ByteSize() noexcept {
    return sizeof(NormVersion) + sizeof(_encoding) + sizeof(_min) +
           sizeof(_max);
  }

  constexpr static bool CheckNumBytes(size_t num_bytes) noexcept {
    return num_bytes == sizeof(uint8_t) || num_bytes == sizeof(uint16_t) ||
           num_bytes == sizeof(uint32_t);
  }

  explicit NormHeader(NormEncoding encoding) noexcept : _encoding{encoding} {
    SDB_ASSERT(CheckNumBytes(static_cast<size_t>(_encoding)));
  }

  void Reset(uint32_t value) noexcept {
    _min = std::min(_min, value);
    _max = std::max(_max, value);
  }

  void Reset(const NormHeader& hdr) noexcept;

  size_t MaxNumBytes() const noexcept {
    if (_max <= std::numeric_limits<uint8_t>::max()) {
      return sizeof(uint8_t);
    } else if (_max <= std::numeric_limits<uint16_t>::max()) {
      return sizeof(uint16_t);
    } else {
      return sizeof(uint32_t);
    }
  }

  auto Encoding() const noexcept { return _encoding; }
  size_t NumBytes() const noexcept { return static_cast<size_t>(Encoding()); }

  static void Write(const NormHeader& hdr, DataOutput& out);
  static std::optional<NormHeader> Read(bytes_view payload) noexcept;

 private:
  uint32_t _min{std::numeric_limits<uint32_t>::max()};
  uint32_t _max{std::numeric_limits<uint32_t>::min()};
  NormEncoding _encoding;  // Number of bytes used for encoding
};

class Norm : public Attribute {
 public:
  using ValueType = uint32_t;

  static constexpr std::string_view type_name() noexcept { return "norm"; }

  static FeatureWriter::ptr MakeWriter(std::span<const bytes_view> payload);

  static uint32_t Read(bytes_view payload) noexcept {
    const auto* p = payload.data();
    switch (payload.size()) {
      case sizeof(uint8_t):
        return *p;
      case sizeof(uint16_t):
        return absl::little_endian::Load16(p);
      case sizeof(uint32_t):
        return absl::little_endian::Load32(p);
      [[unlikely]] default:
        // we should investigate why we failed to find a norm value for doc
        SDB_ASSERT(false);
        return 1;
    }
  }

  template<NormEncoding Encoding>
  IRS_FORCE_INLINE static uint32_t ReadUnchecked(const void* value) noexcept {
    if constexpr (Encoding == NormEncoding::Byte) {
      return absl::little_endian::Load<uint8_t>(value);
    } else if constexpr (Encoding == NormEncoding::Short) {
      return absl::little_endian::Load<uint16_t>(value);
    } else if constexpr (Encoding == NormEncoding::Int) {
      return absl::little_endian::Load<uint32_t>(value);
    } else {
      static_assert(false);
    }
  }

  template<NormEncoding Encoding>
  IRS_FORCE_INLINE static uint32_t Read(bytes_view value) noexcept {
    if (value.size() == std::to_underlying(Encoding)) [[likely]] {
      return ReadUnchecked<Encoding>(value.data());
    }

    // we should investigate why we failed to find a norm value for doc
    SDB_ASSERT(false);

    return 1;
  }

  ValueType value = 0;
  byte_type num_bytes = sizeof(ValueType);
};

static_assert(std::is_nothrow_move_constructible_v<Norm>);
static_assert(std::is_nothrow_move_assignable_v<Norm>);

template<typename T>
class NormWriter : public FeatureWriter {
 public:
  static_assert(std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                std::is_same_v<T, uint32_t>);

  explicit NormWriter() noexcept : _hdr{NormEncoding{sizeof(T)}} {}

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
    if constexpr (sizeof(T) == sizeof(uint8_t)) {
      const auto v = static_cast<uint8_t>(value & 0xFF);
      out.WriteByte(v);
    } else if constexpr (sizeof(T) == sizeof(uint16_t)) {
      const auto v =
        absl::little_endian::FromHost(static_cast<uint16_t>(value & 0xFFFF));
      out.WriteBytes(reinterpret_cast<const byte_type*>(&v), sizeof(T));
    } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
      value = absl::little_endian::FromHost(value);
      out.WriteBytes(reinterpret_cast<const byte_type*>(&value), sizeof(T));
    } else {
      static_assert(false);
    }
  }

  NormHeader _hdr;
};

}  // namespace irs
