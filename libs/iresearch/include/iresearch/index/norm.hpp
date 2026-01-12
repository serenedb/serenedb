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

#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/store_utils.hpp"
#include "iresearch/utils/lz4compression.hpp"

namespace irs {

struct NormReaderContextBase {
  bool Reset(const ColumnProvider& segment, field_id column,
             const DocAttr& doc);
  bool Valid() const noexcept { return doc != nullptr; }

  bytes_view header;
  DocIterator::ptr it;
  const irs::PayAttr* payload{};
  const DocAttr* doc{};
};

static_assert(std::is_nothrow_move_constructible_v<NormReaderContextBase>);
static_assert(std::is_nothrow_move_assignable_v<NormReaderContextBase>);

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

  size_t NumBytes() const noexcept { return static_cast<size_t>(_encoding); }

  static void Write(const NormHeader& hdr, DataOutput& out);
  static std::optional<NormHeader> Read(bytes_view payload) noexcept;

 private:
  uint32_t _min{std::numeric_limits<uint32_t>::max()};
  uint32_t _max{std::numeric_limits<uint32_t>::min()};
  NormEncoding _encoding;  // Number of bytes used for encoding
};

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
    uint32_t value;

    switch (payload.size()) {
      case sizeof(uint8_t): {
        value = payload.front();
      } break;
      case sizeof(uint16_t): {
        auto* p = payload.data();
        value = irs::read<uint16_t>(p);
      } break;
      case sizeof(uint32_t): {
        auto* p = payload.data();
        value = irs::read<uint32_t>(p);
      } break;
      default:
        return;
    }

    _hdr.Reset(value);
    WriteValue(out, value);
  }

  void finish(DataOutput& out) final { NormHeader::Write(_hdr, out); }

 private:
  static void WriteValue(DataOutput& out, uint32_t value) {
    if constexpr (sizeof(T) == sizeof(uint8_t)) {
      out.WriteByte(static_cast<uint8_t>(value & 0xFF));
    }

    if constexpr (sizeof(T) == sizeof(uint16_t)) {
      out.WriteU16(static_cast<uint16_t>(value & 0xFFFF));
    }

    if constexpr (sizeof(T) == sizeof(uint32_t)) {
      out.WriteU32(value);
    }
  }

  NormHeader _hdr;
};

struct NormReaderContext : NormReaderContextBase {
  bool Reset(const ColumnProvider& segment, field_id column,
             const DocAttr& doc);
  bool Valid() const noexcept {
    return NormReaderContextBase::Valid() && num_bytes;
  }

  uint32_t num_bytes{};
  uint32_t max_num_bytes{};
};

class Norm : public Attribute {
 public:
  using ValueType = uint32_t;
  using Context = NormReaderContext;

  static constexpr std::string_view type_name() noexcept { return "norm"; }

  static FeatureWriter::ptr MakeWriter(std::span<const bytes_view> payload);

  template<typename T>
  static auto MakeReader(Context&& ctx) {
    static_assert(std::is_same_v<T, uint8_t> || std::is_same_v<T, uint16_t> ||
                  std::is_same_v<T, uint32_t>);
    SDB_ASSERT(ctx.num_bytes == sizeof(T));
    SDB_ASSERT(ctx.it);
    SDB_ASSERT(ctx.payload);
    SDB_ASSERT(ctx.doc);

    return [ctx = std::move(ctx)]() noexcept -> ValueType {
      if (const doc_id_t doc = ctx.doc->value; doc == ctx.it->seek(doc))
        [[likely]] {
        SDB_ASSERT(sizeof(T) == ctx.payload->value.size());
        const auto* value = ctx.payload->value.data();

        if constexpr (std::is_same_v<T, uint8_t>) {
          return *value;
        } else {
          return irs::read<T>(value);
        }
      }

      // we should investigate why we failed to find a norm value for doc
      SDB_ASSERT(false);

      return 1;
    };
  }

  template<typename Func>
  static auto MakeReader(Context&& ctx, Func&& func) {
    SDB_ASSERT(NormHeader::CheckNumBytes(ctx.num_bytes));

    switch (ctx.num_bytes) {
      case sizeof(uint8_t):
        return func(MakeReader<uint8_t>(std::move(ctx)));
      case sizeof(uint16_t):
        return func(MakeReader<uint16_t>(std::move(ctx)));
      default:
        return func(MakeReader<uint32_t>(std::move(ctx)));
    }
  }

  ValueType value{};
};

static_assert(std::is_nothrow_move_constructible_v<Norm>);
static_assert(std::is_nothrow_move_assignable_v<Norm>);

}  // namespace irs
