////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cstdint>
#include <limits>
#include <string_view>

#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kPage = 4096;
inline constexpr uint32_t kRestartInterval = 16;
inline constexpr uint32_t kFixedHeadInterval = 8;
inline constexpr uint32_t kHeadBytes = sizeof(uint32_t);
constexpr uint64_t FixedHeads(uint64_t count) noexcept {
  return (count + kFixedHeadInterval - 1) / kFixedHeadInterval;
}
constexpr bool PacksKeys(uint64_t stride) noexcept {
  return stride != 0 && stride <= sizeof(uint64_t);
}
constexpr uint64_t PackedBytes(uint64_t count, uint32_t bits) noexcept {
  return (count * bits + 7) / 8;
}

inline constexpr uint32_t kEfSample = 64;
inline constexpr uint32_t kEfSampleBytes = sizeof(uint16_t);
inline constexpr uint32_t kEfWordBits = 64;

struct EfShape {
  uint32_t low_bits;
  uint64_t highs;
};

constexpr uint64_t EfSamples(uint64_t count) noexcept {
  return (count + kEfSample - 1) / kEfSample;
}
constexpr uint64_t EfUpperBytes(uint64_t count, uint64_t highs) noexcept {
  return sizeof(uint64_t) * ((highs + count + kEfWordBits - 1) / kEfWordBits);
}
constexpr uint64_t EfBytes(uint64_t count, EfShape shape) noexcept {
  return kEfSampleBytes * EfSamples(count) +
         PackedBytes(count, shape.low_bits) + EfUpperBytes(count, shape.highs);
}
constexpr EfShape EfShapeOf(uint64_t count, uint64_t universe) noexcept {
  const uint64_t per = universe / count;
  const auto low_bits =
    per == 0 ? uint32_t{0} : static_cast<uint32_t>(std::bit_width(per) - 1);
  const EfShape shape{.low_bits = low_bits, .highs = universe >> low_bits};
  if (low_bits == 0) {
    return shape;
  }
  const EfShape fewer{.low_bits = low_bits - 1,
                      .highs = universe >> (low_bits - 1)};
  return EfBytes(count, fewer) < EfBytes(count, shape) ? fewer : shape;
}

inline constexpr uint32_t kMaxStride = 32;
inline constexpr uint32_t kSlack = block_codec::kInSlack;
inline constexpr uint32_t kVarRestart = sizeof(uint16_t);
inline constexpr uint32_t kNoBlock = std::numeric_limits<uint32_t>::max();

enum class BlockKind : uint8_t {
  Var = 0,
  FixedStride = 1,
  VarFsst = 2,
};

constexpr uint32_t ColumnWidth(uint32_t max) noexcept {
  return max <= 0xFF ? 1 : max <= 0xFFFF ? 2 : 4;
}

inline constexpr uint32_t kHeaderBits = 8;
inline constexpr uint32_t kLenSplits = kHeaderBits + 1;
inline constexpr byte_type kColumnHeaders = kLenSplits;
inline constexpr byte_type kEscapedHeader = 0xFF;
inline constexpr uint32_t kEscapedEntry = sizeof(uint16_t);

constexpr bool Packs(uint32_t lcp, uint32_t len, uint32_t len_bits) noexcept {
  return (lcp >> (kHeaderBits - len_bits)) == 0 && (len >> len_bits) == 0 &&
         ((lcp << len_bits) | len) != kEscapedHeader;
}

constexpr byte_type Pack(uint32_t lcp, uint32_t len,
                         uint32_t len_bits) noexcept {
  return static_cast<byte_type>((lcp << len_bits) | len);
}

inline constexpr size_t kMaxRecordBytes = 128;

struct RecordBytes {
  std::array<byte_type, kMaxRecordBytes> bytes;
};

class RecordOutput final : private RecordBytes, public BufferedOutput {
 public:
  RecordOutput() noexcept
    : RecordBytes{},
      BufferedOutput{bytes.data(), bytes.data() + bytes.size()} {}

  bytes_view View() const noexcept { return {_buf, Length()}; }
  void Clear() noexcept { _pos = _buf; }

 private:
  void WriteDirect(const byte_type* b, size_t len) final;
};

inline constexpr size_t kMaxRangeLevels = 8;

inline constexpr size_t kBlockMaskWords = 2;
inline constexpr size_t kBlockMaskBytes = kBlockMaskWords * sizeof(uint64_t);
using BlockMask = std::array<uint64_t, kBlockMaskWords>;

inline constexpr uint32_t kNoLabel = 256;

[[noreturn]] inline void Corrupted(std::string_view what) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("term dictionary: bad ", what));
}

[[noreturn]] inline void Corrupted(std::string_view what, field_id id) {
  THROW_SQL_ERROR(ERR_CODE(ERRCODE_INDEX_CORRUPTED),
                  ERR_MSG("term dictionary: bad ", what, " of field ", id));
}

inline constexpr uint32_t kMaxOrdinalBytes = sizeof(uint32_t);

constexpr uint32_t OrdinalBytes(uint64_t keyed_dicts) noexcept {
  const uint64_t max = keyed_dicts == 0 ? 0 : keyed_dicts - 1;
  return std::max<uint32_t>(
    1, static_cast<uint32_t>((std::bit_width(max) + 7) / 8));
}

constexpr uint64_t OrdinalLimit(uint32_t ordinal_bytes) noexcept {
  return uint64_t{1} << (8 * ordinal_bytes);
}

inline void AppendOrdinal(bstring& out, uint32_t ordinal,
                          uint32_t ordinal_bytes) {
  SDB_ASSERT(ordinal_bytes != 0 && ordinal_bytes <= kMaxOrdinalBytes);
  SDB_ASSERT(ordinal < OrdinalLimit(ordinal_bytes));
  byte_type bytes[kMaxOrdinalBytes];
  absl::big_endian::Store32(bytes, ordinal);
  out.append(bytes + kMaxOrdinalBytes - ordinal_bytes, ordinal_bytes);
}

}  // namespace irs::term_dict
