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
inline constexpr uint32_t kRecordInterval = 8;
inline constexpr uint32_t kHeadBytes = sizeof(uint32_t);
inline constexpr uint32_t kRecordOffset = sizeof(uint16_t);
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

inline constexpr size_t kMaxRecordBytes = 128;

class RecordOutput final : public DataOutput {
 public:
  RecordOutput() noexcept {
    _begin = _pos = _bytes.data();
    _end = _bytes.data() + _bytes.size();
  }

  bytes_view View() const noexcept {
    return {_begin, static_cast<size_t>(_pos - _begin)};
  }
  void Clear() noexcept { _pos = _begin; }

 private:
  void Overflow(const byte_type* data, size_t size) final;

  std::array<byte_type, kMaxRecordBytes> _bytes;
};

constexpr uint64_t RecordGroups(uint64_t count) noexcept {
  return (count + kRecordInterval - 1) / kRecordInterval;
}

inline constexpr size_t kMaxRangeLevels = 8;

inline constexpr size_t kTermPrefix = sizeof(uint32_t);
inline constexpr uint32_t kNoLabel = 256;

constexpr uint32_t TermPrefix(bytes_view term) noexcept {
  uint32_t value = 0;
  for (size_t i = 0; i != kTermPrefix; ++i) {
    value = value << 8 | (i < term.size() ? term[i] : 0);
  }
  return value;
}

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
