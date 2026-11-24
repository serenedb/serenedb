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
#include <duckdb/common/serializer/write_stream.hpp>
#include <limits>
#include <string_view>

#include "iresearch/formats/posting/block_codec.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/bytes_utils.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::term_dict {

inline constexpr uint32_t kPage = 4096;
constexpr uint64_t PackedBytes(uint64_t count, uint32_t bits) noexcept {
  return (count * bits + 7) / 8;
}
inline uint64_t VSize(uint64_t value) noexcept {
  return bytes_io<uint64_t>::vsize(value);
}

template<typename Value>
void WritePacked(duckdb::WriteStream& out, uint64_t count, uint32_t bits,
                 Value&& value) {
  SDB_ASSERT(bits <= 64);
  if (bits == 0) {
    return;
  }
  std::array<byte_type, sizeof(uint64_t)> word;
  uint64_t pending = 0;
  uint32_t filled = 0;
  for (uint64_t i = 0; i != count; ++i) {
    const uint64_t v = value(i);
    SDB_ASSERT(static_cast<uint32_t>(std::bit_width(v)) <= bits);
    pending |= v << filled;
    filled += bits;
    if (filled >= 64) {
      absl::little_endian::Store64(word.data(), pending);
      out.WriteData(word.data(), word.size());
      filled -= 64;
      pending = filled == 0 ? 0 : v >> (bits - filled);
    }
  }
  absl::little_endian::Store64(word.data(), pending);
  out.WriteData(word.data(), (filled + 7) / 8);
}

inline constexpr uint32_t kMaxStride = 32;
inline constexpr uint32_t kSlack = block_codec::kInSlack;
inline constexpr uint32_t kNoBlock = std::numeric_limits<uint32_t>::max();

inline constexpr size_t kMaxRecordBytes = 128;

class RecordOutput final : public duckdb::WriteStream {
 public:
  RecordOutput() noexcept { Clear(); }
  RecordOutput(const RecordOutput&) = delete;
  RecordOutput& operator=(const RecordOutput&) = delete;

  bytes_view View() const noexcept { return {_bytes.data(), WindowSize()}; }
  void Clear() noexcept { SetWindow(_bytes.data(), _bytes.size()); }

 private:
  void Overflow(duckdb::const_data_ptr_t, duckdb::idx_t) final {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("term dictionary: posting record exceeds ",
                            kMaxRecordBytes, " bytes"));
  }

  std::array<byte_type, kMaxRecordBytes> _bytes;
};

inline constexpr size_t kMaxRangeLevels = 8;

constexpr uint64_t KeyedDictsOf(IndexFeatures features) noexcept {
  return IsSubsetOf(IndexFeatures::Ranges, features) ? 1 + kMaxRangeLevels : 1;
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
