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

#include <cstddef>
#include <cstdint>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/typedefs.hpp>

#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

enum class NumericTransform : uint8_t {
  Raw = 0,
  For = 1,
  Delta = 2,
  Rle = 3,
  Dict = 4,
};

enum class NumericLeaf : uint8_t {
  None = 0,
  Lz4 = 1,
  Zstd = 2,
};

inline constexpr uint8_t kNumericVersion = 1;
inline constexpr size_t kNumericHeaderSize = 64;
inline constexpr size_t kNumericFrameMetaSize = 40;
inline constexpr uint8_t kNumericFrameLog2 = 14;
inline constexpr uint32_t kNumericDictMax = 65536;
inline constexpr uint8_t kNumericShuffled = 1;

constexpr bool NumericWidth(uint64_t w) noexcept {
  return w == 1 || w == 2 || w == 4 || w == 8;
}

struct NumericFrameMeta {
  uint32_t comp_off;
  uint32_t comp_len;
  uint32_t raw_len;
  uint32_t first_row;
  uint64_t base;
  uint64_t min;
  uint64_t max;

  static NumericFrameMeta Load(duckdb::const_data_ptr_t p) {
    return {duckdb::Load<uint32_t>(p),      duckdb::Load<uint32_t>(p + 4),
            duckdb::Load<uint32_t>(p + 8),  duckdb::Load<uint32_t>(p + 12),
            duckdb::Load<uint64_t>(p + 16), duckdb::Load<uint64_t>(p + 24),
            duckdb::Load<uint64_t>(p + 32)};
  }
  void Store(duckdb::data_ptr_t p) const {
    duckdb::Store<uint32_t>(comp_off, p);
    duckdb::Store<uint32_t>(comp_len, p + 4);
    duckdb::Store<uint32_t>(raw_len, p + 8);
    duckdb::Store<uint32_t>(first_row, p + 12);
    duckdb::Store<uint64_t>(base, p + 16);
    duckdb::Store<uint64_t>(min, p + 24);
    duckdb::Store<uint64_t>(max, p + 32);
  }
};

struct NumericHeader {
  uint8_t width = 0;
  NumericTransform transform = NumericTransform::Raw;
  NumericLeaf leaf = NumericLeaf::None;
  uint8_t level = 0;
  uint8_t stored = 0;
  uint8_t run_width = 0;
  uint8_t flags = 0;
  uint8_t frame_log2 = kNumericFrameLog2;
  uint32_t row_count = 0;
  uint32_t frame_count = 0;
  uint32_t dict_count = 0;
  uint32_t off_frames = 0;
  uint32_t off_dict = 0;
  uint32_t off_data = 0;
  uint32_t data_size = 0;
  uint64_t base = 0;
  uint64_t raw_bytes = 0;

  bool Shuffled() const noexcept { return flags & kNumericShuffled; }
  uint32_t FrameBytes() const noexcept { return uint32_t{1} << frame_log2; }

  static NumericHeader Parse(duckdb::const_data_ptr_t p,
                             duckdb::idx_t segment_size) {
    using duckdb::Load;
    SDB_ENSURE(segment_size >= kNumericHeaderSize,
               "numeric codec: segment smaller than its header");
    const auto version = Load<uint8_t>(p);
    SDB_ENSURE(version == kNumericVersion,
               "numeric codec: unsupported segment version ", version);
    NumericHeader h;
    h.width = Load<uint8_t>(p + 1);
    h.transform = static_cast<NumericTransform>(Load<uint8_t>(p + 2));
    h.leaf = static_cast<NumericLeaf>(Load<uint8_t>(p + 3));
    h.level = Load<uint8_t>(p + 4);
    h.stored = Load<uint8_t>(p + 5);
    h.run_width = Load<uint8_t>(p + 6);
    h.flags = Load<uint8_t>(p + 7);
    h.row_count = Load<uint32_t>(p + 8);
    h.frame_count = Load<uint32_t>(p + 12);
    h.dict_count = Load<uint32_t>(p + 16);
    h.off_frames = Load<uint32_t>(p + 20);
    h.off_dict = Load<uint32_t>(p + 24);
    h.off_data = Load<uint32_t>(p + 28);
    h.data_size = Load<uint32_t>(p + 32);
    h.frame_log2 = Load<uint8_t>(p + 36);
    h.base = Load<uint64_t>(p + 40);
    h.raw_bytes = Load<uint64_t>(p + 48);
    SDB_ENSURE(
      NumericWidth(h.width) && NumericWidth(h.stored) && h.stored <= h.width &&
        static_cast<uint8_t>(h.transform) <=
          static_cast<uint8_t>(NumericTransform::Dict) &&
        static_cast<uint8_t>(h.leaf) <=
          static_cast<uint8_t>(NumericLeaf::Zstd) &&
        (h.transform != NumericTransform::Rle || NumericWidth(h.run_width)) &&
        (h.transform == NumericTransform::Dict) == (h.dict_count != 0) &&
        h.dict_count <= kNumericDictMax && h.off_frames >= kNumericHeaderSize &&
        h.off_frames +
            static_cast<uint64_t>(h.frame_count) * kNumericFrameMetaSize <=
          h.off_dict &&
        h.off_dict + static_cast<uint64_t>(h.dict_count) * h.width <=
          h.off_data &&
        static_cast<uint64_t>(h.off_data) + h.data_size <= segment_size &&
        h.frame_log2 >= 10 && h.frame_log2 <= 20 &&
        (h.frame_count != 0 || h.row_count == 0),
      "numeric codec: corrupted segment header");
    return h;
  }

  void Write(duckdb::data_ptr_t p) const {
    using duckdb::Store;
    Store<uint8_t>(kNumericVersion, p);
    Store<uint8_t>(width, p + 1);
    Store<uint8_t>(static_cast<uint8_t>(transform), p + 2);
    Store<uint8_t>(static_cast<uint8_t>(leaf), p + 3);
    Store<uint8_t>(level, p + 4);
    Store<uint8_t>(stored, p + 5);
    Store<uint8_t>(run_width, p + 6);
    Store<uint8_t>(flags, p + 7);
    Store<uint32_t>(row_count, p + 8);
    Store<uint32_t>(frame_count, p + 12);
    Store<uint32_t>(dict_count, p + 16);
    Store<uint32_t>(off_frames, p + 20);
    Store<uint32_t>(off_dict, p + 24);
    Store<uint32_t>(off_data, p + 28);
    Store<uint32_t>(data_size, p + 32);
    Store<uint8_t>(frame_log2, p + 36);
    Store<uint8_t>(0, p + 37);
    Store<uint16_t>(0, p + 38);
    Store<uint64_t>(base, p + 40);
    Store<uint64_t>(raw_bytes, p + 48);
    Store<uint64_t>(0, p + 56);
  }
};

}  // namespace irs::codecs
