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
#include <string_view>

#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

enum class NumericTransform : uint8_t {
  Raw = 0,
  For = 1,
  Delta = 2,
  Rle = 3,
  Dict = 4,
  Ffor = 5,
  RleFfor = 6,
  DictFfor = 7,
};

constexpr bool BitPacked(NumericTransform t) noexcept {
  return t == NumericTransform::Ffor || t == NumericTransform::RleFfor ||
         t == NumericTransform::DictFfor;
}

constexpr bool Runs(NumericTransform t) noexcept {
  return t == NumericTransform::Rle || t == NumericTransform::RleFfor;
}

constexpr bool Coded(NumericTransform t) noexcept {
  return t == NumericTransform::Dict || t == NumericTransform::DictFfor;
}

enum class NumericLeaf : uint8_t {
  None = 0,
  Lz4 = 1,
  Zstd = 2,
};

inline constexpr uint8_t kNumericVersion = 1;
inline constexpr size_t kNumericHeaderSize = 64;
inline constexpr size_t kNumericFrameMetaSize = 40;
inline constexpr uint8_t kNumericFrameLog2 = 14;
inline constexpr uint8_t kNumericLeafFrameLog2 = 16;
inline constexpr uint32_t kNumericDictMax = 65536;
inline constexpr uint8_t kNumericShuffled = 1;
inline constexpr uint32_t kFforFrameRows = 16384;
inline constexpr uint8_t kFforFrameLog2 = 18;
inline constexpr size_t kFforBlockMetaBytes = 16;
inline constexpr uint32_t kRleFforFrameRuns = 1024;

constexpr bool NumericWidth(uint64_t w) noexcept {
  return w == 1 || w == 2 || w == 4 || w == 8;
}

struct NumericFrameMeta {
  FrameMeta frame;
  uint64_t base = 0;
  uint64_t min = 0;
  uint64_t max = 0;

  static NumericFrameMeta Load(duckdb::const_data_ptr_t p) noexcept {
    return LoadLayout<NumericFrameMeta>(p);
  }
  void Store(duckdb::data_ptr_t p) const noexcept { StoreLayout(*this, p); }
};

static_assert(sizeof(NumericFrameMeta) == kNumericFrameMetaSize);
static_assert(offsetof(NumericFrameMeta, frame) == 0);
static_assert(offsetof(NumericFrameMeta, base) == 16);
static_assert(offsetof(NumericFrameMeta, min) == 24);
static_assert(offsetof(NumericFrameMeta, max) == 32);

constexpr std::string_view NumericTransformName(NumericTransform t) noexcept {
  constexpr std::string_view kNames[] = {
    "raw", "for", "delta", "rle", "dict", "ffor", "rle_ffor", "dict_ffor"};
  return kNames[static_cast<uint8_t>(t)];
}

struct NumericHeader {
  uint8_t version = kNumericVersion;
  uint8_t width = 0;
  NumericTransform transform = NumericTransform::Raw;
  NumericLeaf leaf = NumericLeaf::None;
  uint8_t level = 0;
  uint8_t stored = 0;
  uint8_t run_width = 0;
  uint8_t flags = 0;
  uint32_t row_count = 0;
  uint32_t frame_count = 0;
  uint32_t dict_count = 0;
  uint32_t off_frames = 0;
  uint32_t off_dict = 0;
  uint32_t off_data = 0;
  uint32_t data_size = 0;
  uint8_t frame_log2 = kNumericFrameLog2;
  uint8_t reserved0[3] = {};
  uint64_t base = 0;
  uint64_t raw_bytes = 0;
  uint64_t scale = 0;

  bool Shuffled() const noexcept { return flags & kNumericShuffled; }
  uint32_t FrameBytes() const noexcept { return uint32_t{1} << frame_log2; }

  static NumericHeader Parse(duckdb::const_data_ptr_t p,
                             duckdb::idx_t segment_size) {
    SDB_ENSURE(segment_size >= kNumericHeaderSize,
               "numeric codec: segment smaller than its header");
    const auto h = LoadLayout<NumericHeader>(p);
    SDB_ENSURE(h.version == kNumericVersion,
               "numeric codec: unsupported segment version ", h.version);
    SDB_ENSURE(
      NumericWidth(h.width) && NumericWidth(h.stored) && h.stored <= h.width &&
        (h.transform != NumericTransform::Raw || h.stored == h.width) &&
        static_cast<uint8_t>(h.transform) <=
          static_cast<uint8_t>(NumericTransform::DictFfor) &&
        (!BitPacked(h.transform) ||
         (h.leaf == NumericLeaf::None && !h.Shuffled() && h.stored == h.width &&
          h.frame_log2 == kFforFrameLog2 && h.base == 0)) &&
        static_cast<uint8_t>(h.leaf) <=
          static_cast<uint8_t>(NumericLeaf::Zstd) &&
        (h.transform != NumericTransform::Rle || NumericWidth(h.run_width)) &&
        Coded(h.transform) == (h.dict_count != 0) &&
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

  void Write(duckdb::data_ptr_t p) const { StoreLayout(*this, p); }
};

static_assert(sizeof(NumericHeader) == kNumericHeaderSize);
static_assert(offsetof(NumericHeader, version) == 0);
static_assert(offsetof(NumericHeader, width) == 1);
static_assert(offsetof(NumericHeader, transform) == 2);
static_assert(offsetof(NumericHeader, leaf) == 3);
static_assert(offsetof(NumericHeader, level) == 4);
static_assert(offsetof(NumericHeader, stored) == 5);
static_assert(offsetof(NumericHeader, run_width) == 6);
static_assert(offsetof(NumericHeader, flags) == 7);
static_assert(offsetof(NumericHeader, row_count) == 8);
static_assert(offsetof(NumericHeader, frame_count) == 12);
static_assert(offsetof(NumericHeader, dict_count) == 16);
static_assert(offsetof(NumericHeader, off_frames) == 20);
static_assert(offsetof(NumericHeader, off_dict) == 24);
static_assert(offsetof(NumericHeader, off_data) == 28);
static_assert(offsetof(NumericHeader, data_size) == 32);
static_assert(offsetof(NumericHeader, frame_log2) == 36);
static_assert(offsetof(NumericHeader, reserved0) == 37);
static_assert(offsetof(NumericHeader, base) == 40);
static_assert(offsetof(NumericHeader, raw_bytes) == 48);
static_assert(offsetof(NumericHeader, scale) == 56);

}  // namespace irs::codecs
