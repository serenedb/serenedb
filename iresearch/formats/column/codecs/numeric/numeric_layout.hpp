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

#include "iresearch/formats/column/codecs/codec_type.hpp"
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
  DeltaFfor = 8,
};

constexpr bool BitPacked(NumericTransform t) noexcept {
  return t == NumericTransform::Ffor || t == NumericTransform::RleFfor ||
         t == NumericTransform::DictFfor || t == NumericTransform::DeltaFfor;
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

enum class NumericShape : uint8_t {
  Plain,
  Delta,
  Runs,
  Dictionary,
};

constexpr NumericShape ShapeOf(NumericTransform t) noexcept {
  switch (t) {
    case NumericTransform::Raw:
    case NumericTransform::For:
    case NumericTransform::Ffor:
      return NumericShape::Plain;
    case NumericTransform::Delta:
    case NumericTransform::DeltaFfor:
      return NumericShape::Delta;
    case NumericTransform::Rle:
    case NumericTransform::RleFfor:
      return NumericShape::Runs;
    case NumericTransform::Dict:
    case NumericTransform::DictFfor:
      return NumericShape::Dictionary;
  }
  return NumericShape::Plain;
}

inline constexpr CodecType kNumericCodecs[4][3] = {
  {CodecType::Ffor, CodecType::Lz4, CodecType::Zstd},
  {CodecType::DeltaFfor, CodecType::DeltaLz4, CodecType::DeltaZstd},
  {CodecType::RleFfor, CodecType::RleLz4, CodecType::RleZstd},
  {CodecType::DictFfor, CodecType::DictLz4, CodecType::DictZstd},
};

constexpr CodecType NumericCodec(NumericShape shape,
                                 NumericLeaf leaf) noexcept {
  return kNumericCodecs[static_cast<size_t>(shape)][static_cast<size_t>(leaf)];
}

constexpr bool IsNumeric(CodecType type) noexcept {
  for (const auto& shape : kNumericCodecs) {
    for (const auto codec : shape) {
      if (codec == type) {
        return true;
      }
    }
  }
  return false;
}

inline constexpr uint8_t kNumericVersion = 1;
inline constexpr size_t kNumericFrameMetaSize = 40;
inline constexpr uint8_t kNumericFrameLog2 = 14;
inline constexpr uint8_t kNumericLeafFrameLog2 = 16;
inline constexpr uint32_t kNumericDictMax = 65536;
inline constexpr uint8_t kNumericShuffled = 1;
inline constexpr uint8_t kNumericPacked = 2;
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
};

static_assert(sizeof(NumericFrameMeta) == kNumericFrameMetaSize);
static_assert(offsetof(NumericFrameMeta, frame) == 0);
static_assert(offsetof(NumericFrameMeta, base) == 16);
static_assert(offsetof(NumericFrameMeta, min) == 24);
static_assert(offsetof(NumericFrameMeta, max) == 32);

struct FrameHeader {
  uint8_t version = kNumericVersion;
  uint8_t width = 0;
  uint8_t stored = 0;
  NumericShape shape = NumericShape::Plain;
  NumericLeaf leaf = NumericLeaf::None;
  uint8_t level = 0;
  uint8_t flags = 0;
  uint8_t frame_log2 = kNumericFrameLog2;
  uint32_t row_count = 0;
  uint32_t frame_count = 0;
  uint32_t off_frames = 0;
  uint32_t off_data = 0;
  uint32_t data_size = 0;
  uint32_t reserved = 0;
  uint64_t raw_bytes = 0;
  uint64_t scale = 0;

  uint32_t FrameBytes() const noexcept { return uint32_t{1} << frame_log2; }
};

static_assert(sizeof(FrameHeader) == 48);
static_assert(offsetof(FrameHeader, row_count) == 8);
static_assert(offsetof(FrameHeader, off_frames) == 16);
static_assert(offsetof(FrameHeader, raw_bytes) == 32);
static_assert(offsetof(FrameHeader, scale) == 40);

struct PlainHeader {
  uint64_t base = 0;
};

struct DeltaHeader {
  uint64_t base = 0;
};

struct RunsHeader {
  uint64_t base = 0;
  uint8_t run_width = 0;
  uint8_t reserved[7] = {};
};

struct DictionaryHeader {
  uint32_t count = 0;
  uint32_t offset = 0;
};

static_assert(sizeof(PlainHeader) == 8);
static_assert(sizeof(DeltaHeader) == 8);
static_assert(sizeof(RunsHeader) == 16);
static_assert(sizeof(DictionaryHeader) == 8);

constexpr uint32_t HeaderSize(NumericShape shape) noexcept {
  switch (shape) {
    case NumericShape::Plain:
      return sizeof(FrameHeader) + sizeof(PlainHeader);
    case NumericShape::Delta:
      return sizeof(FrameHeader) + sizeof(DeltaHeader);
    case NumericShape::Runs:
      return sizeof(FrameHeader) + sizeof(RunsHeader);
    case NumericShape::Dictionary:
      return sizeof(FrameHeader) + sizeof(DictionaryHeader);
  }
  return sizeof(FrameHeader);
}

constexpr NumericTransform TransformOf(const FrameHeader& f) noexcept {
  const bool packed = f.flags & kNumericPacked;
  switch (f.shape) {
    case NumericShape::Plain:
      return packed                ? NumericTransform::Ffor
             : f.stored == f.width ? NumericTransform::Raw
                                   : NumericTransform::For;
    case NumericShape::Delta:
      return packed ? NumericTransform::DeltaFfor : NumericTransform::Delta;
    case NumericShape::Runs:
      return packed ? NumericTransform::RleFfor : NumericTransform::Rle;
    case NumericShape::Dictionary:
      return packed ? NumericTransform::DictFfor : NumericTransform::Dict;
  }
  return NumericTransform::Raw;
}

inline FrameHeader ParseFrames(duckdb::const_data_ptr_t p, uint64_t size) {
  SDB_ENSURE(size >= sizeof(FrameHeader),
             "numeric codec: segment smaller than its header");
  const auto f = duckdb::Load<FrameHeader>(p);
  SDB_ENSURE(f.version == kNumericVersion,
             "numeric codec: unsupported segment version ", f.version);
  const bool packed = f.flags & kNumericPacked;
  SDB_ENSURE(
    f.shape <= NumericShape::Dictionary && size >= HeaderSize(f.shape) &&
      NumericWidth(f.width) && NumericWidth(f.stored) && f.stored <= f.width &&
      (f.flags & ~(kNumericShuffled | kNumericPacked)) == 0 &&
      f.leaf <= NumericLeaf::Zstd &&
      (!packed ||
       (f.leaf == NumericLeaf::None && !(f.flags & kNumericShuffled) &&
        f.stored == f.width && f.frame_log2 == kFforFrameLog2)) &&
      f.off_frames >= HeaderSize(f.shape) &&
      f.off_frames + uint64_t{f.frame_count} * kNumericFrameMetaSize <=
        f.off_data &&
      uint64_t{f.off_data} + f.data_size <= size && f.frame_log2 >= 10 &&
      f.frame_log2 <= 20 && (f.frame_count != 0 || f.row_count == 0),
    "numeric codec: corrupted segment header");
  return f;
}

}  // namespace irs::codecs
