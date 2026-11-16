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
#include <duckdb/common/types.hpp>
#include <string_view>
#include <type_traits>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
Data layout per segment, one block of one column holding a whole row group:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   FrameHeader, 48 bytes                      |   |
|   +----------------------------------------------+   |
|   value width, stored width, shape, leaf, flags,     |
|   counts, section offsets, frame size, common        |
|   divisor (scale)                                    |
|   +----------------------------------------------+   |
|   |   the function's own header, 8 or 16 bytes   |   |
|   +----------------------------------------------+   |
|   plain and delta: base; runs: base and run          |
|   width; dictionary: entry count and offset          |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   NumericFrameMeta frames[frame_count]       |   |
|   +----------------------------------------------+   |
|   first row, decoded size and stored range of        |
|   each frame in Data, its min and max, and a         |
|   base: delta's previous value, rle_ffor's           |
|   run count                                          |
+------------------------------------------------------+
|                      Dictionary                      |
|   +----------------------------------------------+   |
|   |   value dict[dict_count]                     |   |
|   +----------------------------------------------+   |
|   dict and dict_ffor only                            |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   frames                                     |   |
|   +----------------------------------------------+   |
|   raw, for, delta, dict: one item per row, the       |
|   value, value - base, the difference to the         |
|   previous row minus base, or the dictionary         |
|   index, in the stored width; shuffled into          |
|   byte planes when flagged                           |
|   rle: the frame's run values (value - base),        |
|   then their run lengths                             |
|   these frames are lz4 or zstd compressed when       |
|   the leaf makes them smaller                        |
|   ffor, dict_ffor: 16384 rows per frame, in          |
|   blocks of 1024 values (dictionary indexes for      |
|   dict_ffor): a 16-byte meta (base, bit width)       |
|   per block, then each block's values minus its      |
|   base, bitpacked                                    |
|   rle_ffor: 1024 runs per frame, run values and      |
|   run lengths each packed like ffor                  |
+------------------------------------------------------+
The header holds the section offsets; bit-packed data starts at an
8-byte boundary.
*/

namespace irs::codecs {

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
  uint8_t reserved0[2] = {};
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

constexpr uint32_t HeaderSize(Shape shape) noexcept {
  switch (shape) {
    case Shape::Plain:
      return sizeof(FrameHeader) + sizeof(PlainHeader);
    case Shape::Delta:
      return sizeof(FrameHeader) + sizeof(DeltaHeader);
    case Shape::Runs:
      return sizeof(FrameHeader) + sizeof(RunsHeader);
    case Shape::Dictionary:
      return sizeof(FrameHeader) + sizeof(DictionaryHeader);
  }
  return sizeof(FrameHeader);
}

constexpr std::string_view TransformName(Shape shape,
                                         const FrameHeader& f) noexcept {
  const bool packed = f.flags & kNumericPacked;
  switch (shape) {
    case Shape::Plain:
      return packed ? "ffor" : f.stored == f.width ? "raw" : "for";
    case Shape::Delta:
      return packed ? "delta_ffor" : "delta";
    case Shape::Runs:
      return packed ? "rle_ffor" : "rle";
    case Shape::Dictionary:
      return packed ? "dict_ffor" : "dict";
  }
  return "raw";
}

inline FrameHeader ParseFrames(duckdb::const_data_ptr_t p, uint64_t size,
                               CodecType codec) {
  SDB_ENSURE(size >= sizeof(FrameHeader),
             "numeric codec: segment smaller than its header");
  const auto f = duckdb::Load<FrameHeader>(p);
  SDB_ENSURE(f.version == kNumericVersion,
             "numeric codec: unsupported segment version ", f.version);
  const auto shape = ShapeOf(codec);
  const bool packed = f.flags & kNumericPacked;
  SDB_ENSURE(
    size >= HeaderSize(shape) && NumericWidth(f.width) &&
      NumericWidth(f.stored) && f.stored <= f.width &&
      (f.flags & ~(kNumericShuffled | kNumericPacked)) == 0 &&
      (!(f.flags & kNumericShuffled) || f.stored > 1) &&
      (!packed ||
       (LeafOf(codec) == ByteCodec::None && !(f.flags & kNumericShuffled) &&
        f.stored == f.width && f.frame_log2 == kFforFrameLog2 &&
        (reinterpret_cast<uintptr_t>(p) + f.off_data) % sizeof(uint64_t) ==
          0)) &&
      f.off_frames >= HeaderSize(shape) &&
      f.off_frames + uint64_t{f.frame_count} * kNumericFrameMetaSize <=
        f.off_data &&
      uint64_t{f.off_data} + f.data_size <= size && f.frame_log2 >= 10 &&
      f.frame_log2 <= 20 && (f.frame_count != 0 || f.row_count == 0),
    "numeric codec: corrupted segment header");
  return f;
}

template<typename F>
std::invoke_result_t<F&, std::type_identity<int8_t>> WithNumericType(
  duckdb::PhysicalType physical, F&& f) {
  switch (physical) {
    case duckdb::PhysicalType::INT8:
      return f(std::type_identity<int8_t>{});
    case duckdb::PhysicalType::INT16:
      return f(std::type_identity<int16_t>{});
    case duckdb::PhysicalType::INT32:
      return f(std::type_identity<int32_t>{});
    case duckdb::PhysicalType::INT64:
      return f(std::type_identity<int64_t>{});
    case duckdb::PhysicalType::UINT8:
      return f(std::type_identity<uint8_t>{});
    case duckdb::PhysicalType::UINT16:
      return f(std::type_identity<uint16_t>{});
    case duckdb::PhysicalType::UINT32:
      return f(std::type_identity<uint32_t>{});
    case duckdb::PhysicalType::UINT64:
      return f(std::type_identity<uint64_t>{});
    case duckdb::PhysicalType::FLOAT:
      return f(std::type_identity<float>{});
    case duckdb::PhysicalType::DOUBLE:
      return f(std::type_identity<double>{});
    default:
      return {};
  }
}

}  // namespace irs::codecs
