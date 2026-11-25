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

#include <absl/strings/str_cat.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/hugeint.hpp>
#include <duckdb/common/typedefs.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/uhugeint.hpp>
#include <limits>
#include <string_view>
#include <type_traits>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/utils/system_compiler.hpp"

/*
Data layout per segment, one block of one column holding a whole row group:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   the first 16 bytes of FrameHeader          |   |
|   +----------------------------------------------+   |
|   value width, stored width, leaf level, flags,      |
|   frame size, frame count, size of Data; the row     |
|   count comes with the segment and the section       |
|   offsets follow from the sizes                      |
|   +----------------------------------------------+   |
|   |   common divisor (scale), 8 bytes            |   |
|   +----------------------------------------------+   |
|   only when the scaled flag is set                   |
|   +----------------------------------------------+   |
|   |   the function's own header, 8 or 16 bytes   |   |
|   +----------------------------------------------+   |
|   plain and delta: base; runs: base and run width,   |
|   absent for rle_bitpacking; dictionary: entry       |
|   count and offset                                   |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   NumericFrameMeta frames[frame_count]       |   |
|   +----------------------------------------------+   |
|   first row, decoded size and stored range of each   |
|   frame in Data, its min and max, and a base:        |
|   delta's previous value, the step that              |
|   bitpacking's and delta_bitpacking's blocks of      |
|   differences share (one of their smallest           |
|   differences, the one that packs the frame          |
|   smallest), rle_bitpacking's run count;             |
|   a frame of bitpacking, delta_bitpacking or         |
|   dict_bitpacking takes its offset in Data and, in   |
|   the value's width, its base, min and max; a        |
|   frame of rle_bitpacking its offset in Data, its    |
|   first row, its run count in two bytes and, in      |
|   the value's width, its min and max                 |
+------------------------------------------------------+
|                      Dictionary                      |
|   +----------------------------------------------+   |
|   |   value dict[dict_count]                     |   |
|   +----------------------------------------------+   |
|   dict and dict_bitpacking only                      |
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
|   bitpacking, dict_bitpacking: 16384 rows per        |
|   frame, in blocks of 1024 values (dictionary        |
|   indexes for dict_bitpacking): the blocks' bases    |
|   minus the frame's min, in the fewest whole bytes   |
|   that hold the frame's max minus its min            |
|   (dict_bitpacking: the bases themselves, in the     |
|   bytes that hold the largest index), their width    |
|   bytes, padding to 8 bytes, then each block's       |
|   values minus its base, bitpacked; a block of       |
|   fewer than 1024 values ends with the last word     |
|   its values reach                                   |
|   a block of bitpacking whose width byte has its     |
|   top bit set stores differences: its base is its    |
|   first value, its low six bits the width of the     |
|   differences minus the frame's step, or minus its   |
|   own smallest difference, which then starts the     |
|   block in 8 bytes, when the next bit is set; a      |
|   frame that is one arithmetic run stores nothing:   |
|   its first value is the frame's min, or its max     |
|   when the difference is negative                    |
|   delta_bitpacking: bitpacking that stores           |
|   differences in every block they fit                |
|   rle_bitpacking: 1024 runs per frame: 8 bytes of    |
|   the run values' and the run lengths' width bytes,  |
|   two zero bytes and the lengths' base in 4 bytes;   |
|   the values' base in 8 bytes only when their width  |
|   byte stores differences (else it is the frame's    |
|   min); then the packed values and the packed        |
|   lengths, each minus its base; a block of fewer     |
|   than 1024 runs packs its values 32 at a time,      |
|   each group in 4 * width bytes; values that need    |
|   63 bits are stored in 64; a width byte with its    |
|   top bit set stores differences: the base is the    |
|   first value, 8 bytes hold the smallest             |
|   difference and the low six bits are the width of   |
|   the differences minus it; with the next bit set    |
|   too, the block alternates between its base and     |
|   its base plus those 8 bytes                        |
+------------------------------------------------------+
Bit-packed data starts at an 8-byte boundary.

A segment of 128-bit values starts with a WideHeader of 32 bytes and holds
such segments of 64-bit values: one, of every value's distance from the
smallest value in the header, or two, of the low halves and, at the next
8-byte boundary, of the high halves.
*/

namespace irs::codecs {

inline constexpr uint8_t kNumericVersion = 1;
inline constexpr size_t kRunsFrameMetaSize = 8;
inline constexpr size_t kNumericFrameMetaSize = 40;
inline constexpr uint8_t kNumericFrameLog2 = 14;
inline constexpr uint8_t kNumericLeafFrameLog2 = 16;
inline constexpr uint8_t kNumericMaxFrameLog2 = 20;
inline constexpr uint32_t kNumericDictMax = 65536;
inline constexpr uint8_t kNumericShuffled = 1;
inline constexpr uint8_t kNumericPacked = 2;
inline constexpr uint8_t kNumericScaled = 4;
inline constexpr uint32_t kBitpackingFrameRows = 16384;
inline constexpr uint8_t kBitpackingFrameLog2 = 18;

inline constexpr uint8_t kBlockDelta = 0x80;
inline constexpr uint8_t kBlockOwnStep = 0x40;
inline constexpr uint8_t kBlockDeltaBits = 0x3F;
inline constexpr uint8_t kStreamAlternates = kBlockDelta | kBlockOwnStep;

constexpr size_t BlockMetaBytes(size_t blocks, size_t width) noexcept {
  return (blocks * (width + 1) + 7) / 8 * 8;
}

inline constexpr uint32_t kRleBitpackingFrameRuns = 1024;

constexpr bool NumericWidth(uint64_t w) noexcept {
  return w == 1 || w == 2 || w == 4 || w == 8;
}

constexpr size_t FrameMetaSize(Shape shape, bool packed,
                               size_t width) noexcept {
  if (!packed) {
    return kNumericFrameMetaSize;
  }
  return shape == Shape::Runs
           ? 2 * sizeof(uint32_t) + sizeof(uint16_t) + 2 * width
           : sizeof(uint32_t) + 3 * width;
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
  uint8_t level = 0;
  uint8_t flags = 0;
  uint8_t frame_log2 = kNumericFrameLog2;
  uint16_t reserved = 0;
  uint32_t frame_count = 0;
  uint32_t data_size = 0;
  uint32_t row_count = 0;
  uint32_t off_frames = 0;
  uint32_t off_data = 0;
  uint64_t scale = 0;

  uint32_t FrameBytes() const noexcept { return uint32_t{1} << frame_log2; }
};

inline constexpr uint32_t kNumericHeaderSize = offsetof(FrameHeader, row_count);

static_assert(kNumericHeaderSize == 16);
static_assert(offsetof(FrameHeader, frame_count) == 8);

constexpr uint32_t OwnHeaderOffset(uint8_t flags) noexcept {
  return kNumericHeaderSize + (flags & kNumericScaled ? sizeof(uint64_t) : 0);
}

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

constexpr uint32_t OwnHeaderSize(Shape shape, bool packed) noexcept {
  switch (shape) {
    case Shape::Plain:
      return sizeof(PlainHeader);
    case Shape::Delta:
      return sizeof(DeltaHeader);
    case Shape::Runs:
      return packed ? 0 : sizeof(RunsHeader);
    case Shape::Dictionary:
      return sizeof(DictionaryHeader);
  }
  SDB_UNREACHABLE();
}

constexpr std::string_view TransformName(Shape shape,
                                         const FrameHeader& f) noexcept {
  const bool packed = f.flags & kNumericPacked;
  switch (shape) {
    case Shape::Plain:
      return packed ? "bitpacking" : f.stored == f.width ? "raw" : "for";
    case Shape::Delta:
      return packed ? "delta_bitpacking" : "delta";
    case Shape::Runs:
      return packed ? "rle_bitpacking" : "rle";
    case Shape::Dictionary:
      return packed ? "dict_bitpacking" : "dict";
  }
  SDB_UNREACHABLE();
}

inline FrameHeader ParseFrames(duckdb::const_data_ptr_t p, uint64_t size,
                               uint64_t rows, CodecType codec) {
  if (size < kNumericHeaderSize) [[unlikely]] {
    ThrowCorrupted("numeric", "segment is smaller than its header");
  }
  FrameHeader f;
  std::memcpy(&f, p, kNumericHeaderSize);
  if (f.version != kNumericVersion) [[unlikely]] {
    ThrowCorrupted("numeric", absl::StrCat("unsupported version ", f.version));
  }
  const auto shape = ShapeOf(codec);
  const bool packed = f.flags & kNumericPacked;
  const uint64_t own = OwnHeaderOffset(f.flags);
  const uint64_t frames = own + OwnHeaderSize(shape, packed);
  if (!(size >= frames && NumericWidth(f.width) && NumericWidth(f.stored) &&
        f.stored <= f.width &&
        (f.flags & ~(kNumericShuffled | kNumericPacked | kNumericScaled)) ==
          0 &&
        (!(f.flags & kNumericShuffled) || f.stored > 1) &&
        (!packed ||
         (LeafOf(codec) == ByteCodec::None && !(f.flags & kNumericShuffled) &&
          f.stored == f.width && f.frame_log2 == kBitpackingFrameLog2)) &&
        f.frame_log2 >= 10 && f.frame_log2 <= kNumericMaxFrameLog2 &&
        rows <= std::numeric_limits<uint32_t>::max() && f.frame_count != 0 &&
        (!packed || shape == Shape::Runs ||
         f.frame_count == (rows + kBitpackingFrameRows - 1) /
                            kBitpackingFrameRows))) [[unlikely]] {
    ThrowCorrupted("numeric", "header is inconsistent");
  }
  if (f.flags & kNumericScaled) {
    f.scale = duckdb::Load<uint64_t>(p + kNumericHeaderSize);
  }
  uint64_t data =
    frames + uint64_t{f.frame_count} * FrameMetaSize(shape, packed, f.width);
  if (shape == Shape::Dictionary) {
    data += uint64_t{duckdb::Load<DictionaryHeader>(p + own).count} * f.width;
  }
  if (packed) {
    data = duckdb::AlignValue<uint64_t, 8>(data);
  }
  if (!(data + f.data_size <= size &&
        (!packed ||
         (reinterpret_cast<uintptr_t>(p) + data) % sizeof(uint64_t) == 0)))
    [[unlikely]] {
    ThrowCorrupted("numeric", "header is inconsistent");
  }
  f.row_count = static_cast<uint32_t>(rows);
  f.off_frames = static_cast<uint32_t>(frames);
  f.off_data = static_cast<uint32_t>(data);
  return f;
}

}  // namespace irs::codecs
