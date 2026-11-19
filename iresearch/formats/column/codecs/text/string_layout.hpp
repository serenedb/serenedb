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
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/typedefs.hpp>
#include <utility>

#include "iresearch/formats/column/codecs/codec_type.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

inline constexpr CodecSet kCodesGroup =
  CodecSet{}
    .set(std::to_underlying(CodecType::Ffor))
    .set(std::to_underlying(CodecType::RleFfor));

constexpr bool CodesCodec(CodecType type) noexcept {
  const auto index = std::to_underlying(type);
  return index < kCodesGroup.size() && kCodesGroup.test(index);
}

inline uint64_t Packed(uint64_t count, uint64_t max_value) noexcept {
  return duckdb::BitpackingPrimitives::GetRequiredSize(
    count, duckdb::BitpackingPrimitives::MinimumBitWidth<uint32_t>(
             static_cast<uint32_t>(max_value)));
}

inline constexpr uint8_t kSegmentVersion = 1;
inline constexpr size_t kHeaderSize = 72;
inline constexpr size_t kFrameRawBytes = 64 * 1024;
inline constexpr size_t kFsstFrameRawBytes = 16 * 1024;
inline constexpr size_t kFrameDictionaryBytes = 32 * 1024;
inline constexpr size_t kDictionaryFrameBytes = 16 * 1024;
inline constexpr size_t kZstdDictionaryFrameBytes = 32 * 1024;
inline constexpr size_t kMaxDictionaryBytes = 64 * 1024;
inline constexpr uint32_t kChainRestart = 16;
inline constexpr uint32_t kMeanChainWalk = 4;
inline constexpr duckdb::idx_t kGroup =
  duckdb::BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

/*
Data layout per PlainScan segment of the lz4, zstd and zxc leaves, one block
of one column; a text column's row group is cut into segments of about
kDefaultColSegmentTarget encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header, 72 bytes                           |   |
|   +----------------------------------------------+   |
|   codec of the row codes, level, bit widths,         |
|   counts, section offsets, flags, decoded size       |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked length[entry_count]              |   |
|   +----------------------------------------------+   |
|   one entry per row, empty for a NULL row            |
+------------------------------------------------------+
|                      Dictionary                      |
|   +----------------------------------------------+   |
|   |   packed bytes[symtab_size]                  |   |
|   +----------------------------------------------+   |
|   trained on the frames of this segment and          |
|   packed by the leaf (lz4 for zstd, whose unpacking  |
|   is slower), or as is when that is not smaller;     |
|   dictionary_size bytes once unpacked; empty         |
|   unless flags is Local                              |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   leaf frames of the concatenated entries    |   |
|   +----------------------------------------------+   |
|   each frame compressed with the Dictionary;         |
|   or frame 0 is the dictionary of the later          |
|   frames; or frames without a dictionary;            |
|   the leaf's file gives the frame sizes              |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

/*
Data layout per DictionaryScan segment of the lz4, zstd and zxc leaves, one
block of one column; a text column's row group is cut into segments of about
kDefaultColSegmentTarget encoded bytes:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   Header, 72 bytes                           |   |
|   +----------------------------------------------+   |
|   codec of the row codes, level, bit widths,         |
|   counts, section offsets, flags, decoded size       |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   FrameMeta frames[frame_count]              |   |
|   +----------------------------------------------+   |
|   first entry, decoded size and stored range         |
|   of each frame in Data                              |
+------------------------------------------------------+
|                    Entry lengths                     |
|   +----------------------------------------------+   |
|   |   bitpacked length[entry_count]              |   |
|   +----------------------------------------------+   |
|   entry 0 is NULL, then each distinct value in       |
|   the order the rows first use it                    |
+------------------------------------------------------+
|                      Row codes                       |
|   +----------------------------------------------+   |
|   |   numeric codec segment of code[row_count]   |   |
|   +----------------------------------------------+   |
|   row -> entry, code 0 is NULL                       |
+------------------------------------------------------+
|                      Dictionary                      |
|   +----------------------------------------------+   |
|   |   packed bytes[symtab_size]                  |   |
|   +----------------------------------------------+   |
|   trained on the frames of this segment and          |
|   packed by the leaf (lz4 for zstd, whose unpacking  |
|   is slower), or as is when that is not smaller;     |
|   dictionary_size bytes once unpacked; empty         |
|   unless flags is Local                              |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   leaf frames of the concatenated entries    |   |
|   +----------------------------------------------+   |
|   as in a PlainScan segment                          |
+------------------------------------------------------+
Sections start at 8-byte boundaries.
*/

struct Header {
  uint8_t version = kSegmentVersion;
  CodecType codes = CodecType::Auto;
  uint8_t reserved2 = 0;
  uint8_t level = 0;
  uint8_t reserved0 = 0;
  uint8_t length_width = 0;
  uint8_t reserved1[2] = {};
  uint32_t row_count = 0;
  uint32_t entry_count = 0;
  uint32_t frame_count = 0;
  uint32_t dictionary_size = 0;
  uint32_t off_frames = 0;
  uint32_t off_lengths = 0;
  uint32_t off_lcps = 0;
  uint32_t off_codes = 0;
  uint32_t reserved3 = 0;
  uint32_t off_symtab = 0;
  uint32_t symtab_size = 0;
  uint32_t off_data = 0;
  uint32_t data_size = 0;
  uint8_t lcp_width = 0;
  FrameDictionary flags = FrameDictionary::None;
  uint16_t reserved4 = 0;
  uint64_t raw_bytes = 0;

  static Header Parse(duckdb::const_data_ptr_t p, duckdb::idx_t segment_size,
                      CodecType codec) {
    using duckdb::BitpackingPrimitives;
    SDB_ENSURE(segment_size >= kHeaderSize,
               "col codec: segment smaller than its header");
    const auto h = duckdb::Load<Header>(p);
    const auto leaf = LeafOf(codec);
    SDB_ENSURE(h.version == kSegmentVersion,
               "col codec: unsupported segment version ", h.version);
    SDB_ENSURE(
      (ShapeOf(codec) == Shape::Dictionary ? CodesCodec(h.codes)
                                           : h.codes == CodecType::Auto) &&
        h.length_width <= 32 && h.lcp_width <= 32 &&
        (h.flags == FrameDictionary::None ||
         (h.flags == FrameDictionary::FirstFrame && h.frame_count > 1 &&
          !Sorted(leaf)) ||
         (h.flags == FrameDictionary::Local && h.symtab_size != 0 &&
          h.dictionary_size != 0 && h.dictionary_size <= kMaxDictionaryBytes &&
          !Sorted(leaf))) &&
        (leaf != ByteCodec::FsstPlus ||
         h.symtab_size >= uint64_t{h.dictionary_size} + sizeof(uint32_t)) &&
        static_cast<uint64_t>(h.off_data) + h.data_size <= segment_size &&
        h.off_frames >= kHeaderSize &&
        h.off_frames + static_cast<uint64_t>(h.frame_count) * kFrameMetaSize <=
          h.off_lengths &&
        h.off_lengths + BitpackingPrimitives::GetRequiredSize(h.entry_count,
                                                              h.length_width) <=
          h.off_lcps &&
        h.off_lcps +
            BitpackingPrimitives::GetRequiredSize(h.entry_count, h.lcp_width) <=
          h.off_codes &&
        h.off_codes <= h.off_symtab &&
        h.off_symtab + static_cast<uint64_t>(h.symtab_size) <= h.off_data,
      "col codec: corrupted segment header");
    return h;
  }
};

static_assert(sizeof(Header) == kHeaderSize);
static_assert(offsetof(Header, version) == 0);
static_assert(offsetof(Header, codes) == 1);
static_assert(offsetof(Header, reserved2) == 2);
static_assert(offsetof(Header, level) == 3);
static_assert(offsetof(Header, reserved0) == 4);
static_assert(offsetof(Header, length_width) == 5);
static_assert(offsetof(Header, reserved1) == 6);
static_assert(offsetof(Header, row_count) == 8);
static_assert(offsetof(Header, entry_count) == 12);
static_assert(offsetof(Header, frame_count) == 16);
static_assert(offsetof(Header, dictionary_size) == 20);
static_assert(offsetof(Header, off_frames) == 24);
static_assert(offsetof(Header, off_lengths) == 28);
static_assert(offsetof(Header, off_lcps) == 32);
static_assert(offsetof(Header, off_codes) == 36);
static_assert(offsetof(Header, reserved3) == 40);
static_assert(offsetof(Header, off_symtab) == 44);
static_assert(offsetof(Header, symtab_size) == 48);
static_assert(offsetof(Header, off_data) == 52);
static_assert(offsetof(Header, data_size) == 56);
static_assert(offsetof(Header, lcp_width) == 60);
static_assert(offsetof(Header, flags) == 61);
static_assert(offsetof(Header, reserved4) == 62);
static_assert(offsetof(Header, raw_bytes) == 64);

inline uint64_t LayOut(Header& h, uint64_t data_bytes, uint64_t frames,
                       uint64_t lengths_bytes, uint64_t lcps_bytes,
                       uint64_t codes_bytes, uint64_t symtab_bytes) noexcept {
  h.off_frames = kHeaderSize;
  h.off_lengths =
    duckdb::AlignValue<uint64_t, 8>(kHeaderSize + frames * kFrameMetaSize);
  h.off_lcps = duckdb::AlignValue<uint64_t, 8>(h.off_lengths + lengths_bytes);
  h.off_codes = duckdb::AlignValue<uint64_t, 8>(h.off_lcps + lcps_bytes);
  h.off_symtab = duckdb::AlignValue<uint64_t, 8>(h.off_codes + codes_bytes);
  h.off_data = duckdb::AlignValue<uint64_t, 8>(h.off_symtab + symtab_bytes);
  return h.off_data + data_bytes;
}

}  // namespace irs::codecs
