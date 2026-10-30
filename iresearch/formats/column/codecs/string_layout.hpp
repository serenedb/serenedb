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

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

enum class CodesEncoding : uint8_t {
  Bitpack = 0,
  Rle = 1,
  Numeric = 2,
};

enum class FrameDictionary : uint8_t {
  None = 0,
  FirstFrame = 1,
  Trained = 2,
};

inline constexpr uint8_t kSegmentVersion = 1;
inline constexpr size_t kHeaderSize = 72;
inline constexpr size_t kFrameRawBytes = 64 * 1024;
inline constexpr size_t kFsstFrameRawBytes = 16 * 1024;
inline constexpr size_t kFrameDictionaryBytes = 32 * 1024;
inline constexpr size_t kDictionaryFrameBytes = 16 * 1024;
inline constexpr size_t kZstdDictionaryFrameBytes = 32 * 1024;
inline constexpr uint32_t kChainRestart = 16;
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
|   shape, leaf, level, bit widths, counts,            |
|   section offsets, flags, trained dictionary id      |
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
|                         Data                         |
|   +----------------------------------------------+   |
|   |   leaf frames of the concatenated entries    |   |
|   +----------------------------------------------+   |
|   frame 0 is the dictionary of the later             |
|   frames; or frames without a dictionary;            |
|   or frames compressed with the column's             |
|   trained dictionary, stored once per column;        |
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
|   shape, leaf, level, bit widths, counts,            |
|   section offsets, flags, trained dictionary id      |
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
|   |   bitpacked code[row_count]                  |   |
|   |   or bitpacked run value[run_count]          |   |
|   |   or a numeric codec segment                 |   |
|   +----------------------------------------------+   |
|   row -> entry, code 0 is NULL                       |
+------------------------------------------------------+
|                       Run ends                       |
|   +----------------------------------------------+   |
|   |   bitpacked run_end[run_count]               |   |
|   +----------------------------------------------+   |
|   run-length row codes only                          |
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
  Shape shape = Shape::Dedup;
  ByteCodec codec = ByteCodec::Lz4;
  uint8_t level = 0;
  uint8_t code_width = 0;
  uint8_t length_width = 0;
  CodesEncoding codes_encoding = CodesEncoding::Bitpack;
  uint8_t run_width = 0;
  uint32_t row_count = 0;
  uint32_t entry_count = 0;
  uint32_t frame_count = 0;
  uint32_t run_count = 0;
  uint32_t off_frames = 0;
  uint32_t off_lengths = 0;
  uint32_t off_lcps = 0;
  uint32_t off_codes = 0;
  uint32_t off_runs = 0;
  uint32_t off_symtab = 0;
  uint32_t symtab_size = 0;
  uint32_t off_data = 0;
  uint32_t data_size = 0;
  uint8_t lcp_width = 0;
  FrameDictionary flags = FrameDictionary::None;
  uint16_t dictionary = 0;
  uint64_t raw_bytes = 0;

  static Header Parse(duckdb::const_data_ptr_t p, duckdb::idx_t segment_size) {
    using duckdb::BitpackingPrimitives;
    SDB_ENSURE(segment_size >= kHeaderSize,
               "col codec: segment smaller than its header");
    const auto h = duckdb::Load<Header>(p);
    SDB_ENSURE(h.version == kSegmentVersion,
               "col codec: unsupported segment version ", h.version);
    const bool rle = h.codes_encoding == CodesEncoding::Rle;
    const bool numeric = h.codes_encoding == CodesEncoding::Numeric;
    const uint64_t codes_count = h.shape != Shape::Dedup || numeric ? 0
                                 : rle ? h.run_count
                                       : h.row_count;
    SDB_ENSURE(
      h.shape <= Shape::Plain &&
        static_cast<uint8_t>(h.codec) < kByteCodecCount &&
        h.codes_encoding <= CodesEncoding::Numeric &&
        (!numeric || (h.shape == Shape::Dedup && h.run_count == 0 &&
                      h.off_runs > h.off_codes)) &&
        h.code_width <= 32 && h.length_width <= 32 && h.run_width <= 32 &&
        h.lcp_width <= 32 &&
        (h.flags == FrameDictionary::None ||
         (h.flags == FrameDictionary::FirstFrame && h.frame_count > 1 &&
          h.codec != ByteCodec::Fsst) ||
         (h.flags == FrameDictionary::Trained && h.dictionary != 0 &&
          Trainable(h.codec))) &&
        (h.flags == FrameDictionary::Trained) == (h.dictionary != 0) &&
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
        h.off_codes +
            BitpackingPrimitives::GetRequiredSize(codes_count, h.code_width) <=
          h.off_runs &&
        h.off_runs +
            BitpackingPrimitives::GetRequiredSize(h.run_count, h.run_width) <=
          h.off_symtab &&
        h.off_symtab + static_cast<uint64_t>(h.symtab_size) <= h.off_data,
      "col codec: corrupted segment header");
    return h;
  }
};

static_assert(sizeof(Header) == kHeaderSize);
static_assert(offsetof(Header, version) == 0);
static_assert(offsetof(Header, shape) == 1);
static_assert(offsetof(Header, codec) == 2);
static_assert(offsetof(Header, level) == 3);
static_assert(offsetof(Header, code_width) == 4);
static_assert(offsetof(Header, length_width) == 5);
static_assert(offsetof(Header, codes_encoding) == 6);
static_assert(offsetof(Header, run_width) == 7);
static_assert(offsetof(Header, row_count) == 8);
static_assert(offsetof(Header, entry_count) == 12);
static_assert(offsetof(Header, frame_count) == 16);
static_assert(offsetof(Header, run_count) == 20);
static_assert(offsetof(Header, off_frames) == 24);
static_assert(offsetof(Header, off_lengths) == 28);
static_assert(offsetof(Header, off_lcps) == 32);
static_assert(offsetof(Header, off_codes) == 36);
static_assert(offsetof(Header, off_runs) == 40);
static_assert(offsetof(Header, off_symtab) == 44);
static_assert(offsetof(Header, symtab_size) == 48);
static_assert(offsetof(Header, off_data) == 52);
static_assert(offsetof(Header, data_size) == 56);
static_assert(offsetof(Header, lcp_width) == 60);
static_assert(offsetof(Header, flags) == 61);
static_assert(offsetof(Header, dictionary) == 62);
static_assert(offsetof(Header, raw_bytes) == 64);

}  // namespace irs::codecs
