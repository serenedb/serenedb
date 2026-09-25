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

#include "iresearch/formats/column/codecs/string_choice.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

enum class CodesEncoding : uint8_t {
  Bitpack = 0,
  Rle = 1,
};

inline constexpr uint8_t kSegmentVersion = 1;
inline constexpr size_t kHeaderSize = 72;
inline constexpr size_t kFrameMetaSize = 16;
inline constexpr size_t kFrameRawBytes = 64 * 1024;
inline constexpr size_t kFsstFrameRawBytes = 16 * 1024;
inline constexpr uint32_t kChainRestart = 16;
inline constexpr duckdb::idx_t kGroup =
  duckdb::BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

constexpr uint32_t Align8(uint64_t v) noexcept {
  return static_cast<uint32_t>((v + 7) & ~uint64_t{7});
}

constexpr duckdb::idx_t GroupPadded(duckdb::idx_t count) noexcept {
  return (count + kGroup - 1) / kGroup * kGroup;
}

struct Header {
  uint8_t shape;
  uint8_t codec;
  uint8_t level;
  uint8_t code_width;
  uint8_t length_width;
  uint8_t codes_encoding;
  uint8_t run_width;
  uint8_t lcp_width;
  uint32_t row_count;
  uint32_t entry_count;
  uint32_t frame_count;
  uint32_t run_count;
  uint32_t off_frames;
  uint32_t off_lengths;
  uint32_t off_lcps;
  uint32_t off_codes;
  uint32_t off_runs;
  uint32_t off_symtab;
  uint32_t symtab_size;
  uint32_t off_data;
  uint32_t data_size;
  uint64_t raw_bytes;

  static Header Parse(duckdb::const_data_ptr_t p, duckdb::idx_t segment_size) {
    using duckdb::BitpackingPrimitives;
    using duckdb::Load;
    Header h;
    const auto version = Load<uint8_t>(p);
    SDB_ENSURE(version == kSegmentVersion,
               "col codec: unsupported segment version ", version);
    h.shape = Load<uint8_t>(p + 1);
    h.codec = Load<uint8_t>(p + 2);
    h.level = Load<uint8_t>(p + 3);
    h.code_width = Load<uint8_t>(p + 4);
    h.length_width = Load<uint8_t>(p + 5);
    h.codes_encoding = Load<uint8_t>(p + 6);
    h.run_width = Load<uint8_t>(p + 7);
    h.row_count = Load<uint32_t>(p + 8);
    h.entry_count = Load<uint32_t>(p + 12);
    h.frame_count = Load<uint32_t>(p + 16);
    h.run_count = Load<uint32_t>(p + 20);
    h.off_frames = Load<uint32_t>(p + 24);
    h.off_lengths = Load<uint32_t>(p + 28);
    h.off_lcps = Load<uint32_t>(p + 32);
    h.off_codes = Load<uint32_t>(p + 36);
    h.off_runs = Load<uint32_t>(p + 40);
    h.off_symtab = Load<uint32_t>(p + 44);
    h.symtab_size = Load<uint32_t>(p + 48);
    h.off_data = Load<uint32_t>(p + 52);
    h.data_size = Load<uint32_t>(p + 56);
    h.lcp_width = Load<uint8_t>(p + 60);
    h.raw_bytes = Load<uint64_t>(p + 64);
    const bool rle =
      h.codes_encoding == static_cast<uint8_t>(CodesEncoding::Rle);
    const uint64_t codes_count = h.shape != static_cast<uint8_t>(Shape::Dedup)
                                   ? 0
                                 : rle ? h.run_count
                                       : h.row_count;
    SDB_ENSURE(
      h.shape <= static_cast<uint8_t>(Shape::Plain) &&
        h.codec < kByteCodecCount &&
        h.codes_encoding <= static_cast<uint8_t>(CodesEncoding::Rle) &&
        h.code_width <= 32 && h.length_width <= 32 && h.run_width <= 32 &&
        h.lcp_width <= 32 &&
        static_cast<uint64_t>(h.off_data) + h.data_size <= segment_size &&
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

  void Write(duckdb::data_ptr_t p) const {
    using duckdb::Store;
    Store<uint8_t>(kSegmentVersion, p);
    Store<uint8_t>(shape, p + 1);
    Store<uint8_t>(codec, p + 2);
    Store<uint8_t>(level, p + 3);
    Store<uint8_t>(code_width, p + 4);
    Store<uint8_t>(length_width, p + 5);
    Store<uint8_t>(codes_encoding, p + 6);
    Store<uint8_t>(run_width, p + 7);
    Store<uint32_t>(row_count, p + 8);
    Store<uint32_t>(entry_count, p + 12);
    Store<uint32_t>(frame_count, p + 16);
    Store<uint32_t>(run_count, p + 20);
    Store<uint32_t>(off_frames, p + 24);
    Store<uint32_t>(off_lengths, p + 28);
    Store<uint32_t>(off_lcps, p + 32);
    Store<uint32_t>(off_codes, p + 36);
    Store<uint32_t>(off_runs, p + 40);
    Store<uint32_t>(off_symtab, p + 44);
    Store<uint32_t>(symtab_size, p + 48);
    Store<uint32_t>(off_data, p + 52);
    Store<uint32_t>(data_size, p + 56);
    Store<uint8_t>(lcp_width, p + 60);
    Store<uint8_t>(0, p + 61);
    Store<uint16_t>(0, p + 62);
    Store<uint64_t>(raw_bytes, p + 64);
  }
};

struct FrameMeta {
  uint32_t first_entry;
  uint32_t raw_len;
  uint32_t comp_off;
  uint32_t comp_len;

  static FrameMeta Load(duckdb::const_data_ptr_t p) {
    return {duckdb::Load<uint32_t>(p), duckdb::Load<uint32_t>(p + 4),
            duckdb::Load<uint32_t>(p + 8), duckdb::Load<uint32_t>(p + 12)};
  }
  void Store(duckdb::data_ptr_t p) const {
    duckdb::Store<uint32_t>(first_entry, p);
    duckdb::Store<uint32_t>(raw_len, p + 4);
    duckdb::Store<uint32_t>(comp_off, p + 8);
    duckdb::Store<uint32_t>(comp_len, p + 12);
  }
};

}  // namespace irs::codecs
