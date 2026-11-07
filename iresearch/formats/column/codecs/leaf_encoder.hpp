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

#include <absl/strings/match.h>

#include <cstddef>
#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <span>
#include <string_view>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"

namespace irs::codecs::text {

inline constexpr uint64_t kDriftMinFraction = 4;

struct SegmentEntries {
  std::span<const duckdb::string_t> entries;
  uint32_t first_entry = 0;
  uint64_t raw = 0;
  uint32_t max_len = 0;
  uint64_t first_row = 0;
  uint64_t row_count = 0;
};

inline size_t EntryCount(const SegmentEntries& rows) noexcept {
  return rows.entries.size() + rows.first_entry;
}

inline uint32_t CommonPrefix(const duckdb::string_t& a,
                             const duckdb::string_t& b) noexcept {
  return static_cast<uint32_t>(
    absl::FindLongestCommonPrefix({a.GetData(), a.GetSize()},
                                  {b.GetData(), b.GetSize()})
      .size());
}

struct EncodedFrames {
  ByteCodec codec = ByteCodec::Lz4;
  std::span<const FrameMeta> frames;
  uint32_t max_length = 0;
  std::span<uint32_t> lcps;
  uint32_t max_lcp = 0;
  std::string_view symtab;
  uint32_t dictionary_size = 0;
  FrameDictionary flags = FrameDictionary::None;
};

struct Profile {
  Shape shape = Shape::Dedup;
  uint64_t rows = 0;
  uint64_t runs = 0;
  uint64_t raw = 0;
  uint32_t max_len = 0;
  std::span<const duckdb::string_t> values;
  std::span<const uint32_t> codes;
};

inline duckdb::string_t EntryOf(const Profile& p, size_t i) noexcept {
  const auto code = p.codes[i];
  return code == 0 ? duckdb::string_t{} : p.values[code - 1];
}

uint64_t CodesBytes(Shape shape, uint64_t rows, uint64_t entries, uint64_t runs,
                    const NumericTuning& tuning) noexcept;

uint64_t EstimateEntries(const SegmentEntries& rows,
                         const RatioHistory& hist) noexcept;

void RecordRatio(RatioHistory& hist, uint64_t raw, uint64_t comp,
                 uint32_t target) noexcept;

}  // namespace irs::codecs::text
