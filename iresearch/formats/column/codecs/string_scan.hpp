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

#include <cstdint>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs::codecs::text {

inline constexpr size_t kFsstMaxExpansion = 8;
inline constexpr uint32_t kNoFrame = std::numeric_limits<uint32_t>::max();
inline constexpr duckdb::idx_t kSparseEntries = 64;
inline constexpr uint32_t kLazyFilterEntries = 4096;
inline constexpr uint64_t kNoRow = std::numeric_limits<uint64_t>::max();
inline constexpr duckdb::idx_t kFetchAhead = 16;

std::vector<uint32_t> Unpack(duckdb::data_ptr_t src, duckdb::idx_t count,
                             uint8_t width);

struct PackedReader {
  PackedReader(duckdb::data_ptr_t src, uint8_t width)
    : src{src}, width{width} {}

  uint32_t At(duckdb::idx_t i) {
    const duckdb::idx_t group = i / kGroup;
    if (group != loaded) {
      duckdb::BitpackingPrimitives::UnPackBlock<uint32_t>(
        duckdb::data_ptr_cast(values), src + (group * kGroup * width) / 8,
        width);
      loaded = group;
    }
    return values[i % kGroup];
  }

  uint32_t Single(duckdb::idx_t i) const noexcept {
    if (width == 0) {
      return 0;
    }
    const uint64_t bit = i * width;
    const auto* word = src + bit / 32 * sizeof(uint32_t);
    const auto shift = static_cast<uint32_t>(bit % 32);
    uint32_t lo;
    std::memcpy(&lo, word, sizeof(lo));
    uint64_t value = lo >> shift;
    if (shift + width > 32) {
      uint32_t hi;
      std::memcpy(&hi, word + sizeof(uint32_t), sizeof(hi));
      value |= uint64_t{hi} << (32 - shift);
    }
    return static_cast<uint32_t>(value & ((uint64_t{1} << width) - 1));
  }

  duckdb::data_ptr_t src;
  uint8_t width;
  duckdb::idx_t loaded = std::numeric_limits<duckdb::idx_t>::max();
  uint32_t values[kGroup];
};

uint64_t ChainCapacity(PackedReader& lengths, PackedReader& lcps,
                       uint32_t first, uint32_t end);

struct ChainEnd {
  uint32_t stop;
  uint64_t written;
  uint64_t enc_off;
};

duckdb::shared_ptr<DecodedDictionary> CachedDictionary(
  duckdb::ColumnSegment& segment);

NumericHeader CodesHeader(duckdb::const_data_ptr_t base, const Header& h);

const TrainedDictionary* TrainedOf(duckdb::ColumnSegment& segment,
                                   const Header& header);

using EntryRanges = std::vector<std::pair<uint32_t, uint32_t>>;

EntryRanges Normalize(EntryRanges ranges);

EntryRanges Intersect(const EntryRanges& a, const EntryRanges& b);

const duckdb::Value* StringConstant(const duckdb::Expression& expr) noexcept;

inline std::string_view View(const duckdb::string_t& s) noexcept {
  return {s.GetData(), s.GetSize()};
}

struct EntryAt {
  uint32_t entry;
  duckdb::idx_t at;
};

uint64_t TouchedBytes(const Header& h, duckdb::const_data_ptr_t base,
                      std::span<const EntryAt> order);

void EmitDecoded(const DecodedDictionary& decoded,
                 std::span<const EntryAt> order, duckdb::Vector& result);

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  duckdb::QueryContext, duckdb::ColumnSegment& segment);

}  // namespace irs::codecs::text
