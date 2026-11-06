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

#include <algorithm>
#include <compare>
#include <cstdint>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
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
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/trained_dictionary.hpp"
#include "iresearch/formats/column/read_context.hpp"

namespace irs::codecs::text {

inline constexpr uint32_t kNoFrame = std::numeric_limits<uint32_t>::max();
inline constexpr duckdb::idx_t kSparseEntries = 64;
inline constexpr uint32_t kLazyFilterEntries = 4096;
inline constexpr uint64_t kNoRow = std::numeric_limits<uint64_t>::max();
inline constexpr duckdb::idx_t kFetchAhead = 16;

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

  duckdb::data_ptr_t src;
  uint8_t width;
  duckdb::idx_t loaded = std::numeric_limits<duckdb::idx_t>::max();
  uint32_t values[kGroup];
};

inline FrameMeta FrameAt(duckdb::const_data_ptr_t table, uint32_t f) noexcept {
  return duckdb::Load<FrameMeta>(table + f * kFrameMetaSize);
}

inline uint32_t FrameOfEntry(duckdb::const_data_ptr_t table, uint32_t hi,
                             uint32_t entry) noexcept {
  uint32_t lo = 0;
  while (hi - lo > 1) {
    const auto mid = lo + (hi - lo) / 2;
    if (FrameAt(table, mid).first_entry <= entry) {
      lo = mid;
    } else {
      hi = mid;
    }
  }
  return lo;
}

duckdb::shared_ptr<DecodedDictionary> CachedDictionary(
  const ReadContext::CacheSlot& slot);

Header SegmentHeader(duckdb::const_data_ptr_t base,
                     const duckdb::ColumnSegment& segment, Shape shape,
                     ByteCodec codec);

NumericHeader CodesHeader(duckdb::const_data_ptr_t base, const Header& h);

const TrainedDictionary* TrainedOf(duckdb::ColumnSegment& segment,
                                   const Header& header);

struct EntryRange {
  uint32_t lo;
  uint32_t hi;

  auto operator<=>(const EntryRange&) const noexcept = default;
};

using EntryRanges = std::vector<EntryRange>;

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

class AuxiliaryLink {
 public:
  bool Holds(
    const duckdb::buffer_ptr<duckdb::AuxiliaryDataSet>& set) const noexcept {
    return set && set.get() == _set && !_weak.expired();
  }

  void Set(const duckdb::buffer_ptr<duckdb::AuxiliaryDataSet>& set) {
    _weak = set;
    _set = set.get();
  }

 private:
  duckdb::weak_ptr<duckdb::AuxiliaryDataSet> _weak;
  const duckdb::AuxiliaryDataSet* _set = nullptr;
};

struct HeapReference {
  AuxiliaryLink target;
  const duckdb::AuxiliaryDataSet* source = nullptr;
};

void ReferenceOnce(HeapReference& reference, const duckdb::Vector& source,
                   duckdb::Vector& result);

void EmitDecoded(const DecodedDictionary& decoded,
                 std::span<const EntryAt> order, duckdb::Vector& result,
                 HeapReference& reference);

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  duckdb::QueryContext, duckdb::ColumnSegment& segment);

}  // namespace irs::codecs::text
