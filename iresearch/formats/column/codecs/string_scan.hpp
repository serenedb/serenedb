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
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/planner/expression.hpp>
#include <limits>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/dictionary_cache.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text {

inline constexpr uint32_t kNoFrame = std::numeric_limits<uint32_t>::max();
inline constexpr duckdb::idx_t kSparseEntries = 64;
inline constexpr uint64_t kSingleRowCost = 100;
inline constexpr uint64_t kFrameRowCost = 25;
inline constexpr uint64_t kChainGroupCost = 180;
inline constexpr uint64_t kChainMinRowBytes = 8;
inline constexpr uint32_t kLazyFilterEntries = 4096;
inline constexpr uint64_t kNoRow = std::numeric_limits<uint64_t>::max();
inline constexpr duckdb::idx_t kFetchAhead = 16;

struct PackedReader {
  PackedReader(duckdb::data_ptr_t src, uint8_t width)
    : src{src}, width{width} {}

  void Load(duckdb::idx_t group) {
    if (group != loaded) {
      duckdb::BitpackingPrimitives::UnPackBlock<uint32_t>(
        duckdb::data_ptr_cast(values), src + (group * kGroup * width) / 8,
        width);
      loaded = group;
    }
  }

  uint32_t At(duckdb::idx_t i) {
    Load(i / kGroup);
    return values[i % kGroup];
  }

  uint64_t Sum(duckdb::idx_t first, duckdb::idx_t end) {
    uint64_t sum = 0;
    while (first < end) {
      const duckdb::idx_t group = first / kGroup;
      Load(group);
      const auto to = std::min<duckdb::idx_t>(end - group * kGroup, kGroup);
      for (auto i = first % kGroup; i < to; ++i) {
        sum += values[i];
      }
      first = (group + 1) * kGroup;
    }
    return sum;
  }

  void Read(duckdb::idx_t first, duckdb::idx_t count, uint32_t* out) {
    while (count != 0) {
      const duckdb::idx_t group = first / kGroup;
      Load(group);
      const auto at = first % kGroup;
      const auto take = std::min<duckdb::idx_t>(count, kGroup - at);
      std::memcpy(out, values + at, take * sizeof(uint32_t));
      out += take;
      first += take;
      count -= take;
    }
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

class ChainGroups {
 public:
  template<typename Entries>
  void Reset(uint32_t frame_count, Entries entries) {
    _base.resize(frame_count + 1);
    uint32_t groups = 0;
    for (uint32_t f = 0; f < frame_count; ++f) {
      const auto [first, end] = entries(f);
      SDB_ENSURE(end > first, "col codec: corrupted frame table");
      _base[f] = groups;
      groups += (end - first + kChainRestart - 1) / kChainRestart;
    }
    _base[frame_count] = groups;
    _offsets.resize(groups);
    _known.assign(frame_count, 0);
  }

  void Clear() noexcept { _base.clear(); }
  bool Empty() const noexcept { return _base.empty(); }
  uint32_t Count() const noexcept { return _base.back(); }
  uint32_t Base(uint32_t f) const noexcept { return _base[f]; }
  uint32_t Groups(uint32_t f) const noexcept { return _base[f + 1] - _base[f]; }

  uint64_t Offset(PackedReader& lengths, const FrameMeta& m, uint32_t f,
                  uint32_t g) {
    auto* offsets = _offsets.data() + _base[f];
    auto& known = _known[f];
    if (known == 0) {
      offsets[0] = m.comp_off;
      known = 1;
    }
    const uint64_t end = static_cast<uint64_t>(m.comp_off) + m.comp_len;
    for (; known <= g; ++known) {
      const auto from = m.first_entry + (known - 1) * kChainRestart;
      const auto offset =
        offsets[known - 1] + lengths.Sum(from, from + kChainRestart);
      SDB_ENSURE(offset <= end, "col codec: corrupted front coding");
      offsets[known] = offset;
    }
    return offsets[g];
  }

 private:
  std::vector<uint32_t> _base;
  std::vector<uint32_t> _known;
  ScratchVector<uint64_t> _offsets;
};

duckdb::shared_ptr<DecodedDictionary> CachedDictionary(
  const ReadContext::CacheSlot& slot);

Header SegmentHeader(duckdb::const_data_ptr_t base,
                     const ColumnSegment& segment, Shape shape,
                     ByteCodec codec);

NumericHeader CodesHeader(duckdb::const_data_ptr_t base, const Header& h);

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
  const ColumnSegment& segment);

}  // namespace irs::codecs::text
