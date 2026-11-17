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

#include <absl/functional/function_ref.h>
#include <absl/strings/match.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/text/dedup_scratch.hpp"
#include "iresearch/formats/column/codecs/text/segment.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr uint64_t kEstimateEvery = 64;
inline constexpr uint64_t kEstimateSteps = 16;
inline constexpr uint64_t kDriftMinFraction = 4;

struct SegmentEntries {
  std::span<const duckdb::string_t> entries;
  uint32_t first_entry = 0;
  uint64_t raw = 0;
  uint32_t max_len = 0;
  uint64_t first_row = 0;
  uint64_t row_count = 0;
};

struct SortScratch {
  struct Key {
    uint64_t prefix;
    uint32_t entry;
  };

  std::vector<Key> order;
  std::vector<uint32_t> remap;
  std::vector<duckdb::string_t> sorted;
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
  Shape shape = Shape::Dictionary;
  uint64_t rows = 0;
  uint64_t raw = 0;
  uint32_t max_len = 0;
  std::span<const duckdb::string_t* const> values;
  std::span<const uint32_t> codes;
};

inline duckdb::string_t EntryOf(const Profile& p, size_t i) noexcept {
  const auto code = p.codes[i];
  return code == 0 ? duckdb::string_t{} : *p.values[code - 1];
}

uint64_t CodesBytes(Shape shape, uint64_t rows, uint64_t entries,
                    const NumericTuning& tuning) noexcept;

void CompressCodes(
  std::span<uint32_t> codes, NumericTuning& tuning,
  absl::FunctionRef<void(CodecType codec, std::string_view bytes)> write);

uint64_t EstimateEntries(const SegmentEntries& rows,
                         const RatioHistory& hist) noexcept;

void RecordRatio(RatioHistory& hist, uint64_t raw, uint64_t comp,
                 uint32_t target) noexcept;

class SegmentRows {
 public:
  SegmentRows(const duckdb::LogicalType& type, DedupScratch& dedup,
              NumericTuning& codes_tuning)
    : _stats{type}, _type{type}, _dedup{dedup}, _codes_tuning{codes_tuning} {}

  void Begin(const Config& config, uint64_t first_row, size_t distinct) {
    _shape = config.shape;
    _level = config.level;
    _layout = config.layout;
    _next = first_row;
    _estimated_raw = _raw;
    if (_shape == Shape::Dictionary) {
      _dedup.Begin(distinct);
    }
  }

  template<typename Append>
  IRS_FORCE_INLINE void AddValue(const duckdb::string_t* value,
                                 Append&& append) {
    SDB_ASSERT(_shape == Shape::Plain);
    ++_next;
    if (value) {
      _input += value->GetSize();
      _stats.Update(*value);
      AppendEntry(*value, append);
    } else {
      _stats.SetHasNull();
      ++_nulls;
      AppendEntry(duckdb::string_t{}, append);
    }
    ++_rows;
  }

  template<typename Append>
  IRS_FORCE_INLINE void AddCode(uint32_t code,
                                std::span<const duckdb::string_t* const> values,
                                Append&& append) {
    if (_shape == Shape::Plain) {
      AddValue(code == 0 ? nullptr : values[code - 1], append);
      return;
    }
    ++_next;
    if (code == 0) {
      _stats.SetHasNull();
      ++_nulls;
      _codes.push_back(0);
      ++_rows;
      return;
    }
    const auto& value = *values[code - 1];
    _input += value.GetSize();
    const auto [local, fresh] =
      _dedup.Local(code, static_cast<uint32_t>(_entries.size()) + 1);
    if (fresh) {
      _stats.Update(value);
      AppendEntry(value, append);
    } else {
      _stats.UpdateRepeated(value);
    }
    _codes.push_back(local);
    ++_rows;
  }

  IRS_FORCE_INLINE bool EstimateDue(uint32_t target) noexcept {
    if (_rows % kEstimateEvery != 0 &&
        _raw - _estimated_raw < target / kEstimateSteps) {
      return false;
    }
    _estimated_raw = _raw;
    return true;
  }

  uint64_t Estimate() const noexcept;

  SegmentEntries View() const noexcept {
    return {
      .entries = _entries,
      .first_entry = FirstEntry(),
      .raw = _raw,
      .max_len = _max_len,
      .first_row = _next - _rows,
      .row_count = _rows,
    };
  }

  Shape SegmentShape() const noexcept { return _shape; }

  void Sort(SortScratch& scratch);

  std::span<uint32_t> Lengths() {
    SDB_ASSERT(_rows != 0);
    _lengths.assign(
      duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount()),
      0);
    return std::span{_lengths}.subspan(FirstEntry(), _entries.size());
  }

  void Write(const EncodedFrames& frames, Segment& out);

 private:
  uint32_t FirstEntry() const noexcept {
    return _shape == Shape::Dictionary ? 1 : 0;
  }

  size_t EntryCount() const noexcept { return _entries.size() + FirstEntry(); }

  template<typename Append>
  IRS_FORCE_INLINE void AppendEntry(const duckdb::string_t& value,
                                    Append& append) {
    _entries.emplace_back(value);
    const auto size = value.GetSize();
    _max_len = std::max<uint32_t>(_max_len, size);
    _raw += size;
    append(value, static_cast<uint32_t>(EntryCount()) - 1);
  }

  duckdb::StatsWriter<duckdb::string_t> _stats;
  const duckdb::LogicalType& _type;
  DedupScratch& _dedup;
  NumericTuning& _codes_tuning;
  Shape _shape = Shape::Dictionary;
  uint8_t _level = 0;
  FrameDictionary _layout = FrameDictionary::FirstFrame;
  uint64_t _next = 0;

  std::vector<duckdb::string_t> _entries;
  ScratchVector<uint32_t> _codes;
  uint64_t _rows = 0;
  uint64_t _raw = 0;
  uint64_t _estimated_raw = 0;
  uint32_t _max_len = 0;
  uint64_t _nulls = 0;
  uint64_t _input = 0;
  std::vector<uint32_t> _lengths;
};

}  // namespace irs::codecs::text
