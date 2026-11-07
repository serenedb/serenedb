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
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/segment_buffer.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr uint64_t kEstimateEvery = 64;
inline constexpr uint64_t kEstimateSteps = 16;
inline constexpr uint64_t kDriftMinFraction = 4;

class DedupScratch {
 public:
  struct LocalCode {
    uint32_t local;
    bool fresh;
  };

  void Begin(size_t entries) {
    if (_slots.size() != entries) {
      _slots.assign(entries, Slot{});
    }
    ++_epoch;
  }

  LocalCode Local(uint32_t code, uint32_t next) noexcept {
    auto& slot = _slots[code - 1];
    if (slot.epoch == _epoch) {
      return {slot.local, false};
    }
    slot = {_epoch, next};
    return {next, true};
  }

 private:
  struct Slot {
    uint32_t epoch = 0;
    uint32_t local = 0;
  };

  std::vector<Slot> _slots;
  uint32_t _epoch = 0;
};

struct Config {
  StringChoice choice{};
  uint8_t level = 0;
  FrameLayout layout = FrameLayout::FirstFrame;

  bool operator==(const Config&) const = default;
};

struct Segment {
  explicit Segment(duckdb::BufferManager& buffers) noexcept : bytes{buffers} {}

  Config config;
  uint64_t begin = 0;
  uint64_t rows = 0;
  uint64_t entries = 0;
  uint64_t nulls = 0;
  uint64_t input = 0;
  uint32_t max_len = 0;
  SegmentBuffer bytes;
  std::optional<duckdb::BaseStatistics> stats;

  uint64_t Size() const noexcept { return bytes.Size(); }
};

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
  std::span<const duckdb::string_t* const> values;
  std::span<const uint32_t> codes;
};

inline duckdb::string_t EntryOf(const Profile& p, size_t i) noexcept {
  const auto code = p.codes[i];
  return code == 0 ? duckdb::string_t{} : *p.values[code - 1];
}

uint64_t CodesBytes(Shape shape, uint64_t rows, uint64_t entries, uint64_t runs,
                    const NumericTuning& tuning) noexcept;

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
    _shape = config.choice.shape;
    _level = config.level;
    _layout = config.layout;
    _next = first_row;
    _estimated_raw = _raw;
    if (_shape == Shape::Dedup) {
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
      PushCode(0);
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
    PushCode(local);
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

  void Sort();

  std::span<uint32_t> Lengths() {
    SDB_ASSERT(_rows != 0);
    _lengths.assign(
      duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount()),
      0);
    return std::span{_lengths}.subspan(FirstEntry(), _entries.size());
  }

  void Write(const EncodedFrames& frames, Segment& out);

 private:
  struct SortKey {
    uint64_t prefix;
    uint32_t entry;
  };

  uint32_t FirstEntry() const noexcept {
    return _shape == Shape::Dedup ? 1 : 0;
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

  IRS_FORCE_INLINE void PushCode(uint32_t code) {
    if (_rows == 0 || code != _last_code) {
      ++_runs;
    }
    _last_code = code;
    _codes.push_back(code);
  }

  duckdb::StatsWriter<duckdb::string_t> _stats;
  const duckdb::LogicalType& _type;
  DedupScratch& _dedup;
  NumericTuning& _codes_tuning;
  Shape _shape = Shape::Dedup;
  uint8_t _level = 0;
  FrameLayout _layout = FrameLayout::FirstFrame;
  uint64_t _next = 0;

  std::vector<duckdb::string_t> _entries;
  ScratchVector<uint32_t> _codes;
  uint64_t _rows = 0;
  uint64_t _runs = 0;
  uint32_t _last_code = 0;
  uint64_t _raw = 0;
  uint64_t _estimated_raw = 0;
  uint32_t _max_len = 0;
  uint64_t _nulls = 0;
  uint64_t _input = 0;

  std::vector<SortKey> _order;
  std::vector<uint32_t> _remap;
  std::vector<duckdb::string_t> _sorted;
  std::vector<uint32_t> _lengths;
};

}  // namespace irs::codecs::text
