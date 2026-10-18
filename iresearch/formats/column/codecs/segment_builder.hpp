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
#include <cstdint>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector_size.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/frame_meta.hpp"
#include "iresearch/formats/column/codecs/string_choice.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_writer.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr uint64_t kEstimateEvery = 64;
inline constexpr uint64_t kEstimateSteps = 16;
inline constexpr uint64_t kDriftMinFraction = 4;

uint64_t Packed(uint64_t count, uint32_t max_value) noexcept;

struct CodesPlan {
  CodesEncoding encoding = CodesEncoding::Bitpack;
  uint8_t code_width = 0;
  uint8_t run_width = 0;
  uint64_t codes_count = 0;
  uint64_t run_count = 0;
  uint64_t bytes = 0;
};

CodesPlan PlanCodes(Shape shape, uint64_t rows, uint64_t entries,
                    uint64_t runs) noexcept;

void LayOut(Header& h, uint64_t frames, uint64_t lengths_bytes,
            uint64_t lcps_bytes, const CodesPlan& codes,
            uint64_t symtab_bytes) noexcept;

void Record(RatioHistory& hist, uint64_t raw, uint64_t comp,
            uint32_t target) noexcept;

class DedupScratch {
 public:
  void Begin(size_t entries) {
    if (_slots.size() != entries) {
      _slots.assign(entries, Slot{});
    }
    ++_epoch;
  }

  std::pair<uint32_t, bool> Local(uint32_t code, uint32_t next) noexcept {
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

struct Segment {
  StringChoice choice{};
  uint8_t level = 0;
  FrameLayout layout = FrameLayout::Dictionary;
  uint64_t begin = 0;
  uint64_t rows = 0;
  uint64_t entries = 0;
  uint64_t nulls = 0;
  uint64_t input = 0;
  uint32_t max_len = 0;
  std::string head;
  std::string data;
  std::vector<uint32_t> codes;
  std::optional<duckdb::BaseStatistics> stats;

  uint64_t Size() const noexcept { return head.size() + data.size(); }
};

struct Profile {
  Shape shape = Shape::Dedup;
  uint64_t rows = 0;
  uint64_t runs = 0;
  uint64_t raw = 0;
  uint32_t max_len = 0;
  std::vector<std::string_view> entries;
};

struct EncodedFrames {
  ByteCodec codec = ByteCodec::Lz4;
  std::span<const FrameMeta> frames;
  uint32_t max_length = 0;
  std::span<uint32_t> lcps;
  uint32_t max_lcp = 0;
  std::string_view symtab;
  uint8_t flags = 0;
  uint16_t dictionary = 0;
};

class SegmentBuilder {
 public:
  explicit SegmentBuilder(const StringAccumulator& acc,
                          const duckdb::LogicalType& type, DedupScratch& dedup)
    : _acc{acc}, _stats{type}, _type{type}, _dedup{dedup} {}

  void Begin(Shape shape, uint8_t level, FrameLayout layout, uint64_t begin);

  template<typename Leaf>
  uint64_t Cut(Leaf& leaf, uint32_t target) {
    const uint64_t step = target / kEstimateSteps;
    uint64_t estimated_raw = _raw;
    while (_next < _acc.row_count) {
      AddRow(leaf, _next++);
      if (_rows % kEstimateEvery != 0 && _raw - estimated_raw < step) {
        continue;
      }
      estimated_raw = _raw;
      if (Estimate() + leaf.Estimate(*this) >= target) {
        break;
      }
    }
    return _next;
  }

  template<typename Leaf>
  void AddUntil(Leaf& leaf, uint64_t end) {
    while (_next < end) {
      AddRow(leaf, _next++);
    }
  }

  template<typename Leaf>
  void Finish(Leaf& leaf, Segment& out) {
    SDB_ASSERT(_rows != 0);
    if (_shape == Shape::Dedup) {
      _codes.resize(_rows);
      if constexpr (Leaf::kSorted) {
        SortEntries();
      }
    }
    _lengths.assign(GroupPadded(_entries.size() + FirstEntry()), 0);
    const auto frames = leaf.Finish(
      *this, std::span{_lengths}.subspan(FirstEntry(), _entries.size()));
    WriteSegment(frames, leaf.Data(), out);
  }

  uint32_t FirstEntry() const noexcept {
    return _shape == Shape::Dedup ? 1 : 0;
  }

  std::span<const std::string_view> Entries() const noexcept {
    return _entries;
  }

  uint64_t Raw() const noexcept { return _raw; }

  uint32_t MaxLen() const noexcept { return _max_len; }

 private:
  template<typename Leaf>
  IRS_FORCE_INLINE void AddRow(Leaf& leaf, uint64_t row) {
    const auto code = _acc.codes[row];
    if (code == 0) {
      _stats.SetHasNull();
      ++_nulls;
      if (_shape == Shape::Dedup) {
        PushCode(0);
      } else {
        AppendEntry(leaf, {});
      }
      ++_rows;
      return;
    }
    const auto sv = _acc.entries[code - 1];
    _input += sv.size();
    const duckdb::string_t value{sv.data(), static_cast<uint32_t>(sv.size())};
    if (_shape == Shape::Plain) {
      _stats.Update(value);
      AppendEntry(leaf, sv);
      ++_rows;
      return;
    }
    const auto [local, fresh] =
      _dedup.Local(code, static_cast<uint32_t>(_entries.size()) + 1);
    if (fresh) {
      _stats.Update(value);
      AppendEntry(leaf, sv);
    } else {
      _stats.UpdateRepeated(value);
    }
    PushCode(local);
    ++_rows;
  }

  template<typename Leaf>
  IRS_FORCE_INLINE void AppendEntry(Leaf& leaf, std::string_view sv) {
    _entries.push_back(sv);
    _max_len = std::max<uint32_t>(_max_len, static_cast<uint32_t>(sv.size()));
    _raw += sv.size();
    leaf.Append(sv, FirstEntry() + static_cast<uint32_t>(_entries.size()) - 1);
  }

  IRS_FORCE_INLINE void PushCode(uint32_t code) {
    if (_rows == 0 || code != _last_code) {
      ++_runs;
    }
    _last_code = code;
    if (_rows == _codes.size()) {
      _codes.resize(std::max<size_t>(2 * _rows, STANDARD_VECTOR_SIZE));
    }
    _codes[_rows] = code;
  }

  uint64_t Estimate() const noexcept;
  void SortEntries();
  void WriteSegment(const EncodedFrames& frames, std::string& data,
                    Segment& out);

  const StringAccumulator& _acc;
  duckdb::StatsWriter<duckdb::string_t> _stats;
  duckdb::LogicalType _type;
  DedupScratch& _dedup;
  Shape _shape = Shape::Dedup;
  uint8_t _level = 0;
  FrameLayout _layout = FrameLayout::Dictionary;
  uint64_t _next = 0;

  std::vector<std::string_view> _entries;
  std::vector<uint32_t> _codes;
  uint64_t _rows = 0;
  uint64_t _runs = 0;
  uint32_t _last_code = 0;
  uint64_t _raw = 0;
  uint32_t _max_len = 0;
  uint64_t _nulls = 0;
  uint64_t _input = 0;

  std::vector<std::pair<uint64_t, uint32_t>> _order;
  std::vector<uint32_t> _remap;
  std::vector<std::string_view> _sorted;
  std::vector<uint32_t> _lengths;
  std::vector<uint32_t> _run_values;
  std::vector<uint32_t> _run_ends;
  std::string _bytes;
};

}  // namespace irs::codecs::text
