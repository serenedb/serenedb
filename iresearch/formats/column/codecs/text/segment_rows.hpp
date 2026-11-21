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
  std::string_view shared;
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
                    double ratio) noexcept;

void CompressCodes(
  std::span<uint32_t> codes, double& ratio,
  absl::FunctionRef<void(CodecType codec, std::string_view bytes)> write);

uint64_t EstimateEntries(const SegmentEntries& rows,
                         const RatioHistory& hist) noexcept;

void RecordRatio(RatioHistory& hist, uint64_t raw, uint64_t comp,
                 uint32_t target) noexcept;

class SegmentRows {
 public:
  IRS_FORCE_INLINE bool EstimateDue(uint32_t target) noexcept {
    if (_rows % kEstimateEvery != 0 &&
        _raw - _estimated_raw < target / kEstimateSteps) {
      return false;
    }
    _estimated_raw = _raw;
    return true;
  }

  SegmentEntries View() const noexcept {
    return {
      .entries = _entries,
      .first_entry = _first_entry,
      .raw = _raw,
      .max_len = _max_len,
      .first_row = _next - _rows,
      .row_count = _rows,
    };
  }

  std::span<uint32_t> Lengths() {
    SDB_ASSERT(_rows != 0);
    _lengths.assign(
      duckdb::BitpackingPrimitives::RoundUpToAlgorithmGroupSize(EntryCount()),
      0);
    return std::span{_lengths}.subspan(_first_entry, _entries.size());
  }

 protected:
  SegmentRows(const duckdb::LogicalType& type, uint32_t first_entry)
    : _stats{type}, _type{type}, _first_entry{first_entry} {}

  void Begin(const Config& config, uint64_t first_row) {
    _level = config.level;
    _layout = config.layout;
    _next = first_row;
    _estimated_raw = _raw;
  }

  size_t EntryCount() const noexcept { return _entries.size() + _first_entry; }

  template<typename Append>
  IRS_FORCE_INLINE void AppendEntry(const duckdb::string_t& value,
                                    Append& append) {
    _entries.emplace_back(value);
    const auto size = value.GetSize();
    _max_len = std::max<uint32_t>(_max_len, size);
    _raw += size;
    append(value, static_cast<uint32_t>(EntryCount()) - 1);
  }

  void Assemble(const EncodedFrames& frames, CodecType codes_codec,
                std::string_view codes, Segment& out);

  void Finish(const EncodedFrames& frames, Shape shape, Segment& out);

  duckdb::StatsWriter<duckdb::string_t> _stats;
  const duckdb::LogicalType& _type;
  uint32_t _first_entry;
  uint8_t _level = 0;
  FrameDictionary _layout = FrameDictionary::FirstFrame;
  uint64_t _next = 0;

  std::vector<duckdb::string_t> _entries;
  uint64_t _rows = 0;
  uint64_t _raw = 0;
  uint64_t _estimated_raw = 0;
  uint32_t _max_len = 0;
  uint64_t _nulls = 0;
  uint64_t _input = 0;
  std::vector<uint32_t> _lengths;
};

class PlainRows final : public SegmentRows {
 public:
  static constexpr Shape kShape = Shape::Plain;

  explicit PlainRows(const duckdb::LogicalType& type) : SegmentRows{type, 0} {}

  void Begin(const Config& config, uint64_t first_row, size_t) {
    SegmentRows::Begin(config, first_row);
  }

  template<typename Append>
  IRS_FORCE_INLINE void AddValue(const duckdb::string_t* value,
                                 Append&& append) {
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
    AddValue(code == 0 ? nullptr : values[code - 1], append);
  }

  uint64_t Estimate() const noexcept;

  void Write(const EncodedFrames& frames, Segment& out);
};

class DictionaryRows final : public SegmentRows {
 public:
  static constexpr Shape kShape = Shape::Dictionary;

  DictionaryRows(const duckdb::LogicalType& type, DedupScratch& dedup,
                 double& codes_ratio)
    : SegmentRows{type, 1}, _dedup{dedup}, _codes_ratio{codes_ratio} {}

  void Begin(const Config& config, uint64_t first_row, size_t distinct) {
    SegmentRows::Begin(config, first_row);
    _dedup.Begin(distinct);
  }

  template<typename Append>
  IRS_FORCE_INLINE void AddCode(uint32_t code,
                                std::span<const duckdb::string_t* const> values,
                                Append&& append) {
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

  uint64_t Estimate() const noexcept;

  void Sort(SortScratch& scratch);

  void Write(const EncodedFrames& frames, Segment& out);

 private:
  DedupScratch& _dedup;
  double& _codes_ratio;
  ScratchVector<uint32_t> _codes;
};

template<typename Rows>
Rows MakeRows(const duckdb::LogicalType& type, DedupScratch& dedup,
              double& codes_ratio) {
  if constexpr (Rows::kShape == Shape::Dictionary) {
    return Rows{type, dedup, codes_ratio};
  } else {
    return Rows{type};
  }
}

template<typename Rows>
class SortedCompressionState {
 public:
  static constexpr Shape kShape = Rows::kShape;

  SortedCompressionState(ByteCodec codec, const duckdb::LogicalType& type,
                         DedupScratch& dedup, StringTuning& tuning,
                         uint32_t target)
    : _rows{MakeRows<Rows>(type, dedup, tuning.codes_ratio)},
      _history{tuning.history[static_cast<size_t>(codec)]},
      _target{target} {}

  void InitCompression(const Config& config, uint64_t first_row,
                       size_t distinct, Segment& out) {
    _rows.Begin(config, first_row, distinct);
    _out = &out;
    out.bytes.Truncate(0);
  }

  IRS_FORCE_INLINE void Compress(const duckdb::string_t* value) {
    _rows.AddValue(value, [](const duckdb::string_t&, uint32_t) {});
  }

  IRS_FORCE_INLINE void Compress(
    uint32_t code, std::span<const duckdb::string_t* const> values) {
    _rows.AddCode(code, values, [](const duckdb::string_t&, uint32_t) {});
  }

  IRS_FORCE_INLINE bool Full(uint32_t target) {
    return _rows.EstimateDue(target) &&
           _rows.Estimate() + EstimateEntries(_rows.View(), History()) >=
             target;
  }

 protected:
  void Sort() {
    if constexpr (kShape == Shape::Dictionary) {
      _rows.Sort(_sort);
    }
  }

  RatioHistory& History() const noexcept {
    return _history[static_cast<size_t>(kShape)];
  }

  uint64_t Written() const noexcept { return _out->bytes.Size(); }

  Rows _rows;
  SortScratch _sort;
  RatioHistory* _history;
  uint32_t _target;
  Segment* _out = nullptr;
  std::vector<FrameMeta> _frames;
};

}  // namespace irs::codecs::text
