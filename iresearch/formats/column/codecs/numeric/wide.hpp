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

#include <absl/algorithm/container.h>
#include <absl/numeric/int128.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/helper.hpp>
#include <duckdb/common/hugeint.hpp>
#include <duckdb/common/insertion_order_preserving_map.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/uhugeint.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/numeric/runs.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/internal/write_chunk.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/system_compiler.hpp"

namespace irs::codecs::numeric {

enum class WideForm : uint8_t {
  Offsets = 1,
  Halves = 2,
  Entries = 3,
};

inline constexpr size_t kWideSampleEvery = 8;
inline constexpr size_t kWideSampledBlocks = 16;
inline constexpr size_t kWideSampledRows = 64;
inline constexpr unsigned kWideHalvesBits = 113;
inline constexpr size_t kWideRunRows = 8;
inline constexpr uint32_t kMostWideDictionaryEntries = 2048;

struct WideHeader {
  WideForm form{};
  CodecType codes{};
  uint8_t reserved[2] = {};
  uint32_t entries = 0;
  uint64_t low_size = 0;
  uint64_t base_low = 0;
  uint64_t base_high = 0;
};

static_assert(sizeof(WideHeader) == 32);

template<typename W>
inline constexpr uint64_t kWideBias =
  std::is_signed_v<decltype(W{}.upper)> ? uint64_t{1} << 63 : uint64_t{0};

template<typename W>
constexpr absl::uint128 WideKey(const W& value) noexcept {
  return absl::MakeUint128(static_cast<uint64_t>(value.upper) ^ kWideBias<W>,
                           value.lower);
}

template<typename W>
constexpr W WideValue(absl::uint128 key) noexcept {
  W value;
  value.lower = absl::Uint128Low64(key);
  value.upper =
    static_cast<decltype(value.upper)>(absl::Uint128High64(key) ^ kWideBias<W>);
  return value;
}

template<typename W>
duckdb::BaseStatistics WideStats(const duckdb::LogicalType& type,
                                 const W& least, const W& most, bool has_null) {
  auto stats = duckdb::BaseStatistics::CreateEmpty(type);
  duckdb::StatsWriter<W> writer;
  writer.Update(least);
  writer.Update(most);
  if (has_null) {
    writer.SetHasNull();
  }
  if (stats.GetStatsType() == duckdb::StatisticsType::NUMERIC_STATS) {
    writer.Merge(stats);
  }
  return stats;
}

struct WideSample {
  size_t every;
  size_t rows;
};

constexpr WideSample WideSampleOf(size_t rows) noexcept {
  const size_t blocks = (rows + kBlockValues - 1) / kBlockValues;
  const size_t every =
    std::clamp<size_t>(blocks / kWideSampledBlocks, 1, kWideSampleEvery);
  const size_t sampled = std::max<size_t>((blocks + every - 1) / every, 1);
  return {
    .every = every,
    .rows = std::max(kWideSampledRows, kBlockValues / sampled),
  };
}

inline unsigned SampledBits(std::span<const absl::uint128> block,
                            bool high) noexcept {
  const auto half = [&](size_t i) {
    return high ? absl::Uint128High64(block[i]) : absl::Uint128Low64(block[i]);
  };
  uint64_t lo = half(0);
  uint64_t hi = lo;
  int64_t least = 0;
  int64_t most = 0;
  for (size_t i = 1; i < block.size(); ++i) {
    const auto value = half(i);
    lo = std::min(lo, value);
    hi = std::max(hi, value);
    const auto step = static_cast<int64_t>(value - half(i - 1));
    least = i == 1 ? step : std::min(least, step);
    most = i == 1 ? step : std::max(most, step);
  }
  return static_cast<unsigned>(std::min(
    std::bit_width(hi - lo), std::bit_width(static_cast<uint64_t>(most) -
                                            static_cast<uint64_t>(least))));
}

struct WidePart {
  struct Keep {
    void operator()(const Codec&, duckdb::BaseStatistics, uint64_t,
                    std::string_view segment, std::string_view) const {
      part->bytes = segment;
    }

    WidePart* part;
  };

  WidePart(ColumnDataCheckpointData& outer, std::span<uint64_t> values)
    : chunk{duckdb::Vector{type,
                           reinterpret_cast<duckdb::data_ptr_t>(values.data()),
                           values.size()},
            values.size()},
      data{
        .type = type,
        .flush = keep,
        .params = outer.params,
        .tuning = tuning,
        .contested = outer.contested,
        .rows = {{&chunk, 1}, values.size()},
      } {
    duckdb::FlatVector::SetSize(chunk.data, values.size());
  }

  duckdb::LogicalType type{duckdb::LogicalTypeId::UBIGINT};
  WriteChunk chunk;
  ColumnTuning tuning;
  std::string_view bytes;
  Keep keep{this};
  ColumnDataCheckpointData data;
};

template<typename W>
struct WideValues final : State {
  static constexpr StateId kId = StateId::WideValues;

  explicit WideValues(ColumnDataCheckpointData&) {}

  void Sample(ColumnDataCheckpointData& data) {
    if (!started) {
      started = true;
      SampleChunks(data);
    }
  }

  bool FewInSample(double cap, const ColumnDataCheckpointData& data) {
    if (!counted) {
      containers::FlatHashSet<absl::uint128> distinct;
      distinct.reserve(sample.size());
      for (const auto key : sample) {
        distinct.insert(key);
      }
      sample_distinct = distinct.size();
      counted = true;
    }
    const auto most = std::min(static_cast<double>(data.rows.count) / 2, cap);
    return static_cast<double>(sample_distinct) <=
           most * -std::expm1(-static_cast<double>(sample.size()) / most);
  }

  bool Offers(bool halves, Shape shape, ColumnDataCheckpointData& data) {
    Sample(data);
    if (!read) {
      if (sampled && !sample_narrow && !(halves && MayPack(shape, data))) {
        return false;
      }
      if (shape == Shape::Runs && WithoutRuns(data)) {
        return false;
      }
      Read(data);
    }
    SDB_ASSERT(!low || narrow || high);
    return low && (narrow || (halves && MayPack(shape, data)));
  }

  duckdb::BaseStatistics Stats(const duckdb::LogicalType& type) const {
    return WideStats<W>(type, WideValue<W>(least), WideValue<W>(most),
                        has_null);
  }

  bool usable = false;
  bool narrow = false;
  bool has_null = false;
  bool started = false;
  bool read = false;
  bool sampled = false;
  bool runs_checked = false;
  bool runs_absent = false;
  bool sample_narrow = true;
  bool counted = false;
  uint64_t sample_distinct = 0;
  uint64_t sample_bits = 0;
  uint64_t sample_rows = 0;
  uint64_t sample_pairs = 0;
  uint64_t sample_repeats = 0;
  absl::uint128 base = 0;
  absl::uint128 least = 0;
  absl::uint128 most = 0;
  ScratchVector<absl::uint128> sample;
  ScratchVector<uint64_t> lows;
  ScratchVector<uint64_t> highs;
  std::optional<WidePart> low;
  std::optional<WidePart> high;
  std::string out;

  bool WithoutRuns(ColumnDataCheckpointData& data) {
    if (runs_checked) {
      return runs_absent;
    }
    runs_checked = true;
    bool any = false;
    W last{};
    for (const auto& chunk : data.rows.chunks) {
      const size_t count = chunk.data.size();
      SDB_ASSERT(chunk.data.GetVectorType() ==
                   duckdb::VectorType::FLAT_VECTOR &&
                 count != 0);
      if (!duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
        return false;
      }
      const W* values = duckdb::FlatVector::GetData<W>(chunk.data);
      bool repeats = any && values[0] == last;
      for (size_t i = 1; i < count; ++i) {
        repeats |= values[i] == values[i - 1];
      }
      if (repeats) {
        return false;
      }
      last = values[count - 1];
      any = true;
    }
    runs_absent = any;
    return runs_absent;
  }

  bool MayPack(Shape shape, const ColumnDataCheckpointData& data) {
    switch (shape) {
      case Shape::Plain:
      case Shape::Delta:
        return sample_bits < sample_rows * kWideHalvesBits;
      case Shape::Runs:
        return sample_repeats * 2 >= sample_pairs;
      case Shape::Dictionary:
        return FewInSample(data.contested
                             ? kMostWideDictionaryEntries
                             : std::numeric_limits<uint32_t>::max(),
                           data);
    }
    SDB_UNREACHABLE();
  }

  bool ReadRuns(ColumnDataCheckpointData& data) {
    const size_t limit = data.rows.count / kWideRunRows * 2;
    ScratchVector<absl::uint128> keys(limit);
    ScratchVector<uint32_t> rows(limit);
    absl::uint128 last = 0;
    size_t count = 0;
    size_t row = 0;
    for (const auto& chunk : data.rows.chunks) {
      const size_t size = chunk.data.size();
      SDB_ASSERT(
        chunk.data.GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
        size != 0 && duckdb::FlatVector::Validity(chunk.data).CannotHaveNull());
      const W* values = duckdb::FlatVector::GetData<W>(chunk.data);
      for (size_t i = 0; i < size; ++i) {
        const auto key = WideKey<W>(values[i]);
        if (key == last && count != 0) [[likely]] {
          continue;
        }
        if (count == limit) {
          return false;
        }
        last = key;
        keys[count] = key;
        rows[count++] = static_cast<uint32_t>(row + i);
      }
      row += size;
    }
    SDB_ASSERT(count != 0);
    const std::span<const absl::uint128> found{keys.data(), count};
    const auto [lowest, highest] = absl::c_minmax_element(found);
    if (absl::Uint128High64(*highest - *lowest) != 0) {
      return false;
    }
    usable = true;
    narrow = true;
    least = *lowest;
    most = *highest;
    base = absl::Uint128High64(most) == absl::Uint128High64(least)
             ? absl::MakeUint128(absl::Uint128High64(least), 0)
             : least;
    lows.resize(data.rows.count);
    low.emplace(data, lows);
    ScratchVector<uint64_t> values(count);
    rows.resize(count);
    for (size_t r = 0; r < count; ++r) {
      const uint64_t value = absl::Uint128Low64(keys[r] - base);
      values[r] = value;
      std::fill(lows.data() + rows[r],
                lows.data() + (r + 1 < count ? rows[r + 1] : data.rows.count),
                value);
    }
    low->data.template Get<Values<uint64_t>>(std::span<const uint64_t>{values},
                                             std::span<const uint32_t>{rows});
    low->data.template Get<Runs<uint64_t>>(std::move(values), std::move(rows));
    return true;
  }

  bool ReadLowerHalves(ColumnDataCheckpointData& data) {
    lows.resize(data.rows.count);
    uint64_t upper = 0;
    uint64_t lo = std::numeric_limits<uint64_t>::max();
    uint64_t hi = 0;
    uint64_t last = 0;
    size_t at = 0;
    for (const auto& chunk : data.rows.chunks) {
      const size_t count = chunk.data.size();
      SDB_ASSERT(chunk.data.GetVectorType() ==
                   duckdb::VectorType::FLAT_VECTOR &&
                 count != 0);
      const W* values = duckdb::FlatVector::GetData<W>(chunk.data);
      const auto& validity = duckdb::FlatVector::Validity(chunk.data);
      const bool all_valid = validity.CannotHaveNull();
      size_t first = 0;
      if (!usable) {
        while (first < count && !all_valid &&
               !validity.RowIsValidUnsafe(first)) {
          ++first;
        }
        if (first == count) {
          has_null = true;
          at += count;
          continue;
        }
        upper = static_cast<uint64_t>(values[first].upper);
        last = values[first].lower;
        std::fill_n(lows.data(), at + first, last);
        has_null |= at + first != 0;
        usable = true;
      }
      uint64_t differs = 0;
      if (all_valid) {
        for (size_t i = first; i < count; ++i) {
          const uint64_t lower = values[i].lower;
          differs |= static_cast<uint64_t>(values[i].upper) ^ upper;
          lo = std::min(lo, lower);
          hi = std::max(hi, lower);
          lows[at + i] = lower;
        }
        last = values[count - 1].lower;
      } else {
        bool nulls = false;
        for (size_t i = first; i < count; ++i) {
          const bool valid = validity.RowIsValidUnsafe(i);
          nulls |= !valid;
          differs |= valid ? static_cast<uint64_t>(values[i].upper) ^ upper : 0;
          last = valid ? uint64_t{values[i].lower} : last;
          lo = std::min(lo, last);
          hi = std::max(hi, last);
          lows[at + i] = last;
        }
        has_null |= nulls;
      }
      if (differs != 0) {
        return false;
      }
      at += count;
    }
    if (!usable) {
      return true;
    }
    const uint64_t key = upper ^ kWideBias<W>;
    narrow = true;
    base = absl::MakeUint128(key, 0);
    least = absl::MakeUint128(key, lo);
    most = absl::MakeUint128(key, hi);
    low.emplace(data, lows);
    return true;
  }

  void SampleChunks(ColumnDataCheckpointData& data) {
    const auto [every, rows] = WideSampleOf(data.rows.count);
    size_t row = 0;
    for (const auto& chunk : data.rows.chunks) {
      const size_t count = chunk.data.size();
      SDB_ASSERT(chunk.data.GetVectorType() ==
                   duckdb::VectorType::FLAT_VECTOR &&
                 count != 0);
      if (!duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
        sample.clear();
        return;
      }
      const W* values = duckdb::FlatVector::GetData<W>(chunk.data);
      for (size_t i = 0; i < count;) {
        const size_t block = (row + i) / kBlockValues;
        const size_t at = (row + i) % kBlockValues;
        const size_t stop = std::min(count, i + kBlockValues - at);
        if (block % every == 0 && at < rows) {
          for (size_t k = i, end = std::min(stop, i + rows - at); k < end;
               ++k) {
            sample.push_back(WideKey<W>(values[k]));
          }
        }
        i = stop;
      }
      row += count;
    }
    if (!sample.empty()) {
      SampleKeys(rows);
    }
  }

  void SampleKeys(size_t rows) {
    const std::span<const absl::uint128> keys{sample};
    auto lo = keys[0];
    auto hi = keys[0];
    for (size_t at = 0; at < keys.size(); at += rows) {
      const auto block = keys.subspan(at, std::min(rows, keys.size() - at));
      sample_bits += (SampledBits(block, true) + SampledBits(block, false)) *
                     uint64_t{block.size()};
      sample_rows += block.size();
      sample_pairs += block.size() - 1;
      lo = std::min(lo, block[0]);
      hi = std::max(hi, block[0]);
      for (size_t i = 1; i < block.size(); ++i) {
        sample_repeats += block[i] == block[i - 1];
        lo = std::min(lo, block[i]);
        hi = std::max(hi, block[i]);
      }
    }
    sample_narrow = absl::Uint128High64(hi - lo) == 0;
    sampled = true;
  }

  void Read(ColumnDataCheckpointData& data) {
    read = true;
    if (sampled && sample_narrow &&
        sample_repeats * kWideRunRows >= sample_pairs * (kWideRunRows - 1) &&
        ReadRuns(data)) {
      return;
    }
    if ((!sampled || sample_narrow) && ReadLowerHalves(data)) {
      return;
    }
    usable = false;
    has_null = false;
    lows.resize(data.rows.count);
    highs.resize(data.rows.count);
    ReadHalves(data);
    SDB_ASSERT(usable);
    if (!sampled) {
      const auto [every, rows] = WideSampleOf(lows.size());
      for (size_t at = 0; at < lows.size(); at += every * kBlockValues) {
        for (size_t i = at, stop = std::min(lows.size(), at + rows); i < stop;
             ++i) {
          sample.push_back(absl::MakeUint128(highs[i], lows[i]));
        }
      }
      SampleKeys(rows);
    }
    narrow = absl::Uint128High64(most - least) == 0;
    if (narrow) {
      base = least;
      const uint64_t lowest = absl::Uint128Low64(least);
      for (auto& lower : lows) {
        lower -= lowest;
      }
      low.emplace(data, lows);
      return;
    }
    if (!MayPack(Shape::Plain, data) && !MayPack(Shape::Runs, data) &&
        !MayPack(Shape::Dictionary, data)) {
      return;
    }
    low.emplace(data, lows);
    high.emplace(data, highs);
  }

  void ReadHalves(ColumnDataCheckpointData& data) {
    uint64_t lo_upper = 0;
    uint64_t lo_lower = 0;
    uint64_t hi_upper = 0;
    uint64_t hi_lower = 0;
    uint64_t last_upper = 0;
    uint64_t last_lower = 0;
    size_t at = 0;
    const auto start = [&](uint64_t upper, uint64_t lower) {
      if (usable) {
        return;
      }
      std::fill_n(highs.data(), at, upper);
      std::fill_n(lows.data(), at, lower);
      lo_upper = hi_upper = upper;
      lo_lower = hi_lower = lower;
      usable = true;
    };
    const auto take = [&](uint64_t upper, uint64_t lower) {
      const bool below =
        upper < lo_upper || (upper == lo_upper && lower < lo_lower);
      const bool above =
        upper > hi_upper || (upper == hi_upper && lower > hi_lower);
      lo_upper = below ? upper : lo_upper;
      lo_lower = below ? lower : lo_lower;
      hi_upper = above ? upper : hi_upper;
      hi_lower = above ? lower : hi_lower;
    };
    for (const auto& chunk : data.rows.chunks) {
      const size_t count = chunk.data.size();
      SDB_ASSERT(chunk.data.GetVectorType() ==
                   duckdb::VectorType::FLAT_VECTOR &&
                 count != 0);
      if (duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
        const W* values = duckdb::FlatVector::GetData<W>(chunk.data);
        start(static_cast<uint64_t>(values[0].upper) ^ kWideBias<W>,
              values[0].lower);
        auto* upper_out = highs.data() + at;
        auto* lower_out = lows.data() + at;
        for (size_t i = 0; i < count; ++i) {
          const uint64_t upper =
            static_cast<uint64_t>(values[i].upper) ^ kWideBias<W>;
          const uint64_t lower = values[i].lower;
          take(upper, lower);
          upper_out[i] = upper;
          lower_out[i] = lower;
        }
        last_upper = upper_out[count - 1];
        last_lower = lower_out[count - 1];
        at += count;
        continue;
      }
      for (const auto value : chunk.data.template Values<W>()) {
        if (value.IsValid()) {
          const auto key = WideKey<W>(value.GetValue());
          last_upper = absl::Uint128High64(key);
          last_lower = absl::Uint128Low64(key);
          start(last_upper, last_lower);
          take(last_upper, last_lower);
        } else {
          has_null = true;
        }
        highs[at] = last_upper;
        lows[at++] = last_lower;
      }
    }
    least = absl::MakeUint128(lo_upper, lo_lower);
    most = absl::MakeUint128(hi_upper, hi_lower);
  }
};

template<typename W>
struct WideEntries final : State {
  static constexpr StateId kId = StateId::WideEntries;

  explicit WideEntries(ColumnDataCheckpointData& data) {
    SDB_IF_FAILURE("irs::WideDictionaryOfHalves") { return; }
    ScratchVector<uint32_t> codes;
    fits = Few(data) && Collect(data, codes);
    if (fits) {
      Pack(codes);
    }
  }

  duckdb::BaseStatistics Stats(const duckdb::LogicalType& type) const {
    return WideStats<W>(type, values.front(), values.back(), has_null);
  }

  bool fits = false;
  bool has_null = false;
  std::vector<W> values;
  std::string out;

 private:
  static uint32_t Cap(const ColumnDataCheckpointData& data) noexcept {
    return static_cast<uint32_t>(
      std::min<uint64_t>(kMostWideDictionaryEntries, data.rows.count / 2));
  }

  static bool Few(ColumnDataCheckpointData& data) {
    auto& wide = data.Get<WideValues<W>>();
    wide.Sample(data);
    return !wide.sampled ||
           (!wide.sample_narrow && wide.FewInSample(Cap(data), data));
  }

  bool Collect(const ColumnDataCheckpointData& data,
               ScratchVector<uint32_t>& codes) {
    const auto cap = Cap(data);
    containers::FlatHashMap<absl::uint128, uint32_t> index;
    std::vector<absl::uint128> keys;
    codes.resize(data.rows.count);
    uint64_t at = 0;
    absl::uint128 last = 0;
    uint32_t code = 0;
    for (const auto& chunk : data.rows.chunks) {
      const size_t count = chunk.data.size();
      const W* rows = duckdb::FlatVector::GetData<W>(chunk.data);
      const auto& validity = duckdb::FlatVector::Validity(chunk.data);
      const bool all_valid = validity.CannotHaveNull();
      for (size_t i = 0; i < count; ++i, ++at) {
        if (!all_valid && !validity.RowIsValidUnsafe(i)) {
          has_null = true;
        } else if (const auto key = WideKey<W>(rows[i]);
                   keys.empty() || key != last) {
          const auto [it, added] =
            index.try_emplace(key, static_cast<uint32_t>(keys.size()));
          if (added) {
            if (keys.size() == cap) {
              return false;
            }
            keys.push_back(key);
          }
          code = it->second;
          last = key;
        }
        codes[at] = code;
      }
    }
    if (keys.empty()) {
      return false;
    }
    absl::c_sort(keys);
    if (absl::Uint128High64(keys.back() - keys.front()) == 0) {
      return false;
    }
    ScratchVector<uint32_t> ranks(keys.size());
    values.resize(keys.size());
    for (uint32_t k = 0; k < keys.size(); ++k) {
      ranks[index.find(keys[k])->second] = k;
      values[k] = WideValue<W>(keys[k]);
    }
    for (auto& c : codes) {
      c = ranks[c];
    }
    return true;
  }

  void Pack(ScratchVector<uint32_t>& codes) {
    WideHeader header{
      .form = WideForm::Entries,
      .entries = static_cast<uint32_t>(values.size()),
    };
    out.resize(sizeof(WideHeader) + values.size() * sizeof(W));
    WriteChunk chunk{
      duckdb::Vector{duckdb::LogicalType::UINTEGER,
                     reinterpret_cast<duckdb::data_ptr_t>(codes.data()),
                     codes.size()},
      codes.size()};
    duckdb::FlatVector::SetSize(chunk.data, codes.size());
    ColumnTuning tuning;
    const auto flush = [&](const Codec& codec, duckdb::BaseStatistics, uint64_t,
                           std::string_view bytes, std::string_view) {
      header.codes = codec.type;
      out.append(bytes);
    };
    ColumnDataCheckpointData nested{
      .type = chunk.data.GetType(),
      .flush = flush,
      .tuning = tuning,
      .rows = {{&chunk, 1}, codes.size()},
    };
    CompressAutoCodes(nested);
    SDB_ASSERT(header.codes == CodecType::Bitpacking ||
               header.codes == CodecType::Rle);
    std::memcpy(out.data(), &header, sizeof(WideHeader));
    std::memcpy(out.data() + sizeof(WideHeader), values.data(),
                values.size() * sizeof(W));
  }
};

}  // namespace irs::codecs::numeric
