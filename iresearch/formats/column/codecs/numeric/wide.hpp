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

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_scan.tpp"
#include "iresearch/formats/column/codecs/numeric/numeric_values.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/internal/write_chunk.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs {

inline constexpr uint8_t kWideOffsets = 1;
inline constexpr uint8_t kWideHalves = 2;
inline constexpr size_t kWideSampleEvery = 8;
inline constexpr size_t kWideSampledBlocks = 16;
inline constexpr size_t kWideSampledRows = 64;
inline constexpr unsigned kWideHalvesBits = 113;
inline constexpr size_t kWideRunRows = 8;
inline constexpr uint32_t kMostWideDictionaryEntries = 2048;

struct WideHeader {
  uint8_t form = 0;
  uint8_t reserved[7] = {};
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

constexpr bool StoresHalves(CodecType codec) noexcept {
  return codec == CodecType::Bitpacking || codec == CodecType::Delta ||
         codec == CodecType::Rle || codec == CodecType::Dict;
}

struct WideSample {
  size_t every;
  size_t rows;
};

constexpr WideSample WideSampleOf(size_t rows) noexcept {
  const size_t blocks =
    (rows + numeric::kBlockValues - 1) / numeric::kBlockValues;
  const size_t every =
    std::clamp<size_t>(blocks / kWideSampledBlocks, 1, kWideSampleEvery);
  const size_t sampled = std::max<size_t>((blocks + every - 1) / every, 1);
  return {
    .every = every,
    .rows = std::max(kWideSampledRows, numeric::kBlockValues / sampled),
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
                    std::string_view segment) const {
      part->bytes = segment;
      part->flushed = true;
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
        .chunks = {&chunk, 1},
        .ctx = outer.ctx,
        .flush = keep,
        .params = outer.params,
        .tuning = tuning,
        .disabled = outer.disabled,
        .contested = outer.contested,
        .min_packed_run = outer.min_packed_run,
        .rows = values.size(),
      } {
    duckdb::FlatVector::SetSize(chunk.data, values.size());
    data.disabled.set(std::to_underlying(CodecType::Constant));
  }

  duckdb::LogicalType type{duckdb::LogicalTypeId::UBIGINT};
  WriteChunk chunk;
  ColumnTuning tuning;
  std::string_view bytes;
  bool flushed = false;
  Keep keep{this};
  ColumnDataCheckpointData data;
};

template<typename W>
struct WideValues final : State {
  static constexpr StateId kId = StateId::WideValues;

  explicit WideValues(ColumnDataCheckpointData&) {}

  bool Offers(bool halves, Shape shape, ColumnDataCheckpointData& data) {
    if (!started) {
      started = true;
      SampleChunks(data);
    }
    if (!read) {
      if (sampled && !sample_narrow && !(halves && MayPack(shape, data))) {
        return false;
      }
      if (shape == Shape::Runs && WithoutRuns(data)) {
        return false;
      }
      Read(data);
    }
    return low && (narrow || (high && halves && MayPack(shape, data)));
  }

  duckdb::BaseStatistics Stats(const duckdb::LogicalType& type) const {
    auto stats = duckdb::BaseStatistics::CreateEmpty(type);
    duckdb::StatsWriter<W> writer;
    writer.Update(WideValue<W>(least));
    writer.Update(WideValue<W>(most));
    if (has_null) {
      writer.SetHasNull();
    }
    if (stats.GetStatsType() == duckdb::StatisticsType::NUMERIC_STATS) {
      writer.Merge(stats);
    }
    return stats;
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
  bool sample_few = false;
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
    for (const auto& chunk : data.chunks) {
      const size_t count = chunk.data.size();
      if (chunk.data.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
          !duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
        return false;
      }
      if (count == 0) {
        continue;
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
        if (!counted) {
          containers::FlatHashSet<absl::uint128> distinct;
          distinct.reserve(sample.size());
          for (const auto key : sample) {
            distinct.insert(key);
          }
          const auto most = std::min(
            static_cast<double>(data.rows) / 2,
            static_cast<double>(data.contested
                                  ? kMostWideDictionaryEntries
                                  : std::numeric_limits<uint32_t>::max()));
          sample_few =
            static_cast<double>(distinct.size()) <=
            most * -std::expm1(-static_cast<double>(sample.size()) / most);
          counted = true;
        }
        return sample_few;
    }
    return false;
  }

  bool ReadRuns(ColumnDataCheckpointData& data) {
    const size_t limit = data.rows / kWideRunRows * 2;
    ScratchVector<absl::uint128> keys(limit);
    ScratchVector<uint32_t> rows(limit);
    absl::uint128 last = 0;
    size_t count = 0;
    size_t row = 0;
    for (const auto& chunk : data.chunks) {
      const size_t size = chunk.data.size();
      if (chunk.data.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
          !duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
        return false;
      }
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
    if (count == 0) {
      return false;
    }
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
    lows.resize(data.rows);
    low.emplace(data, lows);
    auto& runs = low->data.template Get<Runs<uint64_t>>();
    runs.values.resize(count);
    rows.resize(count);
    for (size_t r = 0; r < count; ++r) {
      const uint64_t value = absl::Uint128Low64(keys[r] - base);
      runs.values[r] = value;
      std::fill(lows.data() + rows[r],
                lows.data() + (r + 1 < count ? rows[r + 1] : data.rows), value);
    }
    runs.rows = std::move(rows);
    runs.located = true;
    return true;
  }

  bool ReadLowerHalves(ColumnDataCheckpointData& data) {
    lows.resize(data.rows);
    uint64_t upper = 0;
    uint64_t lo = std::numeric_limits<uint64_t>::max();
    uint64_t hi = 0;
    uint64_t last = 0;
    size_t at = 0;
    for (const auto& chunk : data.chunks) {
      if (chunk.data.GetVectorType() != duckdb::VectorType::FLAT_VECTOR) {
        return false;
      }
      const size_t count = chunk.data.size();
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
          has_null |= count != 0;
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
        last = count != 0 ? uint64_t{values[count - 1].lower} : last;
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
    const auto [every, rows] = WideSampleOf(data.rows);
    size_t row = 0;
    for (const auto& chunk : data.chunks) {
      const size_t count = chunk.data.size();
      if (count == 0) {
        continue;
      }
      if (chunk.data.GetVectorType() != duckdb::VectorType::FLAT_VECTOR ||
          !duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
        sample.clear();
        return;
      }
      const W* values = duckdb::FlatVector::GetData<W>(chunk.data);
      for (size_t i = 0; i < count;) {
        const size_t block = (row + i) / numeric::kBlockValues;
        const size_t at = (row + i) % numeric::kBlockValues;
        const size_t stop = std::min(count, i + numeric::kBlockValues - at);
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
    lows.resize(data.rows);
    highs.resize(data.rows);
    ReadHalves(data);
    if (!usable) {
      sampled = true;
      sample_narrow = false;
      return;
    }
    if (!sampled) {
      const auto [every, rows] = WideSampleOf(lows.size());
      for (size_t at = 0; at < lows.size();
           at += every * numeric::kBlockValues) {
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
    for (const auto& chunk : data.chunks) {
      const size_t count = chunk.data.size();
      if (count != 0 &&
          chunk.data.GetVectorType() == duckdb::VectorType::FLAT_VECTOR &&
          duckdb::FlatVector::Validity(chunk.data).CannotHaveNull()) {
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

template<typename W, CodecType kCodec, const CompressionFunction& kInner>
double AnalyzeWide(ColumnDataCheckpointData& data, double bound) {
  auto& wide = data.Get<WideValues<W>>();
  if (!wide.Offers(StoresHalves(kCodec), ShapeOf(kCodec), data)) {
    return kNoScore;
  }
  if (wide.narrow) {
    return kInner.analyze(wide.low->data, bound);
  }
  const auto limit = std::min(bound, static_cast<double>(sizeof(W)));
  constexpr double price = 8.0 * sizeof(W) / kWideHalvesBits;
  const auto low = kInner.analyze(wide.low->data, limit / price);
  if (!(low < limit / price)) {
    return kNoScore;
  }
  const auto score =
    (low + kInner.analyze(wide.high->data, limit / price - low)) * price;
  return score < limit ? score : kNoScore;
}

inline bool CompressPart(const CompressionFunction& inner, WidePart& part) {
  part.flushed = false;
  return inner.compress(part.data) && part.flushed;
}

template<typename W, CodecType kCodec, const CompressionFunction& kInner>
bool CompressWide(ColumnDataCheckpointData& data) {
  auto& wide = data.Get<WideValues<W>>();
  if (!wide.Offers(StoresHalves(kCodec), ShapeOf(kCodec), data) ||
      !CompressPart(kInner, *wide.low)) {
    return false;
  }
  WideHeader header{
    .form = kWideOffsets,
    .base_low = absl::Uint128Low64(wide.base),
    .base_high = absl::Uint128High64(wide.base),
  };
  const auto& low = wide.low->bytes;
  auto& out = wide.out;
  if (wide.narrow) {
    out.resize(sizeof(WideHeader));
    out.append(low);
  } else {
    if (!CompressPart(kInner, *wide.high)) {
      return false;
    }
    header = {.form = kWideHalves, .low_size = low.size()};
    out.assign(sizeof(WideHeader) + duckdb::AlignValue<size_t, 8>(low.size()),
               '\0');
    std::memcpy(out.data() + sizeof(WideHeader), low.data(), low.size());
    out.append(wide.high->bytes);
  }
  std::memcpy(out.data(), &header, sizeof(WideHeader));
  data.FlushSegment(kCodec, wide.Stats(data.type), data.rows, out);
  return true;
}

template<typename W, CodecType kCodec, const CompressionFunction& kInner>
inline constexpr CompressionFunction kWide{
  .analyze = AnalyzeWide<W, kCodec, kInner>,
  .compress = CompressWide<W, kCodec, kInner>,
};

template<typename W, typename R>
class WideReader {
 public:
  using Value = W;

  struct Matches {};

  static constexpr bool kHasRuns = R::kHasRuns;
  static constexpr bool kFiltersRuns = R::kHasRuns;
  static constexpr bool kFiltersEntries = false;

  WideReader(duckdb::const_data_ptr_t base, const ColumnSegment& segment)
    : _header{Header(base, segment.size)},
      _low_frames{Frames(base + sizeof(WideHeader),
                         _header.form == kWideHalves
                           ? _header.low_size
                           : segment.size - sizeof(WideHeader),
                         segment.count, segment.codec)},
      _low{base + sizeof(WideHeader), _low_frames, segment.codec,
           segment.cache_slot} {
    if (_header.form == kWideHalves) {
      const auto at = HighOffset(_header);
      auto high_slot = segment.cache_slot;
      high_slot.file_offset += at;
      _high.emplace(
        base + at,
        Frames(base + at, segment.size - at, segment.count, segment.codec),
        segment.codec, high_slot);
    }
  }

  bool Halves() const noexcept { return _header.form == kWideHalves; }

  FrameHeader LowFrames() const noexcept { return _low_frames; }

  void Read(uint64_t row, uint64_t count, W* out) {
    if (_lows.size() < count) {
      _lows.resize(count);
    }
    if (!_high) {
      if constexpr (R::kHasRuns) {
        ReadRunFrames(row, count, out);
      } else {
        ReadOffsets(row, count, out);
      }
      return;
    }
    _low.Read(row, count, _lows.data());
    if (_highs.size() < count) {
      _highs.resize(count);
    }
    _high->Read(row, count, _highs.data());
    for (uint64_t i = 0; i < count; ++i) {
      out[i] = WideValue<W>(absl::MakeUint128(_highs[i], _lows[i]));
    }
  }

  W Get(uint64_t row) {
    const auto low = _low.Get(row);
    return WideValue<W>(_high ? absl::MakeUint128(_high->Get(row), low)
                              : Base() + low);
  }

  W Fetch(uint64_t row, uint64_t next) {
    const auto low = _low.Fetch(row, next);
    return WideValue<W>(_high ? absl::MakeUint128(_high->Fetch(row, next), low)
                              : Base() + low);
  }

  void FetchRows(const duckdb::idx_t* rows, duckdb::idx_t count, W* out,
                 const duckdb::SelectionVector& sel, duckdb::idx_t offset)
    requires requires(R& half, uint64_t* halves) {
      half.FetchRows(rows, count, halves, sel, offset);
    }
  {
    const duckdb::SelectionVector in_order;
    if (_lows.size() < count) {
      _lows.resize(count);
    }
    _low.FetchRows(rows, count, _lows.data(), in_order, 0);
    if (!_high) {
      const auto base = Base();
      for (duckdb::idx_t i = 0; i < count; ++i) {
        out[offset + sel.get_index(i)] = WideValue<W>(base + _lows[i]);
      }
      return;
    }
    if (_highs.size() < count) {
      _highs.resize(count);
    }
    _high->FetchRows(rows, count, _highs.data(), in_order, 0);
    for (duckdb::idx_t i = 0; i < count; ++i) {
      out[offset + sel.get_index(i)] =
        WideValue<W>(absl::MakeUint128(_highs[i], _lows[i]));
    }
  }

  void ReadOffsets(uint64_t row, uint64_t count, W* out) {
    _low.Read(row, count, _lows.data());
    const auto base = Base();
    for (uint64_t i = 0; i < count; ++i) {
      out[i] = WideValue<W>(base + _lows[i]);
    }
  }

  void ReadRunFrames(uint64_t row, uint64_t count, W* out) {
    const auto base = Base();
    for (uint64_t done = 0; done < count;) {
      const auto at = row + done;
      _low.Seek(at);
      const auto take = std::min<uint64_t>(count - done, _low.FrameEnd() - at);
      if (!_low.LongRuns()) {
        ReadOffsets(at, take, out + done);
        done += take;
        continue;
      }
      const auto runs = _low.ReadRuns(at, take, std::span{_lows.data(), take});
      SDB_ASSERT(runs.count != 0);
      for (uint32_t i = 0; i < runs.count; ++i) {
        const auto rows = _low.RunRows(runs.first + i, at, take);
        std::fill(out + done + rows.begin, out + done + rows.end,
                  WideValue<W>(base + _lows[i]));
      }
      done += take;
    }
  }

  void Seek(uint64_t row) { _low.Seek(row); }

  uint64_t FrameEnd() const noexcept { return _high ? 0 : _low.FrameEnd(); }

  auto ReadRuns(uint64_t row, uint64_t count, std::span<W> values) {
    if (_lows.size() < values.size()) {
      _lows.resize(values.size());
    }
    const auto runs =
      _low.ReadRuns(row, count, std::span{_lows.data(), values.size()});
    const auto base = Base();
    for (uint32_t i = 0; i < runs.count; ++i) {
      values[i] = WideValue<W>(base + _lows[i]);
    }
    return runs;
  }

  auto RunRows(uint32_t run, uint64_t row, uint64_t count) const noexcept {
    return _low.RunRows(run, row, count);
  }

  bool RunHolds(uint64_t row, uint64_t count, W& value) {
    uint64_t low = 0;
    uint64_t high = 0;
    if (!_low.RunHolds(row, count, low) ||
        (_high && !_high->RunHolds(row, count, high))) {
      return false;
    }
    value = WideValue<W>(_high ? absl::MakeUint128(high, low) : Base() + low);
    return true;
  }

  bool ConstantRows(uint64_t row, uint64_t count, W& value)
    requires requires(R& half, uint64_t& part) {
      half.ConstantRows(row, count, part);
    }
  {
    uint64_t low = 0;
    uint64_t high = 0;
    if (!_low.ConstantRows(row, count, low) ||
        (_high && !_high->ConstantRows(row, count, high))) {
      return false;
    }
    value = WideValue<W>(_high ? absl::MakeUint128(high, low) : Base() + low);
    return true;
  }

 private:
  static WideHeader Header(duckdb::const_data_ptr_t base, uint64_t size) {
    if (size < sizeof(WideHeader)) [[unlikely]] {
      ThrowCorrupted("numeric", "segment is smaller than its header");
    }
    const auto header = duckdb::Load<WideHeader>(base);
    if (!(header.form == kWideOffsets ||
          (header.form == kWideHalves && header.low_size <= size &&
           HighOffset(header) <= size))) [[unlikely]] {
      ThrowCorrupted("numeric", "header is inconsistent");
    }
    return header;
  }

  static uint64_t HighOffset(const WideHeader& header) noexcept {
    return sizeof(WideHeader) +
           duckdb::AlignValue<uint64_t, 8>(header.low_size);
  }

  static FrameHeader Frames(duckdb::const_data_ptr_t base, uint64_t size,
                            uint64_t rows, CodecType codec) {
    return ParseFrames(base, size, rows, codec);
  }

  absl::uint128 Base() const noexcept {
    return absl::MakeUint128(_header.base_high, _header.base_low);
  }

  WideHeader _header;
  FrameHeader _low_frames;
  R _low;
  std::optional<R> _high;
  ScratchVector<uint64_t> _lows;
  ScratchVector<uint64_t> _highs;
};

template<typename R>
duckdb::InsertionOrderPreservingMap<std::string> WideSegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  R reader{handle.Ptr(), segment};
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = TransformName(ShapeOf(segment.codec), reader.LowFrames());
  info["wide"] = reader.Halves() ? "halves" : "offsets";
  return info;
}

template<typename R>
inline constexpr DecompressionFunction kWideDecompression{
  .init_scan = numeric::InitScan<R>,
  .scan = numeric::Scan<R>,
  .select = numeric::Select<R>,
  .filter = numeric::Filter<R>,
  .fetch_rows = numeric::FetchRows<R>,
  .get_segment_info = WideSegmentInfo<R>,
};

}  // namespace irs::codecs
