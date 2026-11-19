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
#include "iresearch/formats/column/codecs/preferences.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/formats/column/internal/write_chunk.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/containers/flat_hash_set.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

inline constexpr uint8_t kWideOffsets = 1;
inline constexpr uint8_t kWideHalves = 2;
inline constexpr size_t kWideSampleEvery = 8;
inline constexpr size_t kWideSampledValues = 4096;

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
  return codec == CodecType::Ffor || codec == CodecType::DeltaFfor ||
         codec == CodecType::RleFfor || codec == CodecType::DictFfor;
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

inline bool HalvesMayPack(std::span<const absl::uint128> keys) {
  const size_t blocks =
    (keys.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
  uint64_t bits = 0;
  uint64_t sampled = 0;
  uint64_t repeats = 0;
  for (size_t b = 0; b < blocks; b += kWideSampleEvery) {
    const auto block = keys.subspan(
      b * numeric::kBlockValues,
      std::min(numeric::kBlockValues, keys.size() - b * numeric::kBlockValues));
    bits += (SampledBits(block, true) + SampledBits(block, false)) *
            uint64_t{block.size()};
    sampled += block.size();
    for (size_t i = 1; i < block.size(); ++i) {
      repeats += block[i] == block[i - 1];
    }
  }
  if (bits * 8 <= sampled * 128 * 7 || repeats * 2 >= sampled) {
    return true;
  }
  const size_t count = std::min(keys.size(), kWideSampledValues);
  const size_t stride = keys.size() / count;
  containers::FlatHashSet<absl::uint128> distinct;
  distinct.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    distinct.insert(keys[i * stride]);
  }
  return distinct.size() * 2 <= count;
}

struct WidePart {
  struct Keep {
    void operator()(const Codec&, duckdb::BaseStatistics, uint64_t,
                    std::string_view segment) const {
      part->bytes.assign(segment);
      part->flushed = true;
    }

    WidePart* part;
  };

  WidePart(ColumnDataCheckpointData& outer, std::span<uint64_t> values)
    : chunk{duckdb::Vector{type,
                           reinterpret_cast<duckdb::data_ptr_t>(values.data()),
                           values.size()},
            values.size()},
      preferences{*outer.preferences},
      data{
        .type = type,
        .chunks = {&chunk, 1},
        .ctx = outer.ctx,
        .flush = keep,
        .params = outer.params,
        .tuning = tuning,
        .preferences = &preferences,
        .disabled = outer.disabled,
        .min_packed_run = outer.min_packed_run,
        .rows = values.size(),
      } {
    duckdb::FlatVector::SetSize(chunk.data, values.size());
    preferences.contest_grown = false;
    data.disabled.set(std::to_underlying(CodecType::Constant));
  }

  duckdb::LogicalType type{duckdb::LogicalTypeId::UBIGINT};
  WriteChunk chunk;
  ColumnTuning tuning;
  Preferences preferences;
  std::string bytes;
  bool flushed = false;
  Keep keep{this};
  ColumnDataCheckpointData data;
};

template<typename W>
struct WideValues final : State {
  static constexpr StateId kId = StateId::WideValues;

  explicit WideValues(ColumnDataCheckpointData& data) {
    ScratchVector<absl::uint128> keys;
    keys.reserve(data.rows);
    absl::uint128 last = 0;
    for (const auto& chunk : data.chunks) {
      for (const auto value : chunk.data.template Values<W>()) {
        if (!value.IsValid()) {
          has_null = true;
          keys.push_back(last);
          continue;
        }
        last = WideKey<W>(value.GetValue());
        if (!usable) {
          absl::c_fill(keys, last);
          least = last;
          most = last;
          usable = true;
        }
        least = std::min(least, last);
        most = std::max(most, last);
        keys.push_back(last);
      }
    }
    if (!usable) {
      return;
    }
    narrow = absl::Uint128High64(most - least) == 0;
    if (!narrow && !HalvesMayPack(keys)) {
      return;
    }
    lows.resize(keys.size());
    if (narrow) {
      for (size_t i = 0; i < keys.size(); ++i) {
        lows[i] = absl::Uint128Low64(keys[i] - least);
      }
      low.emplace(data, lows);
      return;
    }
    highs.resize(keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      lows[i] = absl::Uint128Low64(keys[i]);
      highs[i] = absl::Uint128High64(keys[i]);
    }
    low.emplace(data, lows);
    high.emplace(data, highs);
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
  bool contested = false;
  absl::uint128 least = 0;
  absl::uint128 most = 0;
  ScratchVector<uint64_t> lows;
  ScratchVector<uint64_t> highs;
  std::optional<WidePart> low;
  std::optional<WidePart> high;
  std::string out;
};

template<typename W, const CompressionFunction& kInner>
double AnalyzeWide(ColumnDataCheckpointData& data, double bound) {
  auto& wide = data.Get<WideValues<W>>();
  wide.contested = true;
  if (!wide.low) {
    return kNoScore;
  }
  if (wide.narrow) {
    return kInner.analyze(wide.low->data, bound);
  }
  if (!StoresHalves(kInner.codec)) {
    return kNoScore;
  }
  const auto price = data.preferences->halves_price;
  const auto low = kInner.analyze(wide.low->data, bound / price);
  if (!(low < bound / price)) {
    return kNoScore;
  }
  const auto score =
    (low + kInner.analyze(wide.high->data, bound / price - low)) * price;
  return score < bound ? score : kNoScore;
}

inline bool CompressPart(const CompressionFunction& inner, WidePart& part) {
  part.flushed = false;
  return inner.compress(part.data) && part.flushed;
}

template<typename W, const CompressionFunction& kInner>
bool CompressWide(ColumnDataCheckpointData& data) {
  auto& wide = data.Get<WideValues<W>>();
  if (!wide.low || !CompressPart(kInner, *wide.low)) {
    return false;
  }
  WideHeader header{
    .form = kWideOffsets,
    .base_low = absl::Uint128Low64(wide.least),
    .base_high = absl::Uint128High64(wide.least),
  };
  const auto& low = wide.low->bytes;
  auto& out = wide.out;
  if (wide.narrow) {
    out.resize(sizeof(WideHeader));
    out.append(low);
  } else {
    if (!StoresHalves(kInner.codec) || !CompressPart(kInner, *wide.high)) {
      return false;
    }
    header = {.form = kWideHalves, .low_size = low.size()};
    out.assign(sizeof(WideHeader) + duckdb::AlignValue<size_t, 8>(low.size()),
               '\0');
    std::memcpy(out.data() + sizeof(WideHeader), low.data(), low.size());
    out.append(wide.high->bytes);
  }
  std::memcpy(out.data(), &header, sizeof(WideHeader));
  if (!wide.contested && data.preferences->contest_grown &&
      data.tuning.reuse.Grew(static_cast<double>(out.size()),
                             static_cast<double>(RawInput(data)))) {
    return false;
  }
  data.FlushSegment(kInner.codec, wide.Stats(data.type), data.rows, out);
  return true;
}

template<typename W, const CompressionFunction& kInner>
inline constexpr CompressionFunction kWide{
  .analyze = AnalyzeWide<W, kInner>,
  .compress = CompressWide<W, kInner>,
  .codec = kInner.codec,
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

 private:
  static WideHeader Header(duckdb::const_data_ptr_t base, uint64_t size) {
    SDB_ENSURE(size >= sizeof(WideHeader),
               "numeric codec: segment smaller than its header");
    const auto header = duckdb::Load<WideHeader>(base);
    SDB_ENSURE(header.form == kWideOffsets ||
                 (header.form == kWideHalves && header.low_size <= size &&
                  HighOffset(header) <= size),
               "numeric codec: corrupted segment header");
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
