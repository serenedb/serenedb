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

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_writer.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/text/blob.hpp"
#include "iresearch/formats/column/codecs/text/dedup_scratch.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_scan.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/text/values.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs::text::blob {

inline constexpr size_t kSampleFrames = 8;
inline constexpr uint64_t kInflateByteCost = 6;
inline constexpr uint64_t kFrameCopyByteCost = 2;
inline constexpr uint64_t kFrameEntryCost = 72;
inline constexpr uint64_t kPrefixEntryCost = 16;
inline constexpr uint64_t kHitByteCost = 2;
inline constexpr uint64_t kHitCost = 128;
inline constexpr uint8_t kLz4Fast = 1;
inline constexpr uint64_t kLz4TrialRawBytes = 512 * 1024;
inline constexpr uint64_t kLz4TuneMinEntries = 2;
inline constexpr double kLz4EstimateSlack = 0.02;
inline constexpr double kLz4LevelTolerance = 0.02;
inline constexpr double kLz4WideFramesGain = 0.03;
inline constexpr uint64_t kLz4RetuneGrowth = 4;
inline constexpr size_t kLz4MaxLevels = 4;
inline constexpr uint8_t kLz4FlushLevels[] = {1};
inline constexpr uint8_t kLz4MergeLevels[] = {1, 4, 6};

template<typename Codec>
class SizeEstimator {
 public:
  static constexpr ByteCodec kCodec = Codec::kCodec;
  static_assert(!Sorted(kCodec));

  uint64_t EstimateSize(const Profile& p, std::string_view bytes,
                        const FrameSpans& spans, uint64_t codes_bytes,
                        uint8_t level, FrameDictionary layout) {
    ResetCompressor(_codec, _level, level);
    uint64_t data = 0;
    if (!spans.empty()) {
      const bool whole =
        layout == FrameDictionary::Local && spans.size() >= kLocalMinFrames;
      SDB_ASSERT(!whole || bytes.size() == p.raw);
      const auto frame = [&](size_t i) {
        return whole ? bytes.substr(spans[i].offset, spans[i].raw)
                     : Assemble(p, spans[i].first, spans[i].end);
      };
      bool trained = false;
      if (whole) {
        spans.Sizes(_sizes);
        trained = _codec->Train(bytes, _sizes);
      }
      if (trained) {
        data += _codec->Stored().size();
      }
      const auto first = frame(0);
      data += CompressedSize<Codec>(first, _scratch, *_codec);
      if (!trained && spans.Shape().first != 0 && !first.empty() &&
          spans.size() > 1) {
        _codec->Use(first);
      }
      uint64_t sampled_raw = 0;
      uint64_t sampled = 0;
      spans.Sample(
        kSampleFrames, [](size_t j) { return Jitter(j); },
        [&](size_t i) {
          const auto raw = frame(i);
          sampled_raw += raw.size();
          sampled += CompressedSize<Codec>(raw, _scratch, *_codec);
        });
      data += Scale(sampled, sampled_raw, p.raw - spans[0].raw);
    }
    const auto entry_count =
      p.codes.size() + (p.shape == Shape::Dictionary ? 1 : 0);
    Header h;
    return LayOut(h, data, spans.size(), Packed(entry_count, p.max_len), 0,
                  codes_bytes, 0);
  }

 private:
  static double Jitter(uint64_t stratum) noexcept {
    uint64_t z = stratum + 1;
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
    z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
    z ^= z >> 31;
    return static_cast<double>(z >> 11) * 0x1.0p-53;
  }

  std::string_view Assemble(const Profile& p, size_t first, size_t end) {
    _frame.clear();
    for (auto k = first; k < end; ++k) {
      const auto entry = EntryOf(p, k);
      _frame.append(entry.GetData(), entry.GetSize());
    }
    return _frame;
  }

  std::optional<typename Codec::Compressor> _codec;
  uint8_t _level = 0;
  std::string _frame;
  std::string _scratch;
  std::vector<size_t> _sizes;
};

template<typename Rows>
class Lz4CompressionState final : public CompressionState<Lz4, Rows> {
 public:
  using CompressionState<Lz4, Rows>::kShape;

  struct Measured {
    Config config;
    uint64_t bytes = 0;
  };

  Lz4CompressionState(ColumnDataCheckpointData& data, DedupScratch& dedup)
    : CompressionState<Lz4, Rows>{data, dedup}, _tuning{data.tuning.strings} {}

  template<typename Encoder, typename Score>
  Measured Measure(Encoder& encoder, Values& values, WriteTier tier,
                   double bound, Score&& score) {
    auto& tuning = _tuning.lz4[std::to_underlying(kShape)];
    const auto levels = Levels(tier);
    const auto held = Tuned(tier);
    const auto size = [&](const Config& config) {
      return Size(encoder, values, config);
    };
    Measured measured{.config = held};
    const auto input = values.trial.input;
    if (values.TrialEntries() < kLz4TuneMinEntries ||
        (tuning.tuned && input < tuning.tuned_on * kLz4RetuneGrowth)) {
      measured.bytes = size(held);
    } else {
      measured = Tune(levels, tier != WriteTier::Flush, size);
      tuning = {
        .tuned = true,
        .level = measured.config.level,
        .layout = measured.config.layout,
        .tuned_on = input,
      };
    }
    if (!encoder.Find(measured.config)) {
      const auto priced = score(measured.bytes);
      if (measured.config.level <= kLz4Fast
            ? priced <= bound * (1.0 + kLz4EstimateSlack)
            : priced < bound) {
        measured.bytes = encoder.Trial(measured.config, values).Size();
      }
    }
    return measured;
  }

  template<typename Analyze>
  double AnalyzedOnce(Analyze&& analyze) {
    if (!_analyzed) {
      _analyzed = true;
      _score = analyze();
    }
    return _score;
  }

  template<typename Encoder>
  uint64_t Size(Encoder& encoder, Values& values, const Config& config) {
    if (const auto* trial = encoder.Find(config)) {
      return trial->Size();
    }
    if (Lz4::HighCompression(config.level)) {
      return Estimate(values, config);
    }
    const auto& trial = values.trial;
    SDB_ASSERT(config.shape == kShape);
    const auto raw =
      kShape == Shape::Dictionary
        ? std::min<uint64_t>(trial.input, values.TrialEntries() * trial.max_len)
        : trial.input;
    if (raw <= kLz4TrialRawBytes) {
      return encoder.Trial(config, values).Size();
    }
    return Estimate(values, config);
  }

 private:
  static std::span<const uint8_t> Levels(WriteTier tier) noexcept {
    return tier == WriteTier::Flush ? std::span<const uint8_t>{kLz4FlushLevels}
                                    : std::span<const uint8_t>{kLz4MergeLevels};
  }

  Config Tuned(WriteTier tier) const noexcept {
    const auto& tuning = _tuning.lz4[std::to_underlying(kShape)];
    const auto levels = Levels(tier);
    return {
      .shape = kShape,
      .leaf = ByteCodec::Lz4,
      .level = absl::c_linear_search(levels, tuning.level) ? tuning.level
                                                           : levels.front(),
      .layout = tuning.layout,
    };
  }

  template<typename Size>
  static Measured Tune(std::span<const uint8_t> levels, bool local,
                       Size&& size) {
    Config config{.shape = kShape, .leaf = ByteCodec::Lz4};
    uint64_t sizes[kLz4MaxLevels];
    auto smallest = std::numeric_limits<uint64_t>::max();
    for (size_t i = 0; i < levels.size(); ++i) {
      config.level = levels[i];
      sizes[i] = size(config);
      smallest = std::min(smallest, sizes[i]);
    }
    size_t level = 0;
    while (static_cast<double>(sizes[level]) >
           static_cast<double>(smallest) * (1.0 + kLz4LevelTolerance)) {
      ++level;
    }
    config.level = levels[level];
    Measured tuned{.config = config, .bytes = sizes[level]};
    const auto measure_layout = [&](FrameDictionary layout, double min_gain) {
      auto candidate = tuned.config;
      candidate.layout = layout;
      const auto bytes = size(candidate);
      if (static_cast<double>(bytes) <
          static_cast<double>(tuned.bytes) * (1.0 - min_gain)) {
        tuned = {.config = candidate, .bytes = bytes};
      }
    };
    if (local) {
      measure_layout(FrameDictionary::Local, 0.0);
    }
    measure_layout(FrameDictionary::None, kLz4WideFramesGain);
    return tuned;
  }

  uint64_t Estimate(Values& values, const Config& config) {
    auto& rows = values.TrialRows(kShape);
    const auto whole = config.layout == FrameDictionary::Local
                         ? Values::Bytes(rows)
                         : std::string_view{};
    return _estimator.EstimateSize(
      rows.profile, whole,
      Values::Split(rows, FrameShapeOf<Lz4>(config.layout)),
      values.CodesSize(rows), config.level, config.layout);
  }

  StringTuning& _tuning;
  SizeEstimator<Lz4> _estimator;
  bool _analyzed = false;
  double _score = kNoScore;
};

struct Lz4Scan final : Scan<Lz4> {
  using Scan<Lz4>::Scan;

  template<typename Frames, typename Entry>
  void Select(Frames& s, duckdb::Vector& result, duckdb::idx_t result_offset,
              duckdb::idx_t count, Entry entry_at, uint32_t window_first,
              uint32_t window_end) {
    auto writer = duckdb::FlatVector::Writer<duckdb::string_t>(result, count,
                                                               result_offset);
    auto& arena = duckdb::StringVector::GetStringAllocator(result);
    bool referenced = false;
    uint32_t f = kNoFrame;
    for (duckdb::idx_t i = 0; i < count;) {
      auto e = static_cast<uint32_t>(entry_at(i));
      if (f == kNoFrame || e < s.frames[f].first_entry || e >= s.FrameEnd(f)) {
        f = s.FrameOf(e);
      }
      const auto first = s.frames[f].first_entry;
      const auto end = s.FrameEnd(f);
      auto j = i + 1;
      for (; j < count; ++j) {
        const auto next = static_cast<uint32_t>(entry_at(j));
        if (next < e || next >= end) {
          break;
        }
        e = next;
      }
      if (!s.decoded[f]) {
        const uint64_t rows = end - first;
        const uint64_t prefix = e - first + 1;
        const uint64_t hits = j - i;
        const uint64_t entry_bytes = s.frames[f].raw_len / rows;
        const uint64_t frame_byte_cost =
          kInflateByteCost +
          (s.header.flags == FrameDictionary::None ? 0 : kFrameCopyByteCost);
        const bool done =
          end <= window_end ||
          count * uint64_t{end - window_end} < window_end - window_first;
        if (done &&
            prefix * (entry_bytes * kInflateByteCost + kPrefixEntryCost) +
                hits * (entry_bytes * kHitByteCost + kHitCost) <=
              rows * (entry_bytes * frame_byte_cost + kFrameEntryCost)) {
          EmitRows(s, f, i, j, entry_at, arena, writer);
          i = j;
          continue;
        }
        s.DecodeFrame(f);
      }
      referenced = true;
      for (; i < j; ++i) {
        writer.WriteStringRef(s.values[entry_at(i)]);
      }
    }
    if (referenced) {
      s.ReferenceHeap(result);
    }
  }

  template<typename Frames, typename Entry, typename Writer>
  void EmitRows(Frames& s, uint32_t f, duckdb::idx_t i, duckdb::idx_t j,
                Entry entry_at, duckdb::ArenaAllocator& arena, Writer& writer) {
    const auto& m = s.frames[f];
    const auto first = m.first_entry;
    const auto prefix = static_cast<uint32_t>(entry_at(j - 1)) - first + 1;
    if (ends.size() < prefix) {
      ends.resize(prefix);
    }
    s.length_reader.Read(first, prefix, ends.data());
    uint64_t end = 0;
    for (uint32_t k = 0; k < prefix; ++k) {
      end += ends[k];
      ends[k] = static_cast<uint32_t>(end);
    }
    SDB_ENSURE(end <= m.raw_len, "col codec: corrupted entry lengths");
    if (raw.size() < m.raw_len) {
      raw.resize_and_overwrite(m.raw_len, [](char*, size_t n) { return n; });
    }
    const auto got = inflater.Inflate(s.header, s.base, m, f, raw.data(), end);
    SDB_ENSURE(got.size() >= end, "col codec: corrupted frame");
    const auto span = [&](duckdb::idx_t k) {
      const auto at = static_cast<uint32_t>(entry_at(k)) - first;
      const auto begin = at == 0 ? 0 : ends[at - 1];
      return std::string_view{got.data() + begin, ends[at] - begin};
    };
    uint64_t bytes = 0;
    for (auto k = i; k < j; ++k) {
      const auto len = span(k).size();
      bytes += len > duckdb::string_t::INLINE_LENGTH ? len : 0;
    }
    auto* out =
      bytes != 0 ? duckdb::char_ptr_cast(arena.Allocate(bytes)) : nullptr;
    for (auto k = i; k < j; ++k) {
      const auto value = span(k);
      const auto len = static_cast<uint32_t>(value.size());
      if (len <= duckdb::string_t::INLINE_LENGTH) {
        writer.WriteStringRef(duckdb::string_t{value.data(), len});
        continue;
      }
      std::memcpy(out, value.data(), len);
      writer.WriteStringRef(duckdb::string_t{out, len});
      out += len;
    }
  }

  std::string raw;
  std::vector<uint32_t> ends;
};

}  // namespace irs::codecs::text::blob
