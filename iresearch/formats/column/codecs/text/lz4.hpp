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
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/text/values.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"

namespace irs::codecs::text::blob {

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

class Lz4CompressionState {
 public:
  static constexpr ByteCodec kCodec = ByteCodec::Lz4;

  struct Measured {
    Config config;
    uint64_t bytes = 0;
  };

  Lz4CompressionState(const duckdb::LogicalType& type, DedupScratch& dedup,
                      StringTuning& tuning)
    : _type{type}, _dedup{dedup}, _tuning{tuning} {}

  template<typename Visit>
  void With(const Config& config, Visit&& visit) {
    if (Lz4Hc::Serves(config.level)) {
      visit(_high ? *_high : _high.emplace(_type, _dedup, _tuning));
    } else {
      visit(_fast ? *_fast : _fast.emplace(_type, _dedup, _tuning));
    }
  }

  template<typename Encoder, typename Score>
  Measured Measure(Encoder& encoder, Values& values, Shape shape,
                   WriteTier tier, double bound, Score&& score) {
    auto& tuning = _tuning.lz4[std::to_underlying(shape)];
    const auto levels = Levels(tier);
    const auto held = Tuned(shape, tier);
    const auto size = [&](const Config& config) {
      return Size(encoder, values, config);
    };
    Measured measured{.config = held};
    const auto input = values.trial.input;
    if (values.TrialEntries() < kLz4TuneMinEntries ||
        (tuning.tuned && input < tuning.tuned_on * kLz4RetuneGrowth)) {
      measured.bytes = size(held);
    } else {
      measured = Tune(shape, levels, tier != WriteTier::Flush, size);
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

  template<typename Encoder>
  uint64_t Size(Encoder& encoder, Values& values, const Config& config) {
    if (const auto* trial = encoder.Find(config)) {
      return trial->Size();
    }
    if (Lz4Hc::Serves(config.level)) {
      return Estimate(_high_estimator, values, config);
    }
    const auto& trial = values.trial;
    const auto raw =
      config.shape == Shape::Dictionary
        ? std::min<uint64_t>(trial.input, values.TrialEntries() * trial.max_len)
        : trial.input;
    if (raw <= kLz4TrialRawBytes) {
      return encoder.Trial(config, values).Size();
    }
    return Estimate(_fast_estimator, values, config);
  }

 private:
  static std::span<const uint8_t> Levels(WriteTier tier) noexcept {
    return tier == WriteTier::Flush ? std::span<const uint8_t>{kLz4FlushLevels}
                                    : std::span<const uint8_t>{kLz4MergeLevels};
  }

  Config Tuned(Shape shape, WriteTier tier) const noexcept {
    const auto& tuning = _tuning.lz4[std::to_underlying(shape)];
    const auto levels = Levels(tier);
    return {
      .shape = shape,
      .leaf = ByteCodec::Lz4,
      .level = absl::c_linear_search(levels, tuning.level) ? tuning.level
                                                           : levels.front(),
      .layout = tuning.layout,
    };
  }

  template<typename Size>
  static Measured Tune(Shape shape, std::span<const uint8_t> levels, bool local,
                       Size&& size) {
    Config config{.shape = shape, .leaf = ByteCodec::Lz4};
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

  template<typename Codec>
  static uint64_t Estimate(std::optional<SizeEstimator<Codec>>& estimator,
                           Values& values, const Config& config) {
    auto& rows = values.TrialRows(config.shape);
    if (!estimator) {
      estimator.emplace();
    }
    const auto whole = config.layout == FrameDictionary::Local
                         ? Values::Bytes(rows)
                         : std::string_view{};
    return estimator->EstimateSize(
      rows.profile, whole,
      Values::Split(rows, FrameShapeOf<Codec>(config.layout)),
      values.CodesSize(rows), config.level, config.layout, 0);
  }

  const duckdb::LogicalType& _type;
  DedupScratch& _dedup;
  StringTuning& _tuning;
  std::optional<CompressionState<Lz4>> _fast;
  std::optional<CompressionState<Lz4Hc>> _high;
  std::optional<SizeEstimator<Lz4>> _fast_estimator;
  std::optional<SizeEstimator<Lz4Hc>> _high_estimator;
};

}  // namespace irs::codecs::text::blob
