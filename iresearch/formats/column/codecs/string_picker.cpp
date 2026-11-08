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

#include "iresearch/formats/column/codecs/string_picker.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <limits>
#include <span>

#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_window.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr uint64_t kTuneMinEntries = 2;
constexpr uint8_t kLz4Fast = 1;
constexpr double kLevelTolerance = 0.02;
constexpr double kWideFramesGain = 0.03;
constexpr size_t kMaxLevels = 4;

struct AnalyzeResult {
  Config config;
  uint64_t score = std::numeric_limits<uint64_t>::max();
};

struct CompressionMethod {
  ByteCodec leaf = ByteCodec::Lz4;
  std::span<const uint8_t> levels;
  bool local = false;
  uint8_t max_trial_level = 0;
};

struct TierMethods {
  std::span<const CompressionMethod> methods;
  double fsst_preference = 0;
};

constexpr uint8_t kLz4FlushLevels[] = {1};
constexpr uint8_t kLz4MergeLevels[] = {1, 4, 6};
constexpr uint8_t kZxcLevels[] = {1, 3};

constexpr CompressionMethod kFlushMethods[] = {
  {.leaf = ByteCodec::Lz4, .levels = kLz4FlushLevels, .max_trial_level = 1},
};

constexpr CompressionMethod kMergeMethods[] = {
  {
    .leaf = ByteCodec::Lz4,
    .levels = kLz4MergeLevels,
    .local = true,
    .max_trial_level = 1,
  },
  {.leaf = ByteCodec::Zxc, .levels = kZxcLevels, .local = true},
};

constexpr TierMethods kFlush{.methods = kFlushMethods, .fsst_preference = 0.05};
constexpr TierMethods kMerge{.methods = kMergeMethods};

const TierMethods& MethodsOf(WriteTier tier) noexcept {
  return tier == WriteTier::Flush ? kFlush : kMerge;
}

AnalyzeResult FinalAnalyze(StringWindow& window, Shape shape,
                           const CompressionMethod& method, bool tune,
                           TunedParameters& tuned) {
  const auto levels = method.levels;
  SDB_ASSERT(!levels.empty() && levels.size() <= kMaxLevels);
  const auto leaf = static_cast<size_t>(method.leaf);
  const auto score = [&](const Config& config) {
    return config.level <= method.max_trial_level ? window.Encode(config).Size()
                                                  : window.Price(config);
  };
  Config config{.choice = {shape, method.leaf}};
  if (!tune) {
    config.level = absl::c_linear_search(levels, tuned.level[leaf])
                     ? tuned.level[leaf]
                     : levels.front();
    config.layout = tuned.layout[leaf];
    return {config, score(config)};
  }
  uint64_t scores[kMaxLevels];
  uint64_t best_score = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < levels.size(); ++i) {
    config.level = levels[i];
    scores[i] = window.Price(config);
    best_score = std::min(best_score, scores[i]);
  }
  size_t level = 0;
  while (static_cast<double>(scores[level]) >
         static_cast<double>(best_score) * (1.0 + kLevelTolerance)) {
    ++level;
  }
  config.level = levels[level];
  AnalyzeResult best{config, scores[level]};
  const auto analyze_layout = [&](FrameLayout layout, double min_gain) {
    auto candidate = best.config;
    candidate.layout = layout;
    const auto candidate_score = window.Price(candidate);
    if (static_cast<double>(candidate_score) <
        static_cast<double>(best.score) * (1.0 - min_gain)) {
      best = {candidate, candidate_score};
    }
  };
  if (method.local) {
    analyze_layout(FrameLayout::Local, 0.0);
  }
  analyze_layout(FrameLayout::Wide, kWideFramesGain);
  tuned.level[leaf] = best.config.level;
  tuned.layout[leaf] = best.config.layout;
  if (best.config.level <= method.max_trial_level) {
    best.score = window.Encode(best.config).Size();
  }
  return best;
}

bool Repeats(uint64_t entries, uint64_t values) noexcept {
  return entries * kDedupMinRepeat <= values;
}

bool PlainWins(const Segment& dedup, uint64_t plain) noexcept {
  if (!Repeats(dedup.entries, dedup.rows - dedup.nulls)) {
    return plain <= dedup.Size();
  }
  return static_cast<double>(plain) <
           static_cast<double>(dedup.Size()) * kPlainWinsBelow &&
         plain + kPlainMinSaving <= dedup.Size();
}

Config Fast(Shape shape) noexcept {
  return {.choice = {shape, ByteCodec::Lz4}, .level = kLz4Fast};
}

}  // namespace

Config NamedConfig(const StringChoice& named,
                   const ColCodecParams& params) noexcept {
  return {
    .choice = named,
    .level = params.compression_level,
    .layout = params.tier == WriteTier::Flush ? FrameLayout::FirstFrame
                                              : FrameLayout::Local,
  };
}

PooledSegment StringPicker::First() {
  auto& reuse = _tuning.reuse;
  const bool due = !reuse.pick || reuse.clock.Due();
  if (_acc.Rows() == 0) {
    return {};
  }
  auto first = _encoder.Cut(Current(), _acc, 0);
  if (const bool drift = !due && Drifted(*first); due || drift) {
    return Pick(std::move(first), drift);
  }
  return first;
}

Config StringPicker::Current() const noexcept {
  if (_tuning.reuse.pick) {
    return *_tuning.reuse.pick;
  }
  return Fast(Repeats(_acc.values.size(), _acc.Rows() - _acc.null_count)
                ? Shape::Dedup
                : Shape::Plain);
}

bool StringPicker::Drifted(const Segment& seg) const noexcept {
  if (seg.input == 0) {
    return false;
  }
  const auto ratio =
    static_cast<double>(seg.Size()) / static_cast<double>(seg.input);
  const auto reference = _tuning.reuse.bytes_per_input;
  return ratio > reference * kDrift || ratio * kDrift < reference;
}

PooledSegment StringPicker::Pick(PooledSegment first, bool drift) {
  const auto input = first->input;
  StringWindow window{_acc, _encoder, *first, _tuning.estimators.codes};
  const auto& dedup = window.Encode(Fast(Shape::Dedup));
  auto shape = Shape::Dedup;
  if (PlainWins(dedup, kHeaderSize + Packed(dedup.rows, dedup.max_len) +
                         dedup.input / kLz4MaxRatio) &&
      PlainWins(dedup, window.Encode(Fast(Shape::Plain)).Size())) {
    shape = Shape::Plain;
  }
  auto& tuned = _tuning.tuned;
  const bool tune = dedup.entries >= kTuneMinEntries && !tuned.done;
  const auto& tier = MethodsOf(_params.tier);
  const Config fsst_config{.choice = {shape, ByteCodec::Fsst}};
  auto& fsst = window.Encode(fsst_config);
  AnalyzeResult best{fsst_config, fsst.Size()};
  for (const auto& method : tier.methods) {
    if (const auto result = FinalAnalyze(window, shape, method, tune, tuned);
        result.score < best.score) {
      best = result;
    }
  }
  if (tune) {
    tuned.done = true;
  }
  auto* picked = &window.Encode(best.config);
  for (auto* seg : window.Encodings()) {
    if (seg->config.choice.shape == shape && seg->Size() < picked->Size()) {
      picked = seg;
    }
  }
  if (tier.fsst_preference > 0 &&
      picked->config.choice.leaf != ByteCodec::Fsst &&
      static_cast<double>(fsst.Size()) <=
        static_cast<double>(picked->Size()) * (1.0 + tier.fsst_preference)) {
    picked = &fsst;
  }
  auto& reuse = _tuning.reuse;
  const bool kept = reuse.pick && reuse.pick->choice == picked->config.choice;
  reuse.clock.Settle(kept && !drift);
  reuse.pick = picked->config;
  if (input != 0) {
    reuse.bytes_per_input =
      static_cast<double>(picked->Size()) / static_cast<double>(input);
  }
  return picked == first.get() ? std::move(first) : window.Take(*picked);
}

}  // namespace irs::codecs::text
