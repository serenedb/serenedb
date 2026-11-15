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

#include "iresearch/formats/column/codecs/text/string_picker.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <limits>
#include <span>

#include "iresearch/formats/column/codecs/numeric/numeric.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_window.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr uint64_t kTuneMinEntries = 2;
constexpr uint8_t kLz4Fast = 1;
constexpr double kShapeSlack = 0.08;
constexpr double kScoreSlack = 0.02;
constexpr double kLevelTolerance = 0.02;
constexpr double kWideFramesGain = 0.03;
constexpr size_t kMaxLevels = 4;

struct CompressionMethod {
  std::span<const uint8_t> levels;
  bool local = false;
  uint8_t max_trial_level = 0;
};

struct AnalyzeInput {
  StringWindow& window;
  Shape shape;
  const CompressionMethod& method;
  bool tune = false;
  TunedParameters& tuned;
};

struct Analysis {
  Config config;
  uint64_t score = 0;
};

Analysis AnalyzeFsst(const AnalyzeInput& input) {
  const Config config{.shape = input.shape, .leaf = ByteCodec::Fsst};
  auto& window = input.window;
  return {.config = config,
          .score = window.At(window.TrialCompress(config)).Size()};
}

Analysis AnalyzeLz4(const AnalyzeInput& input) {
  Analysis state{.config = {.shape = input.shape, .leaf = ByteCodec::Lz4}};
  auto& window = input.window;
  const auto& method = input.method;
  const auto levels = method.levels;
  SDB_ASSERT(!levels.empty() && levels.size() <= kMaxLevels);
  const auto leaf = static_cast<size_t>(state.config.leaf);
  auto& tuned = input.tuned;
  auto config = state.config;
  if (!input.tune) {
    config.level = absl::c_linear_search(levels, tuned.level[leaf])
                     ? tuned.level[leaf]
                     : levels.front();
    config.layout = tuned.layout[leaf];
    state.config = config;
    state.score = window.Analyze(config);
    return state;
  }
  uint64_t scores[kMaxLevels];
  uint64_t best_score = std::numeric_limits<uint64_t>::max();
  for (size_t i = 0; i < levels.size(); ++i) {
    config.level = levels[i];
    scores[i] = window.Analyze(config);
    best_score = std::min(best_score, scores[i]);
  }
  size_t level = 0;
  while (static_cast<double>(scores[level]) >
         static_cast<double>(best_score) * (1.0 + kLevelTolerance)) {
    ++level;
  }
  config.level = levels[level];
  state.config = config;
  state.score = scores[level];
  const auto analyze_layout = [&](FrameLayout layout, double min_gain) {
    auto candidate = state.config;
    candidate.layout = layout;
    const auto candidate_score = window.Analyze(candidate);
    if (static_cast<double>(candidate_score) <
        static_cast<double>(state.score) * (1.0 - min_gain)) {
      state.config = candidate;
      state.score = candidate_score;
    }
  };
  if (method.local) {
    analyze_layout(FrameLayout::Local, 0.0);
  }
  analyze_layout(FrameLayout::Wide, kWideFramesGain);
  tuned.level[leaf] = state.config.level;
  tuned.layout[leaf] = state.config.layout;
  return state;
}

struct Candidate {
  ByteCodec leaf = ByteCodec::Lz4;
  Analysis (*analyze)(const AnalyzeInput& input) = nullptr;
  CompressionMethod method;
};

struct TierCandidates {
  std::span<const Candidate> candidates;
  double dedup_fsst_preference = 0;
  double plain_fsst_preference = 0;
};

constexpr uint8_t kLz4FlushLevels[] = {1};
constexpr uint8_t kLz4MergeLevels[] = {1, 4, 6};

constexpr Candidate kFlushCandidates[] = {
  {.leaf = ByteCodec::Fsst, .analyze = AnalyzeFsst},
  {
    .leaf = ByteCodec::Lz4,
    .analyze = AnalyzeLz4,
    .method = {.levels = kLz4FlushLevels, .max_trial_level = 1},
  },
};

constexpr Candidate kMergeCandidates[] = {
  {.leaf = ByteCodec::Fsst, .analyze = AnalyzeFsst},
  {
    .leaf = ByteCodec::Lz4,
    .analyze = AnalyzeLz4,
    .method = {.levels = kLz4MergeLevels, .local = true, .max_trial_level = 1},
  },
};

constexpr TierCandidates kFlush{
  .candidates = kFlushCandidates,
  .dedup_fsst_preference = 0.05,
  .plain_fsst_preference = 0.05,
};

constexpr TierCandidates kMerge{
  .candidates = kMergeCandidates,
  .plain_fsst_preference = 0.05,
};

constexpr size_t kMaxCandidates =
  std::max(std::size(kFlushCandidates), std::size(kMergeCandidates));

struct Scored {
  Config config;
  double score = 0;
  bool trial = false;
};

const TierCandidates& CandidatesOf(WriteTier tier) noexcept {
  return tier == WriteTier::Flush ? kFlush : kMerge;
}

bool Repeats(uint64_t entries, uint64_t values) noexcept {
  return entries * kDedupMinRepeat <= values;
}

bool PlainWins(uint64_t entries, uint64_t values, double dedup,
               double plain) noexcept {
  if (!Repeats(entries, values)) {
    return plain <= dedup;
  }
  return plain < dedup * kPlainWinsBelow &&
         plain + static_cast<double>(kPlainMinSaving) <= dedup;
}

Config Fast(Shape shape) noexcept {
  return {.shape = shape, .leaf = ByteCodec::Lz4, .level = kLz4Fast};
}

}  // namespace

Config NamedConfig(Shape shape, ByteCodec leaf,
                   const ColCodecParams& params) noexcept {
  return {
    .shape = shape,
    .leaf = leaf,
    .level = params.compression_level,
    .layout = params.tier == WriteTier::Flush ? FrameLayout::FirstFrame
                                              : FrameLayout::Local,
  };
}

PooledSegment StringPicker::First() {
  const bool due = _tuning.reuse.Due();
  if (_acc.Rows() == 0) {
    return {};
  }
  const auto current = Current();
  auto first = _compressor.CompressSegment(current, _acc, 0);
  if (const bool drift = !due && Drifted(*first); due || drift) {
    return DetectBestCompressionMethod(std::move(first), drift);
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
  return seg.input != 0 &&
         _tuning.reuse.Drifted(static_cast<double>(seg.Size()),
                               static_cast<double>(seg.input));
}

PooledSegment StringPicker::DetectBestCompressionMethod(PooledSegment first,
                                                        bool drift) {
  const auto input = first->input;
  const auto values = first->rows - first->nulls;
  const auto lengths = Packed(first->rows, first->max_len);
  StringWindow window{_acc, _compressor, std::move(first),
                      _tuning.estimators.codes};
  const auto entries = window.Entries();
  const auto slack = [&](Shape shape) {
    return window.Exact(Fast(shape)) ? 0.0 : kShapeSlack;
  };
  auto shape = Shape::Dedup;
  auto dedup = static_cast<double>(window.Analyze(Fast(Shape::Dedup)));
  if (PlainWins(
        entries, values, dedup * (1.0 + slack(Shape::Dedup)),
        static_cast<double>(kHeaderSize + lengths + input / kLz4MaxRatio))) {
    auto plain = static_cast<double>(window.Analyze(Fast(Shape::Plain)));
    const auto margin = std::max(slack(Shape::Dedup), slack(Shape::Plain));
    if (PlainWins(entries, values, dedup * (1.0 - margin), plain) !=
        PlainWins(entries, values, dedup * (1.0 + margin), plain)) {
      dedup = static_cast<double>(
        window.At(window.TrialCompress(Fast(Shape::Dedup))).Size());
      plain = static_cast<double>(
        window.At(window.TrialCompress(Fast(Shape::Plain))).Size());
    }
    if (PlainWins(entries, values, dedup, plain)) {
      shape = Shape::Plain;
    }
  }
  auto& tuned = _tuning.tuned;
  const bool tune = entries >= kTuneMinEntries && !tuned.done;
  const auto& tier = CandidatesOf(_params.tier);
  const double fsst_preference = shape == Shape::Plain
                                   ? tier.plain_fsst_preference
                                   : tier.dedup_fsst_preference;
  std::array<Scored, kMaxCandidates> scored;
  size_t count = 0;
  size_t best = 0;
  for (const auto& candidate : tier.candidates) {
    const auto analysis = candidate.analyze({
      .window = window,
      .shape = shape,
      .method = candidate.method,
      .tune = tune,
      .tuned = tuned,
    });
    auto score = static_cast<double>(analysis.score);
    if (candidate.leaf != ByteCodec::Fsst) {
      score *= 1.0 + fsst_preference;
    }
    scored[count] = {
      .config = analysis.config,
      .score = score,
      .trial = analysis.config.level <= candidate.method.max_trial_level,
    };
    if (score < scored[best].score) {
      best = count;
    }
    ++count;
  }
  if (tune) {
    tuned.done = true;
  }
  auto picked = window.TrialCompress(scored[best].config);
  for (size_t i = 0; i < count; ++i) {
    if (scored[i].trial &&
        scored[i].score <= scored[best].score * (1.0 + kScoreSlack)) {
      window.TrialCompress(scored[i].config);
    }
  }
  for (size_t i = 0; i < window.Encodings(); ++i) {
    const auto& seg = window.At(i);
    if (seg.config.shape == shape && seg.Size() < window.At(picked).Size()) {
      picked = i;
    }
  }
  if (fsst_preference > 0 && window.At(picked).config.leaf != ByteCodec::Fsst) {
    const auto fsst =
      window.TrialCompress({.shape = shape, .leaf = ByteCodec::Fsst});
    if (static_cast<double>(window.At(fsst).Size()) <=
        static_cast<double>(window.At(picked).Size()) *
          (1.0 + fsst_preference)) {
      picked = fsst;
    }
  }
  auto& reuse = _tuning.reuse;
  const auto& seg = window.At(picked);
  const bool kept = reuse.pick && reuse.pick->shape == seg.config.shape &&
                    reuse.pick->leaf == seg.config.leaf;
  reuse.Settle(seg.config, kept && !drift, static_cast<double>(seg.Size()),
               static_cast<double>(input));
  return std::move(window).Take(picked);
}

}  // namespace irs::codecs::text
