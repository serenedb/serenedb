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

#include <algorithm>
#include <array>
#include <iterator>
#include <span>

#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/string_compression_function.hpp"
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
constexpr double kShapeSlack = 0.08;
constexpr double kScoreSlack = 0.02;

struct Candidate {
  ByteCodec leaf = ByteCodec::Lz4;
  Analysis (*analyze)(const AnalyzeInput& input) = nullptr;
  CompressionMethod method;
};

struct TierCandidates {
  std::span<const Candidate> candidates;
  double fsst_preference = 0;
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
  .fsst_preference = 0.05,
};

constexpr TierCandidates kMerge{.candidates = kMergeCandidates};

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
  const bool due = _tuning.reuse.Due();
  if (_acc.Rows() == 0) {
    return {};
  }
  const auto current = Current();
  auto first = GetStringCompressionFunction(current).CompressSegment(
    _compressor, current, _acc, 0);
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
  StringWindow window{_acc, _compressor, *first, _tuning.estimators.codes};
  const auto entries = window.Entries();
  const auto slack = [&](Shape shape) {
    return window.Exact(Fast(shape)) ? 0.0 : kShapeSlack;
  };
  auto shape = Shape::Dedup;
  auto dedup = static_cast<double>(window.Analyze(Fast(Shape::Dedup)));
  if (PlainWins(
        entries, values, dedup * (1.0 + slack(Shape::Dedup)),
        static_cast<double>(kHeaderSize + Packed(first->rows, first->max_len) +
                            input / kLz4MaxRatio))) {
    auto plain = static_cast<double>(window.Analyze(Fast(Shape::Plain)));
    const auto margin = std::max(slack(Shape::Dedup), slack(Shape::Plain));
    if (PlainWins(entries, values, dedup * (1.0 - margin), plain) !=
        PlainWins(entries, values, dedup * (1.0 + margin), plain)) {
      dedup =
        static_cast<double>(window.TrialCompress(Fast(Shape::Dedup)).Size());
      plain =
        static_cast<double>(window.TrialCompress(Fast(Shape::Plain)).Size());
    }
    if (PlainWins(entries, values, dedup, plain)) {
      shape = Shape::Plain;
    }
  }
  auto& tuned = _tuning.tuned;
  const bool tune = entries >= kTuneMinEntries && !tuned.done;
  const auto& tier = CandidatesOf(_params.tier);
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
      score *= 1.0 + tier.fsst_preference;
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
  auto* picked = &window.TrialCompress(scored[best].config);
  for (size_t i = 0; i < count; ++i) {
    if (scored[i].trial &&
        scored[i].score <= scored[best].score * (1.0 + kScoreSlack)) {
      window.TrialCompress(scored[i].config);
    }
  }
  for (auto* seg : window.Encodings()) {
    if (seg->config.choice.shape == shape && seg->Size() < picked->Size()) {
      picked = seg;
    }
  }
  if (tier.fsst_preference > 0 &&
      picked->config.choice.leaf != ByteCodec::Fsst) {
    auto& fsst = window.TrialCompress({.choice = {shape, ByteCodec::Fsst}});
    if (static_cast<double>(fsst.Size()) <=
        static_cast<double>(picked->Size()) * (1.0 + tier.fsst_preference)) {
      picked = &fsst;
    }
  }
  auto& reuse = _tuning.reuse;
  const bool kept = reuse.pick && reuse.pick->choice == picked->config.choice;
  reuse.Settle(picked->config, kept && !drift,
               static_cast<double>(picked->Size()), static_cast<double>(input));
  return picked == first.get() ? std::move(first) : window.Take(*picked);
}

}  // namespace irs::codecs::text
