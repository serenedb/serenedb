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

#include <limits>
#include <span>

#include "iresearch/formats/column/codecs/fsst_analysis.hpp"
#include "iresearch/formats/column/codecs/lz4_analysis.hpp"
#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_window.hpp"
#include "iresearch/formats/column/codecs/zxc_analysis.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr uint64_t kTuneMinEntries = 2;
constexpr uint8_t kLz4Fast = 1;

using Analyze = Estimate (*)(StringWindow&, Shape, const Ladder&, bool,
                             TunedParameters&);

struct LeafCandidate {
  Analyze analyze;
  Ladder ladder;
};

struct TierCandidates {
  std::span<const LeafCandidate> leaves;
  double fsst_preference = 0;
};

constexpr uint8_t kLz4FlushLevels[] = {1};
constexpr uint8_t kLz4MergeLevels[] = {1, 4, 6};
constexpr uint8_t kZxcLevels[] = {1, 3};

constexpr LeafCandidate kFlushLeaves[] = {
  {&AnalyzeLz4, {.levels = kLz4FlushLevels}},
};

constexpr LeafCandidate kMergeLeaves[] = {
  {&AnalyzeLz4, {.levels = kLz4MergeLevels, .local = true}},
  {&AnalyzeZxc, {.levels = kZxcLevels, .local = true}},
};

constexpr TierCandidates kFlushCandidates{
  .leaves = kFlushLeaves,
  .fsst_preference = 0.05,
};

constexpr TierCandidates kMergeCandidates{.leaves = kMergeLeaves};

const TierCandidates& CandidatesOf(WriteTier tier) noexcept {
  return tier == WriteTier::Flush ? kFlushCandidates : kMergeCandidates;
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

Segment* StringPicker::First() {
  auto& reuse = _tuning.reuse;
  const bool due = !reuse.pick || reuse.clock.Due();
  if (_acc.Rows() == 0) {
    return nullptr;
  }
  auto* first = _encoder.Cut(Current(), _acc, 0);
  if (const bool drift = !due && Drifted(*first); due || drift) {
    return Pick(*first, drift);
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

Segment* StringPicker::Pick(Segment& first, bool drift) {
  const auto input = first.input;
  StringWindow window{_acc, _encoder, first, _tuning.estimators.codes};
  const auto& dedup = window.Encode(Fast(Shape::Dedup));
  auto shape = Shape::Dedup;
  if (PlainWins(dedup, kHeaderSize + Packed(dedup.rows, dedup.max_len) +
                         dedup.input / kLz4MaxRatio) &&
      PlainWins(dedup, window.Encode(Fast(Shape::Plain)).Size())) {
    shape = Shape::Plain;
  }
  auto& tuned = _tuning.tuned;
  const bool tune = dedup.entries >= kTuneMinEntries && !tuned.done;
  Estimate best{.bytes = std::numeric_limits<uint64_t>::max()};
  const auto consider = [&](const Estimate& candidate) {
    if (candidate.bytes < best.bytes) {
      best = candidate;
    }
  };
  const auto& candidates = CandidatesOf(_params.tier);
  consider(AnalyzeFsst(window, shape));
  for (const auto& leaf : candidates.leaves) {
    consider(leaf.analyze(window, shape, leaf.ladder, tune, tuned));
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
  if (candidates.fsst_preference > 0 &&
      picked->config.choice.leaf != ByteCodec::Fsst) {
    if (auto* fsst = window.Encoded({.choice = {shape, ByteCodec::Fsst}});
        fsst && static_cast<double>(fsst->Size()) <=
                  static_cast<double>(picked->Size()) *
                    (1.0 + candidates.fsst_preference)) {
      picked = fsst;
    }
  }
  auto& reuse = _tuning.reuse;
  const bool kept = reuse.pick && reuse.pick->choice == picked->config.choice;
  reuse.clock.Settle(kept && !drift);
  reuse.pick = picked->config;
  if (input != 0) {
    reuse.bytes_per_input =
      static_cast<double>(picked->Size()) / static_cast<double>(input);
  }
  window.Keep(*picked);
  return picked;
}

}  // namespace irs::codecs::text
