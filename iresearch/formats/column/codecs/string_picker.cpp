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

struct Candidate {
  ByteCodec leaf = ByteCodec::Lz4;
  CompressionMethod method;
};

struct TierCandidates {
  std::span<const Candidate> candidates;
  double fsst_preference = 0;
};

constexpr uint8_t kLz4FlushLevels[] = {1};
constexpr uint8_t kLz4MergeLevels[] = {1, 4, 6};

constexpr Candidate kFlushCandidates[] = {
  {.leaf = ByteCodec::Fsst},
  {
    .leaf = ByteCodec::Lz4,
    .method = {.levels = kLz4FlushLevels, .max_trial_level = 1},
  },
};

constexpr Candidate kMergeCandidates[] = {
  {.leaf = ByteCodec::Fsst},
  {
    .leaf = ByteCodec::Lz4,
    .method = {.levels = kLz4MergeLevels, .local = true, .max_trial_level = 1},
  },
};

constexpr TierCandidates kFlush{
  .candidates = kFlushCandidates,
  .fsst_preference = 0.05,
};

constexpr TierCandidates kMerge{.candidates = kMergeCandidates};

const TierCandidates& CandidatesOf(WriteTier tier) noexcept {
  return tier == WriteTier::Flush ? kFlush : kMerge;
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
  const bool due = _tuning.reuse.Due();
  if (_acc.Rows() == 0) {
    return {};
  }
  const auto current = Current();
  auto first = GetStringCompressionFunction(current).compress_segment(
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
  StringWindow window{_acc, _compressor, *first, _tuning.estimators.codes};
  const auto& dedup = window.TrialCompress(Fast(Shape::Dedup));
  auto shape = Shape::Dedup;
  if (PlainWins(dedup, kHeaderSize + Packed(dedup.rows, dedup.max_len) +
                         dedup.input / kLz4MaxRatio) &&
      PlainWins(dedup, window.TrialCompress(Fast(Shape::Plain)).Size())) {
    shape = Shape::Plain;
  }
  auto& tuned = _tuning.tuned;
  const bool tune = dedup.entries >= kTuneMinEntries && !tuned.done;
  const auto& tier = CandidatesOf(_params.tier);
  Config best_config;
  uint64_t best_score = std::numeric_limits<uint64_t>::max();
  for (const auto& candidate : tier.candidates) {
    const auto& function =
      GetStringCompressionFunction({.choice = {shape, candidate.leaf}});
    const AnalyzeInput input{
      .window = window,
      .shape = shape,
      .method = candidate.method,
      .tune = tune,
      .tuned = tuned,
    };
    SDB_ASSERT(function.init_analyze && function.analyze &&
               function.final_analyze);
    auto state = function.init_analyze(input);
    function.analyze(*state, input);
    if (const auto score = function.final_analyze(*state); score < best_score) {
      best_score = score;
      best_config = state->config;
    }
  }
  if (tune) {
    tuned.done = true;
  }
  auto* picked = &window.TrialCompress(best_config);
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
