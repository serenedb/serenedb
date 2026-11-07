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

#include "iresearch/formats/column/codecs/calibration.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <limits>
#include <optional>

#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr double kLevelTolerance = 0.02;
constexpr uint64_t kTuneMinEntries = 2;
constexpr size_t kMaxRungs = 8;
constexpr double kWideFramesGain = 0.03;
constexpr double kFsstPreference = 0.05;

constexpr uint8_t kLz4Fast = 1;
constexpr uint8_t kRefreshLz4Levels[] = {1};
constexpr uint8_t kLz4Levels[] = {1, 4, 6};
constexpr uint8_t kZxcLevels[] = {1, 3};
constexpr uint8_t kNoLevel[] = {0};

constexpr LeafPlan kRefreshPlan[] = {{ByteCodec::Fsst, kNoLevel},
                                     {ByteCodec::Lz4, kRefreshLz4Levels}};
constexpr LeafPlan kCompactionPlan[] = {{ByteCodec::Fsst, kNoLevel},
                                        {ByteCodec::Lz4, kLz4Levels},
                                        {ByteCodec::Zxc, kZxcLevels}};

std::span<const LeafPlan> PlanFor(const ColCodecParams& params) noexcept {
  if (params.tier == WriteTier::Flush) {
    return kRefreshPlan;
  }
  return kCompactionPlan;
}

constexpr size_t Index(ByteCodec leaf) noexcept {
  return static_cast<size_t>(leaf);
}

bool Cheap(ByteCodec leaf, uint8_t level) noexcept {
  return Sorted(leaf) ||
         (leaf == ByteCodec::Lz4 && EffectiveLevel<Lz4>(level) <= 1);
}

bool Repeats(uint64_t entries, uint64_t values) noexcept {
  return entries * kDedupMinRepeat <= values;
}

bool Repeats(const Segment& dedup) noexcept {
  return Repeats(dedup.entries, dedup.rows - dedup.nulls);
}

bool SavesEnough(uint64_t dedup, uint64_t plain) noexcept {
  return static_cast<double>(plain) <
           static_cast<double>(dedup) * kPlainWinsBelow &&
         plain + kPlainMinSaving <= dedup;
}

bool PlainWins(const Segment& dedup, uint64_t plain) noexcept {
  if (Repeats(dedup)) {
    return SavesEnough(dedup.Size(), plain);
  }
  return plain <= dedup.Size();
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

Config Calibration::CutConfig() const noexcept {
  if (_named) {
    return NamedConfig(*_named, _params);
  }
  if (_tuning.choice) {
    const auto leaf = Index(_tuning.choice->leaf);
    return {
      .choice = *_tuning.choice,
      .level = _tuning.level[leaf],
      .layout = _tuning.layout[leaf],
    };
  }
  const bool repeats =
    Repeats(_acc.values.size(), _acc.Rows() - _acc.null_count);
  return {
    .choice = {repeats ? Shape::Dedup : Shape::Plain, ByteCodec::Lz4},
    .level = kLz4Fast,
    .layout = FrameLayout::FirstFrame,
  };
}

bool Calibration::Drifted(const Segment& seg, bool full) const noexcept {
  if (seg.input == 0 ||
      (!full && seg.Size() * kDriftMinFraction < _params.segment_target)) {
    return false;
  }
  const auto ratio =
    static_cast<double>(seg.Size()) / static_cast<double>(seg.input);
  return ratio > _tuning.bytes_per_input * kDrift ||
         ratio * kDrift < _tuning.bytes_per_input;
}

Segment* Calibration::Calibrate(Segment* cut, bool retune, bool drift) {
  _begin = cut->begin;
  _end = cut->begin + cut->rows;
  _live.clear();
  _live.push_back(cut);
  const auto layout =
    retune ? FrameLayout::FirstFrame : _tuning.layout[Index(ByteCodec::Lz4)];
  auto* dedup = Trial({
    .choice = {Shape::Dedup, ByteCodec::Lz4},
    .level = kLz4Fast,
    .layout = layout,
  });
  Segment* plain = nullptr;
  if (PlainWins(*dedup, kHeaderSize + Packed(dedup->rows, dedup->max_len) +
                          dedup->input / kLz4MaxRatio)) {
    plain = Trial({
      .choice = {Shape::Plain, ByteCodec::Lz4},
      .level = kLz4Fast,
      .layout = layout,
    });
  }
  const auto shape =
    plain && PlainWins(*dedup, plain->Size()) ? Shape::Plain : Shape::Dedup;
  const bool tune = retune && dedup->entries >= kTuneMinEntries;
  _smallest = {.bytes = std::numeric_limits<uint64_t>::max()};
  Candidate chosen{.bytes = std::numeric_limits<uint64_t>::max()};
  for (const auto& plan : PlanFor(_params)) {
    const auto c = Tune(shape, plan, tune);
    if (c.bytes < chosen.bytes) {
      chosen = c;
    }
  }
  Trial(_smallest.bytes < chosen.bytes ? _smallest.config : chosen.config);
  auto* best = Smallest(shape, shape == Shape::Dedup ? dedup : plain);
  if (_params.tier == WriteTier::Flush &&
      best->config.choice.leaf != ByteCodec::Fsst) {
    if (auto* fsst = Cached({
          .choice = {shape, ByteCodec::Fsst},
          .level = 0,
          .layout = FrameLayout::FirstFrame,
        });
        fsst && static_cast<double>(fsst->Size()) <=
                  static_cast<double>(best->Size()) * (1.0 + kFsstPreference)) {
      best = fsst;
    }
  }
  const auto chosen_bytes = best->Size();
  const StringChoice picked{shape, best->config.choice.leaf};
  const bool kept = _tuning.choice && *_tuning.choice == picked;
  _tuning.clock.Settle(kept && !drift);
  if (tune) {
    _tuning.levels_tuned = true;
  }
  _tuning.choice = picked;
  if (cut->input != 0) {
    _tuning.bytes_per_input =
      static_cast<double>(chosen_bytes) / static_cast<double>(cut->input);
  }
  for (auto* seg : _live) {
    if (seg != best) {
      _encoder.Release(seg);
    }
  }
  _live.clear();
  return best;
}

Calibration::Candidate Calibration::Tune(Shape shape, const LeafPlan& plan,
                                         bool retune) {
  const auto leaf = plan.leaf;
  const auto ladder = plan.ladder;
  auto& tuned = _tuning.level[Index(leaf)];
  auto& tuned_layout = _tuning.layout[Index(leaf)];
  size_t rung = 0;
  if (!retune) {
    const auto it = absl::c_find(ladder, tuned);
    rung = it == ladder.end() ? 0 : static_cast<size_t>(it - ladder.begin());
  }
  auto layout = retune ? FrameLayout::FirstFrame : tuned_layout;
  std::optional<uint64_t> bytes;
  if (retune && !Sorted(leaf)) {
    SDB_ASSERT(ladder.size() <= kMaxRungs);
    uint64_t prices[kMaxRungs];
    uint64_t lowest = std::numeric_limits<uint64_t>::max();
    for (size_t r = 0; r < ladder.size(); ++r) {
      prices[r] = PriceOf({
        .choice = {shape, leaf},
        .level = ladder[r],
        .layout = FrameLayout::FirstFrame,
      });
      lowest = std::min(lowest, prices[r]);
    }
    while (static_cast<double>(prices[rung]) >
           static_cast<double>(lowest) * (1.0 + kLevelTolerance)) {
      ++rung;
    }
    bytes = prices[rung];
    const auto prefer = [&](FrameLayout candidate, double gain) {
      const auto price = PriceOf({
        .choice = {shape, leaf},
        .level = ladder[rung],
        .layout = candidate,
      });
      if (static_cast<double>(price) <
          static_cast<double>(*bytes) * (1.0 - gain)) {
        layout = candidate;
        bytes = price;
      }
    };
    if (_params.tier != WriteTier::Flush) {
      prefer(FrameLayout::Local, 0.0);
    }
    prefer(FrameLayout::Wide, kWideFramesGain);
    tuned_layout = layout;
  }
  tuned = ladder[rung];
  const Config config{
    .choice = {shape, leaf},
    .level = tuned,
    .layout = layout,
  };
  auto* seg = Cached(config);
  if (!seg && Cheap(leaf, tuned)) {
    seg = Trial(config);
  }
  if (seg) {
    return {.config = config, .bytes = seg->Size()};
  }
  if (!bytes) {
    bytes = PriceOf(config);
  }
  return {.config = config, .bytes = *bytes};
}

uint64_t Calibration::PriceOf(Config config) {
  if (const auto* seg = Cached(config)) {
    return seg->Size();
  }
  const auto bytes = _encoder.Price(config, _acc, _begin, _end);
  if (bytes < _smallest.bytes) {
    _smallest = {.config = config, .bytes = bytes};
  }
  return bytes;
}

Segment* Calibration::Smallest(Shape shape, Segment* best) const noexcept {
  for (auto* seg : _live) {
    if (seg->config.choice.shape == shape && seg->Size() < best->Size()) {
      best = seg;
    }
  }
  return best;
}

Segment* Calibration::Cached(const Config& config) const noexcept {
  for (auto* seg : _live) {
    if (seg->config == config) {
      return seg;
    }
  }
  return nullptr;
}

Segment* Calibration::Trial(Config config) {
  if (auto* seg = Cached(config)) {
    return seg;
  }
  auto* seg = _encoder.Encode(config, _acc, _begin, _end);
  _live.push_back(seg);
  return seg;
}

}  // namespace irs::codecs::text
