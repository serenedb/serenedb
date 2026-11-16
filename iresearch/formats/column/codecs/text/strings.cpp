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

#include "iresearch/formats/column/codecs/text/strings.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <array>
#include <limits>
#include <span>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
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
constexpr uint8_t kLz4TrialLevel = 1;
constexpr double kShapeSlack = 0.08;
constexpr double kScoreSlack = 0.02;
constexpr double kLevelTolerance = 0.02;
constexpr double kWideFramesGain = 0.03;
constexpr size_t kMaxLevels = 4;

struct Tier {
  std::span<const uint8_t> lz4_levels;
  bool lz4_local = false;
  double dedup_fsst_preference = 0;
  double plain_fsst_preference = 0;
};

constexpr uint8_t kLz4FlushLevels[] = {1};
constexpr uint8_t kLz4MergeLevels[] = {1, 4, 6};

constexpr Tier kFlush{
  .lz4_levels = kLz4FlushLevels,
  .dedup_fsst_preference = 0.05,
  .plain_fsst_preference = 0.05,
};

constexpr Tier kMerge{
  .lz4_levels = kLz4MergeLevels,
  .lz4_local = true,
  .plain_fsst_preference = 0.05,
};

const Tier& TierOf(WriteTier tier) noexcept {
  return tier == WriteTier::Flush ? kFlush : kMerge;
}

struct Analysis {
  Config config;
  uint64_t score = 0;
};

Analysis AnalyzeLz4(StringWindow& window, Shape shape, const Tier& tier,
                    bool tune, TunedParameters& tuned) {
  Analysis state{.config = {.shape = shape, .leaf = ByteCodec::Lz4}};
  const auto levels = tier.lz4_levels;
  SDB_ASSERT(!levels.empty() && levels.size() <= kMaxLevels);
  const auto leaf = static_cast<size_t>(state.config.leaf);
  auto config = state.config;
  if (!tune) {
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
  const auto analyze_layout = [&](FrameDictionary layout, double min_gain) {
    auto candidate = state.config;
    candidate.layout = layout;
    const auto candidate_score = window.Analyze(candidate);
    if (static_cast<double>(candidate_score) <
        static_cast<double>(state.score) * (1.0 - min_gain)) {
      state.config = candidate;
      state.score = candidate_score;
    }
  };
  if (tier.lz4_local) {
    analyze_layout(FrameDictionary::Local, 0.0);
  }
  analyze_layout(FrameDictionary::None, kWideFramesGain);
  tuned.level[leaf] = state.config.level;
  tuned.layout[leaf] = state.config.layout;
  return state;
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

bool Enabled(const CodecSet& disabled, Shape shape, ByteCodec leaf) noexcept {
  return !disabled.test(std::to_underlying(CodecOf(shape, leaf)));
}

bool AnyEnabled(const CodecSet& disabled, Shape shape) noexcept {
  return Enabled(disabled, shape, ByteCodec::Fsst) ||
         Enabled(disabled, shape, ByteCodec::Lz4);
}

uint64_t PlainStorageBytes(const Segment& seg) noexcept {
  return kHeaderSize + seg.input + seg.rows * sizeof(int32_t);
}

Config NamedConfig(Shape shape, ByteCodec leaf,
                   const ColCodecParams& params) noexcept {
  return {
    .shape = shape,
    .leaf = leaf,
    .level = params.compression_level,
    .layout = params.tier == WriteTier::Flush ? FrameDictionary::FirstFrame
                                              : FrameDictionary::Local,
  };
}

Shape ChooseShape(StringWindow& window, uint64_t entries, uint64_t values,
                  double plain_bound) {
  const auto slack = [&](Shape shape) {
    return window.Exact(Fast(shape)) ? 0.0 : kShapeSlack;
  };
  auto dedup = static_cast<double>(window.Analyze(Fast(Shape::Dictionary)));
  if (!PlainWins(entries, values, dedup * (1.0 + slack(Shape::Dictionary)),
                 plain_bound)) {
    return Shape::Dictionary;
  }
  auto plain = static_cast<double>(window.Analyze(Fast(Shape::Plain)));
  const auto margin = std::max(slack(Shape::Dictionary), slack(Shape::Plain));
  if (PlainWins(entries, values, dedup * (1.0 - margin), plain) !=
      PlainWins(entries, values, dedup * (1.0 + margin), plain)) {
    dedup = static_cast<double>(
      window.At(window.TrialCompress(Fast(Shape::Dictionary))).Size());
    plain = static_cast<double>(
      window.At(window.TrialCompress(Fast(Shape::Plain))).Size());
  }
  return PlainWins(entries, values, dedup, plain) ? Shape::Plain
                                                  : Shape::Dictionary;
}

struct Scored {
  Config config;
  double score = 0;
  bool trial = false;
};

size_t ChooseEncoding(StringWindow& window, Shape shape, const Tier& tier,
                      const CodecSet& disabled, bool tune,
                      TunedParameters& tuned) {
  const double fsst_preference = shape == Shape::Plain
                                   ? tier.plain_fsst_preference
                                   : tier.dedup_fsst_preference;
  std::array<Scored, 2> scored;
  size_t count = 0;
  if (Enabled(disabled, shape, ByteCodec::Fsst)) {
    const Config config{.shape = shape, .leaf = ByteCodec::Fsst};
    scored[count++] = {
      .config = config,
      .score =
        static_cast<double>(window.At(window.TrialCompress(config)).Size()),
      .trial = true,
    };
  }
  if (Enabled(disabled, shape, ByteCodec::Lz4)) {
    const auto lz4 = AnalyzeLz4(window, shape, tier, tune, tuned);
    scored[count++] = {
      .config = lz4.config,
      .score = static_cast<double>(lz4.score) * (1.0 + fsst_preference),
      .trial = lz4.config.level <= kLz4TrialLevel,
    };
  }
  SDB_ASSERT(count != 0);
  if (tune && count == scored.size()) {
    tuned.done = true;
  }
  size_t best = 0;
  for (size_t i = 1; i < count; ++i) {
    if (scored[i].score < scored[best].score) {
      best = i;
    }
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
    if (seg.config.shape == shape &&
        Enabled(disabled, shape, seg.config.leaf) &&
        seg.Size() < window.At(picked).Size()) {
      picked = i;
    }
  }
  if (fsst_preference > 0 && Enabled(disabled, shape, ByteCodec::Fsst) &&
      window.At(picked).config.leaf != ByteCodec::Fsst) {
    const auto fsst =
      window.TrialCompress({.shape = shape, .leaf = ByteCodec::Fsst});
    if (static_cast<double>(window.At(fsst).Size()) <=
        static_cast<double>(window.At(picked).Size()) *
          (1.0 + fsst_preference)) {
      picked = fsst;
    }
  }
  return picked;
}

}  // namespace

Strings::Strings(ColumnDataCheckpointData& data)
  : _tuning{data.tuning->strings},
    _compressor{data.params, data.type, _tuning.estimators,
                data.ctx->BufferManager()} {}

const StringAccumulator& Strings::Accumulated(
  std::span<const WriteChunk> chunks) {
  if (!_accumulated) {
    _accumulated = true;
    _acc.Reserve(CountRows(chunks), _tuning.last_distinct);
    for (const auto& chunk : chunks) {
      _acc.Add(chunk.data);
    }
    _tuning.last_distinct = _acc.values.size();
  }
  return _acc;
}

Estimate Strings::Analyze(ColumnDataCheckpointData& data) {
  _first = DetectBestCompressionMethod(data);
  if (!_first) {
    return {};
  }
  const auto& seg = *_first;
  return {
    .codec = CodecOf(seg.config.shape, seg.config.leaf),
    .score = static_cast<double>(seg.Size()) /
             static_cast<double>(std::max<uint64_t>(seg.input, 1)),
  };
}

bool Strings::Compress(ColumnDataCheckpointData& data) {
  if (data.method != CodecType::Auto) {
    const auto config =
      NamedConfig(ShapeOf(data.method), LeafOf(data.method), data.params);
    if (config.shape == Shape::Plain) {
      _compressor.CompressChunks(config, data);
    } else {
      _compressor.Compress(config, Accumulated(data.chunks), 0, data);
    }
    return true;
  }
  const bool held = !_first;
  if (held) {
    const auto& config = _tuning.config;
    const auto& acc = Accumulated(data.chunks);
    if (!config || acc.Rows() == 0) {
      return false;
    }
    SDB_ASSERT(Enabled(data.disabled, config->shape, config->leaf));
    _first = _compressor.CompressSegment(*config, acc, 0);
  }
  if (held && data.tuning->reuse.Drifted(
                static_cast<double>(_first->Size()),
                static_cast<double>(std::max<uint64_t>(_first->input, 1)))) {
    return false;
  }
  if (_first->Size() > PlainStorageBytes(*_first)) {
    return false;
  }
  const auto config = _first->config;
  const auto rest = _first->begin + _first->rows;
  data.FlushSegment(CodecOf(config.shape, config.leaf),
                    std::move(*_first->stats), _first->rows,
                    _first->bytes.View());
  _first.reset();
  _compressor.Compress(config, _acc, rest, data);
  return true;
}

PooledSegment Strings::DetectBestCompressionMethod(
  const ColumnDataCheckpointData& data) {
  const auto& acc = Accumulated(data.chunks);
  const auto& disabled = data.disabled;
  auto first = std::move(_first);
  if (acc.Rows() == 0 || (!AnyEnabled(disabled, Shape::Dictionary) &&
                          !AnyEnabled(disabled, Shape::Plain))) {
    return {};
  }
  if (!first) {
    first = _compressor.CompressSegment(
      _tuning.config
        ? *_tuning.config
        : Fast(Repeats(acc.values.size(), acc.Rows() - acc.null_count)
                 ? Shape::Dictionary
                 : Shape::Plain),
      acc, 0);
  }
  const auto input = first->input;
  const auto values = first->rows - first->nulls;
  const auto lengths = Packed(first->rows, first->max_len);
  StringWindow window{acc, _compressor, std::move(first),
                      _tuning.estimators.codes};
  const auto entries = window.Entries();
  auto shape = ChooseShape(
    window, entries, values,
    static_cast<double>(kHeaderSize + lengths + input / kLz4MaxRatio));
  auto& tuned = _tuning.tuned;
  const bool tune = entries >= kTuneMinEntries && !tuned.done;
  if (!AnyEnabled(disabled, shape)) {
    shape = shape == Shape::Dictionary ? Shape::Plain : Shape::Dictionary;
  }
  SDB_ASSERT(AnyEnabled(disabled, shape));
  const auto picked = ChooseEncoding(window, shape, TierOf(data.params.tier),
                                     disabled, tune, tuned);
  _tuning.config = window.At(picked).config;
  return std::move(window).Take(picked);
}

const CompressionFunction kStringsFunction{
  .analyze = [](ColumnDataCheckpointData& data,
                double) { return data.Get<Strings>().Analyze(data); },
  .compress =
    [](ColumnDataCheckpointData& data) {
      return data.Get<Strings>().Compress(data);
    },
};

}  // namespace irs::codecs::text
