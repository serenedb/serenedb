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

#include "iresearch/formats/column/codecs/string_writer.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/types/string_type.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/blob_encoder.tpp"
#include "iresearch/formats/column/codecs/fsst12_chain_encoder.hpp"
#include "iresearch/formats/column/codecs/fsst_chain_encoder.hpp"
#include "iresearch/formats/column/codecs/fsst_plus_encoder.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/numeric.hpp"
#include "iresearch/formats/column/codecs/onpair_encoder.hpp"
#include "iresearch/formats/column/codecs/segment_builder.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/codecs/zxc.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;
using duckdb::string_t;
using text::BlobEncoder;
using text::Config;
using text::DedupScratch;
using text::Fsst12ChainEncoder;
using text::FsstChainEncoder;
using text::FsstPlusEncoder;
using text::kDriftMinFraction;
using text::OnPairEncoder;
using text::Profile;
using text::Segment;
using text::SegmentBuilder;

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

struct LeafPlan {
  ByteCodec leaf;
  std::span<const uint8_t> ladder;
};

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

struct LeafEncoders {
  std::optional<BlobEncoder<Lz4>> lz4;
  std::optional<BlobEncoder<Lz4Hc>> lz4_hc;
  std::optional<BlobEncoder<Zstd>> zstd;
  std::optional<BlobEncoder<Zxc>> zxc;
  std::optional<FsstChainEncoder> fsst;
  std::optional<FsstPlusEncoder> fsst_plus;
  std::optional<OnPairEncoder> onpair;
  std::optional<Fsst12ChainEncoder> fsst12;
};

template<typename Encoder>
void Retrain(std::optional<Encoder>& encoder) noexcept {
  if (encoder) {
    encoder->Retrain();
  }
}

class SegmentWriter {
 public:
  SegmentWriter(const StringAccumulator& acc, const StringChoice* named,
                const ColCodecParams& params, const duckdb::LogicalType& type,
                StringTuning& tuning)
    : _acc{acc},
      _named{named},
      _params{params},
      _tuning{tuning},
      _builder{acc, type, _dedup, tuning.codes} {}

  SealOutcome Run(SegmentSink sink) {
    SealOutcome outcome{.sealed = true};
    uint64_t row = 0;
    bool first = true;
    const bool due = !_named && (!_tuning.choice || _tuning.clock.Due());
    while (row < _acc.codes.size()) {
      const auto cutter = Cutter();
      auto* seg = Acquire();
      const auto end = With(cutter, [&](auto& enc) {
        _builder.Begin(enc, cutter, row);
        const auto stop = _builder.Cut(enc, _params.segment_target);
        _builder.Finish(enc, *seg);
        return stop;
      });
      if (!_named) {
        const bool scheduled = first && due;
        const bool drift =
          !scheduled && Drifted(*seg, first || end < _acc.codes.size());
        const bool retune = drift && !first;
        if (retune) {
          Retrain(_encoders.fsst);
          Retrain(_encoders.fsst_plus);
          Retrain(_encoders.onpair);
          Retrain(_encoders.fsst12);
        }
        if (scheduled || drift) {
          seg = Calibrate(seg, !_tuning.levels_tuned || retune, drift);
        }
        if (first && seg->Size() > PlainStorageBytes(*seg)) {
          Release(seg);
          return {};
        }
      }
      outcome.all_dedup =
        outcome.all_dedup && seg->config.choice.shape == Shape::Dedup;
      const std::string_view parts[] = {seg->head, seg->data};
      sink(seg->config.choice, std::move(*seg->stats), seg->rows, parts);
      Release(seg);
      row = end;
      first = false;
    }
    return outcome;
  }

 private:
  Config Cutter() const noexcept {
    if (_named) {
      return {
        .choice = *_named,
        .level = _params.compression_level,
        .layout = _params.tier == WriteTier::Flush ? FrameLayout::FirstFrame
                                                   : FrameLayout::Local,
      };
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
      Repeats(_acc.entries.size(), _acc.codes.size() - _acc.null_count);
    return {
      .choice = {repeats ? Shape::Dedup : Shape::Plain, ByteCodec::Lz4},
      .level = kLz4Fast,
      .layout = FrameLayout::FirstFrame,
    };
  }

  Segment* Calibrate(Segment* cut, bool retune, bool drift) {
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
    _measured = false;
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
          fsst &&
          static_cast<double>(fsst->Size()) <=
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
        Release(seg);
      }
    }
    _live.clear();
    return best;
  }

  struct Candidate {
    Config config;
    uint64_t bytes = 0;
  };

  static bool Cheap(ByteCodec leaf, uint8_t level) noexcept {
    return Sorted(leaf) ||
           (leaf == ByteCodec::Lz4 && EffectiveLevel<Lz4>(level) <= 1);
  }

  Candidate Tune(Shape shape, const LeafPlan& plan, bool retune) {
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

  void Measure(Shape shape) {
    if (_measured) {
      return;
    }
    _measured = true;
    auto& p = _profile;
    p.shape = shape;
    p.rows = _end - _begin;
    p.runs = 0;
    p.raw = 0;
    p.max_len = 0;
    p.entries.clear();
    if (shape == Shape::Plain) {
      for (auto row = _begin; row < _end; ++row) {
        const auto code = _acc.codes[row];
        Append(code == 0 ? std::string_view{} : _acc.entries[code - 1]);
      }
      return;
    }
    _dedup.Begin(_acc.entries.size());
    uint32_t last = 0;
    for (auto row = _begin; row < _end; ++row) {
      const auto code = _acc.codes[row];
      uint32_t local = 0;
      if (code != 0) {
        const auto mapped =
          _dedup.Local(code, static_cast<uint32_t>(p.entries.size()) + 1);
        local = mapped.local;
        if (mapped.fresh) {
          Append(_acc.entries[code - 1]);
        }
      }
      if (row == _begin || local != last) {
        ++p.runs;
      }
      last = local;
    }
  }

  void Append(std::string_view sv) {
    _profile.entries.push_back(sv);
    _profile.raw += sv.size();
    _profile.max_len =
      std::max<uint32_t>(_profile.max_len, static_cast<uint32_t>(sv.size()));
  }

  uint64_t PriceOf(Config config) {
    if (const auto* seg = Cached(config)) {
      return seg->Size();
    }
    Measure(config.choice.shape);
    const auto bytes = With(config, [&](auto& enc) -> uint64_t {
      if constexpr (Sorted(std::remove_reference_t<decltype(enc)>::kCodec)) {
        SDB_UNREACHABLE();
      } else {
        return enc.Price(_profile, config.level, config.layout, _begin);
      }
    });
    if (bytes < _smallest.bytes) {
      _smallest = {.config = config, .bytes = bytes};
    }
    return bytes;
  }

  Segment* Smallest(Shape shape, Segment* best) const noexcept {
    for (auto* seg : _live) {
      if (seg->config.choice.shape == shape && seg->Size() < best->Size()) {
        best = seg;
      }
    }
    return best;
  }

  Segment* Cached(const Config& config) const noexcept {
    for (auto* seg : _live) {
      if (seg->config == config) {
        return seg;
      }
    }
    return nullptr;
  }

  Segment* Trial(Config config) {
    if (auto* seg = Cached(config)) {
      return seg;
    }
    auto* seg = Acquire();
    With(config, [&](auto& enc) {
      _builder.Begin(enc, config, _begin);
      _builder.AddUntil(enc, _end);
      _builder.Finish(enc, *seg);
      return _end;
    });
    _live.push_back(seg);
    return seg;
  }

  static bool Repeats(uint64_t entries, uint64_t values) noexcept {
    return entries * kDedupMinRepeat <= values;
  }

  static bool Repeats(const Segment& dedup) noexcept {
    return Repeats(dedup.entries, dedup.rows - dedup.nulls);
  }

  static bool PlainWins(const Segment& dedup, uint64_t plain) noexcept {
    if (Repeats(dedup)) {
      return SavesEnough(dedup.Size(), plain);
    }
    return plain <= dedup.Size();
  }

  static bool SavesEnough(uint64_t dedup, uint64_t plain) noexcept {
    return static_cast<double>(plain) <
             static_cast<double>(dedup) * kPlainWinsBelow &&
           plain + kPlainMinSaving <= dedup;
  }

  bool Drifted(const Segment& seg, bool full) const noexcept {
    if (seg.input == 0 ||
        (!full && seg.Size() * kDriftMinFraction < _params.segment_target)) {
      return false;
    }
    const auto ratio =
      static_cast<double>(seg.Size()) / static_cast<double>(seg.input);
    return ratio > _tuning.bytes_per_input * kDrift ||
           ratio * kDrift < _tuning.bytes_per_input;
  }

  static uint64_t PlainStorageBytes(const Segment& seg) noexcept {
    return kHeaderSize + seg.input + seg.rows * sizeof(int32_t);
  }

  template<typename F>
  std::invoke_result_t<F&, BlobEncoder<Lz4>&> With(const Config& config,
                                                   F&& f) {
    switch (config.choice.leaf) {
      case ByteCodec::Lz4:
        if (EffectiveLevel<Lz4>(config.level) > 1) {
          return f(Get(_encoders.lz4_hc));
        }
        return f(Get(_encoders.lz4));
      case ByteCodec::Zstd:
        return f(Get(_encoders.zstd));
      case ByteCodec::Zxc:
        return f(Get(_encoders.zxc));
      case ByteCodec::Fsst:
        return f(Get(_encoders.fsst));
      case ByteCodec::FsstPlus:
        return f(Get(_encoders.fsst_plus));
      case ByteCodec::OnPair:
        return f(Get(_encoders.onpair));
      case ByteCodec::Fsst12:
        return f(Get(_encoders.fsst12));
    }
    SDB_UNREACHABLE();
  }

  template<typename Encoder>
  Encoder& Get(std::optional<Encoder>& slot) {
    if (!slot) {
      if constexpr (Sorted(Encoder::kCodec)) {
        slot.emplace(_tuning, _params.segment_target);
      } else {
        slot.emplace(_tuning, _dictionaries);
      }
    }
    return *slot;
  }

  Segment* Acquire() {
    if (_free.empty()) {
      return _pool.emplace_back(std::make_unique<Segment>()).get();
    }
    auto* seg = _free.back();
    _free.pop_back();
    return seg;
  }

  void Release(Segment* seg) { _free.push_back(seg); }

  const StringAccumulator& _acc;
  const StringChoice* _named;
  const ColCodecParams& _params;
  StringTuning& _tuning;
  DedupScratch _dedup;
  SegmentBuilder _builder;
  text::TrainedDictionaries _dictionaries;
  LeafEncoders _encoders;
  std::vector<std::unique_ptr<Segment>> _pool;
  std::vector<Segment*> _free;
  std::vector<Segment*> _live;
  uint64_t _begin = 0;
  uint64_t _end = 0;
  Profile _profile;
  bool _measured = false;
  Candidate _smallest;
};

}  // namespace

void StringAccumulator::Reserve(uint64_t rows, uint64_t distinct) {
  codes.reserve(rows);
  if (!_dedup) {
    entries.reserve(rows);
    return;
  }
  const auto expected = distinct == 0 ? rows : std::min(rows, distinct);
  entries.reserve(expected);
  _map.reserve(expected);
}

void StringAccumulator::Add(const duckdb::Vector& input) {
  const auto base = codes.size();
  codes.resize(base + input.size());
  auto* out = codes.data() + base;
  for (const auto entry : input.Values<string_t>()) {
    auto& code = out[entry.GetIndex()];
    if (!entry.IsValid()) {
      code = 0;
      ++null_count;
      continue;
    }
    const auto& value = entry.GetValue();
    const std::string_view sv{value.GetData(), value.GetSize()};
    if (_dedup && _last_code != 0 && value == _last) {
      code = _last_code;
      continue;
    }
    const auto next = static_cast<uint32_t>(entries.size() + 1);
    if (_dedup) {
      const auto [it, inserted] = _map.try_emplace(sv, next);
      _last = value;
      _last_code = it->second;
      code = it->second;
      if (!inserted) {
        continue;
      }
    } else {
      code = next;
    }
    entries.emplace_back(sv);
  }
}

SealOutcome SealSegments(const StringAccumulator& acc,
                         const StringChoice* named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink) {
  return SegmentWriter{acc, named, params, type, tuning}.Run(sink);
}

}  // namespace irs::codecs
