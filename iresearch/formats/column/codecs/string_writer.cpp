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

#include <algorithm>
#include <duckdb/common/types/string_type.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/blob_encoder.tpp"
#include "iresearch/formats/column/codecs/fsst_chain_encoder.hpp"
#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/numeric.hpp"
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
using text::DedupScratch;
using text::FsstChainEncoder;
using text::kDriftMinFraction;
using text::Packed;
using text::Profile;
using text::Segment;
using text::SegmentBuilder;

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr double kDrift = 1.25;
constexpr uint32_t kMaxCalibrationGap = 16;
constexpr double kLevelTolerance = 0.02;
constexpr size_t kMaxRungs = 8;
constexpr double kWideFramesGain = 0.03;
constexpr double kUntrainedGain = 0.02;
constexpr double kFsstPreference = 0.05;

constexpr uint8_t kLz4Fast = 1;
constexpr uint8_t kRefreshLz4Levels[] = {1, 4};
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

class SegmentWriter {
 public:
  SegmentWriter(const StringAccumulator& acc, std::optional<StringChoice> named,
                const ColCodecParams& params, const duckdb::LogicalType& type,
                StringTuning& tuning)
    : _acc{acc}, _named{named}, _params{params}, _type{type}, _tuning{tuning} {}

  SealOutcome Run(SegmentSink sink) {
    SealOutcome outcome{.sealed = true};
    uint64_t row = 0;
    bool first = true;
    const bool due = !_named && CalibrationDue();
    while (row < _acc.row_count) {
      const auto cutter = Cutter();
      auto* seg = Acquire();
      const auto end = With(cutter.choice.leaf, [&](auto& enc) {
        _builder.Begin(cutter.choice.shape, cutter.level, cutter.layout, row);
        enc.Begin(cutter.choice.shape, cutter.level, cutter.layout);
        const auto stop = _builder.Cut(enc, _params.segment_target);
        _builder.Finish(enc, *seg);
        return stop;
      });
      if (!_named) {
        const bool scheduled = first && due;
        const bool drift =
          !scheduled && Drifted(*seg, first || end < _acc.row_count);
        const bool retune = drift && !first;
        if (retune) {
          if (auto& fsst =
                std::get<std::optional<FsstChainEncoder>>(_encoders)) {
            fsst->Retrain();
          }
        }
        if (scheduled || drift) {
          seg =
            Calibrate(seg, row, end, !_tuning.levels_tuned || retune, drift);
        }
        if (first && seg->Size() > PlainStorageBytes(*seg)) {
          Release(seg);
          return {};
        }
      }
      outcome.all_dedup =
        outcome.all_dedup && seg->choice.shape == Shape::Dedup;
      RecodeCodes(*seg);
      const std::string_view parts[] = {seg->head, seg->data};
      sink(seg->choice, std::move(*seg->stats), seg->rows, parts);
      Release(seg);
      row = end;
      first = false;
    }
    return outcome;
  }

 private:
  struct Pick {
    StringChoice choice;
    uint8_t level;
    FrameLayout layout;
  };

  void RecodeCodes(Segment& seg) const {
    if (seg.choice.shape != Shape::Dedup) {
      return;
    }
    auto* base = reinterpret_cast<duckdb::data_ptr_t>(seg.head.data());
    auto h = Header::Parse(base, seg.Size());
    SDB_ASSERT(seg.codes.size() == h.row_count);
    const uint64_t current = h.off_symtab - h.off_codes;
    auto encoded = EncodeCodes(seg.codes, current, _tuning.codes);
    if (!encoded) {
      return;
    }
    const auto symtab =
      std::string_view{seg.head}.substr(h.off_symtab, h.symtab_size);
    std::string head{seg.head, 0, h.off_codes};
    head.append(encoded->bytes);
    head.resize(Align8(head.size()), '\0');
    h.off_runs = static_cast<uint32_t>(head.size());
    h.off_symtab = h.off_runs;
    head.append(symtab);
    head.resize(Align8(head.size()), '\0');
    h.off_data = static_cast<uint32_t>(head.size());
    h.codes_encoding = static_cast<uint8_t>(CodesEncoding::Numeric);
    h.run_count = 0;
    h.run_width = 0;
    h.Write(reinterpret_cast<duckdb::data_ptr_t>(head.data()));
    seg.head.swap(head);
  }

  Pick Cutter() const noexcept {
    if (_named) {
      return {*_named, _params.compression_level, FrameLayout::Dictionary};
    }
    if (_tuning.choice) {
      const auto leaf = Index(_tuning.choice->leaf);
      return {*_tuning.choice, _tuning.level[leaf], _tuning.layout[leaf]};
    }
    const bool repeats =
      Repeats(_acc.entries.size(), _acc.row_count - _acc.null_count);
    return {{repeats ? Shape::Dedup : Shape::Plain, ByteCodec::Lz4},
            kLz4Fast,
            FrameLayout::Dictionary};
  }

  bool CalibrationDue() noexcept {
    if (!_tuning.choice) {
      return true;
    }
    return ++_tuning.since_calibration >= _tuning.calibration_gap;
  }

  Segment* Calibrate(Segment* cut, uint64_t begin, uint64_t end, bool retune,
                     bool drift) {
    _live.clear();
    _live.push_back(cut);
    const auto base = kLz4Fast;
    const auto tuned = _tuning.layout[Index(ByteCodec::Lz4)];
    const auto layout = retune || tuned == FrameLayout::Dictionary
                          ? FrameLayout::FirstFrame
                          : tuned;
    auto* dedup =
      Trial({Shape::Dedup, ByteCodec::Lz4}, base, begin, end, layout);
    Segment* plain = nullptr;
    if (PlainMayWin(*dedup)) {
      plain = Trial({Shape::Plain, ByteCodec::Lz4}, base, begin, end, layout);
    }
    const auto shape =
      plain && PlainWins(*dedup, *plain) ? Shape::Plain : Shape::Dedup;
    _measured = false;
    _smallest = {.bytes = std::numeric_limits<uint64_t>::max()};
    Candidate chosen{.bytes = std::numeric_limits<uint64_t>::max()};
    for (const auto& plan : PlanFor(_params)) {
      const auto c = Tune(shape, plan, begin, end, retune);
      if (c.bytes < chosen.bytes) {
        chosen = c;
      }
    }
    const auto& write = _smallest.bytes < chosen.bytes ? _smallest : chosen;
    Trial({shape, write.leaf}, write.level, begin, end, write.layout);
    auto* best = Smallest(shape, shape == Shape::Dedup ? dedup : plain);
    if (_params.tier == WriteTier::Flush &&
        best->choice.leaf != ByteCodec::Fsst) {
      if (auto* fsst = Cached({shape, ByteCodec::Fsst}, 0, begin, end,
                              FrameLayout::Dictionary);
          fsst &&
          static_cast<double>(fsst->Size()) <=
            static_cast<double>(best->Size()) * (1.0 + kFsstPreference)) {
        best = fsst;
      }
    }
    const auto chosen_bytes = best->Size();
    const StringChoice picked{shape, best->choice.leaf};
    const bool kept = _tuning.choice && *_tuning.choice == picked;
    _tuning.calibration_gap =
      drift || !kept
        ? 1
        : std::min(_tuning.calibration_gap * 2, kMaxCalibrationGap);
    _tuning.since_calibration = 0;
    _tuning.levels_tuned = true;
    _tuning.choice = picked;
    _tuning.bytes_per_input =
      cut->input == 0
        ? 0
        : static_cast<double>(chosen_bytes) / static_cast<double>(cut->input);
    for (auto* seg : _live) {
      if (seg != best) {
        Release(seg);
      }
    }
    _live.clear();
    return best;
  }

  struct Candidate {
    ByteCodec leaf = ByteCodec::Lz4;
    uint8_t level = 0;
    FrameLayout layout = FrameLayout::Dictionary;
    uint64_t bytes = 0;
  };

  static bool Cheap(ByteCodec leaf, uint8_t level) noexcept {
    return leaf == ByteCodec::Fsst ||
           (leaf == ByteCodec::Lz4 && EffectiveLevel<Lz4>(level) <= 1);
  }

  Candidate Tune(Shape shape, const LeafPlan& plan, uint64_t begin,
                 uint64_t end, bool retune) {
    const auto leaf = plan.leaf;
    const auto ladder = plan.ladder;
    auto& tuned = _tuning.level[Index(leaf)];
    auto& tuned_layout = _tuning.layout[Index(leaf)];
    size_t rung = 0;
    if (!retune) {
      const auto it = std::find(ladder.begin(), ladder.end(), tuned);
      rung = it == ladder.end() ? 0 : static_cast<size_t>(it - ladder.begin());
    }
    auto layout = retune ? FrameLayout::Dictionary : tuned_layout;
    std::optional<uint64_t> bytes;
    if (retune && leaf != ByteCodec::Fsst) {
      SDB_ASSERT(ladder.size() <= kMaxRungs);
      uint64_t prices[kMaxRungs];
      uint64_t lowest = std::numeric_limits<uint64_t>::max();
      for (size_t r = 0; r < ladder.size(); ++r) {
        prices[r] =
          PriceOf(shape, leaf, ladder[r], FrameLayout::Dictionary, begin, end);
        lowest = std::min(lowest, prices[r]);
      }
      while (static_cast<double>(prices[rung]) >
             static_cast<double>(lowest) * (1.0 + kLevelTolerance)) {
        ++rung;
      }
      bytes = prices[rung];
      const auto wide =
        PriceOf(shape, leaf, ladder[rung], FrameLayout::Wide, begin, end);
      if (static_cast<double>(wide) <
          static_cast<double>(*bytes) * (1.0 - kWideFramesGain)) {
        layout = FrameLayout::Wide;
        bytes = wide;
      }
      if (Trainable(leaf) && _tuning.dictionary) {
        const auto first = PriceOf(shape, leaf, ladder[rung],
                                   FrameLayout::FirstFrame, begin, end);
        if (static_cast<double>(first) <
            static_cast<double>(*bytes) * (1.0 - kUntrainedGain)) {
          layout = FrameLayout::FirstFrame;
          bytes = first;
        }
      }
      tuned_layout = layout;
    }
    tuned = ladder[rung];
    auto* seg = Cached({shape, leaf}, tuned, begin, end, layout);
    if (!seg && Cheap(leaf, tuned)) {
      seg = Trial({shape, leaf}, tuned, begin, end, layout);
    }
    if (seg) {
      return {leaf, tuned, layout, seg->Size()};
    }
    if (!bytes) {
      bytes = PriceOf(shape, leaf, tuned, layout, begin, end);
    }
    return {leaf, tuned, layout, *bytes};
  }

  void Measure(Shape shape, uint64_t begin, uint64_t end) {
    if (_measured) {
      return;
    }
    _measured = true;
    auto& p = _profile;
    p.shape = shape;
    p.rows = end - begin;
    p.runs = 0;
    p.raw = 0;
    p.max_len = 0;
    p.entries.clear();
    if (shape == Shape::Plain) {
      for (auto row = begin; row < end; ++row) {
        const auto code = _acc.codes[row];
        Append(code == 0 ? std::string_view{} : _acc.entries[code - 1]);
      }
      return;
    }
    _dedup.Begin(_acc.entries.size());
    uint32_t last = 0;
    for (auto row = begin; row < end; ++row) {
      const auto code = _acc.codes[row];
      uint32_t local = 0;
      if (code != 0) {
        bool fresh = false;
        std::tie(local, fresh) =
          _dedup.Local(code, static_cast<uint32_t>(p.entries.size()) + 1);
        if (fresh) {
          Append(_acc.entries[code - 1]);
        }
      }
      if (row == begin || local != last) {
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

  FrameLayout Normalize(ByteCodec leaf, FrameLayout layout) const noexcept {
    if (layout == FrameLayout::FirstFrame &&
        (!_tuning.dictionary || !Trainable(leaf))) {
      return FrameLayout::Dictionary;
    }
    return layout;
  }

  uint64_t PriceOf(Shape shape, ByteCodec leaf, uint8_t level,
                   FrameLayout layout, uint64_t begin, uint64_t end) {
    layout = Normalize(leaf, layout);
    if (const auto* seg = Cached({shape, leaf}, level, begin, end, layout)) {
      return seg->Size();
    }
    Measure(shape, begin, end);
    const auto bytes = With(leaf, [&](auto& enc) -> uint64_t {
      if constexpr (std::remove_reference_t<decltype(enc)>::kCodec ==
                    ByteCodec::Fsst) {
        SDB_UNREACHABLE();
      } else {
        return enc.Price(_profile, level, layout, begin);
      }
    });
    if (bytes < _smallest.bytes) {
      _smallest = {leaf, level, layout, bytes};
    }
    return bytes;
  }

  Segment* Smallest(Shape shape, Segment* best) const noexcept {
    for (auto* seg : _live) {
      if (seg->choice.shape == shape && seg->Size() < best->Size()) {
        best = seg;
      }
    }
    return best;
  }

  Segment* Cached(StringChoice choice, uint8_t level, uint64_t begin,
                  uint64_t end, FrameLayout layout) const noexcept {
    for (auto* seg : _live) {
      if (seg->choice.shape == choice.shape &&
          seg->choice.leaf == choice.leaf && seg->level == level &&
          seg->layout == layout && seg->begin == begin &&
          seg->rows == end - begin) {
        return seg;
      }
    }
    return nullptr;
  }

  Segment* Trial(StringChoice choice, uint8_t level, uint64_t begin,
                 uint64_t end, FrameLayout layout = FrameLayout::Dictionary) {
    layout = Normalize(choice.leaf, layout);
    if (auto* seg = Cached(choice, level, begin, end, layout)) {
      return seg;
    }
    auto* seg = Acquire();
    With(choice.leaf, [&](auto& enc) {
      _builder.Begin(choice.shape, level, layout, begin);
      enc.Begin(choice.shape, level, layout);
      _builder.AddUntil(enc, end);
      _builder.Finish(enc, *seg);
      return end;
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

  static bool PlainMayWin(const Segment& dedup) noexcept {
    const auto floor = kHeaderSize + Packed(dedup.rows, dedup.max_len) +
                       dedup.input / kLz4MaxRatio;
    if (Repeats(dedup)) {
      return SavesEnough(dedup.Size(), floor);
    }
    return floor <= dedup.Size();
  }

  static bool PlainWins(const Segment& dedup, const Segment& plain) noexcept {
    if (Repeats(dedup)) {
      return SavesEnough(dedup.Size(), plain.Size());
    }
    return plain.Size() <= dedup.Size();
  }

  static bool SavesEnough(uint64_t dedup, uint64_t plain) noexcept {
    return static_cast<double>(plain) <
             static_cast<double>(dedup) * kPlainWinsBelow &&
           plain + kPlainMinSaving <= dedup;
  }

  bool Drifted(const Segment& seg, bool full) const noexcept {
    if (seg.input == 0 || _tuning.bytes_per_input == 0 ||
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
  std::invoke_result_t<F&, BlobEncoder<Lz4>&> With(ByteCodec leaf, F&& f) {
    switch (leaf) {
      case ByteCodec::Lz4:
        return f(Get<BlobEncoder<Lz4>>());
      case ByteCodec::Zstd:
        return f(Get<BlobEncoder<Zstd>>());
      case ByteCodec::Zxc:
        return f(Get<BlobEncoder<Zxc>>());
      case ByteCodec::Fsst:
        return f(Get<FsstChainEncoder>());
    }
    SDB_UNREACHABLE();
  }

  template<typename Encoder>
  Encoder& Get() {
    auto& slot = std::get<std::optional<Encoder>>(_encoders);
    if (!slot) {
      slot.emplace(_tuning, _params.segment_target);
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
  std::optional<StringChoice> _named;
  const ColCodecParams& _params;
  const duckdb::LogicalType& _type;
  StringTuning& _tuning;
  DedupScratch _dedup;
  SegmentBuilder _builder{_acc, _type, _dedup};
  std::tuple<std::optional<BlobEncoder<Lz4>>, std::optional<BlobEncoder<Zstd>>,
             std::optional<BlobEncoder<Zxc>>, std::optional<FsstChainEncoder>>
    _encoders;
  std::vector<std::unique_ptr<Segment>> _pool;
  std::vector<Segment*> _free;
  std::vector<Segment*> _live;
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
  duckdb::UnifiedVectorFormat vdata;
  input.ToUnifiedFormat(vdata);
  const auto* strings = duckdb::UnifiedVectorFormat::GetData<string_t>(vdata);
  const auto count = input.size();
  const auto base = codes.size();
  codes.resize(base + count);
  auto* out = codes.data() + base;
  for (idx_t i = 0; i < count; ++i) {
    const auto idx = vdata.sel->get_index(i);
    if (!vdata.validity.RowIsValid(idx)) {
      out[i] = 0;
      ++null_count;
      continue;
    }
    const auto& value = strings[idx];
    const std::string_view sv{value.GetData(), value.GetSize()};
    if (_dedup && _last_code != 0 && value == _last) {
      out[i] = _last_code;
      continue;
    }
    const auto next = static_cast<uint32_t>(entries.size() + 1);
    if (_dedup) {
      const auto [it, inserted] = _map.try_emplace(sv, next);
      _last = value;
      _last_code = it->second;
      out[i] = it->second;
      if (!inserted) {
        continue;
      }
    } else {
      out[i] = next;
    }
    entries.emplace_back(sv);
  }
  row_count += count;
}

bool TrainsDictionary(std::optional<StringChoice> named,
                      const ColCodecParams& params) noexcept {
  if (params.tier == WriteTier::Flush) {
    return false;
  }
  return !named || Trainable(named->leaf);
}

SealOutcome SealSegments(const StringAccumulator& acc,
                         std::optional<StringChoice> named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink, DictionarySink dictionaries) {
  SDB_ASSERT(acc.codes.size() == acc.row_count);
  if (!tuning.sampling_done && TrainsDictionary(named, params)) {
    if (!tuning.sampler.Ready()) {
      tuning.sampler.Add(acc.entries);
    }
    if (tuning.sampler.Ready()) {
      tuning.sampling_done = true;
      if (auto bytes = tuning.sampler.Train()) {
        tuning.dictionary_id = dictionaries(*bytes);
        tuning.dictionary =
          std::make_shared<const TrainedDictionary>(std::move(*bytes));
        tuning.levels_tuned = false;
        tuning.calibration_gap = 1;
        tuning.since_calibration = 0;
      }
    }
  }
  return SegmentWriter{acc, named, params, type, tuning}.Run(sink);
}

}  // namespace irs::codecs
