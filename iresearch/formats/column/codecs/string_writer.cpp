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

#include <absl/base/internal/endian.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/formats/column/codecs/fsst_codec.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::idx_t;
using duckdb::string_t;

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kEstimateEvery = 64;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr double kFsstFirstRatio = 0.5;
constexpr double kDrift = 1.25;
constexpr uint64_t kDriftMinFraction = 4;
constexpr double kLevelStepGain = 0.02;

constexpr uint8_t kLz4Fast[] = {1};
constexpr uint8_t kLz4Ladder[] = {1, 4, 9};
constexpr uint8_t kZstdLadder[] = {1, 3, 6, 9, 12};
constexpr uint8_t kZxcFastLookups[] = {1, 3, 5};
constexpr uint8_t kZxcLadder[] = {1, 3, 5, 7};
constexpr uint8_t kNoLevel[] = {0};

struct LeafPlan {
  ByteCodec leaf;
  std::span<const uint8_t> ladder;
};

constexpr LeafPlan kSpeedPlan[] = {{ByteCodec::Lz4, kLz4Fast}};
constexpr LeafPlan kBalancedPlan[] = {{ByteCodec::Fsst, kNoLevel},
                                      {ByteCodec::Lz4, kLz4Ladder},
                                      {ByteCodec::Zxc, kZxcFastLookups}};
constexpr LeafPlan kSizePlan[] = {{ByteCodec::Fsst, kNoLevel},
                                  {ByteCodec::Lz4, kLz4Ladder},
                                  {ByteCodec::Zstd, kZstdLadder},
                                  {ByteCodec::Zxc, kZxcLadder}};

std::span<const LeafPlan> PlanFor(AutoObjective objective) noexcept {
  switch (objective) {
    case AutoObjective::Speed:
      return kSpeedPlan;
    case AutoObjective::Size:
      return kSizePlan;
    case AutoObjective::Balanced:
      break;
  }
  return kBalancedPlan;
}

constexpr size_t Index(ByteCodec leaf) noexcept {
  return static_cast<size_t>(leaf);
}

uint64_t Packed(uint64_t count, uint32_t max_value) noexcept {
  return BitpackingPrimitives::GetRequiredSize(
    count, BitpackingPrimitives::MinimumBitWidth<uint32_t>(max_value));
}

uint64_t CodesBytes(Shape shape, uint64_t rows, uint64_t entries, uint64_t runs,
                    CodesEncoding* chosen = nullptr) noexcept {
  if (shape != Shape::Dedup) {
    return 0;
  }
  const auto max_code = static_cast<uint32_t>(entries);
  const auto bitpack = Packed(rows, max_code);
  const auto rle =
    Packed(runs, max_code) + Packed(runs, static_cast<uint32_t>(rows));
  if (chosen) {
    *chosen = rle < bitpack ? CodesEncoding::Rle : CodesEncoding::Bitpack;
  }
  return std::min(bitpack, rle);
}

uint32_t Lcp(std::string_view a, std::string_view b) noexcept {
  const auto n = std::min(a.size(), b.size());
  size_t i = 0;
  for (; i + 8 <= n; i += 8) {
    const auto x = absl::little_endian::Load64(a.data() + i);
    const auto y = absl::little_endian::Load64(b.data() + i);
    if (x != y) {
      return static_cast<uint32_t>(i + (std::countr_zero(x ^ y) >> 3));
    }
  }
  while (i < n && a[i] == b[i]) {
    ++i;
  }
  return static_cast<uint32_t>(i);
}

struct DedupScratch {
  std::vector<uint32_t> epoch;
  std::vector<uint32_t> local;
  uint32_t current = 0;
};

struct Segment {
  StringChoice choice{};
  uint8_t level = 0;
  uint64_t begin = 0;
  uint64_t rows = 0;
  uint64_t entries = 0;
  uint64_t nulls = 0;
  uint64_t input = 0;
  uint32_t max_len = 0;
  std::string head;
  std::string data;
  std::optional<duckdb::BaseStatistics> stats;

  uint64_t Size() const noexcept { return head.size() + data.size(); }
};

template<ByteCodec C>
class Encoder {
 public:
  static constexpr bool kFsst = C == ByteCodec::Fsst;

  Encoder(const StringAccumulator& acc, const duckdb::LogicalType& type,
          RatioHistory* history, DedupScratch& dedup)
    : _acc{acc}, _stats{type}, _type{type}, _history{history}, _dedup{dedup} {
    if constexpr (kFsst) {
      _codec.emplace();
    } else {
      _codec.emplace(uint8_t{0});
    }
  }

  void Begin(Shape shape, uint8_t level, uint64_t begin) {
    _shape = shape;
    _level = level;
    _next = begin;
    if constexpr (!kFsst) {
      _codec->SetLevel(level);
    }
    _data.clear();
    if (shape == Shape::Dedup) {
      if (_dedup.epoch.size() != _acc.entries.size()) {
        _dedup.epoch.assign(_acc.entries.size(), 0);
        _dedup.local.assign(_acc.entries.size(), 0);
      }
      ++_dedup.current;
    }
  }

  uint64_t Cut(uint32_t target) {
    while (_next < _acc.row_count) {
      AddRow(_next++);
      if (_rows % kEstimateEvery == 0 && Estimate() >= target) {
        break;
      }
    }
    return _next;
  }

  void AddUntil(uint64_t end) {
    while (_next < end) {
      AddRow(_next++);
    }
  }

  void Finish(Segment& out) {
    SDB_ASSERT(_rows != 0);
    std::string_view symtab;
    if constexpr (kFsst) {
      EncodeFsst();
      symtab = _codec->SymbolTable();
    } else {
      if (_frame_entries != 0) {
        CloseFrame();
      }
      _entry_lengths.resize(_entries.size());
      for (size_t i = 0; i < _entries.size(); ++i) {
        _entry_lengths[i] = static_cast<uint32_t>(_entries[i].size());
      }
      _max_enc = _max_len;
      _max_lcp = 0;
      _lcps.clear();
    }
    const auto entry_count = _entries.size() + FirstEntry();
    SDB_ENSURE(_rows <= std::numeric_limits<uint32_t>::max() &&
                 entry_count <= std::numeric_limits<uint32_t>::max() &&
                 _data.size() <= std::numeric_limits<uint32_t>::max(),
               "col codec: segment too large");

    Header h;
    h.shape = static_cast<uint8_t>(_shape);
    h.codec = static_cast<uint8_t>(C);
    h.level = _level;
    h.length_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(_max_enc);
    h.lcp_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(_max_lcp);
    h.code_width = _shape == Shape::Dedup
                     ? BitpackingPrimitives::MinimumBitWidth<uint32_t>(
                         static_cast<uint32_t>(_entries.size()))
                     : 0;
    h.row_count = static_cast<uint32_t>(_rows);
    h.entry_count = static_cast<uint32_t>(entry_count);
    h.frame_count = static_cast<uint32_t>(_frames.size());
    h.raw_bytes = _raw;

    CodesEncoding codes_encoding = CodesEncoding::Bitpack;
    CodesBytes(_shape, _rows, _entries.size(), _runs, &codes_encoding);
    uint64_t codes_count = 0;
    if (_shape == Shape::Dedup) {
      if (codes_encoding == CodesEncoding::Rle) {
        _run_values.clear();
        _run_ends.clear();
        for (size_t i = 0; i < _codes.size(); ++i) {
          if (i == 0 || _codes[i] != _codes[i - 1]) {
            _run_values.push_back(_codes[i]);
            _run_ends.push_back(static_cast<uint32_t>(i));
          }
        }
        for (size_t r = 0; r + 1 < _run_ends.size(); ++r) {
          _run_ends[r] = _run_ends[r + 1];
        }
        _run_ends.back() = static_cast<uint32_t>(_codes.size());
        codes_count = _run_values.size();
      } else {
        codes_count = _rows;
      }
    }
    h.codes_encoding = static_cast<uint8_t>(codes_encoding);
    h.run_count = codes_encoding == CodesEncoding::Rle
                    ? static_cast<uint32_t>(_run_values.size())
                    : 0;
    h.run_width = codes_encoding == CodesEncoding::Rle
                    ? BitpackingPrimitives::MinimumBitWidth<uint32_t>(
                        static_cast<uint32_t>(_rows))
                    : 0;

    _lengths.assign(GroupPadded(entry_count), 0);
    std::copy(_entry_lengths.begin(), _entry_lengths.end(),
              _lengths.begin() + FirstEntry());
    _lcp_stream.assign(kFsst ? GroupPadded(entry_count) : 0, 0);
    if constexpr (kFsst) {
      std::copy(_lcps.begin(), _lcps.end(), _lcp_stream.begin() + FirstEntry());
    }
    const auto lengths_bytes =
      BitpackingPrimitives::GetRequiredSize(entry_count, h.length_width);
    const auto lcps_bytes =
      kFsst ? BitpackingPrimitives::GetRequiredSize(entry_count, h.lcp_width)
            : 0;
    const auto codes_bytes =
      BitpackingPrimitives::GetRequiredSize(codes_count, h.code_width);
    const auto runs_bytes =
      BitpackingPrimitives::GetRequiredSize(h.run_count, h.run_width);

    h.off_frames = kHeaderSize;
    h.off_lengths = Align8(h.off_frames + _frames.size() * kFrameMetaSize);
    h.off_lcps = Align8(h.off_lengths + lengths_bytes);
    h.off_codes = Align8(h.off_lcps + lcps_bytes);
    h.off_runs = Align8(h.off_codes + codes_bytes);
    h.off_symtab = Align8(h.off_runs + runs_bytes);
    h.symtab_size = static_cast<uint32_t>(symtab.size());
    h.off_data = Align8(h.off_symtab + symtab.size());
    h.data_size = static_cast<uint32_t>(_data.size());

    _bytes.assign(static_cast<size_t>(h.off_data), '\0');
    auto* base = reinterpret_cast<duckdb::data_ptr_t>(_bytes.data());
    h.Write(base);
    for (size_t i = 0; i < _frames.size(); ++i) {
      _frames[i].Store(base + h.off_frames + i * kFrameMetaSize);
    }
    BitpackingPrimitives::PackBuffer<uint32_t, true>(
      base + h.off_lengths, _lengths.data(), _lengths.size(), h.length_width);
    if constexpr (kFsst) {
      BitpackingPrimitives::PackBuffer<uint32_t, true>(
        base + h.off_lcps, _lcp_stream.data(), _lcp_stream.size(), h.lcp_width);
    }
    if (_shape == Shape::Dedup) {
      if (codes_encoding == CodesEncoding::Rle) {
        _run_values.resize(GroupPadded(_run_values.size()), 0);
        _run_ends.resize(GroupPadded(_run_ends.size()), 0);
        BitpackingPrimitives::PackBuffer<uint32_t, true>(
          base + h.off_codes, _run_values.data(), _run_values.size(),
          h.code_width);
        BitpackingPrimitives::PackBuffer<uint32_t, true>(
          base + h.off_runs, _run_ends.data(), _run_ends.size(), h.run_width);
      } else {
        _codes.resize(GroupPadded(_rows), 0);
        BitpackingPrimitives::PackBuffer<uint32_t, true>(
          base + h.off_codes, _codes.data(), _codes.size(), h.code_width);
      }
    }
    if (!symtab.empty()) {
      std::memcpy(base + h.off_symtab, symtab.data(), symtab.size());
    }
    if constexpr (kFsst) {
      auto& hist = History();
      hist.raw += _raw;
      hist.comp += _data.size() + symtab.size();
    }

    auto stats = duckdb::BaseStatistics::CreateEmpty(_type);
    _stats.Merge(stats);
    out.choice = StringChoice{_shape, C};
    out.level = _level;
    out.begin = _next - _rows;
    out.rows = _rows;
    out.entries = _entries.size();
    out.nulls = _nulls;
    out.input = _input;
    out.max_len = _max_len;
    out.stats.emplace(std::move(stats));
    out.head.swap(_bytes);
    out.data.swap(_data);

    _stats.Clear();
    _codes.clear();
    _entries.clear();
    _entry_lengths.clear();
    _frames.clear();
    _data.clear();
    _rows = 0;
    _runs = 0;
    _raw = 0;
    _max_len = 0;
    _nulls = 0;
    _input = 0;
  }

 private:
  uint32_t FirstEntry() const noexcept {
    return _shape == Shape::Dedup ? 1 : 0;
  }

  RatioHistory& History() noexcept {
    return _history[static_cast<size_t>(_shape)];
  }

  double Ratio() const noexcept {
    const auto& hist = _history[static_cast<size_t>(_shape)];
    if (hist.raw != 0) {
      return static_cast<double>(hist.comp) / static_cast<double>(hist.raw);
    }
    return kFsst ? kFsstFirstRatio : 1.0;
  }

  void PushCode(uint32_t code) {
    if (_rows == 0 || code != _last_code) {
      ++_runs;
    }
    _last_code = code;
    _codes.push_back(code);
  }

  void AddRow(uint64_t row) {
    const auto code = _acc.codes[row];
    if (code == 0) {
      _stats.SetHasNull();
      ++_nulls;
      if (_shape == Shape::Dedup) {
        PushCode(0);
      } else {
        AppendEntry({});
      }
      ++_rows;
      return;
    }
    const auto sv = _acc.entries[code - 1];
    _input += sv.size();
    if (_shape == Shape::Plain) {
      _stats.Update(string_t{sv.data(), static_cast<uint32_t>(sv.size())});
      AppendEntry(sv);
      ++_rows;
      return;
    }
    if (_dedup.epoch[code - 1] != _dedup.current) {
      _dedup.epoch[code - 1] = _dedup.current;
      _dedup.local[code - 1] = static_cast<uint32_t>(_entries.size()) + 1;
      _stats.Update(string_t{sv.data(), static_cast<uint32_t>(sv.size())});
      AppendEntry(sv);
    }
    PushCode(_dedup.local[code - 1]);
    ++_rows;
  }

  void AppendEntry(std::string_view sv) {
    _entries.push_back(sv);
    _max_len = std::max<uint32_t>(_max_len, static_cast<uint32_t>(sv.size()));
    _raw += sv.size();
    if constexpr (!kFsst) {
      if (_frame_entries != 0 && _frame.size() + sv.size() > kFrameRawBytes) {
        CloseFrame();
      }
      if (_frame_entries == 0) {
        _frame_first =
          FirstEntry() + static_cast<uint32_t>(_entries.size()) - 1;
      }
      _frame.append(sv);
      ++_frame_entries;
    }
  }

  void CloseFrame() {
    const auto bound = Leaf<C>::Bound(_frame.size());
    const auto comp_off = _data.size();
    _data.resize(comp_off + bound);
    const auto n = _codec->Compress(_frame.data(), _frame.size(),
                                    _data.data() + comp_off, bound);
    _data.resize(comp_off + n);
    _frames.push_back(
      FrameMeta{_frame_first, static_cast<uint32_t>(_frame.size()),
                static_cast<uint32_t>(comp_off), static_cast<uint32_t>(n)});
    auto& hist = History();
    hist.raw += _frame.size();
    hist.comp += n;
    _frame.clear();
    _frame_entries = 0;
  }

  uint64_t Estimate() const noexcept {
    const auto entry_count = _entries.size() + FirstEntry();
    const auto ratio = Ratio();
    uint64_t est = kHeaderSize + Packed(entry_count, _max_len) +
                   CodesBytes(_shape, _rows, _entries.size(), _runs);
    if constexpr (kFsst) {
      est += Packed(entry_count, _max_len) +
             (_raw / kFsstFrameRawBytes + 1) * kFrameMetaSize +
             static_cast<uint64_t>(static_cast<double>(_raw) * ratio);
    } else {
      est += (_frames.size() + 1) * kFrameMetaSize + _data.size() +
             static_cast<uint64_t>(static_cast<double>(_frame.size()) * ratio);
    }
    return est;
  }

  static uint64_t PrefixKey(std::string_view sv, size_t skip) noexcept {
    char buf[8] = {};
    if (sv.size() > skip) {
      std::memcpy(buf, sv.data() + skip,
                  std::min<size_t>(sv.size() - skip, sizeof buf));
    }
    return absl::big_endian::Load64(buf);
  }

  void SortEntries() {
    const auto n = static_cast<uint32_t>(_entries.size());
    size_t common = _entries.empty() ? 0 : _entries[0].size();
    for (uint32_t i = 1; i < n && common != 0; ++i) {
      common = std::min<size_t>(common, Lcp(_entries[0], _entries[i]));
    }
    _order.resize(n);
    for (uint32_t i = 0; i < n; ++i) {
      _order[i] = {PrefixKey(_entries[i], common), i};
    }
    std::sort(_order.begin(), _order.end(),
              [&](const std::pair<uint64_t, uint32_t>& a,
                  const std::pair<uint64_t, uint32_t>& b) {
                if (a.first != b.first) {
                  return a.first < b.first;
                }
                return _entries[a.second] < _entries[b.second];
              });
    _remap.assign(n + 1, 0);
    _sorted.resize(n);
    for (uint32_t pos = 0; pos < n; ++pos) {
      _remap[_order[pos].second + 1] = pos + 1;
      _sorted[pos] = _entries[_order[pos].second];
    }
    _entries.swap(_sorted);
    for (auto& code : _codes) {
      code = _remap[code];
    }
  }

  void EncodeFsst() {
    if (_shape == Shape::Dedup) {
      SortEntries();
    }
    _suffixes.clear();
    _lcps.clear();
    _frames.clear();
    std::string_view prev;
    uint64_t frame_raw = 0;
    uint64_t frame_first = 0;
    for (size_t i = 0; i < _entries.size(); ++i) {
      const auto sv = _entries[i];
      uint32_t lcp = Lcp(prev, sv);
      if (frame_raw != 0 && frame_raw + sv.size() > kFsstFrameRawBytes) {
        _frames.push_back(FrameMeta{static_cast<uint32_t>(frame_first),
                                    static_cast<uint32_t>(frame_raw), 0, 0});
        frame_raw = 0;
        frame_first = i;
      }
      if ((i - frame_first) % kChainRestart == 0) {
        lcp = 0;
      }
      _lcps.push_back(lcp);
      _suffixes.push_back(sv.substr(lcp));
      frame_raw += sv.size();
      prev = sv;
    }
    if (!_entries.empty()) {
      _frames.push_back(FrameMeta{static_cast<uint32_t>(frame_first),
                                  static_cast<uint32_t>(frame_raw), 0, 0});
    }
    _codec->Encode(_suffixes, _data, _entry_lengths);
    size_t off = 0;
    size_t next = 0;
    for (size_t f = 0; f < _frames.size(); ++f) {
      const auto end =
        f + 1 < _frames.size() ? _frames[f + 1].first_entry : _entries.size();
      _frames[f].comp_off = static_cast<uint32_t>(off);
      for (; next < end; ++next) {
        off += _entry_lengths[next];
      }
      _frames[f].comp_len = static_cast<uint32_t>(off - _frames[f].comp_off);
      _frames[f].first_entry += FirstEntry();
    }
    _max_enc = 0;
    for (const auto len : _entry_lengths) {
      _max_enc = std::max(_max_enc, len);
    }
    _max_lcp = 0;
    for (const auto lcp : _lcps) {
      _max_lcp = std::max(_max_lcp, lcp);
    }
  }

  const StringAccumulator& _acc;
  Shape _shape = Shape::Dedup;
  uint8_t _level = 0;
  uint64_t _next = 0;
  using Codec = std::conditional_t<kFsst, FsstEncoder, LeafCompressor<C>>;
  std::optional<Codec> _codec;
  duckdb::StatsWriter<string_t> _stats;
  duckdb::LogicalType _type;
  RatioHistory* _history;
  DedupScratch& _dedup;

  std::vector<std::string_view> _entries;
  std::vector<uint32_t> _codes;
  uint64_t _rows = 0;
  uint64_t _runs = 0;
  uint32_t _last_code = 0;
  uint64_t _raw = 0;
  uint32_t _max_len = 0;
  uint64_t _nulls = 0;
  uint64_t _input = 0;

  std::string _frame;
  uint32_t _frame_first = 0;
  uint32_t _frame_entries = 0;
  std::vector<FrameMeta> _frames;
  std::string _data;
  std::vector<uint32_t> _entry_lengths;
  std::vector<std::string_view> _suffixes;
  std::vector<uint32_t> _lcps;
  uint32_t _max_enc = 0;
  uint32_t _max_lcp = 0;

  std::vector<std::pair<uint64_t, uint32_t>> _order;
  std::vector<uint32_t> _remap;
  std::vector<std::string_view> _sorted;
  std::vector<uint32_t> _lengths;
  std::vector<uint32_t> _lcp_stream;
  std::vector<uint32_t> _run_values;
  std::vector<uint32_t> _run_ends;
  std::string _bytes;
};

class SegmentWriter {
 public:
  SegmentWriter(const StringAccumulator& acc, std::optional<StringChoice> named,
                const ColCodecParams& params, const duckdb::LogicalType& type,
                StringTuning& tuning)
    : _acc{acc},
      _named{named},
      _params{params},
      _type{type},
      _tuning{tuning},
      _fixed{params.compression_level} {}

  SealOutcome Run(SegmentSink sink) {
    SealOutcome outcome{.sealed = true};
    uint64_t row = 0;
    bool first = true;
    while (row < _acc.row_count) {
      const auto cutter = Cutter();
      auto* seg = Acquire();
      const auto end = With(cutter.choice.leaf, [&](auto& enc) {
        enc.Begin(cutter.choice.shape, cutter.level, row);
        const auto stop = enc.Cut(_params.segment_target);
        enc.Finish(*seg);
        return stop;
      });
      if (!_named) {
        const bool drift = !first && Drifted(*seg);
        if (first || drift) {
          seg = Calibrate(seg, row, end, !_tuning.levels_tuned || drift);
        }
        if (first && seg->Size() > PlainStorageBytes(*seg)) {
          Release(seg);
          return {};
        }
      }
      outcome.all_dedup =
        outcome.all_dedup && seg->choice.shape == Shape::Dedup;
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
  };

  Pick Cutter() const noexcept {
    if (_named) {
      return {*_named, _params.compression_level};
    }
    if (_tuning.choice) {
      return {*_tuning.choice, _tuning.level[Index(_tuning.choice->leaf)]};
    }
    const bool repeats = _acc.entries.size() * kDedupMinRepeat <=
                         _acc.row_count - _acc.null_count;
    return {{repeats ? Shape::Dedup : Shape::Plain, ByteCodec::Lz4},
            Ladder(ByteCodec::Lz4)[0]};
  }

  std::span<const uint8_t> Ladder(ByteCodec leaf) const noexcept {
    if (leaf != ByteCodec::Fsst && _params.compression_level != 0) {
      return _fixed;
    }
    for (const auto& plan : PlanFor(_params.objective)) {
      if (plan.leaf == leaf) {
        return plan.ladder;
      }
    }
    return kNoLevel;
  }

  Segment* Calibrate(Segment* cut, uint64_t begin, uint64_t end,
                     bool retune) {
    _live.clear();
    _live.push_back(cut);
    const auto base = Ladder(ByteCodec::Lz4)[0];
    auto* dedup = Trial({Shape::Dedup, ByteCodec::Lz4}, base, begin, end);
    Segment* plain = nullptr;
    if (PlainMayWin(*dedup)) {
      plain = Trial({Shape::Plain, ByteCodec::Lz4}, base, begin, end);
    }
    const auto shape =
      plain && PlainWins(*dedup, *plain) ? Shape::Plain : Shape::Dedup;
    auto* best = shape == Shape::Dedup ? dedup : plain;
    if (cut->choice.shape == shape && cut->Size() < best->Size()) {
      best = cut;
    }
    auto chosen = ByteCodec::Lz4;
    auto chosen_bytes = std::numeric_limits<uint64_t>::max();
    for (const auto& plan : PlanFor(_params.objective)) {
      const auto ladder = Ladder(plan.leaf);
      auto& tuned = _tuning.level[Index(plan.leaf)];
      size_t rung = 0;
      if (!retune) {
        const auto it = std::find(ladder.begin(), ladder.end(), tuned);
        rung = it == ladder.end() ? 0 : static_cast<size_t>(it - ladder.begin());
      }
      auto* cur = Trial({shape, plan.leaf}, ladder[rung], begin, end);
      if (retune) {
        while (rung + 1 < ladder.size()) {
          auto* next =
            Trial({shape, plan.leaf}, ladder[rung + 1], begin, end);
          if (next->Size() < best->Size()) {
            best = next;
          }
          if (static_cast<double>(next->Size()) >
              static_cast<double>(cur->Size()) * (1.0 - kLevelStepGain)) {
            break;
          }
          cur = next;
          ++rung;
        }
      }
      tuned = ladder[rung];
      if (cur->Size() < best->Size()) {
        best = cur;
      }
      if (cur->Size() < chosen_bytes) {
        chosen_bytes = cur->Size();
        chosen = plan.leaf;
      }
    }
    _tuning.levels_tuned = true;
    _tuning.choice = StringChoice{shape, chosen};
    _tuning.bytes_per_input =
      cut->input == 0 ? 0
                      : static_cast<double>(chosen_bytes) /
                          static_cast<double>(cut->input);
    for (auto* seg : _live) {
      if (seg != best) {
        Release(seg);
      }
    }
    _live.clear();
    return best;
  }

  Segment* Trial(StringChoice choice, uint8_t level, uint64_t begin,
                 uint64_t end) {
    for (auto* seg : _live) {
      if (seg->choice.shape == choice.shape &&
          seg->choice.leaf == choice.leaf && seg->level == level &&
          seg->begin == begin && seg->rows == end - begin) {
        return seg;
      }
    }
    auto* seg = Acquire();
    With(choice.leaf, [&](auto& enc) {
      enc.Begin(choice.shape, level, begin);
      enc.AddUntil(end);
      enc.Finish(*seg);
      return end;
    });
    _live.push_back(seg);
    return seg;
  }

  static bool Repeats(const Segment& dedup) noexcept {
    return dedup.entries * kDedupMinRepeat <= dedup.rows - dedup.nulls;
  }

  static bool PlainMayWin(const Segment& dedup) noexcept {
    const auto floor = kHeaderSize + Packed(dedup.rows, dedup.max_len) +
                       dedup.input / kLz4MaxRatio;
    if (Repeats(dedup)) {
      return static_cast<double>(floor) <
             static_cast<double>(dedup.Size()) * kPlainWinsBelow;
    }
    return floor <= dedup.Size();
  }

  static bool PlainWins(const Segment& dedup, const Segment& plain) noexcept {
    if (Repeats(dedup)) {
      return static_cast<double>(plain.Size()) <
             static_cast<double>(dedup.Size()) * kPlainWinsBelow;
    }
    return plain.Size() <= dedup.Size();
  }

  bool Drifted(const Segment& seg) const noexcept {
    if (seg.input == 0 || _tuning.bytes_per_input == 0 ||
        seg.Size() * kDriftMinFraction < _params.segment_target) {
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
  uint64_t With(ByteCodec leaf, F&& f) {
    switch (leaf) {
      case ByteCodec::Lz4:
        return f(Get<ByteCodec::Lz4>());
      case ByteCodec::Zstd:
        return f(Get<ByteCodec::Zstd>());
      case ByteCodec::Zxc:
        return f(Get<ByteCodec::Zxc>());
      case ByteCodec::Fsst:
        return f(Get<ByteCodec::Fsst>());
    }
    SDB_UNREACHABLE();
  }

  template<ByteCodec C>
  Encoder<C>& Get() {
    auto& slot = std::get<std::optional<Encoder<C>>>(_encoders);
    if (!slot) {
      slot.emplace(_acc, _type, _tuning.history[Index(C)], _dedup);
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
  uint8_t _fixed[1];
  DedupScratch _dedup;
  std::tuple<std::optional<Encoder<ByteCodec::Lz4>>,
             std::optional<Encoder<ByteCodec::Zstd>>,
             std::optional<Encoder<ByteCodec::Zxc>>,
             std::optional<Encoder<ByteCodec::Fsst>>>
    _encoders;
  std::vector<std::unique_ptr<Segment>> _pool;
  std::vector<Segment*> _free;
  std::vector<Segment*> _live;
};

}  // namespace

void StringAccumulator::Add(const duckdb::Vector& input) {
  duckdb::UnifiedVectorFormat vdata;
  input.ToUnifiedFormat(vdata);
  const auto* strings = duckdb::UnifiedVectorFormat::GetData<string_t>(vdata);
  const auto count = input.size();
  for (idx_t i = 0; i < count; ++i) {
    const auto idx = vdata.sel->get_index(i);
    if (!vdata.validity.RowIsValid(idx)) {
      if (codes.empty() || codes.back() != 0) {
        ++runs;
      }
      codes.push_back(0);
      ++null_count;
      continue;
    }
    const std::string_view sv{strings[idx].GetData(), strings[idx].GetSize()};
    raw_bytes += sv.size();
    const auto next = static_cast<uint32_t>(entries.size() + 1);
    if (_dedup) {
      const auto [it, inserted] = dedup.try_emplace(sv, next);
      if (codes.empty() || codes.back() != it->second) {
        ++runs;
      }
      codes.push_back(it->second);
      if (!inserted) {
        continue;
      }
    } else {
      ++runs;
      codes.push_back(next);
    }
    entries.push_back(sv);
    entry_bytes += sv.size();
    max_len = std::max<uint32_t>(max_len, static_cast<uint32_t>(sv.size()));
  }
  row_count += count;
}

SealOutcome SealSegments(const StringAccumulator& acc,
                         std::optional<StringChoice> named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink) {
  SDB_ASSERT(acc.codes.size() == acc.row_count);
  return SegmentWriter{acc, named, params, type, tuning}.Run(sink);
}

}  // namespace irs::codecs
