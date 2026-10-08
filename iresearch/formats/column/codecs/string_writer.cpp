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
#include <absl/strings/match.h>

#include <algorithm>
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
#include "iresearch/formats/column/codecs/numeric_writer.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::idx_t;
using duckdb::string_t;

constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kPlainMinSaving = 4096;
constexpr uint64_t kDedupMinRepeat = 2;
constexpr uint64_t kEstimateEvery = 64;
constexpr uint64_t kEstimateSteps = 16;
constexpr uint64_t kLz4MaxRatio = 255;
constexpr double kFsstFirstRatio = 0.5;
constexpr double kDrift = 1.25;
constexpr uint64_t kDriftMinFraction = 4;
constexpr uint32_t kMaxCalibrationGap = 16;
constexpr double kMispredicted = 2.0;
constexpr double kLevelTolerance = 0.02;
constexpr size_t kMaxRungs = 8;
constexpr double kWideFramesGain = 0.03;
constexpr double kUntrainedGain = 0.02;
constexpr double kFsstPreference = 0.05;
constexpr size_t kPriceFrames = 8;

constexpr uint8_t kLz4Fast[] = {1};
constexpr uint8_t kLz4Levels[] = {1, 4, 6};
constexpr uint8_t kZstdLevels[] = {9};
constexpr uint8_t kZxcLevels[] = {1, 3};
constexpr uint8_t kNoLevel[] = {0};

struct LeafPlan {
  ByteCodec leaf;
  std::span<const uint8_t> ladder;
};

constexpr LeafPlan kRefreshPlan[] = {{ByteCodec::Fsst, kNoLevel},
                                     {ByteCodec::Lz4, kLz4Fast}};
constexpr LeafPlan kCompactionPlan[] = {{ByteCodec::Fsst, kNoLevel},
                                        {ByteCodec::Lz4, kLz4Levels},
                                        {ByteCodec::Zstd, kZstdLevels},
                                        {ByteCodec::Zxc, kZxcLevels}};

struct FrameShape {
  size_t frame;
  size_t dictionary;
};

template<ByteCodec C>
constexpr FrameShape ShapeOf() noexcept {
  if constexpr (C == ByteCodec::Zstd) {
    return {kZstdDictionaryFrameBytes, kFrameDictionaryBytes};
  } else {
    return {kDictionaryFrameBytes, kFrameDictionaryBytes};
  }
}

std::span<const LeafPlan> PlanFor(const ColCodecParams& params) noexcept {
  if (params.tier == WriteTier::Flush) {
    return kRefreshPlan;
  }
  return kCompactionPlan;
}

constexpr size_t Index(ByteCodec leaf) noexcept {
  return static_cast<size_t>(leaf);
}

uint64_t Packed(uint64_t count, uint32_t max_value) noexcept {
  return BitpackingPrimitives::GetRequiredSize(
    count, BitpackingPrimitives::MinimumBitWidth<uint32_t>(max_value));
}

struct CodesPlan {
  CodesEncoding encoding = CodesEncoding::Bitpack;
  uint8_t code_width = 0;
  uint8_t run_width = 0;
  uint64_t codes_count = 0;
  uint64_t run_count = 0;
  uint64_t bytes = 0;
};

CodesPlan PlanCodes(Shape shape, uint64_t rows, uint64_t entries,
                    uint64_t runs) noexcept {
  CodesPlan plan;
  if (shape != Shape::Dedup) {
    return plan;
  }
  const auto max_code = static_cast<uint32_t>(entries);
  const auto bitpack = Packed(rows, max_code);
  const auto rle =
    Packed(runs, max_code) + Packed(runs, static_cast<uint32_t>(rows));
  plan.code_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(max_code);
  if (rle < bitpack) {
    plan.encoding = CodesEncoding::Rle;
    plan.run_width = BitpackingPrimitives::MinimumBitWidth<uint32_t>(
      static_cast<uint32_t>(rows));
    plan.codes_count = runs;
    plan.run_count = runs;
    plan.bytes = rle;
  } else {
    plan.codes_count = rows;
    plan.bytes = bitpack;
  }
  return plan;
}

void LayOut(Header& h, uint64_t frames, uint64_t lengths_bytes,
            uint64_t lcps_bytes, const CodesPlan& codes,
            uint64_t symtab_bytes) noexcept {
  h.off_frames = kHeaderSize;
  h.off_lengths = Align8(kHeaderSize + frames * kFrameMetaSize);
  h.off_lcps = Align8(h.off_lengths + lengths_bytes);
  h.off_codes = Align8(h.off_lcps + lcps_bytes);
  h.off_runs = Align8(h.off_codes + BitpackingPrimitives::GetRequiredSize(
                                      codes.codes_count, codes.code_width));
  h.off_symtab = Align8(h.off_runs + BitpackingPrimitives::GetRequiredSize(
                                       codes.run_count, codes.run_width));
  h.off_data = Align8(h.off_symtab + symtab_bytes);
}

void Record(RatioHistory& hist, uint64_t raw, uint64_t comp,
            uint32_t target) noexcept {
  if (hist.raw != 0 && raw != 0 && comp * kDriftMinFraction >= target) {
    const auto predicted = static_cast<double>(raw) *
                           static_cast<double>(hist.comp) /
                           static_cast<double>(hist.raw);
    const auto actual = static_cast<double>(comp);
    if (actual > predicted * kMispredicted ||
        actual * kMispredicted < predicted) {
      hist = {};
    }
  }
  hist.raw += raw;
  hist.comp += comp;
}

class DedupScratch {
 public:
  void Begin(size_t entries) {
    if (_slots.size() != entries) {
      _slots.assign(entries, Slot{});
    }
    ++_epoch;
  }

  std::pair<uint32_t, bool> Local(uint32_t code, uint32_t next) noexcept {
    auto& slot = _slots[code - 1];
    if (slot.epoch == _epoch) {
      return {slot.local, false};
    }
    slot = {_epoch, next};
    return {next, true};
  }

 private:
  struct Slot {
    uint32_t epoch = 0;
    uint32_t local = 0;
  };

  std::vector<Slot> _slots;
  uint32_t _epoch = 0;
};

struct Segment {
  StringChoice choice{};
  uint8_t level = 0;
  FrameLayout layout = FrameLayout::Dictionary;
  uint64_t begin = 0;
  uint64_t rows = 0;
  uint64_t entries = 0;
  uint64_t nulls = 0;
  uint64_t input = 0;
  uint32_t max_len = 0;
  std::string head;
  std::string data;
  std::vector<uint32_t> codes;
  std::optional<duckdb::BaseStatistics> stats;

  uint64_t Size() const noexcept { return head.size() + data.size(); }
};

struct Profile {
  Shape shape = Shape::Dedup;
  uint64_t rows = 0;
  uint64_t runs = 0;
  uint64_t raw = 0;
  uint32_t max_len = 0;
  std::vector<std::string_view> entries;
};

double Jitter(uint64_t seed, uint64_t stratum) noexcept {
  uint64_t z = seed * 0x9E3779B97F4A7C15ULL + stratum + 1;
  z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9ULL;
  z = (z ^ (z >> 27)) * 0x94D049BB133111EBULL;
  z ^= z >> 31;
  return static_cast<double>(z >> 11) * 0x1.0p-53;
}

template<ByteCodec C>
class Encoder {
 public:
  static constexpr bool kFsst = C == ByteCodec::Fsst;
  static constexpr bool kTrainable = Trainable(C);

  Encoder(const StringAccumulator& acc, const duckdb::LogicalType& type,
          StringTuning& tuning, DedupScratch& dedup, uint32_t target)
    : _acc{acc},
      _stats{type},
      _type{type},
      _tuning{tuning},
      _history{tuning.history[static_cast<size_t>(C)]},
      _dedup{dedup},
      _target{target} {}

  void Begin(Shape shape, uint8_t level, FrameLayout layout, uint64_t begin) {
    _shape = shape;
    _level = level;
    _frame_layout = layout;
    _next = begin;
    if constexpr (!kFsst) {
      _layout = Configure(level, layout);
    }
    _dictionary.clear();
    _data.clear();
    if (shape == Shape::Dedup) {
      _dedup.Begin(_acc.entries.size());
    }
  }

  uint64_t Cut() {
    const uint64_t step = _target / kEstimateSteps;
    uint64_t estimated_raw = _raw;
    while (_next < _acc.row_count) {
      AddRow(_next++);
      if (_rows % kEstimateEvery != 0 && _raw - estimated_raw < step) {
        continue;
      }
      estimated_raw = _raw;
      if (Estimate() >= _target) {
        break;
      }
    }
    return _next;
  }

  void Retrain() noexcept {
    if constexpr (kFsst) {
      _codec.Reset();
    }
  }

  uint64_t Price(const Profile& p, uint8_t level, FrameLayout frame_layout,
                 uint64_t seed)
    requires(!kFsst)
  {
    const auto frame_shape = Configure(level, frame_layout);
    SplitFrames(p, frame_shape);
    uint64_t data = 0;
    if (!_spans.empty()) {
      Assemble(p, _spans[0], _price_dictionary);
      data += CompressFrame(_price_dictionary);
      if (frame_shape.dictionary != 0 && !_price_dictionary.empty() &&
          _spans.size() > 1) {
        _codec.LoadDictionary(_price_dictionary);
      }
      const size_t rest = _spans.size() - 1;
      if (rest <= kPriceFrames) {
        for (size_t i = 1; i < _spans.size(); ++i) {
          Assemble(p, _spans[i], _price_frame);
          data += CompressFrame(_price_frame);
        }
      } else {
        uint64_t sampled_raw = 0;
        uint64_t sampled = 0;
        size_t count = 0;
        size_t last = 0;
        for (size_t j = 0; j < kPriceFrames; ++j) {
          const auto i =
            1 + static_cast<size_t>((static_cast<double>(j) + Jitter(seed, j)) *
                                    static_cast<double>(rest) /
                                    static_cast<double>(kPriceFrames));
          if (i == last) {
            continue;
          }
          last = i;
          Assemble(p, _spans[i], _price_frame);
          sampled += CompressFrame(_price_frame);
          sampled_raw += _spans[i].raw;
          ++count;
        }
        const auto rest_raw = p.raw - _spans[0].raw;
        data += sampled_raw == 0
                  ? sampled * rest / count
                  : static_cast<uint64_t>(static_cast<double>(sampled) *
                                          static_cast<double>(rest_raw) /
                                          static_cast<double>(sampled_raw));
      }
      auto& hist = _history[static_cast<size_t>(p.shape)];
      hist.raw += p.raw;
      hist.comp += data;
    }
    const auto entry_count =
      p.entries.size() + (p.shape == Shape::Dedup ? 1 : 0);
    Header h{};
    LayOut(h, _spans.size(),
           BitpackingPrimitives::GetRequiredSize(
             entry_count,
             BitpackingPrimitives::MinimumBitWidth<uint32_t>(p.max_len)),
           0, PlanCodes(p.shape, p.rows, p.entries.size(), p.runs), 0);
    return h.off_data + data;
  }

  void AddUntil(uint64_t end) {
    while (_next < end) {
      AddRow(_next++);
    }
  }

  void Finish(Segment& out) {
    SDB_ASSERT(_rows != 0);
    if (_shape == Shape::Dedup) {
      _codes.resize(_rows);
    }
    std::string_view symtab;
    if constexpr (kFsst) {
      EncodeFsst();
      symtab = _codec.SymbolTable();
    } else {
      if (_frame_entries != 0) {
        CloseFrame();
      }
      _max_enc = _max_len;
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
    const auto codes = PlanCodes(_shape, _rows, _entries.size(), _runs);
    h.code_width = codes.code_width;
    h.row_count = static_cast<uint32_t>(_rows);
    h.entry_count = static_cast<uint32_t>(entry_count);
    h.frame_count = static_cast<uint32_t>(_frames.size());
    h.flags = _trained                                     ? kTrainedDictionary
              : !_dictionary.empty() && _frames.size() > 1 ? kFrameDictionary
                                                           : 0;
    h.dictionary = _trained ? _tuning.dictionary_id : 0;
    h.raw_bytes = _raw;

    if (codes.encoding == CodesEncoding::Rle) {
      _run_values.clear();
      _run_ends.clear();
      for (size_t i = 0; i < _codes.size(); ++i) {
        if (i != 0 && _codes[i] == _codes[i - 1]) {
          continue;
        }
        if (i != 0) {
          _run_ends.push_back(static_cast<uint32_t>(i));
        }
        _run_values.push_back(_codes[i]);
      }
      _run_ends.push_back(static_cast<uint32_t>(_codes.size()));
      SDB_ASSERT(_run_values.size() == codes.run_count);
    }
    h.codes_encoding = static_cast<uint8_t>(codes.encoding);
    h.run_count = static_cast<uint32_t>(codes.run_count);
    h.run_width = codes.run_width;

    _lengths.assign(GroupPadded(entry_count), 0);
    if constexpr (kFsst) {
      std::copy(_entry_lengths.begin(), _entry_lengths.end(),
                _lengths.begin() + FirstEntry());
      _lcp_stream.resize(GroupPadded(entry_count), 0);
    } else {
      for (size_t i = 0; i < _entries.size(); ++i) {
        _lengths[FirstEntry() + i] = static_cast<uint32_t>(_entries[i].size());
      }
    }
    LayOut(h, _frames.size(),
           BitpackingPrimitives::GetRequiredSize(entry_count, h.length_width),
           kFsst
             ? BitpackingPrimitives::GetRequiredSize(entry_count, h.lcp_width)
             : 0,
           codes, symtab.size());
    h.symtab_size = static_cast<uint32_t>(symtab.size());
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
      if (codes.encoding == CodesEncoding::Rle) {
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
        _codes.resize(_rows);
      }
    }
    if (!symtab.empty()) {
      std::memcpy(base + h.off_symtab, symtab.data(), symtab.size());
    }
    if constexpr (kFsst) {
      Record(History(), _raw, _data.size() + symtab.size(), _target);
    }

    auto stats = duckdb::BaseStatistics::CreateEmpty(_type);
    _stats.Merge(stats);
    out.choice = StringChoice{_shape, C};
    out.level = _level;
    out.layout = _frame_layout;
    out.begin = _next - _rows;
    out.rows = _rows;
    out.entries = _entries.size();
    out.nulls = _nulls;
    out.input = _input;
    out.max_len = _max_len;
    out.stats.emplace(std::move(stats));
    out.head.swap(_bytes);
    out.data.swap(_data);
    out.codes.swap(_codes);

    _stats.Clear();
    _codes.clear();
    _entries.clear();
    _entry_lengths.clear();
    _frames.clear();
    _data.clear();
    _dictionary.clear();
    _rows = 0;
    _runs = 0;
    _raw = 0;
    _max_len = 0;
    _nulls = 0;
    _input = 0;
  }

 private:
  FrameShape Configure(uint8_t level, FrameLayout layout)
    requires(!kFsst)
  {
    _codec.SetLevel(level);
    _trained = nullptr;
    if constexpr (kTrainable) {
      if (layout == FrameLayout::Dictionary && _tuning.dictionary) {
        _trained = _tuning.dictionary.get();
        _codec.LoadTrained(*_trained);
        return {kTrainedFrameBytes, 0};
      }
    }
    if (layout == FrameLayout::Wide) {
      return {kFrameRawBytes, 0};
    }
    return ShapeOf<C>();
  }

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
    if (_rows == _codes.size()) {
      _codes.resize(std::max<size_t>(2 * _rows, STANDARD_VECTOR_SIZE));
    }
    _codes[_rows] = code;
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
    const string_t value{sv.data(), static_cast<uint32_t>(sv.size())};
    const auto [local, fresh] =
      _dedup.Local(code, static_cast<uint32_t>(_entries.size()) + 1);
    if (fresh) {
      _stats.Update(value);
      AppendEntry(sv);
    } else {
      _stats.UpdateRepeated(value);
    }
    PushCode(local);
    ++_rows;
  }

  void AppendEntry(std::string_view sv) {
    _entries.push_back(sv);
    _max_len = std::max<uint32_t>(_max_len, static_cast<uint32_t>(sv.size()));
    _raw += sv.size();
    if constexpr (!kFsst) {
      if (_frame_entries != 0 && _frame.size() + sv.size() > FrameLimit()) {
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
    size_t n = 0;
    _data.resize_and_overwrite(comp_off + bound, [&](char* buf, size_t) {
      n = _codec.Compress(_frame.data(), _frame.size(), buf + comp_off, bound);
      return comp_off + n;
    });
    _frames.push_back(
      FrameMeta{_frame_first, static_cast<uint32_t>(_frame.size()),
                static_cast<uint32_t>(comp_off), static_cast<uint32_t>(n)});
    auto& hist = History();
    hist.raw += _frame.size();
    hist.comp += n;
    if (_frames.size() == 1 && _layout.dictionary != 0 && !_frame.empty()) {
      _dictionary.swap(_frame);
      _codec.LoadDictionary(_dictionary);
    }
    _frame.clear();
    _frame_entries = 0;
  }

  size_t FrameLimit() const noexcept {
    return _frames.empty() && _layout.dictionary != 0 ? _layout.dictionary
                                                      : _layout.frame;
  }

  struct Span {
    uint64_t first;
    uint64_t end;
    uint64_t raw;
  };

  void SplitFrames(const Profile& p, const FrameShape& frame_shape) {
    _spans.clear();
    uint64_t first = 0;
    uint64_t raw = 0;
    for (uint64_t i = 0; i < p.entries.size(); ++i) {
      const auto limit = _spans.empty() && frame_shape.dictionary != 0
                           ? frame_shape.dictionary
                           : frame_shape.frame;
      const auto len = p.entries[i].size();
      if (i != first && raw + len > limit) {
        _spans.push_back(Span{first, i, raw});
        first = i;
        raw = 0;
      }
      raw += len;
    }
    if (first < p.entries.size()) {
      _spans.push_back(Span{first, p.entries.size(), raw});
    }
  }

  static void Assemble(const Profile& p, const Span& span, std::string& out) {
    out.clear();
    out.reserve(span.raw);
    for (auto k = span.first; k < span.end; ++k) {
      out.append(p.entries[k]);
    }
  }

  uint64_t CompressFrame(const std::string& frame) {
    const auto bound = Leaf<C>::Bound(frame.size());
    size_t n = 0;
    _price_out.resize_and_overwrite(bound, [&](char* buf, size_t) {
      n = _codec.Compress(frame.data(), frame.size(), buf, bound);
      return n;
    });
    return n;
  }

  uint64_t Estimate() const noexcept {
    const auto entry_count = _entries.size() + FirstEntry();
    const auto ratio = Ratio();
    uint64_t est = kHeaderSize + Packed(entry_count, _max_len) +
                   PlanCodes(_shape, _rows, _entries.size(), _runs).bytes;
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
      common = std::min(
        common, absl::FindLongestCommonPrefix(_entries[0], _entries[i]).size());
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
    _lcp_stream.assign(FirstEntry(), 0);
    _max_lcp = 0;
    _frames.clear();
    std::string_view prev;
    uint64_t frame_raw = 0;
    uint64_t frame_first = 0;
    for (size_t i = 0; i < _entries.size(); ++i) {
      const auto sv = _entries[i];
      auto lcp =
        static_cast<uint32_t>(absl::FindLongestCommonPrefix(prev, sv).size());
      if (frame_raw != 0 && frame_raw + sv.size() > kFsstFrameRawBytes) {
        _frames.push_back(FrameMeta{static_cast<uint32_t>(frame_first),
                                    static_cast<uint32_t>(frame_raw), 0, 0});
        frame_raw = 0;
        frame_first = i;
      }
      if ((i - frame_first) % kChainRestart == 0) {
        lcp = 0;
      }
      _lcp_stream.push_back(lcp);
      _max_lcp = std::max(_max_lcp, lcp);
      _suffixes.push_back(sv.substr(lcp));
      frame_raw += sv.size();
      prev = sv;
    }
    if (!_entries.empty()) {
      _frames.push_back(FrameMeta{static_cast<uint32_t>(frame_first),
                                  static_cast<uint32_t>(frame_raw), 0, 0});
    }
    _codec.Encode(_suffixes, _data, _entry_lengths);
    size_t off = 0;
    size_t next = 0;
    _max_enc = 0;
    for (size_t f = 0; f < _frames.size(); ++f) {
      const auto end =
        f + 1 < _frames.size() ? _frames[f + 1].first_entry : _entries.size();
      _frames[f].comp_off = static_cast<uint32_t>(off);
      for (; next < end; ++next) {
        off += _entry_lengths[next];
        _max_enc = std::max(_max_enc, _entry_lengths[next]);
      }
      _frames[f].comp_len = static_cast<uint32_t>(off - _frames[f].comp_off);
      _frames[f].first_entry += FirstEntry();
    }
  }

  const StringAccumulator& _acc;
  Shape _shape = Shape::Dedup;
  uint8_t _level = 0;
  uint64_t _next = 0;
  using Codec = std::conditional_t<kFsst, FsstEncoder, LeafCompressor<C>>;
  Codec _codec;
  duckdb::StatsWriter<string_t> _stats;
  duckdb::LogicalType _type;
  const StringTuning& _tuning;
  RatioHistory* _history;
  const TrainedDictionary* _trained = nullptr;
  DedupScratch& _dedup;
  uint32_t _target;

  std::vector<std::string_view> _entries;
  std::vector<uint32_t> _codes;
  uint64_t _rows = 0;
  uint64_t _runs = 0;
  uint32_t _last_code = 0;
  uint64_t _raw = 0;
  uint32_t _max_len = 0;
  uint64_t _nulls = 0;
  uint64_t _input = 0;

  FrameLayout _frame_layout = FrameLayout::Dictionary;
  FrameShape _layout{kFrameRawBytes, 0};
  std::string _frame;
  std::string _dictionary;
  uint32_t _frame_first = 0;
  uint32_t _frame_entries = 0;
  std::vector<FrameMeta> _frames;
  std::string _data;
  std::vector<uint32_t> _entry_lengths;
  std::vector<std::string_view> _suffixes;
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

  std::vector<Span> _spans;
  std::string _price_dictionary;
  std::string _price_frame;
  std::string _price_out;
};

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
        enc.Begin(cutter.choice.shape, cutter.level, cutter.layout, row);
        const auto stop = enc.Cut();
        enc.Finish(*seg);
        return stop;
      });
      if (!_named) {
        const bool scheduled = first && due;
        const bool drift =
          !scheduled && Drifted(*seg, first || end < _acc.row_count);
        const bool retune = drift && !first;
        if (retune) {
          if (auto& fsst =
                std::get<std::optional<Encoder<ByteCodec::Fsst>>>(_encoders)) {
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
            kLz4Fast[0],
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
    const auto base = kLz4Fast[0];
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
    if (best->choice.leaf != ByteCodec::Fsst) {
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
           (leaf == ByteCodec::Lz4 &&
            EffectiveLevel<ByteCodec::Lz4>(level) <= 1);
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
      if constexpr (std::remove_reference_t<decltype(enc)>::kFsst) {
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
      enc.Begin(choice.shape, level, layout, begin);
      enc.AddUntil(end);
      enc.Finish(*seg);
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
  std::invoke_result_t<F&, Encoder<ByteCodec::Lz4>&> With(ByteCodec leaf,
                                                          F&& f) {
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
      slot.emplace(_acc, _type, _tuning, _dedup, _params.segment_target);
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
  std::tuple<std::optional<Encoder<ByteCodec::Lz4>>,
             std::optional<Encoder<ByteCodec::Zstd>>,
             std::optional<Encoder<ByteCodec::Zxc>>,
             std::optional<Encoder<ByteCodec::Fsst>>>
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
