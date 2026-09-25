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
#include <optional>
#include <span>
#include <type_traits>

#include "iresearch/formats/column/codecs/fsst_codec.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::BitpackingPrimitives;
using duckdb::idx_t;
using duckdb::string_t;

constexpr size_t kSampleBytes = 64 * 1024;
constexpr size_t kSampleWindows = 4;
constexpr double kPlainWinsBelow = 0.9;
constexpr uint64_t kDedupMinRepeat = 2;

struct Sample {
  std::vector<std::string_view> entries;
  uint64_t bytes = 0;
};

Sample MakeSample(const StringAccumulator& acc) {
  Sample out;
  if (acc.entry_bytes <= kSampleBytes) {
    out.entries = acc.entries;
    out.bytes = acc.entry_bytes;
    return out;
  }
  const auto n = acc.entries.size();
  constexpr auto kWindowBytes = kSampleBytes / kSampleWindows;
  for (size_t w = 0; w < kSampleWindows; ++w) {
    const auto end = n * (w + 1) / kSampleWindows;
    uint64_t taken = 0;
    for (auto i = n * w / kSampleWindows; i < end && taken < kWindowBytes;
         ++i) {
      out.entries.push_back(acc.entries[i]);
      taken += acc.entries[i].size();
    }
    out.bytes += taken;
  }
  return out;
}

std::span<const ByteCodec> AutoLeaves(AutoObjective objective,
                                      Shape shape) noexcept {
  static constexpr ByteCodec kSpeed[] = {ByteCodec::Lz4};
  static constexpr ByteCodec kBalanced[] = {ByteCodec::Fsst, ByteCodec::Lz4};
  static constexpr ByteCodec kSizeDedup[] = {ByteCodec::Fsst, ByteCodec::Lz4,
                                             ByteCodec::Zstd, ByteCodec::Zxc};
  static constexpr ByteCodec kSizePlain[] = {ByteCodec::Fsst, ByteCodec::Lz4,
                                             ByteCodec::Zstd, ByteCodec::Zxc};
  switch (objective) {
    case AutoObjective::Speed:
      return kSpeed;
    case AutoObjective::Size:
      if (shape == Shape::Dedup) {
        return kSizeDedup;
      }
      return kSizePlain;
    case AutoObjective::Balanced:
      break;
  }
  return kBalanced;
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

std::string Concat(const Sample& sample) {
  std::string out;
  out.reserve(sample.bytes);
  for (const auto sv : sample.entries) {
    out.append(sv);
  }
  return out;
}

template<ByteCodec C>
double BlobRatio(const Sample& sample, uint8_t level) {
  if (sample.bytes == 0) {
    return 1.0;
  }
  const auto raw = Concat(sample);
  LeafCompressor<C> compressor{level};
  std::string out(Leaf<C>::Bound(raw.size()), '\0');
  const auto n =
    compressor.Compress(raw.data(), raw.size(), out.data(), out.size());
  return static_cast<double>(n) / static_cast<double>(raw.size());
}

double FsstRatio(std::span<const std::string_view> ordered, uint64_t raw) {
  if (raw == 0) {
    return 1.0;
  }
  std::vector<std::string_view> suffixes;
  suffixes.reserve(ordered.size());
  std::string_view prev;
  for (const auto sv : ordered) {
    suffixes.push_back(sv.substr(Lcp(prev, sv)));
    prev = sv;
  }
  FsstEncoder encoder;
  std::string out;
  std::vector<uint32_t> lengths;
  encoder.Encode(suffixes, out, lengths);
  const auto bytes = out.size() + encoder.SymbolTable().size();
  return static_cast<double>(bytes) / static_cast<double>(raw);
}

double FsstRatio(const Sample& sample, Shape shape) {
  if (shape == Shape::Plain) {
    return FsstRatio(sample.entries, sample.bytes);
  }
  auto sorted = sample.entries;
  std::sort(sorted.begin(), sorted.end());
  return FsstRatio(sorted, sample.bytes);
}

double BlobRatio(const Sample& sample, ByteCodec leaf, uint8_t level) {
  switch (leaf) {
    case ByteCodec::Lz4:
      return BlobRatio<ByteCodec::Lz4>(sample, level);
    case ByteCodec::Zstd:
      return BlobRatio<ByteCodec::Zstd>(sample, level);
    case ByteCodec::Zxc:
      return BlobRatio<ByteCodec::Zxc>(sample, level);
    case ByteCodec::Fsst:
      break;
  }
  return 1.0;
}

double Ratio(const Sample& sample, StringChoice choice, uint8_t level) {
  if (choice.leaf == ByteCodec::Fsst) {
    return FsstRatio(sample, choice.shape);
  }
  return BlobRatio(sample, choice.leaf, level);
}

constexpr uint64_t kEstimateEvery = 64;

uint64_t EstimateWithRatio(const StringAccumulator& acc, StringChoice choice,
                           double ratio, uint32_t target) noexcept {
  const bool dedup = choice.shape == Shape::Dedup;
  const bool fsst = choice.leaf == ByteCodec::Fsst;
  const auto entry_count = dedup ? acc.entries.size() + 1 : acc.row_count;
  const auto payload = dedup ? acc.entry_bytes : acc.raw_bytes;
  const auto frames =
    payload / (fsst ? kFsstFrameRawBytes : kFrameRawBytes) + 1;
  const auto lengths = Packed(entry_count, acc.max_len);
  const auto body =
    frames * kFrameMetaSize + lengths + (fsst ? lengths : 0) +
    CodesBytes(choice.shape, acc.row_count, acc.entries.size(), acc.runs) +
    static_cast<uint64_t>(static_cast<double>(payload) * ratio);
  return body + (body / target + 1) * kHeaderSize;
}

template<ByteCodec C>
class Sealer {
 public:
  static constexpr bool kFsst = C == ByteCodec::Fsst;

  Sealer(const StringAccumulator& acc, Shape shape, double ratio,
         const ColCodecParams& params, const duckdb::LogicalType& type)
    : _acc{acc},
      _shape{shape},
      _target{params.segment_target},
      _level{params.compression_level},
      _ratio{ratio},
      _stats{type},
      _type{type} {
    if constexpr (kFsst) {
      _codec.emplace();
    } else {
      _codec.emplace(params.compression_level);
    }
    _epoch.assign(acc.entries.size(), 0);
    _local.assign(acc.entries.size(), 0);
  }

  void Run(SegmentSink sink) {
    for (uint64_t row = 0; row < _acc.row_count; ++row) {
      AddRow(row);
      if (_rows % kEstimateEvery == 0 && Estimate() >= _target) {
        Seal(sink);
      }
    }
    Seal(sink);
  }

 private:
  uint32_t FirstEntry() const noexcept {
    return _shape == Shape::Dedup ? 1 : 0;
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
      if (_shape == Shape::Dedup) {
        PushCode(0);
      } else {
        AppendEntry({});
      }
      ++_rows;
      return;
    }
    const auto sv = _acc.entries[code - 1];
    if (_shape == Shape::Plain) {
      _stats.Update(string_t{sv.data(), static_cast<uint32_t>(sv.size())});
      AppendEntry(sv);
      ++_rows;
      return;
    }
    if (_epoch[code - 1] != _segment_epoch) {
      _epoch[code - 1] = _segment_epoch;
      _local[code - 1] = static_cast<uint32_t>(_entries.size()) + 1;
      _stats.Update(string_t{sv.data(), static_cast<uint32_t>(sv.size())});
      AppendEntry(sv);
    }
    PushCode(_local[code - 1]);
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
    _closed_raw += _frame.size();
    _closed_comp += n;
    _frame.clear();
    _frame_entries = 0;
  }

  uint64_t Estimate() const noexcept {
    const auto entry_count = _entries.size() + FirstEntry();
    const auto ratio = _closed_raw != 0 ? static_cast<double>(_closed_comp) /
                                            static_cast<double>(_closed_raw)
                                        : _ratio;
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

  void Seal(SegmentSink sink) {
    if (_rows == 0) {
      return;
    }
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

    auto stats = duckdb::BaseStatistics::CreateEmpty(_type);
    _stats.Merge(stats);
    const std::string_view parts[] = {_bytes, _data};
    sink(std::move(stats), _rows, parts);

    if constexpr (kFsst) {
      _closed_raw += _raw;
      _closed_comp += _data.size() + symtab.size();
    }
    _stats.Clear();
    ++_segment_epoch;
    _codes.clear();
    _entries.clear();
    _entry_lengths.clear();
    _frames.clear();
    _data.clear();
    _rows = 0;
    _runs = 0;
    _raw = 0;
    _max_len = 0;
  }

  const StringAccumulator& _acc;
  Shape _shape;
  uint32_t _target;
  uint8_t _level;
  double _ratio;
  using Codec = std::conditional_t<kFsst, FsstEncoder, LeafCompressor<C>>;
  std::optional<Codec> _codec;
  duckdb::StatsWriter<string_t> _stats;
  duckdb::LogicalType _type;

  std::vector<uint32_t> _epoch;
  std::vector<uint32_t> _local;
  uint32_t _segment_epoch = 1;

  std::vector<std::string_view> _entries;
  std::vector<uint32_t> _codes;
  uint64_t _rows = 0;
  uint64_t _runs = 0;
  uint32_t _last_code = 0;
  uint64_t _raw = 0;
  uint32_t _max_len = 0;

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
  uint64_t _closed_raw = 0;
  uint64_t _closed_comp = 0;
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

PricedChoice Price(const StringAccumulator& acc, StringChoice choice,
                   const ColCodecParams& params) {
  const auto ratio = Ratio(MakeSample(acc), choice, params.compression_level);
  return {choice, EstimateWithRatio(acc, choice, ratio, params.segment_target),
          ratio};
}

PricedChoice ChooseAuto(const StringAccumulator& acc,
                        const ColCodecParams& params) {
  const auto sample = MakeSample(acc);
  std::optional<double> blob[kByteCodecCount];
  PricedChoice best[2];
  for (const auto shape : {Shape::Dedup, Shape::Plain}) {
    auto& b = best[static_cast<size_t>(shape)];
    b = {{shape, ByteCodec::Lz4}, std::numeric_limits<uint64_t>::max(), 1.0};
    for (const auto leaf : AutoLeaves(params.objective, shape)) {
      double ratio;
      if (leaf == ByteCodec::Fsst) {
        ratio = FsstRatio(sample, shape);
      } else {
        auto& cached = blob[static_cast<size_t>(leaf)];
        if (!cached) {
          cached = BlobRatio(sample, leaf, params.compression_level);
        }
        ratio = *cached;
      }
      const auto bytes = EstimateWithRatio(acc, StringChoice{shape, leaf},
                                           ratio, params.segment_target);
      if (bytes < b.bytes) {
        b = {{shape, leaf}, bytes, ratio};
      }
    }
  }
  const auto& dedup = best[static_cast<size_t>(Shape::Dedup)];
  const auto& plain = best[static_cast<size_t>(Shape::Plain)];
  const bool repeats =
    acc.entries.size() * kDedupMinRepeat <= acc.row_count - acc.null_count;
  if (!repeats) {
    return plain.bytes <= dedup.bytes ? plain : dedup;
  }
  if (static_cast<double>(plain.bytes) <
      static_cast<double>(dedup.bytes) * kPlainWinsBelow) {
    return plain;
  }
  return dedup;
}

void SealSegments(const StringAccumulator& acc, const PricedChoice& priced,
                  const ColCodecParams& params, const duckdb::LogicalType& type,
                  SegmentSink sink) {
  SDB_ASSERT(acc.codes.size() == acc.row_count);
  const auto shape = priced.choice.shape;
  switch (priced.choice.leaf) {
    case ByteCodec::Lz4:
      Sealer<ByteCodec::Lz4>{acc, shape, priced.ratio, params, type}.Run(sink);
      return;
    case ByteCodec::Zstd:
      Sealer<ByteCodec::Zstd>{acc, shape, priced.ratio, params, type}.Run(sink);
      return;
    case ByteCodec::Zxc:
      Sealer<ByteCodec::Zxc>{acc, shape, priced.ratio, params, type}.Run(sink);
      return;
    case ByteCodec::Fsst:
      Sealer<ByteCodec::Fsst>{acc, shape, priced.ratio, params, type}.Run(sink);
      return;
  }
}

}  // namespace irs::codecs
