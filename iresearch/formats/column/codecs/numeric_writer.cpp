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

#include "iresearch/formats/column/codecs/numeric_writer.hpp"

#include <algorithm>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <limits>
#include <span>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/byte_codec.hpp"
#include "iresearch/formats/column/codecs/numeric_kernels.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;

constexpr size_t kSampleFrames = 4;
constexpr double kLz4Penalty = 0.05;
constexpr double kZstdPenalty = 0.15;

struct LeafOption {
  NumericLeaf leaf;
  uint8_t level;
  double penalty;

  double Price(uint64_t bytes) const noexcept {
    return static_cast<double>(bytes) * (1.0 + penalty);
  }
};

constexpr LeafOption kLeaflessPlan[] = {{NumericLeaf::None, 0, 0}};
constexpr LeafOption kCompactionPlan[] = {
  {NumericLeaf::None, 0, 0},
  {NumericLeaf::Lz4, 1, kLz4Penalty},
  {NumericLeaf::Zstd, 1, kZstdPenalty},
};

struct Candidate {
  NumericTransform transform = NumericTransform::Raw;
  uint8_t stored = 0;
  uint8_t run_width = 0;
  bool shuffled = false;
  uint64_t base = 0;
  uint32_t items = 0;
  std::span<const uint8_t> values;
  std::span<const uint8_t> lengths;
  std::span<const uint32_t> run_rows;
  std::span<const uint8_t> dict;
  uint32_t dict_count = 0;

  uint32_t ItemBytes() const noexcept { return stored + run_width; }
};

class Leaves {
 public:
  size_t Compress(NumericLeaf leaf, uint8_t level, const uint8_t* src,
                  size_t size) {
    const auto bound = leaf == NumericLeaf::Lz4
                         ? Leaf<ByteCodec::Lz4>::Bound(size)
                         : Leaf<ByteCodec::Zstd>::Bound(size);
    if (_out.size() < bound) {
      _out.resize(bound);
    }
    const auto* in = reinterpret_cast<const char*>(src);
    if (leaf == NumericLeaf::Lz4) {
      _lz4.SetLevel(level);
      return _lz4.Compress(in, size, _out.data(), _out.size());
    }
    _zstd.SetLevel(level);
    return _zstd.Compress(in, size, _out.data(), _out.size());
  }

  const char* Out() const noexcept { return _out.data(); }

 private:
  LeafCompressor<ByteCodec::Lz4> _lz4;
  LeafCompressor<ByteCodec::Zstd> _zstd;
  std::vector<char> _out;
};

Leaves& ThreadLeaves() {
  thread_local Leaves leaves;
  return leaves;
}

template<typename T>
class TypedSealer final : public NumericSealer {
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint8_t kWidth = sizeof(T);

 public:
  TypedSealer(const duckdb::LogicalType& type, uint64_t rows) : _type{type} {
    _values.reserve(rows);
  }

  void Add(const duckdb::Vector& input) final {
    duckdb::UnifiedVectorFormat vdata;
    input.ToUnifiedFormat(vdata);
    const auto* data = duckdb::UnifiedVectorFormat::GetData<T>(vdata);
    const auto count = input.size();
    for (idx_t i = 0; i < count; ++i) {
      const auto idx = vdata.sel->get_index(i);
      if (!vdata.validity.RowIsValid(idx)) {
        _stats.SetHasNull();
        _values.push_back(_last);
        continue;
      }
      const T v = data[idx];
      _stats.Update(v);
      U bits;
      std::memcpy(&bits, &v, sizeof(bits));
      if (!_any_valid) {
        std::fill(_values.begin(), _values.end(), bits);
        _any_valid = true;
      }
      _last = bits;
      _values.push_back(bits);
    }
  }

  void AddCodes(std::span<const U> values) {
    _values.insert(_values.end(), values.begin(), values.end());
    _any_valid = _any_valid || !values.empty();
    _codes_only = true;
  }

  std::optional<NumericSegment> Seal(const ColCodecParams& params,
                                     uint64_t rival_bytes,
                                     NumericTuning& tuning, bool due) final {
    if (std::is_floating_point_v<T> || params.tier == WriteTier::Flush) {
      return SealWith(kLeaflessPlan, rival_bytes, tuning, due);
    }
    return SealWith(kCompactionPlan, rival_bytes, tuning, due);
  }

  std::optional<NumericSegment> SealWith(std::span<const LeafOption> plan,
                                         uint64_t rival_bytes,
                                         NumericTuning& tuning, bool due) {
    if (!_any_valid || _values.size() > std::numeric_limits<uint32_t>::max()) {
      return std::nullopt;
    }
    const auto rows = static_cast<double>(_values.size());
    if (!due) {
      if (!tuning.pick) {
        return std::nullopt;
      }
      BuildCandidates(tuning.pick->transform);
      const auto* c = Find(*tuning.pick);
      const auto option = std::ranges::find_if(plan, [&](const auto& o) {
        return o.leaf == tuning.pick->leaf && o.level == tuning.pick->level;
      });
      std::optional<NumericSegment> out;
      bool drift = true;
      if (c && option != plan.end()) {
        out = Emit(*c, *option);
        const auto bytes = out->bytes.size();
        drift = static_cast<double>(bytes) > tuning.bytes_per_row * rows * 1.25;
        if (option->Price(bytes) >= static_cast<double>(rival_bytes)) {
          out.reset();
        }
      }
      if (!out) {
        return SealWith(plan, rival_bytes, tuning, true);
      }
      if (drift) {
        tuning.gap = 1;
        tuning.since = 0;
      }
      return out;
    }

    BuildCandidates(std::nullopt);
    double best_price = static_cast<double>(rival_bytes);
    uint64_t best_bytes = rival_bytes;
    const Candidate* best = nullptr;
    LeafOption best_leaf{NumericLeaf::None, 0, 0};
    for (const auto& option : plan) {
      for (const auto& c : _candidates) {
        if (c.shuffled && option.leaf == NumericLeaf::None) {
          continue;
        }
        if (c.transform == NumericTransform::Ffor &&
            option.leaf != NumericLeaf::None) {
          continue;
        }
        const auto bytes = Estimate(c, option);
        const auto price = option.Price(bytes);
        if (price < best_price) {
          best_price = price;
          best_bytes = bytes;
          best = &c;
          best_leaf = option;
        }
      }
    }
    std::optional<NumericSegment> out;
    if (best) {
      out = Emit(*best, best_leaf);
      const auto bytes = out->bytes.size();
      if (best_leaf.Price(bytes) >= static_cast<double>(rival_bytes)) {
        out.reset();
        best = nullptr;
        best_bytes = rival_bytes;
      } else {
        best_bytes = bytes;
      }
    }
    std::optional<NumericChoice> pick;
    if (best) {
      pick = NumericChoice{best->transform, best_leaf.leaf, best_leaf.level,
                           best->shuffled};
    }
    const bool kept = tuning.calibrated && tuning.pick == pick;
    tuning.gap = kept ? std::min<uint32_t>(tuning.gap * 2, kMaxGap) : 1;
    tuning.since = 0;
    tuning.calibrated = true;
    tuning.pick = pick;
    tuning.bytes_per_row = static_cast<double>(best_bytes) / rows;
    return out;
  }

 private:
  static constexpr uint32_t kMaxGap = 16;

  static constexpr uint8_t FrameLog2(const Candidate& c,
                                     const LeafOption& option) noexcept {
    if (c.transform == NumericTransform::Ffor) {
      return kFforFrameLog2;
    }
    return option.leaf == NumericLeaf::None ? kNumericFrameLog2
                                            : kNumericLeafFrameLog2;
  }

  const Candidate* Find(const NumericChoice& choice) const noexcept {
    const auto it = std::ranges::find_if(_candidates, [&](const auto& c) {
      return c.transform == choice.transform && c.shuffled == choice.shuffled;
    });
    return it == _candidates.end() ? nullptr : &*it;
  }

  NumericSegment Emit(const Candidate& c, const LeafOption& leaf) {
    NumericSegment out{
      .stats = duckdb::BaseStatistics::CreateEmpty(_type),
      .rows = _values.size(),
      .choice = {c.transform, leaf.leaf, leaf.level, c.shuffled},
    };
    _stats.Merge(out.stats);
    Write(c, leaf, out.bytes);
    return out;
  }

  static bool Less(U a, U b) noexcept {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      return static_cast<T>(a) < static_cast<T>(b);
    } else {
      return a < b;
    }
  }

  void BuildCandidates(std::optional<NumericTransform> only) {
    const auto wanted = [&](NumericTransform t) { return !only || *only == t; };
    const auto n = static_cast<uint32_t>(_values.size());
    using S = std::make_signed_t<U>;
    U lo = _values[0];
    U hi = _values[0];
    S dlo = std::numeric_limits<S>::max();
    S dhi = std::numeric_limits<S>::min();
    uint32_t runs = 1;
    for (uint32_t i = 1; i < n; ++i) {
      const U v = _values[i];
      if (Less(v, lo)) {
        lo = v;
      }
      if (Less(hi, v)) {
        hi = v;
      }
      const auto d = static_cast<S>(static_cast<U>(v - _values[i - 1]));
      dlo = std::min(dlo, d);
      dhi = std::max(dhi, d);
      runs += v != _values[i - 1];
    }
    if (n == 1) {
      dlo = 0;
      dhi = 0;
    }
    const U range = static_cast<U>(hi - lo);
    const auto for_stored = numeric::BytesFor(range);
    const auto delta_offset = static_cast<U>(dlo);
    const auto delta_stored =
      numeric::BytesFor(static_cast<U>(static_cast<U>(dhi) - delta_offset));

    _candidates.clear();
    const std::span<const uint8_t> raw{
      reinterpret_cast<const uint8_t*>(_values.data()), size_t{n} * kWidth};
    Candidate c;
    c.items = n;
    c.transform = NumericTransform::Raw;
    c.stored = kWidth;
    c.values = raw;
    if (wanted(NumericTransform::Raw)) {
      AddWithShuffle(c);
    }

    if (wanted(NumericTransform::For) && for_stored < kWidth) {
      _for.resize(size_t{n} * for_stored);
      std::vector<U> shifted(n);
      for (uint32_t i = 0; i < n; ++i) {
        shifted[i] = static_cast<U>(_values[i] - lo);
      }
      numeric::Narrow(shifted.data(), n, for_stored, _for.data());
      c.transform = NumericTransform::For;
      c.stored = for_stored;
      c.base = lo;
      c.values = _for;
      AddWithShuffle(c);
    }

    if constexpr (std::is_integral_v<T>) {
      if (wanted(NumericTransform::Ffor)) {
        BuildFfor(n);
        c.transform = NumericTransform::Ffor;
        c.stored = kWidth;
        c.base = 0;
        c.values = raw;
        c.shuffled = false;
        _candidates.push_back(c);
      }
    }

    if (wanted(NumericTransform::Delta)) {
      _delta.resize(size_t{n} * delta_stored);
      std::vector<U> z(n);
      z[0] = 0;
      for (uint32_t i = 1; i < n; ++i) {
        z[i] = static_cast<U>(_values[i] - _values[i - 1] - delta_offset);
      }
      numeric::Narrow(z.data(), n, delta_stored, _delta.data());
      c.transform = NumericTransform::Delta;
      c.stored = delta_stored;
      c.base = delta_offset;
      c.values = _delta;
      AddWithShuffle(c);
    }

    if (wanted(NumericTransform::Rle) && runs * 2 <= n) {
      std::vector<U> run_values;
      std::vector<uint32_t> run_lengths;
      run_values.reserve(runs);
      run_lengths.reserve(runs);
      _run_rows.clear();
      _run_rows.reserve(runs);
      uint32_t longest = 0;
      for (uint32_t i = 0; i < n;) {
        uint32_t j = i + 1;
        while (j < n && _values[j] == _values[i]) {
          ++j;
        }
        run_values.push_back(static_cast<U>(_values[i] - lo));
        run_lengths.push_back(j - i);
        _run_rows.push_back(i);
        longest = std::max(longest, j - i);
        i = j;
      }
      const auto run_width = numeric::BytesFor(longest);
      _rle_values.resize(size_t{runs} * for_stored);
      _rle_lengths.resize(size_t{runs} * run_width);
      numeric::Narrow(run_values.data(), runs, for_stored, _rle_values.data());
      numeric::Narrow(run_lengths.data(), runs, run_width, _rle_lengths.data());
      Candidate r;
      r.transform = NumericTransform::Rle;
      r.stored = for_stored;
      r.run_width = run_width;
      r.base = lo;
      r.items = runs;
      r.values = _rle_values;
      r.lengths = _rle_lengths;
      r.run_rows = _run_rows;
      _candidates.push_back(r);
    }

    if (wanted(NumericTransform::Dict) && !_codes_only && for_stored > 1) {
      const uint32_t cap = for_stored >= 4 ? kNumericDictMax : 256;
      if (BuildDictionary(cap)) {
        Candidate d;
        d.transform = NumericTransform::Dict;
        d.stored = numeric::BytesFor(_dict_count - 1);
        d.items = n;
        d.values = _codes;
        d.dict = _dict_bytes;
        d.dict_count = _dict_count;
        if (d.stored < for_stored) {
          AddWithShuffle(d);
        }
      }
    }
  }

  void BuildFfor(uint32_t n) {
    const size_t blocks =
      (size_t{n} + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _ffor_base.resize(blocks);
    _ffor_bits.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min<size_t>(n, begin + numeric::kBlockValues);
      U lo = _values[begin];
      U hi = lo;
      for (size_t i = begin + 1; i < end; ++i) {
        if (Less(_values[i], lo)) {
          lo = _values[i];
        }
        if (Less(hi, _values[i])) {
          hi = _values[i];
        }
      }
      _ffor_base[b] = lo;
      _ffor_bits[b] =
        static_cast<uint8_t>(numeric::BitsFor<U>(static_cast<U>(hi - lo)));
    }
  }

  uint64_t FforBytes(uint32_t frames) const noexcept {
    uint64_t bytes = kNumericHeaderSize +
                     uint64_t{frames} * kNumericFrameMetaSize +
                     _ffor_bits.size() * kFforBlockMetaBytes;
    for (const auto bits : _ffor_bits) {
      bytes += numeric::PackedBytes(bits);
    }
    return bytes;
  }

  std::span<const uint8_t> FforFrame(uint32_t begin, uint32_t rows) {
    using Word = numeric::LaneWord<U>;
    const size_t first = begin / numeric::kBlockValues;
    const size_t blocks =
      (size_t{rows} + numeric::kBlockValues - 1) / numeric::kBlockValues;
    size_t off = blocks * kFforBlockMetaBytes;
    size_t total = off;
    for (size_t b = 0; b < blocks; ++b) {
      total += numeric::PackedBytes(_ffor_bits[first + b]);
    }
    _frame.resize(total);
    std::memset(_frame.data(), 0, off);
    _ffor_block.resize(numeric::kBlockValues);
    for (size_t b = 0; b < blocks; ++b) {
      const auto base = _ffor_base[first + b];
      const unsigned bits = _ffor_bits[first + b];
      const uint64_t stored_base = base;
      std::memcpy(_frame.data() + b * kFforBlockMetaBytes, &stored_base,
                  sizeof(stored_base));
      _frame[b * kFforBlockMetaBytes + sizeof(stored_base)] =
        static_cast<uint8_t>(bits);
      const size_t row = size_t{begin} + b * numeric::kBlockValues;
      const size_t count = std::min(numeric::kBlockValues,
                                    size_t{rows} - b * numeric::kBlockValues);
      for (size_t i = 0; i < numeric::kBlockValues; ++i) {
        _ffor_block[i] =
          i < count ? static_cast<U>(_values[row + i] - base) : U{0};
      }
      numeric::kPack<U>[bits](_ffor_block.data(),
                              reinterpret_cast<Word*>(_frame.data() + off));
      off += numeric::PackedBytes(bits);
    }
    return _frame;
  }

  bool BuildDictionary(uint32_t cap) {
    const auto n = static_cast<uint32_t>(_values.size());
    _dict_map.clear();
    std::vector<U> distinct;
    for (uint32_t i = 0; i < n; ++i) {
      if (i != 0 && _values[i] == _values[i - 1]) {
        continue;
      }
      if (_dict_map.try_emplace(_values[i], 0).second) {
        if (_dict_map.size() > cap) {
          return false;
        }
        distinct.push_back(_values[i]);
      }
    }
    std::ranges::sort(distinct, [](U a, U b) { return Less(a, b); });
    for (uint32_t k = 0; k < distinct.size(); ++k) {
      _dict_map[distinct[k]] = k;
    }
    _dict_count = static_cast<uint32_t>(distinct.size());
    const auto code_width = numeric::BytesFor(_dict_count - 1);
    std::vector<uint32_t> codes(n);
    uint32_t last_code = 0;
    for (uint32_t i = 0; i < n; ++i) {
      if (i == 0 || _values[i] != _values[i - 1]) {
        last_code = _dict_map.find(_values[i])->second;
      }
      codes[i] = last_code;
    }
    _codes.resize(size_t{n} * code_width);
    numeric::Narrow(codes.data(), n, code_width, _codes.data());
    _dict_bytes.resize(size_t{_dict_count} * kWidth);
    std::memcpy(_dict_bytes.data(), distinct.data(), _dict_bytes.size());
    return true;
  }

  void AddWithShuffle(Candidate c) {
    c.shuffled = false;
    _candidates.push_back(c);
    if (c.stored > 1) {
      c.shuffled = true;
      _candidates.push_back(c);
    }
  }

  static uint32_t ItemsPerFrame(const Candidate& c, uint8_t log2) noexcept {
    if (c.transform == NumericTransform::Ffor) {
      return kFforFrameRows;
    }
    return (uint32_t{1} << log2) / c.ItemBytes();
  }

  static uint32_t FrameCount(const Candidate& c, uint8_t log2) noexcept {
    const auto per = ItemsPerFrame(c, log2);
    return (c.items + per - 1) / per;
  }

  std::span<const uint8_t> FrameRaw(const Candidate& c, uint32_t f,
                                    uint8_t log2) {
    const auto per = ItemsPerFrame(c, log2);
    const auto begin = f * per;
    const auto k = std::min(c.items - begin, per);
    if (c.transform == NumericTransform::Ffor) {
      return FforFrame(begin, k);
    }
    const auto* values = c.values.data() + size_t{begin} * c.stored;
    if (c.transform == NumericTransform::Rle) {
      _frame.resize(size_t{k} * c.ItemBytes());
      std::memcpy(_frame.data(), values, size_t{k} * c.stored);
      std::memcpy(_frame.data() + size_t{k} * c.stored,
                  c.lengths.data() + size_t{begin} * c.run_width,
                  size_t{k} * c.run_width);
      return _frame;
    }
    if (c.shuffled) {
      _frame.resize(size_t{k} * c.stored);
      numeric::Shuffle(values, k, c.stored, _frame.data());
      return _frame;
    }
    return {values, size_t{k} * c.stored};
  }

  uint64_t Estimate(const Candidate& c, const LeafOption& option) {
    const auto log2 = FrameLog2(c, option);
    const auto frames = FrameCount(c, log2);
    if (c.transform == NumericTransform::Ffor) {
      return FforBytes(frames);
    }
    const uint64_t overhead = kNumericHeaderSize +
                              uint64_t{frames} * kNumericFrameMetaSize +
                              c.dict.size();
    const uint64_t total = uint64_t{c.items} * c.ItemBytes();
    if (option.leaf == NumericLeaf::None) {
      return overhead + total;
    }
    auto& leaves = ThreadLeaves();
    uint64_t raw = 0;
    uint64_t comp = 0;
    const auto samples = std::min<uint32_t>(frames, kSampleFrames);
    for (uint32_t k = 0; k < samples; ++k) {
      const auto f = static_cast<uint32_t>(uint64_t{k} * frames / samples);
      const auto bytes = FrameRaw(c, f, log2);
      const auto n =
        leaves.Compress(option.leaf, option.level, bytes.data(), bytes.size());
      raw += bytes.size();
      comp += std::min<uint64_t>(n, bytes.size());
    }
    return overhead + (raw == 0 ? 0 : total * comp / raw);
  }

  void Write(const Candidate& c, const LeafOption& option, std::string& out) {
    const auto log2 = FrameLog2(c, option);
    const auto frames = FrameCount(c, log2);
    const auto per = ItemsPerFrame(c, log2);
    NumericHeader h;
    h.width = kWidth;
    h.transform = c.transform;
    h.leaf = option.leaf;
    h.level = option.level;
    h.stored = c.stored;
    h.run_width = c.run_width;
    h.flags = c.shuffled ? kNumericShuffled : 0;
    h.frame_log2 = log2;
    h.row_count = static_cast<uint32_t>(_values.size());
    h.frame_count = frames;
    h.dict_count = c.dict_count;
    h.off_frames = kNumericHeaderSize;
    h.off_dict = h.off_frames + frames * kNumericFrameMetaSize;
    h.off_data = h.off_dict + static_cast<uint32_t>(c.dict.size());
    h.base = c.base;
    h.raw_bytes = uint64_t{c.items} * c.ItemBytes();

    out.clear();
    out.resize(h.off_data);
    if (!c.dict.empty()) {
      std::memcpy(out.data() + h.off_dict, c.dict.data(), c.dict.size());
    }
    auto& leaves = ThreadLeaves();
    std::vector<NumericFrameMeta> metas(frames);
    for (uint32_t f = 0; f < frames; ++f) {
      const auto begin = f * per;
      auto& m = metas[f];
      m.frame.first_entry =
        c.transform == NumericTransform::Rle ? c.run_rows[begin] : begin;
      if (c.transform == NumericTransform::Delta) {
        m.base =
          begin == 0 ? static_cast<U>(_values[0] - c.base) : _values[begin - 1];
      }
      const auto bytes = FrameRaw(c, f, log2);
      m.frame.comp_off = static_cast<uint32_t>(out.size() - h.off_data);
      m.frame.raw_len = static_cast<uint32_t>(bytes.size());
      if (option.leaf != NumericLeaf::None) {
        const auto n = leaves.Compress(option.leaf, option.level, bytes.data(),
                                       bytes.size());
        if (n < bytes.size()) {
          out.append(leaves.Out(), n);
          m.frame.comp_len = static_cast<uint32_t>(n);
          continue;
        }
      }
      out.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
      m.frame.comp_len = m.frame.raw_len;
    }
    for (uint32_t f = 0; f < frames; ++f) {
      auto& m = metas[f];
      const uint32_t end =
        f + 1 < frames ? metas[f + 1].frame.first_entry : h.row_count;
      U lo = _values[m.frame.first_entry];
      U hi = lo;
      for (auto r = m.frame.first_entry + 1; r < end; ++r) {
        if (Less(_values[r], lo)) {
          lo = _values[r];
        }
        if (Less(hi, _values[r])) {
          hi = _values[r];
        }
      }
      m.min = lo;
      m.max = hi;
    }
    SDB_ENSURE(out.size() <= std::numeric_limits<uint32_t>::max(),
               "numeric codec: segment too large");
    h.data_size = static_cast<uint32_t>(out.size() - h.off_data);
    auto* head = reinterpret_cast<duckdb::data_ptr_t>(out.data());
    h.Write(head);
    for (uint32_t f = 0; f < frames; ++f) {
      metas[f].Store(head + h.off_frames + f * kNumericFrameMetaSize);
    }
  }

  duckdb::LogicalType _type;
  duckdb::StatsWriter<T> _stats;
  std::vector<U> _values;
  U _last = 0;
  bool _any_valid = false;
  bool _codes_only = false;
  std::vector<Candidate> _candidates;
  std::vector<uint8_t> _for;
  std::vector<uint8_t> _delta;
  std::vector<uint8_t> _rle_values;
  std::vector<uint8_t> _rle_lengths;
  std::vector<uint32_t> _run_rows;
  std::vector<uint8_t> _codes;
  std::vector<uint8_t> _dict_bytes;
  uint32_t _dict_count = 0;
  containers::FlatHashMap<U, uint32_t> _dict_map;
  std::vector<uint8_t> _frame;
  std::vector<U> _ffor_base;
  std::vector<uint8_t> _ffor_bits;
  std::vector<U> _ffor_block;
};

}  // namespace

std::optional<NumericSegment> EncodeCodes(std::span<const uint32_t> codes,
                                          uint64_t rival_bytes,
                                          NumericTuning& tuning) {
  TypedSealer<uint32_t> sealer{duckdb::LogicalType::UINTEGER, codes.size()};
  sealer.AddCodes(codes);
  return sealer.SealWith(kLeaflessPlan, rival_bytes, tuning, tuning.Due());
}

bool NumericApplies(duckdb::PhysicalType physical) noexcept {
  switch (physical) {
    case duckdb::PhysicalType::INT8:
    case duckdb::PhysicalType::INT16:
    case duckdb::PhysicalType::INT32:
    case duckdb::PhysicalType::INT64:
    case duckdb::PhysicalType::UINT8:
    case duckdb::PhysicalType::UINT16:
    case duckdb::PhysicalType::UINT32:
    case duckdb::PhysicalType::UINT64:
    case duckdb::PhysicalType::FLOAT:
    case duckdb::PhysicalType::DOUBLE:
      return true;
    default:
      return false;
  }
}

std::unique_ptr<NumericSealer> NumericSealer::Make(
  const duckdb::LogicalType& type, uint64_t rows) {
  switch (type.InternalType()) {
    case duckdb::PhysicalType::INT8:
      return std::make_unique<TypedSealer<int8_t>>(type, rows);
    case duckdb::PhysicalType::INT16:
      return std::make_unique<TypedSealer<int16_t>>(type, rows);
    case duckdb::PhysicalType::INT32:
      return std::make_unique<TypedSealer<int32_t>>(type, rows);
    case duckdb::PhysicalType::INT64:
      return std::make_unique<TypedSealer<int64_t>>(type, rows);
    case duckdb::PhysicalType::UINT8:
      return std::make_unique<TypedSealer<uint8_t>>(type, rows);
    case duckdb::PhysicalType::UINT16:
      return std::make_unique<TypedSealer<uint16_t>>(type, rows);
    case duckdb::PhysicalType::UINT32:
      return std::make_unique<TypedSealer<uint32_t>>(type, rows);
    case duckdb::PhysicalType::UINT64:
      return std::make_unique<TypedSealer<uint64_t>>(type, rows);
    case duckdb::PhysicalType::FLOAT:
      return std::make_unique<TypedSealer<float>>(type, rows);
    case duckdb::PhysicalType::DOUBLE:
      return std::make_unique<TypedSealer<double>>(type, rows);
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
