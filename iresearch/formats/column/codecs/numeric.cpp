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

#include "iresearch/formats/column/codecs/numeric.hpp"

#include <absl/algorithm/container.h>
#include <absl/strings/str_cat.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <duckdb/common/bitpacking.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/main/database.hpp>
#include <duckdb/planner/filter/zonemap_checker.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/buffer/buffer_handle.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <functional>
#include <limits>
#include <numeric>
#include <optional>
#include <span>
#include <string>
#include <type_traits>
#include <vector>

#include "iresearch/formats/column/codecs/lz4.hpp"
#include "iresearch/formats/column/codecs/numeric_decoder.hpp"
#include "iresearch/formats/column/codecs/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/zstd.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/enum_name.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

/*
Data layout per segment, one block of one column holding a whole row group:
+------------------------------------------------------+
|                        Header                        |
|   +----------------------------------------------+   |
|   |   NumericHeader, 64 bytes                    |   |
|   +----------------------------------------------+   |
|   value width, transform, leaf, stored width,        |
|   run width, flags, counts, section offsets,         |
|   frame size, base, common divisor (scale)           |
+------------------------------------------------------+
|                     Frame table                      |
|   +----------------------------------------------+   |
|   |   NumericFrameMeta frames[frame_count]       |   |
|   +----------------------------------------------+   |
|   first row, decoded size and stored range of        |
|   each frame in Data, its min and max, and a         |
|   base: delta's previous value, rle_ffor's           |
|   run count                                          |
+------------------------------------------------------+
|                      Dictionary                      |
|   +----------------------------------------------+   |
|   |   value dict[dict_count]                     |   |
|   +----------------------------------------------+   |
|   dict and dict_ffor only                            |
+------------------------------------------------------+
|                         Data                         |
|   +----------------------------------------------+   |
|   |   frames                                     |   |
|   +----------------------------------------------+   |
|   raw, for, delta, dict: one item per row, the       |
|   value, value - base, the difference to the         |
|   previous row minus base, or the dictionary         |
|   index, in the stored width; shuffled into          |
|   byte planes when flagged                           |
|   rle: the frame's run values (value - base),        |
|   then their run lengths                             |
|   these frames are lz4 or zstd compressed when       |
|   the leaf makes them smaller                        |
|   ffor, dict_ffor: 16384 rows per frame, in          |
|   blocks of 1024 values (dictionary indexes for      |
|   dict_ffor): a 16-byte meta (base, bit width)       |
|   per block, then each block's values minus its      |
|   base, bitpacked                                    |
|   rle_ffor: 1024 runs per frame, run values and      |
|   run lengths each packed like ffor                  |
+------------------------------------------------------+
The header holds the section offsets; bit-packed data starts at an
8-byte boundary.
*/

namespace irs::codecs {
namespace {

using duckdb::const_data_ptr_t;
using duckdb::idx_t;

constexpr size_t kSampleFrames = 4;
constexpr uint32_t kPackedCodeRunRows = 4;
constexpr double kLz4Penalty = 0.05;
constexpr double kZstdPenalty = 0.15;
constexpr uint8_t kLeafLevel = 1;

struct LeafOption {
  NumericLeaf leaf;
  double penalty;

  double Price(uint64_t bytes) const noexcept {
    return static_cast<double>(bytes) * (1.0 + penalty);
  }
};

constexpr LeafOption kLeaflessPlan[] = {{NumericLeaf::None, 0}};
constexpr LeafOption kRefreshPlan[] = {{NumericLeaf::None, 0},
                                       {NumericLeaf::Lz4, kLz4Penalty}};
constexpr LeafOption kCompactionPlan[] = {
  {NumericLeaf::None, 0},
  {NumericLeaf::Lz4, kLz4Penalty},
  {NumericLeaf::Zstd, kZstdPenalty},
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

  uint32_t ItemBytes() const noexcept { return stored + run_width; }
};

struct LeafSamples {
  std::vector<uint32_t> frames;
  std::vector<std::string> bytes;
};

struct SampleFrames {
  uint32_t ready = 0;
  ScratchVector<uint8_t> bytes[kSampleFrames];
};

class Leaves {
 public:
  size_t Compress(NumericLeaf leaf, const uint8_t* src, size_t size) {
    const auto bound =
      leaf == NumericLeaf::Lz4 ? Lz4::Bound(size) : Zstd::Bound(size);
    if (_out.size() < bound) {
      _out.resize(bound);
    }
    const auto* in = reinterpret_cast<const char*>(src);
    if (leaf == NumericLeaf::Lz4) {
      auto& lz4 = _lz4 ? *_lz4 : _lz4.emplace(kLeafLevel);
      return lz4.Compress(in, size, _out.data(), _out.size());
    }
    auto& zstd = _zstd ? *_zstd : _zstd.emplace(kLeafLevel);
    return zstd.Compress(in, size, _out.data(), _out.size());
  }

  const char* Out() const noexcept { return _out.data(); }

 private:
  std::optional<Lz4::Compressor> _lz4;
  std::optional<Zstd::Compressor> _zstd;
  ScratchVector<char> _out;
};

template<typename V>
class Blocks {
 public:
  template<typename Less>
  void Plan(std::span<const V> v, Less less) {
    const size_t blocks =
      (v.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.resize(blocks);
    _bits.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(v.size(), begin + numeric::kBlockValues);
      V lo = v[begin];
      V hi = lo;
      for (size_t i = begin + 1; i < end; ++i) {
        if (less(v[i], lo)) {
          lo = v[i];
        }
        if (less(hi, v[i])) {
          hi = v[i];
        }
      }
      _base[b] = lo;
      _bits[b] = static_cast<uint8_t>(std::bit_width(static_cast<V>(hi - lo)));
    }
  }

  uint64_t Bytes(size_t first, size_t count) const noexcept {
    uint64_t bytes = count * kFforBlockMetaBytes;
    for (size_t b = first; b < first + count; ++b) {
      bytes += numeric::PackedBytes(_bits[b]);
    }
    return bytes;
  }

  uint64_t Bytes() const noexcept { return Bytes(0, _bits.size()); }

  void Append(std::span<const V> v, size_t first, size_t count,
              std::string& out) {
    _scratch.resize(numeric::kBlockValues);
    const size_t at = out.size();
    out.resize_and_overwrite(at + Bytes(first, count), [&](char* buf,
                                                           size_t size) {
      auto* meta = reinterpret_cast<uint8_t*>(buf + at);
      auto* packed = meta + count * kFforBlockMetaBytes;
      for (size_t b = 0; b < count; ++b) {
        const size_t g = first + b;
        const uint64_t stored_base = _base[g];
        std::memset(meta, 0, kFforBlockMetaBytes);
        duckdb::Store(stored_base, meta);
        meta[sizeof(stored_base)] = _bits[g];
        const size_t begin = g * numeric::kBlockValues;
        const size_t n = std::min(numeric::kBlockValues, v.size() - begin);
        const V* in = v.data() + begin;
        if (n < numeric::kBlockValues) {
          std::memcpy(_scratch.data(), in, n * sizeof(V));
          std::fill_n(_scratch.data() + n, numeric::kBlockValues - n, _base[g]);
          in = _scratch.data();
        }
        numeric::kPack<V>[_bits[g]](
          in, reinterpret_cast<numeric::LaneWord<V>*>(packed), _base[g]);
        meta += kFforBlockMetaBytes;
        packed += numeric::PackedBytes(_bits[g]);
      }
      return size;
    });
  }

 private:
  std::vector<V> _base;
  std::vector<uint8_t> _bits;
  ScratchVector<V> _scratch;
};

template<typename T>
class TypedSealer final : public NumericSealer {
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint8_t kWidth = sizeof(T);

 public:
  TypedSealer(const duckdb::LogicalType& type, uint64_t rows) : _type{type} {
    _owned.reserve(rows);
  }

  TypedSealer(const duckdb::LogicalType& type, std::span<const U> codes)
    : _type{type},
      _values{codes},
      _any_valid{!codes.empty()},
      _codes_only{true} {}

  void Add(const duckdb::Vector& input) final {
    for (const auto value : input.Values<T>()) {
      if (!value.IsValid()) {
        _stats.SetHasNull();
        _owned.push_back(_last);
        continue;
      }
      const T v = value.GetValue();
      _stats.Update(v);
      const auto bits = std::bit_cast<U>(v);
      if (!_any_valid) {
        absl::c_fill(_owned, bits);
        _any_valid = true;
      }
      _last = bits;
      _owned.push_back(bits);
    }
  }

  NumericSegment Seal(const ColCodecParams& params, uint64_t rival_bytes,
                      NumericTuning& tuning, bool due) final {
    if (std::is_floating_point_v<T>) {
      return SealWith(kLeaflessPlan, rival_bytes, tuning, due);
    }
    if (params.tier == WriteTier::Flush) {
      return SealWith(kRefreshPlan, rival_bytes, tuning, due);
    }
    return SealWith(kCompactionPlan, rival_bytes, tuning, due);
  }

  NumericSegment SealWith(std::span<const LeafOption> plan,
                          uint64_t rival_bytes, NumericTuning& tuning,
                          bool due) {
    if (!_codes_only) {
      _values = _owned;
    }
    if (!_any_valid || _values.size() > std::numeric_limits<uint32_t>::max()) {
      return NoSegment();
    }
    DivideByGcd();
    const auto rows = static_cast<double>(_values.size());
    auto& reuse = tuning.reuse;
    if (!due) {
      if (!reuse.pick) {
        return NoSegment();
      }
      BuildCandidates(&reuse.pick->transform);
      const auto* c = Find(*reuse.pick);
      const auto option = absl::c_find_if(
        plan, [&](const auto& o) { return o.leaf == reuse.pick->leaf; });
      if (c && option != plan.end()) {
        auto out = Emit(*c, *option, nullptr);
        const auto bytes = out.bytes.size();
        const bool drift = reuse.Grew(static_cast<double>(bytes), rows);
        const bool keep =
          rival_bytes == kNoRival
            ? !drift
            : option->Price(bytes) < static_cast<double>(rival_bytes);
        if (keep) {
          if (drift) {
            reuse.clock.Restart();
          }
          return out;
        }
      }
      if (rival_bytes == kNoRival) {
        reuse.clock.Restart();
        return NoSegment();
      }
    }

    BuildCandidates(nullptr);
    double best_price = static_cast<double>(rival_bytes);
    const Candidate* best = nullptr;
    LeafOption best_leaf{NumericLeaf::None, 0};
    for (const auto& option : plan) {
      for (const auto& c : _candidates) {
        if (option.leaf == NumericLeaf::None ? c.shuffled
                                             : BitPacked(c.transform)) {
          continue;
        }
        if (const auto price = option.Price(Estimate(c, option, best_price));
            price < best_price) {
          best_price = price;
          best = &c;
          best_leaf = option;
          if (option.leaf == NumericLeaf::None || BitPacked(c.transform)) {
            _best_samples.frames.clear();
          } else {
            std::swap(_samples, _best_samples);
          }
        }
      }
    }
    const auto calibrate = [&](std::optional<NumericChoice> pick,
                               uint64_t bytes) {
      reuse.Settle(pick, reuse.calibrated && reuse.pick == pick,
                   static_cast<double>(bytes), rows);
    };
    if (best) {
      auto out = Emit(*best, best_leaf, &_best_samples);
      if (const auto bytes = out.bytes.size();
          best_leaf.Price(bytes) < static_cast<double>(rival_bytes)) {
        calibrate(
          NumericChoice{best->transform, best_leaf.leaf, best->shuffled},
          bytes);
        return out;
      }
    }
    calibrate(std::nullopt, rival_bytes);
    return NoSegment();
  }

 private:
  struct Geometry {
    uint8_t log2;
    uint32_t per;
    uint32_t frames;
    uint64_t off_data;
  };

  static Geometry GeometryOf(const Candidate& c,
                             const LeafOption& option) noexcept {
    const bool packed = BitPacked(c.transform);
    uint8_t log2 = kFforFrameLog2;
    uint32_t per = c.transform == NumericTransform::RleFfor ? kRleFforFrameRuns
                                                            : kFforFrameRows;
    if (!packed) {
      log2 = option.leaf == NumericLeaf::None ? kNumericFrameLog2
                                              : kNumericLeafFrameLog2;
      per = (uint32_t{1} << log2) / c.ItemBytes();
    }
    const uint32_t frames = (c.items + per - 1) / per;
    const uint64_t off = kNumericHeaderSize +
                         uint64_t{frames} * kNumericFrameMetaSize +
                         c.dict.size();
    return {
      .log2 = log2,
      .per = per,
      .frames = frames,
      .off_data = packed ? duckdb::AlignValue<uint64_t, 8>(off) : off,
    };
  }

  const Candidate* Find(const NumericChoice& choice) const noexcept {
    const auto it = absl::c_find_if(_candidates, [&](const auto& c) {
      return c.transform == choice.transform && c.shuffled == choice.shuffled;
    });
    return it == _candidates.end() ? nullptr : &*it;
  }

  NumericSegment Emit(const Candidate& c, const LeafOption& leaf,
                      const LeafSamples* samples) {
    auto out = NoSegment();
    _stats.Merge(out.stats);
    Write(c, leaf, samples, out.bytes);
    return out;
  }

  NumericSegment NoSegment() const {
    return {.stats = duckdb::BaseStatistics::CreateEmpty(_type)};
  }

  void DivideByGcd() noexcept {
    if constexpr (std::is_integral_v<T>) {
      if (_codes_only || _scale != 0) {
        return;
      }
      U gcd = 0;
      for (const U bits : _owned) {
        U magnitude = bits;
        if constexpr (std::is_signed_v<T>) {
          if (static_cast<T>(bits) < 0) {
            magnitude = static_cast<U>(U{0} - bits);
          }
        }
        gcd = std::gcd(gcd, magnitude);
        if (gcd == 1) {
          break;
        }
      }
      _scale = gcd > 1 ? gcd : 1;
      if (_scale > 1) {
        numeric::DivideExact<std::is_signed_v<T>>(_owned.data(), _owned.size(),
                                                  _scale);
      }
    }
  }

  static bool Less(U a, U b) noexcept {
    if constexpr (std::is_integral_v<T> && std::is_signed_v<T>) {
      return static_cast<T>(a) < static_cast<T>(b);
    } else {
      return a < b;
    }
  }

  void BuildCandidates(const NumericTransform* only) {
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
    if (wanted(NumericTransform::Raw)) {
      AddWithShuffle({
        .transform = NumericTransform::Raw,
        .stored = kWidth,
        .items = n,
        .values = raw,
      });
    }

    if (wanted(NumericTransform::For) && for_stored < kWidth) {
      _for.resize(size_t{n} * for_stored);
      numeric::Narrow(n, for_stored, _for.data(),
                      [v = _values.data(), lo](size_t i) {
                        return static_cast<U>(v[i] - lo);
                      });
      AddWithShuffle({
        .transform = NumericTransform::For,
        .stored = for_stored,
        .base = lo,
        .items = n,
        .values = _for,
      });
    }

    if constexpr (std::is_integral_v<T>) {
      if (wanted(NumericTransform::Ffor)) {
        _ffor.Plan(_values, Less);
        _candidates.push_back({
          .transform = NumericTransform::Ffor,
          .stored = kWidth,
          .items = n,
        });
      }
    }

    if (wanted(NumericTransform::Delta)) {
      _delta.resize(size_t{n} * delta_stored);
      std::memset(_delta.data(), 0, delta_stored);
      numeric::Narrow(n - 1, delta_stored, _delta.data() + delta_stored,
                      [v = _values.data(), delta_offset](size_t i) {
                        return static_cast<U>(v[i + 1] - v[i] - delta_offset);
                      });
      AddWithShuffle({
        .transform = NumericTransform::Delta,
        .stored = delta_stored,
        .base = delta_offset,
        .items = n,
        .values = _delta,
      });
    }

    if ((wanted(NumericTransform::Rle) || wanted(NumericTransform::RleFfor)) &&
        runs * 2 <= n) {
      BuildRuns(runs);
      if (wanted(NumericTransform::Rle)) {
        const auto run_width =
          numeric::BytesFor(std::ranges::max(_run_lengths));
        _rle_values.resize(size_t{runs} * for_stored);
        _rle_lengths.resize(size_t{runs} * run_width);
        numeric::Narrow(runs, for_stored, _rle_values.data(),
                        [v = _run_values.data(), lo](size_t r) {
                          return static_cast<U>(v[r] - lo);
                        });
        numeric::Narrow(runs, run_width, _rle_lengths.data(),
                        [l = _run_lengths.data()](size_t r) { return l[r]; });
        _candidates.push_back({
          .transform = NumericTransform::Rle,
          .stored = for_stored,
          .run_width = run_width,
          .base = lo,
          .items = runs,
          .values = _rle_values,
          .lengths = _rle_lengths,
          .run_rows = _run_rows,
        });
      }
      if (wanted(NumericTransform::RleFfor) &&
          (!_codes_only || runs * kPackedCodeRunRows <= n)) {
        _run_blocks.Plan(std::span<const U>{_run_values}, Less);
        _length_blocks.Plan(std::span<const uint32_t>{_run_lengths},
                            std::less<uint32_t>{});
        _candidates.push_back({
          .transform = NumericTransform::RleFfor,
          .stored = kWidth,
          .items = runs,
          .run_rows = _run_rows,
        });
      }
    }

    if ((wanted(NumericTransform::Dict) ||
         wanted(NumericTransform::DictFfor)) &&
        !_codes_only && for_stored > 1) {
      const uint32_t cap = for_stored >= 4 ? kNumericDictMax : 256;
      if (BuildDictionary(cap)) {
        const auto dict_count = static_cast<uint32_t>(_dict.size());
        const std::span<const uint8_t> dict{
          reinterpret_cast<const uint8_t*>(_dict.data()),
          size_t{dict_count} * kWidth};
        if (wanted(NumericTransform::Dict)) {
          const auto code_width = numeric::BytesFor(dict_count - 1);
          if (code_width < for_stored) {
            _codes.resize(size_t{n} * code_width);
            numeric::Narrow(
              n, code_width, _codes.data(),
              [k = _code_values.data()](size_t i) { return k[i]; });
            AddWithShuffle({
              .transform = NumericTransform::Dict,
              .stored = code_width,
              .items = n,
              .values = _codes,
              .dict = dict,
            });
          }
        }
        if (wanted(NumericTransform::DictFfor)) {
          _code_blocks.Plan(std::span<const uint32_t>{_code_values},
                            std::less<uint32_t>{});
          _candidates.push_back({
            .transform = NumericTransform::DictFfor,
            .stored = kWidth,
            .items = n,
            .dict = dict,
          });
        }
      }
    }
    _sample_frames.resize(_candidates.size());
    for (auto& frames : _sample_frames) {
      frames.ready = 0;
    }
  }

  void BuildRuns(uint32_t runs) {
    const auto n = static_cast<uint32_t>(_values.size());
    _run_values.clear();
    _run_lengths.clear();
    _run_rows.clear();
    _run_values.reserve(runs);
    _run_lengths.reserve(runs);
    _run_rows.reserve(runs);
    for (uint32_t i = 0; i < n;) {
      uint32_t j = i + 1;
      while (j < n && _values[j] == _values[i]) {
        ++j;
      }
      _run_values.push_back(_values[i]);
      _run_lengths.push_back(j - i);
      _run_rows.push_back(i);
      i = j;
    }
  }

  void AppendPacked(const Candidate& c, uint32_t begin, uint32_t items,
                    std::string& out) {
    const size_t first = begin / numeric::kBlockValues;
    const size_t blocks =
      (size_t{items} + numeric::kBlockValues - 1) / numeric::kBlockValues;
    switch (c.transform) {
      case NumericTransform::Ffor:
        _ffor.Append(_values, first, blocks, out);
        break;
      case NumericTransform::RleFfor:
        _run_blocks.Append(std::span<const U>{_run_values}, first, blocks, out);
        _length_blocks.Append(std::span<const uint32_t>{_run_lengths}, first,
                              blocks, out);
        break;
      default:
        _code_blocks.Append(std::span<const uint32_t>{_code_values}, first,
                            blocks, out);
        break;
    }
  }

  void AppendFrame(const Candidate& c, uint32_t begin, uint32_t items,
                   std::string& out) {
    if (BitPacked(c.transform)) {
      AppendPacked(c, begin, items, out);
      return;
    }
    const auto* values = reinterpret_cast<const char*>(c.values.data());
    out.append(values + size_t{begin} * c.stored, size_t{items} * c.stored);
    if (c.transform == NumericTransform::Rle) {
      const auto* lengths = reinterpret_cast<const char*>(c.lengths.data());
      out.append(lengths + size_t{begin} * c.run_width,
                 size_t{items} * c.run_width);
    }
  }

  uint64_t PackedBlocks(const Candidate& c) const noexcept {
    switch (c.transform) {
      case NumericTransform::Ffor:
        return _ffor.Bytes();
      case NumericTransform::RleFfor:
        return _run_blocks.Bytes() + _length_blocks.Bytes();
      default:
        return _code_blocks.Bytes();
    }
  }

  bool BuildDictionary(uint32_t cap) {
    const auto n = static_cast<uint32_t>(_values.size());
    _dict_map.clear();
    _dict.clear();
    for (uint32_t i = 0; i < n; ++i) {
      if (i != 0 && _values[i] == _values[i - 1]) {
        continue;
      }
      if (_dict_map.try_emplace(_values[i], 0).second) {
        if (_dict_map.size() > cap) {
          return false;
        }
        _dict.push_back(_values[i]);
      }
    }
    absl::c_sort(_dict, [](U a, U b) { return Less(a, b); });
    for (uint32_t k = 0; k < _dict.size(); ++k) {
      _dict_map[_dict[k]] = k;
    }
    _code_values.resize(n);
    uint32_t last_code = 0;
    for (uint32_t i = 0; i < n; ++i) {
      if (i == 0 || _values[i] != _values[i - 1]) {
        last_code = _dict_map.find(_values[i])->second;
      }
      _code_values[i] = last_code;
    }
    return true;
  }

  void AddWithShuffle(Candidate c) {
    _candidates.push_back(c);
    if (c.stored > 1) {
      c.shuffled = true;
      _candidates.push_back(c);
    }
  }

  static bool FrameCopied(const Candidate& c) noexcept {
    return c.transform == NumericTransform::Rle || c.shuffled;
  }

  static std::span<const uint8_t> FrameRaw(const Candidate& c, uint32_t f,
                                           uint32_t per,
                                           ScratchVector<uint8_t>& scratch) {
    SDB_ASSERT(!BitPacked(c.transform));
    const auto begin = f * per;
    const auto k = std::min(c.items - begin, per);
    const auto* values = c.values.data() + size_t{begin} * c.stored;
    if (c.transform == NumericTransform::Rle) {
      scratch.resize(size_t{k} * c.ItemBytes());
      std::memcpy(scratch.data(), values, size_t{k} * c.stored);
      std::memcpy(scratch.data() + size_t{k} * c.stored,
                  c.lengths.data() + size_t{begin} * c.run_width,
                  size_t{k} * c.run_width);
      return scratch;
    }
    if (c.shuffled) {
      scratch.resize(size_t{k} * c.stored);
      numeric::Shuffle(values, k, c.stored, scratch.data());
      return scratch;
    }
    return {values, size_t{k} * c.stored};
  }

  std::span<const uint8_t> SampleRaw(const Candidate& c, uint32_t k, uint32_t f,
                                     uint32_t per) {
    if (!FrameCopied(c)) {
      return FrameRaw(c, f, per, _frame);
    }
    auto& cached = _sample_frames[static_cast<size_t>(&c - _candidates.data())];
    if (k < cached.ready) {
      return cached.bytes[k];
    }
    SDB_ASSERT(k == cached.ready);
    cached.ready = k + 1;
    return FrameRaw(c, f, per, cached.bytes[k]);
  }

  uint64_t Estimate(const Candidate& c, const LeafOption& option,
                    double bound) {
    const auto g = GeometryOf(c, option);
    if (BitPacked(c.transform)) {
      return g.off_data + PackedBlocks(c);
    }
    const uint64_t total = uint64_t{c.items} * c.ItemBytes();
    if (option.leaf == NumericLeaf::None) {
      return g.off_data + total;
    }
    const auto samples = std::min<uint32_t>(g.frames, kSampleFrames);
    const auto frame = [&](uint32_t k) {
      return static_cast<uint32_t>(uint64_t{k} * g.frames / samples);
    };
    uint64_t raw = 0;
    for (uint32_t k = 0; k < samples; ++k) {
      raw +=
        uint64_t{std::min(c.items - frame(k) * g.per, g.per)} * c.ItemBytes();
    }
    if (raw == 0) {
      return g.off_data;
    }
    _samples.frames.clear();
    _samples.bytes.resize(samples);
    uint64_t comp = 0;
    for (uint32_t k = 0; k < samples; ++k) {
      const auto bytes = SampleRaw(c, k, frame(k), g.per);
      SDB_ASSERT(bytes.size() ==
                 uint64_t{std::min(c.items - frame(k) * g.per, g.per)} *
                   c.ItemBytes());
      const auto n = _leaves.Compress(option.leaf, bytes.data(), bytes.size());
      comp += std::min<uint64_t>(n, bytes.size());
      _samples.frames.push_back(frame(k));
      if (n < bytes.size()) {
        _samples.bytes[k].assign(_leaves.Out(), n);
      } else {
        _samples.bytes[k].clear();
      }
      if (const auto least = g.off_data + total * comp / raw;
          option.Price(least) >= bound) {
        return least;
      }
    }
    return g.off_data + total * comp / raw;
  }

  void Write(const Candidate& c, const LeafOption& option,
             const LeafSamples* samples, std::string& out) {
    const auto [log2, per, frames, off_data] = GeometryOf(c, option);
    NumericHeader h;
    h.width = kWidth;
    h.transform = c.transform;
    h.leaf = option.leaf;
    h.level = option.leaf == NumericLeaf::None ? uint8_t{0} : kLeafLevel;
    h.stored = c.stored;
    h.run_width = c.run_width;
    h.flags = c.shuffled ? kNumericShuffled : 0;
    h.frame_log2 = log2;
    h.row_count = static_cast<uint32_t>(_values.size());
    h.frame_count = frames;
    h.dict_count = static_cast<uint32_t>(c.dict.size() / kWidth);
    h.off_frames = kNumericHeaderSize;
    h.off_dict = h.off_frames + frames * kNumericFrameMetaSize;
    h.off_data = static_cast<uint32_t>(off_data);
    h.base = c.base;
    h.raw_bytes = uint64_t{c.items} * c.ItemBytes();
    h.scale = _scale > 1 ? _scale : 0;

    const bool direct = option.leaf == NumericLeaf::None && !c.shuffled;
    if (direct) {
      out.reserve(h.off_data +
                  (BitPacked(c.transform) ? PackedBlocks(c) : h.raw_bytes));
    }
    out.resize(h.off_data);
    if (!c.dict.empty()) {
      std::memcpy(out.data() + h.off_dict, c.dict.data(), c.dict.size());
    }
    std::vector<NumericFrameMeta> metas(frames);
    size_t sample = 0;
    for (uint32_t f = 0; f < frames; ++f) {
      const auto begin = f * per;
      auto& m = metas[f];
      m.frame.first_entry = Runs(c.transform) ? c.run_rows[begin] : begin;
      if (c.transform == NumericTransform::RleFfor) {
        m.base = std::min(c.items - begin, per);
      }
      if (c.transform == NumericTransform::Delta) {
        m.base =
          begin == 0 ? static_cast<U>(_values[0] - c.base) : _values[begin - 1];
      }
      m.frame.comp_off = static_cast<uint32_t>(out.size() - h.off_data);
      if (direct) {
        AppendFrame(c, begin, std::min(c.items - begin, per), out);
        m.frame.raw_len =
          static_cast<uint32_t>(out.size() - h.off_data - m.frame.comp_off);
        m.frame.comp_len = m.frame.raw_len;
        continue;
      }
      bool compressible = option.leaf != NumericLeaf::None;
      if (samples && sample < samples->frames.size() &&
          samples->frames[sample] == f) {
        const auto& stored = samples->bytes[sample++];
        if (!stored.empty()) {
          m.frame.raw_len = static_cast<uint32_t>(
            uint64_t{std::min(c.items - begin, per)} * c.ItemBytes());
          out.append(stored);
          m.frame.comp_len = static_cast<uint32_t>(stored.size());
          continue;
        }
        compressible = false;
      }
      const auto bytes = FrameRaw(c, f, per, _frame);
      m.frame.raw_len = static_cast<uint32_t>(bytes.size());
      if (compressible) {
        const auto n =
          _leaves.Compress(option.leaf, bytes.data(), bytes.size());
        if (n < bytes.size()) {
          out.append(_leaves.Out(), n);
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
    duckdb::Store(h, head);
    for (uint32_t f = 0; f < frames; ++f) {
      duckdb::Store(metas[f], head + h.off_frames + f * kNumericFrameMetaSize);
    }
  }

  duckdb::LogicalType _type;
  duckdb::StatsWriter<T> _stats;
  std::vector<U> _owned;
  std::span<const U> _values;
  U _last = 0;
  U _scale = 0;
  bool _any_valid = false;
  bool _codes_only = false;
  std::vector<Candidate> _candidates;
  ScratchVector<uint8_t> _for;
  ScratchVector<uint8_t> _delta;
  ScratchVector<uint8_t> _rle_values;
  ScratchVector<uint8_t> _rle_lengths;
  std::vector<U> _run_values;
  std::vector<uint32_t> _run_lengths;
  std::vector<uint32_t> _run_rows;
  ScratchVector<uint8_t> _codes;
  ScratchVector<uint32_t> _code_values;
  std::vector<U> _dict;
  containers::FlatHashMap<U, uint32_t> _dict_map;
  ScratchVector<uint8_t> _frame;
  Leaves _leaves;
  LeafSamples _samples;
  LeafSamples _best_samples;
  std::vector<SampleFrames> _sample_frames;
  Blocks<U> _ffor;
  Blocks<U> _run_blocks;
  Blocks<uint32_t> _length_blocks;
  Blocks<uint32_t> _code_blocks;
};

NumericHeader SegmentHeader(const_data_ptr_t base,
                            const ColumnSegment& segment) {
  const auto h = NumericHeader::Parse(base, segment.size);
  SDB_ENSURE(h.row_count == segment.count, "numeric codec: row count mismatch");
  return h;
}

template<typename T>
struct ScanState final : SegmentScanState {
  ScanState(const ColumnSegment& segment, duckdb::BufferHandle handle_p)
    : handle{std::move(handle_p)},
      frames{handle.Ptr(), SegmentHeader(handle.Ptr(), segment),
             segment.cache_slot} {}

  duckdb::BufferHandle handle;
  FrameDecoder<T> frames;
  duckdb::unique_ptr<duckdb::BaseStatistics> filter_stats;
};

template<typename T>
std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  return std::make_unique<ScanState<T>>(segment, segment.Pin());
}

template<typename T>
void ScanPartial(const ColumnSegment&, ColumnScanState& state, idx_t scan_count,
                 duckdb::Vector& result, idx_t result_offset) {
  state.scan_state->Cast<ScanState<T>>().frames.Read(
    state.GetPositionInSegment(), scan_count,
    duckdb::FlatVector::GetDataMutable<T>(result) + result_offset);
}

template<typename T>
void ScanVector(const ColumnSegment& segment, ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result) {
  ScanPartial<T>(segment, state, scan_count, result, 0);
}

template<typename T>
void Select(const ColumnSegment&, ColumnScanState& state, idx_t,
            duckdb::Vector& result, const duckdb::SelectionVector& sel,
            idx_t sel_count) {
  auto& frames = state.scan_state->Cast<ScanState<T>>().frames;
  auto* out = duckdb::FlatVector::GetDataMutable<T>(result);
  const uint64_t start = state.GetPositionInSegment();
  for (idx_t i = 0; i < sel_count; ++i) {
    const uint64_t row = start + sel.get_index(i);
    out[i] = frames.Get(row);
  }
}

template<typename T>
duckdb::FilterPropagateResult FramesVerdict(
  ScanState<T>& scan, const duckdb::LogicalType& type, uint64_t begin,
  uint64_t end, const duckdb::ZonemapChecker& checker) {
  if (!scan.filter_stats) {
    scan.filter_stats = duckdb::NumericStats::CreateEmpty(type).ToUnique();
    scan.filter_stats->SetHasNoNullFast();
  }
  auto& stats = *scan.filter_stats;
  const auto check = [&](uint32_t f) {
    duckdb::NumericStats::SetMin<T>(stats, scan.frames.FrameMin(f));
    duckdb::NumericStats::SetMax<T>(stats, scan.frames.FrameMax(f));
    return checker.Check(stats, nullptr);
  };
  const auto first = scan.frames.FrameOf(begin);
  const auto last = scan.frames.FrameOf(end - 1);
  const auto verdict = check(first);
  for (auto f = first + 1;
       f <= last &&
       verdict != duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
       ++f) {
    if (check(f) != verdict) {
      return duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
    }
  }
  return verdict;
}

template<typename T>
void Filter(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector& sel, idx_t& sel_count,
            const duckdb::TableFilter& filter,
            duckdb::TableFilterState& filter_state) {
  auto& scan = state.scan_state->Cast<ScanState<T>>();
  const uint64_t start = state.GetPositionInSegment();
  auto verdict = duckdb::FilterPropagateResult::NO_PRUNING_POSSIBLE;
  if constexpr (std::is_integral_v<T>) {
    if (filter.filter_type == duckdb::TableFilterType::EXPRESSION_FILTER) {
      const auto& checker =
        *filter_state.Cast<duckdb::ExpressionFilterState>().zonemap_checker;
      if (checker.IsFullyCompiled()) {
        verdict = FramesVerdict(scan, segment.stats.GetType(), start,
                                start + vector_count, checker);
      }
    }
  }
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_FALSE ||
      verdict == duckdb::FilterPropagateResult::FILTER_FALSE_OR_NULL) {
    sel_count = 0;
    return;
  }
  result.SetVectorType(duckdb::VectorType::FLAT_VECTOR);
  ScanPartial<T>(segment, state, vector_count, result, 0);
  if (verdict == duckdb::FilterPropagateResult::FILTER_ALWAYS_TRUE) {
    return;
  }
  duckdb::ColumnSegment::FilterSelection(sel, result, filter_state,
                                         vector_count, sel_count);
}

template<typename T>
struct FetchCache final : SegmentScanState {
  std::optional<FrameDecoder<T>> frames;
};

template<typename T>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  auto& handle = state.GetOrInsertHandle(segment);
  auto& cache = state.GetOrCreateSegmentState<FetchCache<T>>(
    [] { return std::make_unique<FetchCache<T>>(); });
  if (!cache.frames) {
    const auto* base = handle.Ptr();
    cache.frames.emplace(base, SegmentHeader(base, segment),
                         segment.cache_slot);
  }
  auto& frames = *cache.frames;
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  constexpr auto kNone = std::numeric_limits<uint64_t>::max();
  uint64_t previous = kNone;
  for (idx_t i = 0; i < count; ++i) {
    const auto row = static_cast<uint64_t>(rows[i]);
    SDB_ENSURE(row < segment.count, "numeric codec: row out of range");
    const uint64_t next = i + 1 < count ? rows[i + 1] : kNone;
    data[result_offset + result_sel.get_index(i)] =
      frames.Fetch(row, previous, next);
    previous = row;
  }
}

duckdb::InsertionOrderPreservingMap<std::string> SegmentInfo(
  const ColumnSegment& segment) {
  auto handle = segment.Pin();
  const auto h = NumericHeader::Parse(handle.Ptr(), segment.size);
  duckdb::InsertionOrderPreservingMap<std::string> info;
  info["transform"] = SnakeCaseName(h.transform);
  info["leaf"] = SnakeCaseName(h.leaf);
  info["level"] = absl::StrCat(static_cast<uint32_t>(h.level));
  info["stored"] = absl::StrCat(static_cast<uint32_t>(h.stored));
  info["shuffled"] = h.Shuffled() ? "true" : "false";
  info["frames"] = absl::StrCat(h.frame_count);
  info["entries"] = absl::StrCat(h.dict_count);
  info["raw_bytes"] = absl::StrCat(h.raw_bytes);
  info["data_bytes"] = absl::StrCat(h.data_size);
  info["scale"] = absl::StrCat(h.scale > 1 ? h.scale : 1);
  return info;
}

template<typename T>
constexpr CompressionFunction MakeFunction(duckdb::PhysicalType physical) {
  return {
    .type = CodecType::Numeric,
    .framed = true,
    .init_scan = InitScan<T>,
    .scan_vector = ScanVector<T>,
    .scan_partial = ScanPartial<T>,
    .select = Select<T>,
    .filter = Filter<T>,
    .fetch_rows = FetchRows<T>,
    .skip = EmptySkip,
    .get_segment_info = SegmentInfo,
  };
}

template<typename F>
std::invoke_result_t<F&, std::type_identity<int8_t>> WithNumericType(
  duckdb::PhysicalType physical, F&& f) {
  switch (physical) {
    case duckdb::PhysicalType::INT8:
      return f(std::type_identity<int8_t>{});
    case duckdb::PhysicalType::INT16:
      return f(std::type_identity<int16_t>{});
    case duckdb::PhysicalType::INT32:
      return f(std::type_identity<int32_t>{});
    case duckdb::PhysicalType::INT64:
      return f(std::type_identity<int64_t>{});
    case duckdb::PhysicalType::UINT8:
      return f(std::type_identity<uint8_t>{});
    case duckdb::PhysicalType::UINT16:
      return f(std::type_identity<uint16_t>{});
    case duckdb::PhysicalType::UINT32:
      return f(std::type_identity<uint32_t>{});
    case duckdb::PhysicalType::UINT64:
      return f(std::type_identity<uint64_t>{});
    case duckdb::PhysicalType::FLOAT:
      return f(std::type_identity<float>{});
    case duckdb::PhysicalType::DOUBLE:
      return f(std::type_identity<double>{});
    default:
      return {};
  }
}

uint64_t CodesBound(uint64_t rows, uint64_t entries, uint64_t runs) noexcept {
  return std::min(Packed(rows, entries),
                  Packed(runs, entries) + Packed(runs, rows));
}

}  // namespace

uint64_t Packed(uint64_t count, uint64_t max_value) noexcept {
  return duckdb::BitpackingPrimitives::GetRequiredSize(
    count, duckdb::BitpackingPrimitives::MinimumBitWidth<uint32_t>(
             static_cast<uint32_t>(max_value)));
}

uint64_t EstimateCodes(const NumericTuning& tuning, uint64_t rows,
                       uint64_t entries, uint64_t runs) noexcept {
  return static_cast<uint64_t>(
    static_cast<double>(CodesBound(rows, entries, runs)) * tuning.codes_ratio);
}

std::string EncodeCodes(std::span<const uint32_t> codes, uint64_t entries,
                        uint64_t runs, NumericTuning& tuning) {
  TypedSealer<uint32_t> sealer{duckdb::LogicalType::UINTEGER, codes};
  const auto bound = CodesBound(codes.size(), entries, runs);
  bool due = tuning.reuse.Due();
  auto bytes = sealer.SealWith(kLeaflessPlan, kNoRival, tuning, due).bytes;
  if (bytes.empty() ||
      (!due && static_cast<double>(bytes.size()) >
                 static_cast<double>(bound) * tuning.codes_ratio * kDrift)) {
    due = true;
    bytes = sealer.SealWith(kLeaflessPlan, kNoRival, tuning, true).bytes;
  }
  if (due && bound != 0) {
    tuning.codes_ratio =
      static_cast<double>(bytes.size()) / static_cast<double>(bound);
  }
  return bytes;
}

bool UpdateConstant(const duckdb::Vector& input,
                    duckdb::BaseStatistics& stats) {
  if (stats.GetStatsType() != duckdb::StatisticsType::NUMERIC_STATS) {
    return false;
  }
  return WithNumericType(stats.GetType().InternalType(),
                         [&]<typename T>(std::type_identity<T>) {
                           for (const auto value : input.Values<T>()) {
                             if (!value.IsValid()) {
                               stats.SetHasNullFast();
                               continue;
                             }
                             stats.SetHasNoNullFast();
                             stats.UpdateNumericStats<T>(value.GetValue());
                           }
                           return duckdb::NumericStats::IsConstant(stats);
                         });
}

std::unique_ptr<NumericSealer> NumericSealer::Make(
  const duckdb::LogicalType& type, uint64_t rows) {
  return WithNumericType(
    type.InternalType(),
    [&]<typename T>(std::type_identity<T>) -> std::unique_ptr<NumericSealer> {
      return std::make_unique<TypedSealer<T>>(type, rows);
    });
}

const CompressionFunction* NumericFunction(duckdb::PhysicalType physical) {
  return WithNumericType(
    physical,
    [&]<typename T>(std::type_identity<T>) -> const CompressionFunction* {
      static const auto kFunction = MakeFunction<T>(physical);
      return &kFunction;
    });
}

}  // namespace irs::codecs
