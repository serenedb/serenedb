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

#pragma once

#include <absl/algorithm/container.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <limits>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/bytes/lz4.hpp"
#include "iresearch/formats/column/codecs/bytes/zstd.hpp"
#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_values.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

inline constexpr size_t kSampleFrames = 4;
inline constexpr uint8_t kLeafLevel = 1;

struct LeafOption {
  ByteCodec leaf = ByteCodec::None;
  double penalty = 0;

  double Price(uint64_t bytes) const noexcept {
    return static_cast<double>(bytes) * (1.0 + penalty);
  }
};

inline constexpr LeafOption kLeaflessPlan[] = {{ByteCodec::None, 0}};
inline constexpr LeafOption kRefreshPlan[] = {{ByteCodec::None, 0},
                                              {ByteCodec::Lz4, 0.05}};
inline constexpr LeafOption kCompactionPlan[] = {
  {ByteCodec::None, 0},
  {ByteCodec::Lz4, 0.05},
  {ByteCodec::Zstd, 0.15},
};

static_assert([] {
  for (size_t i = 0; i < std::size(kCompactionPlan); ++i) {
    if (std::to_underlying(kCompactionPlan[i].leaf) != i ||
        (i < std::size(kRefreshPlan) &&
         kRefreshPlan[i].leaf != kCompactionPlan[i].leaf) ||
        (i < std::size(kLeaflessPlan) &&
         kLeaflessPlan[i].leaf != kCompactionPlan[i].leaf)) {
      return false;
    }
  }
  return true;
}());

struct Candidate {
  bool packed = false;
  bool narrowed = false;
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

inline bool FrameCopied(const Candidate& c) noexcept {
  return c.run_width != 0 || c.shuffled;
}

inline std::span<const uint8_t> FrameRaw(const Candidate& c, uint32_t f,
                                         uint32_t per,
                                         ScratchVector<uint8_t>& scratch) {
  SDB_ASSERT(!c.packed);
  const auto begin = f * per;
  const auto k = std::min(c.items - begin, per);
  const auto* values = c.values.data() + size_t{begin} * c.stored;
  if (c.run_width != 0) {
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

struct LeafSamples {
  std::vector<uint32_t> frames;
  std::vector<std::string> bytes;
};

struct SampleFrames {
  uint32_t ready = 0;
  ScratchVector<uint8_t> bytes[kSampleFrames];
};

class Leaves final : public State {
 public:
  explicit Leaves(ColumnDataCheckpointData& data)
    : _level{data.method != CodecType::Auto && data.params.compression_level
               ? data.params.compression_level
               : kLeafLevel} {}

  uint8_t Level() const noexcept { return _level; }

  size_t Compress(ByteCodec leaf, const uint8_t* src, size_t size) {
    const auto bound =
      leaf == ByteCodec::Lz4 ? Lz4::Bound(size) : Zstd::Bound(size);
    if (_out.size() < bound) {
      _out.resize(bound);
    }
    const auto* in = reinterpret_cast<const char*>(src);
    if (leaf == ByteCodec::Lz4 && _level > 1) {
      auto& lz4 = _lz4_hc ? *_lz4_hc : _lz4_hc.emplace(_level);
      return lz4.Compress(in, size, _out.data(), _out.size());
    }
    if (leaf == ByteCodec::Lz4) {
      auto& lz4 = _lz4 ? *_lz4 : _lz4.emplace(_level);
      return lz4.Compress(in, size, _out.data(), _out.size());
    }
    auto& zstd = _zstd ? *_zstd : _zstd.emplace(_level);
    return zstd.Compress(in, size, _out.data(), _out.size());
  }

  const char* Out() const noexcept { return _out.data(); }

 private:
  std::optional<Lz4::Compressor> _lz4;
  std::optional<Lz4Hc::Compressor> _lz4_hc;
  std::optional<Zstd::Compressor> _zstd;
  ScratchVector<char> _out;
  uint8_t _level;
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

  void PlanDeltas(std::span<const V> deltas, std::span<const V> values) {
    const size_t blocks =
      (deltas.size() + numeric::kBlockValues - 1) / numeric::kBlockValues;
    _base.assign(blocks, 0);
    _bits.resize(blocks);
    _first.resize(blocks);
    for (size_t b = 0; b < blocks; ++b) {
      const size_t begin = b * numeric::kBlockValues;
      const size_t end = std::min(deltas.size(), begin + numeric::kBlockValues);
      V hi = 0;
      for (size_t i = begin + 1; i < end; ++i) {
        hi = std::max(hi, deltas[i]);
      }
      _first[b] = values[begin];
      _bits[b] = static_cast<uint8_t>(std::bit_width(hi));
    }
  }

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
        const uint64_t stored_base = _first.empty() ? _base[g] : _first[g];
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
  std::vector<V> _first;
  std::vector<uint8_t> _bits;
  ScratchVector<V> _scratch;
};

template<typename T>
class Frames : public State {
 public:
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint8_t kWidth = sizeof(T);

  Estimate Analyze(ColumnDataCheckpointData& data, double bound,
                   ByteCodec leaf) {
    if (!_values.Usable()) {
      return {};
    }
    if (!_complete) {
      Rebuild(nullptr);
    }
    const auto* offered = Offered(data, leaf);
    if (!offered) {
      return {};
    }
    const auto& option = *offered;
    const auto rows = static_cast<double>(_values.Get().size());
    auto best_price = bound * rows;
    bool improved = false;
    for (const auto& c : _candidates) {
      if (option.leaf == ByteCodec::None ? c.shuffled : c.packed) {
        continue;
      }
      const auto bytes = EstimateBytes(c, option, best_price);
      if (const auto price = option.Price(bytes); price < best_price) {
        best_price = price;
        improved = true;
        _best = &c;
        _leaf = option;
        _estimated = bytes;
        if (option.leaf == ByteCodec::None) {
          _best_samples.frames.clear();
        } else {
          std::swap(_samples, _best_samples);
        }
      }
    }
    if (!improved) {
      return {};
    }
    return {
      .codec = CodecOf(_shape, _leaf.leaf),
      .ratio = static_cast<double>(_estimated) / rows,
      .cost = 1.0 + _leaf.penalty,
    };
  }

  bool Compress(ColumnDataCheckpointData& data, ByteCodec leaf) {
    auto& choice = data.tuning->numeric;
    const LeafSamples* samples = &_best_samples;
    if (data.method != CodecType::Auto) {
      if (leaf == ByteCodec::None
            ? Analyze(data, std::numeric_limits<double>::infinity(), leaf)
                  .codec == CodecType::Auto
            : !Smallest(leaf)) {
        return false;
      }
    } else if (!_best) {
      SDB_ASSERT(choice.shape == _shape);
      SDB_ASSERT(choice.leaf == leaf);
      if (!_values.Usable()) {
        return false;
      }
      Rebuild(&choice);
      const auto* option = Offered(data, leaf);
      const auto candidate = absl::c_find_if(_candidates, [&](const auto& c) {
        return c.shuffled == choice.shuffled;
      });
      if (candidate == _candidates.end() || !option) {
        return false;
      }
      _best = &*candidate;
      _leaf = *option;
      samples = nullptr;
    }
    _out.clear();
    Write(*_best, _leaf, samples, _out);
    const bool held = data.method == CodecType::Auto && !samples;
    const auto ratio =
      static_cast<double>(_out.size()) /
      std::max(static_cast<double>(PackedBound(data)) / 8, 1.0);
    if (held &&
        ((_leaf.leaf == ByteCodec::None && choice.bound_ratio != 0 &&
          ratio > choice.bound_ratio * kDrift) ||
         data.tuning->reuse.Grew(static_cast<double>(_out.size()),
                                 static_cast<double>(RawInput(data))))) {
      return false;
    }
    choice = {
      .shape = _shape,
      .packed = _best->packed,
      .narrowed = _best->narrowed,
      .leaf = _leaf.leaf,
      .shuffled = _best->shuffled,
      .bound_ratio = held ? choice.bound_ratio : ratio,
    };
    data.FlushSegment(CodecOf(_shape, _leaf.leaf), _values.Stats(data.type),
                      _values.Get().size(), _out);
    return true;
  }

 private:
  uint64_t PackedBits(Shape shape) {
    const auto& facts = _values.GetFacts();
    const uint64_t rows = _values.Get().size();
    switch (shape) {
      case Shape::Plain:
        return rows * facts.for_bits;
      case Shape::Delta:
        return rows * facts.delta_bits;
      case Shape::Runs:
        return uint64_t{facts.runs} * (facts.for_bits + std::bit_width(rows));
      case Shape::Dictionary:
        break;
    }
    return std::numeric_limits<uint64_t>::max();
  }

  uint64_t PackedBound(const ColumnDataCheckpointData& data) {
    auto bound = std::numeric_limits<uint64_t>::max();
    for (const auto shape : {Shape::Plain, Shape::Delta, Shape::Runs}) {
      if (!data.disabled.test(
            std::to_underlying(CodecOf(shape, ByteCodec::None)))) {
        bound = std::min(bound, PackedBits(shape));
      }
    }
    return bound;
  }

 protected:
  Frames(ColumnDataCheckpointData& data, Shape shape)
    : _values{data.Get<Values<T>>()},
      _named{data.method != CodecType::Auto},
      _leaves{data.Get<Leaves>()},
      _shape{shape} {}

  static bool Wanted(const NumericChoice* only, bool packed,
                     bool narrowed = false) noexcept {
    return !only || (only->packed == packed && only->narrowed == narrowed);
  }

  void Add(Candidate c) { _candidates.push_back(c); }

  void AddWithShuffle(Candidate c) {
    _candidates.push_back(c);
    if (c.stored > 1) {
      c.shuffled = true;
      _candidates.push_back(c);
    }
  }

  virtual void Build(const NumericChoice* only) = 0;

  virtual uint64_t PackedBlocks(const Candidate&) const noexcept = 0;

  virtual void AppendBlocks(const Candidate&, size_t first, size_t blocks,
                            std::string& out) = 0;

  virtual void StoreHeader(const Candidate& c, uint32_t off_dict,
                           duckdb::data_ptr_t own) const = 0;

  Values<T>& _values;
  bool _named;

 private:
  struct Geometry {
    uint8_t log2;
    uint32_t per;
    uint32_t frames;
    uint64_t off_data;
  };

  Geometry GeometryOf(const Candidate& c,
                      const LeafOption& option) const noexcept {
    uint8_t log2 = kFforFrameLog2;
    uint32_t per = _shape == Shape::Runs ? kRleFforFrameRuns : kFforFrameRows;
    if (!c.packed) {
      log2 = option.leaf == ByteCodec::None ? kNumericFrameLog2
                                            : kNumericLeafFrameLog2;
      per = (uint32_t{1} << log2) / c.ItemBytes();
    }
    const uint32_t frames = (c.items + per - 1) / per;
    const uint64_t off = HeaderSize(_shape) +
                         uint64_t{frames} * kNumericFrameMetaSize +
                         c.dict.size();
    return {
      .log2 = log2,
      .per = per,
      .frames = frames,
      .off_data = c.packed ? duckdb::AlignValue<uint64_t, 8>(off) : off,
    };
  }

  std::span<const LeafOption> Plan(
    const ColumnDataCheckpointData& data) const noexcept {
    if (data.method != CodecType::Auto) {
      return kCompactionPlan;
    }
    if constexpr (std::is_floating_point_v<T>) {
      return kLeaflessPlan;
    }
    if (data.params.tier == WriteTier::Flush) {
      return kRefreshPlan;
    }
    return kCompactionPlan;
  }

  const LeafOption* Offered(const ColumnDataCheckpointData& data,
                            ByteCodec leaf) const noexcept {
    const auto plan = Plan(data);
    const auto index = std::to_underlying(leaf);
    return index < plan.size() ? &plan[index] : nullptr;
  }

  bool Smallest(ByteCodec leaf) {
    if (!_values.Usable()) {
      return false;
    }
    if (!_complete) {
      Rebuild(nullptr);
    }
    const Candidate* best = nullptr;
    uint64_t best_bytes = 0;
    for (const auto& c : _candidates) {
      const auto bytes = uint64_t{c.items} * c.ItemBytes();
      if (!c.packed && (!best || bytes < best_bytes ||
                        (bytes == best_bytes && c.shuffled))) {
        best = &c;
        best_bytes = bytes;
      }
    }
    if (!best) {
      return false;
    }
    _best = best;
    _leaf = kCompactionPlan[std::to_underlying(leaf)];
    _best_samples.frames.clear();
    return true;
  }

  void Rebuild(const NumericChoice* only) {
    _complete = !only;
    _candidates.clear();
    _best = nullptr;
    Build(only);
    _sample_frames.resize(_candidates.size());
    for (auto& frames : _sample_frames) {
      frames.ready = 0;
    }
  }

  void AppendFrame(const Candidate& c, uint32_t begin, uint32_t items,
                   std::string& out) {
    if (c.packed) {
      AppendBlocks(
        c, begin / numeric::kBlockValues,
        (size_t{items} + numeric::kBlockValues - 1) / numeric::kBlockValues,
        out);
      return;
    }
    const auto* values = reinterpret_cast<const char*>(c.values.data());
    out.append(values + size_t{begin} * c.stored, size_t{items} * c.stored);
    if (c.run_width != 0) {
      const auto* lengths = reinterpret_cast<const char*>(c.lengths.data());
      out.append(lengths + size_t{begin} * c.run_width,
                 size_t{items} * c.run_width);
    }
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

  uint64_t EstimateBytes(const Candidate& c, const LeafOption& option,
                         double bound) {
    const auto g = GeometryOf(c, option);
    if (c.packed) {
      return g.off_data + PackedBlocks(c);
    }
    const uint64_t total = uint64_t{c.items} * c.ItemBytes();
    if (option.leaf == ByteCodec::None) {
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
    SDB_ASSERT(raw != 0);
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
    const auto values = _values.Get();
    const auto [log2, per, frames, off_data] = GeometryOf(c, option);
    FrameHeader h;
    h.width = kWidth;
    h.stored = c.stored;
    h.level = option.leaf == ByteCodec::None ? uint8_t{0} : _leaves.Level();
    h.flags = static_cast<uint8_t>((c.shuffled ? kNumericShuffled : 0) |
                                   (c.packed ? kNumericPacked : 0));
    h.frame_log2 = log2;
    h.row_count = static_cast<uint32_t>(values.size());
    h.frame_count = frames;
    h.off_frames = HeaderSize(_shape);
    h.off_data = static_cast<uint32_t>(off_data);
    h.raw_bytes = uint64_t{c.items} * c.ItemBytes();
    h.scale = _values.Scale() > 1 ? _values.Scale() : 0;
    const uint32_t off_dict = h.off_frames + frames * kNumericFrameMetaSize;

    const bool direct = option.leaf == ByteCodec::None;
    SDB_ASSERT(!direct || !c.shuffled);
    if (direct) {
      out.reserve(h.off_data + (c.packed ? PackedBlocks(c) : h.raw_bytes));
    }
    out.resize(h.off_data);
    if (!c.dict.empty()) {
      std::memcpy(out.data() + off_dict, c.dict.data(), c.dict.size());
    }
    std::vector<NumericFrameMeta> metas(frames);
    size_t sample = 0;
    for (uint32_t f = 0; f < frames; ++f) {
      const auto begin = f * per;
      auto& m = metas[f];
      m.frame.first_entry = _shape == Shape::Runs ? c.run_rows[begin] : begin;
      if (_shape == Shape::Runs && c.packed) {
        m.base = std::min(c.items - begin, per);
      }
      if (_shape == Shape::Delta && !c.packed) {
        m.base =
          begin == 0 ? static_cast<U>(values[0] - c.base) : values[begin - 1];
      }
      m.frame.comp_off = static_cast<uint32_t>(out.size() - h.off_data);
      if (direct) {
        AppendFrame(c, begin, std::min(c.items - begin, per), out);
        m.frame.raw_len =
          static_cast<uint32_t>(out.size() - h.off_data - m.frame.comp_off);
        m.frame.comp_len = m.frame.raw_len;
        continue;
      }
      bool compressible = option.leaf != ByteCodec::None;
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
      U lo = values[m.frame.first_entry];
      U hi = lo;
      for (auto r = m.frame.first_entry + 1; r < end; ++r) {
        if (Values<T>::Less(values[r], lo)) {
          lo = values[r];
        }
        if (Values<T>::Less(hi, values[r])) {
          hi = values[r];
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
    StoreHeader(c, off_dict, head + sizeof(FrameHeader));
    for (uint32_t f = 0; f < frames; ++f) {
      duckdb::Store(metas[f], head + h.off_frames + f * kNumericFrameMetaSize);
    }
  }

  Leaves& _leaves;
  Shape _shape;
  std::vector<Candidate> _candidates;
  std::vector<SampleFrames> _sample_frames;
  const Candidate* _best = nullptr;
  bool _complete = false;
  LeafOption _leaf;
  uint64_t _estimated = 0;
  LeafSamples _samples;
  LeafSamples _best_samples;
  ScratchVector<uint8_t> _frame;
  std::string _out;
};

template<typename Form, ByteCodec kLeaf>
inline constexpr CompressionFunction kNumericFunction{
  .analyze =
    [](ColumnDataCheckpointData& data, double bound) {
      return data.Get<Form>().Analyze(data, bound, kLeaf);
    },
  .compress =
    [](ColumnDataCheckpointData& data) {
      return data.Get<Form>().Compress(data, kLeaf);
    },
};

}  // namespace irs::codecs
