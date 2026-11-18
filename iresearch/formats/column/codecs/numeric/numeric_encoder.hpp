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
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/ffor_blocks.hpp"
#include "iresearch/formats/column/codecs/numeric/leaves.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_values.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

inline constexpr size_t kSampleFrames = 4;

inline constexpr double kLeafPenalty[] = {0, 0.05, 0.15};

static_assert(std::size(kLeafPenalty) == kNumericLeafCount);

inline double Price(ByteCodec leaf, uint64_t bytes) noexcept {
  return static_cast<double>(bytes) *
         (1.0 + kLeafPenalty[std::to_underlying(leaf)]);
}

struct NumericChoice {
  bool packed = false;
  bool narrowed = false;
  bool shuffled = false;
};

inline constexpr uint8_t kShuffledLayout = 1;
inline constexpr uint8_t kNarrowedLayout = 2;
inline constexpr uint8_t kPackedLayout = 4;

constexpr NumericChoice ChoiceOf(uint8_t layout) noexcept {
  return {
    .packed = (layout & kPackedLayout) != 0,
    .narrowed = (layout & kNarrowedLayout) != 0,
    .shuffled = (layout & kShuffledLayout) != 0,
  };
}

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

inline uint8_t LayoutOf(const Candidate& c) noexcept {
  return static_cast<uint8_t>((c.shuffled ? kShuffledLayout : 0) |
                              (c.narrowed ? kNarrowedLayout : 0) |
                              (c.packed ? kPackedLayout : 0));
}

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

template<typename T>
class NumericEncoder : public State {
 public:
  using U = numeric::Bits<sizeof(T)>;
  static constexpr uint8_t kWidth = sizeof(T);

  double EstimateLayout(double bound, ByteCodec leaf, uint8_t layout,
                        bool opens) {
    if (!Ready()) {
      return kNoScore;
    }
    if (opens) {
      _running_price = bound * RowCount();
    }
    const auto* c = Find(layout);
    return c && Fits(*c, leaf) && Improves(*c, leaf) ? Estimated() : kNoScore;
  }

  bool Encode(ColumnDataCheckpointData& data, ByteCodec leaf, uint8_t layout) {
    const LeafSamples* samples = nullptr;
    bool held = false;
    if (_best && _leaf == leaf && LayoutOf(*_best) == layout) {
      samples = &_best_samples;
    } else if (_complete) {
      _best = Find(layout);
      _leaf = leaf;
      if (!_best) {
        return false;
      }
    } else {
      held = true;
      if (!Hold(leaf, ChoiceOf(layout))) {
        return false;
      }
    }
    _out.clear();
    Write(*_best, _leaf, samples, _out);
    if (held && data.tuning.reuse.Grew(static_cast<double>(_out.size()),
                                       static_cast<double>(RawInput(data)))) {
      return false;
    }
    data.FlushSegment(CodecOf(_shape, _leaf), _values.Stats(data.type),
                      _values.Get().size(), _out);
    return true;
  }

 protected:
  NumericEncoder(ColumnDataCheckpointData& data, Shape shape)
    : _values{data.Get<Values<T>>()},
      _forced{data.method != CodecType::Auto},
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

  virtual void AddCandidates(const NumericChoice* only) = 0;

  virtual uint64_t PackedBlocks(const Candidate&) const noexcept = 0;

  virtual void AppendBlocks(const Candidate&, size_t first, size_t blocks,
                            std::string& out) = 0;

  virtual void StoreHeader(const Candidate& c, uint32_t off_dict,
                           duckdb::data_ptr_t own) const = 0;

  Values<T>& _values;
  bool _forced;

 private:
  struct Geometry {
    uint8_t log2;
    uint32_t per;
    uint32_t frames;
    uint64_t off_data;
  };

  Geometry GeometryOf(const Candidate& c, ByteCodec leaf) const noexcept {
    uint8_t log2 = kFforFrameLog2;
    uint32_t per = _shape == Shape::Runs ? kRleFforFrameRuns : kFforFrameRows;
    if (!c.packed) {
      log2 =
        leaf == ByteCodec::None ? kNumericFrameLog2 : kNumericLeafFrameLog2;
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

  bool Ready() {
    if (!_values.Usable()) {
      return false;
    }
    if (!_complete) {
      Rebuild(nullptr);
    }
    return true;
  }

  double RowCount() const noexcept {
    return static_cast<double>(_values.Get().size());
  }

  static bool Fits(const Candidate& c, ByteCodec leaf) noexcept {
    return leaf == ByteCodec::None ? !c.shuffled : !c.packed;
  }

  bool Improves(const Candidate& c, ByteCodec leaf) {
    const auto bytes = EstimateBytes(c, leaf, _running_price);
    const auto price = Price(leaf, bytes);
    if (price >= _running_price) {
      return false;
    }
    _running_price = price;
    _best = &c;
    _leaf = leaf;
    _estimated = bytes;
    if (leaf == ByteCodec::None) {
      _best_samples.frames.clear();
    } else {
      std::swap(_samples, _best_samples);
    }
    return true;
  }

  double Estimated() const noexcept {
    return static_cast<double>(_estimated) / RowCount() *
           (1.0 + kLeafPenalty[std::to_underlying(_leaf)]);
  }

  const Candidate* Find(uint8_t layout) const noexcept {
    const auto it = absl::c_find_if(
      _candidates, [&](const Candidate& c) { return LayoutOf(c) == layout; });
    return it == _candidates.end() ? nullptr : &*it;
  }

  bool Hold(ByteCodec leaf, const NumericChoice& choice) {
    if (!_values.Usable()) {
      return false;
    }
    Rebuild(&choice);
    const auto candidate = absl::c_find_if(_candidates, [&](const auto& c) {
      return c.shuffled == choice.shuffled;
    });
    if (candidate == _candidates.end()) {
      return false;
    }
    _best = &*candidate;
    _leaf = leaf;
    return true;
  }

  void Rebuild(const NumericChoice* only) {
    _complete = !only;
    _candidates.clear();
    _best = nullptr;
    AddCandidates(only);
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

  uint64_t EstimateBytes(const Candidate& c, ByteCodec leaf, double bound) {
    const auto g = GeometryOf(c, leaf);
    if (c.packed) {
      return g.off_data + PackedBlocks(c);
    }
    const uint64_t total = uint64_t{c.items} * c.ItemBytes();
    if (leaf == ByteCodec::None) {
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
      const auto n = _leaves.Compress(leaf, bytes.data(), bytes.size());
      comp += std::min<uint64_t>(n, bytes.size());
      _samples.frames.push_back(frame(k));
      if (n < bytes.size()) {
        _samples.bytes[k].assign(_leaves.Out(), n);
      } else {
        _samples.bytes[k].clear();
      }
      if (const auto least = g.off_data + total * comp / raw;
          Price(leaf, least) >= bound) {
        return least;
      }
    }
    return g.off_data + total * comp / raw;
  }

  void Write(const Candidate& c, ByteCodec leaf, const LeafSamples* samples,
             std::string& out) {
    const auto values = _values.Get();
    const auto [log2, per, frames, off_data] = GeometryOf(c, leaf);
    FrameHeader h;
    h.width = kWidth;
    h.stored = c.stored;
    h.level = leaf == ByteCodec::None ? uint8_t{0} : _leaves.Level();
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

    const bool direct = leaf == ByteCodec::None;
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
      bool compressible = leaf != ByteCodec::None;
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
        const auto n = _leaves.Compress(leaf, bytes.data(), bytes.size());
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
  ByteCodec _leaf = ByteCodec::None;
  uint64_t _estimated = 0;
  double _running_price = 0;
  LeafSamples _samples;
  LeafSamples _best_samples;
  ScratchVector<uint8_t> _frame;
  std::string _out;
};

template<typename Form, ByteCodec kLeaf, uint8_t kLayout, bool kOpens>
inline constexpr CompressionFunction kNumericLayout{
  .analyze =
    [](ColumnDataCheckpointData& data, double bound) {
      return data.Get<Form>().EstimateLayout(bound, kLeaf, kLayout, kOpens);
    },
  .compress =
    [](ColumnDataCheckpointData& data) {
      return data.Get<Form>().Encode(data, kLeaf, kLayout);
    },
};

}  // namespace irs::codecs
