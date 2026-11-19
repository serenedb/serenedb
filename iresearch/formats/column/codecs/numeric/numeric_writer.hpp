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

#include <algorithm>
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
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

inline constexpr size_t kSampleFrames = 4;

struct Geometry {
  uint8_t log2 = 0;
  uint32_t per = 0;
  uint32_t frames = 0;
  uint64_t off_data = 0;
};

inline Geometry BytesGeometry(Shape shape, ByteCodec leaf, uint32_t items,
                              uint32_t item_bytes, size_t dictionary) noexcept {
  const uint8_t log2 =
    leaf == ByteCodec::None ? kNumericFrameLog2 : kNumericLeafFrameLog2;
  const uint32_t per = (uint32_t{1} << log2) / item_bytes;
  const uint32_t frames = (items + per - 1) / per;
  return {
    .log2 = log2,
    .per = per,
    .frames = frames,
    .off_data =
      HeaderSize(shape) + uint64_t{frames} * kNumericFrameMetaSize + dictionary,
  };
}

inline Geometry PackedGeometry(Shape shape, uint32_t items,
                               size_t dictionary) noexcept {
  const uint32_t per =
    shape == Shape::Runs ? kRleFforFrameRuns : kFforFrameRows;
  const uint32_t frames = (items + per - 1) / per;
  return {
    .log2 = kFforFrameLog2,
    .per = per,
    .frames = frames,
    .off_data = duckdb::AlignValue<uint64_t, 8>(
      HeaderSize(shape) + uint64_t{frames} * kNumericFrameMetaSize +
      dictionary),
  };
}

inline uint32_t FrameTableEnd(Shape shape, const Geometry& geometry) noexcept {
  return HeaderSize(shape) +
         geometry.frames * static_cast<uint32_t>(kNumericFrameMetaSize);
}

inline double ScoreOf(uint64_t bytes, size_t rows, double price) noexcept {
  return static_cast<double>(bytes) / static_cast<double>(rows) * price;
}

struct LeafSamples {
  std::vector<uint32_t> frames;
  std::vector<std::string> bytes;
};

struct Analysis {
  LeafSamples samples;
  ByteCodec leaf = ByteCodec::None;
  ScratchVector<uint8_t> copies[kSampleFrames];
  uint32_t copied = 0;
  bool done = false;
};

struct NumericScratch final : State {
  static constexpr StateId kId = StateId::NumericScratch;

  explicit NumericScratch(ColumnDataCheckpointData&) {}

  LeafSamples samples;
  ScratchVector<uint8_t> frame;
  ScratchVector<uint8_t> shuffled;
  std::string out;
};

struct FrameStore {
  ByteCodec leaf = ByteCodec::None;
  bool shuffled = false;
  CompressLeafFn compress = nullptr;
};

template<typename Raw, typename Beaten>
uint64_t EstimateLeaf(const Geometry& geometry, uint32_t items,
                      uint32_t item_bytes, ColumnDataCheckpointData& data,
                      FrameStore store, LeafSamples& samples, Raw&& raw,
                      Beaten&& beaten) {
  const uint64_t total = uint64_t{items} * item_bytes;
  const auto count = std::min<uint32_t>(geometry.frames, kSampleFrames);
  const auto frame = [&](uint32_t k) {
    return static_cast<uint32_t>(uint64_t{k} * geometry.frames / count);
  };
  uint64_t sampled = 0;
  for (uint32_t k = 0; k < count; ++k) {
    sampled +=
      uint64_t{std::min(items - frame(k) * geometry.per, geometry.per)} *
      item_bytes;
  }
  SDB_ASSERT(sampled != 0);
  samples.frames.clear();
  samples.bytes.resize(count);
  uint64_t compressed = 0;
  for (uint32_t k = 0; k < count; ++k) {
    const std::span<const uint8_t> bytes = raw(k, frame(k));
    SDB_ASSERT(
      bytes.size() ==
      uint64_t{std::min(items - frame(k) * geometry.per, geometry.per)} *
        item_bytes);
    const auto stored = store.compress(data, bytes);
    compressed += std::min(stored.size(), bytes.size());
    samples.frames.push_back(frame(k));
    if (stored.size() < bytes.size()) {
      samples.bytes[k].assign(stored.data(), stored.size());
    } else {
      samples.bytes[k].clear();
    }
    if (const auto least = geometry.off_data + total * compressed / sampled;
        beaten(least)) {
      return least;
    }
  }
  return geometry.off_data + total * compressed / sampled;
}

template<typename T>
class SegmentWriter {
 public:
  using U = numeric::Bits<sizeof(T)>;

  SegmentWriter(Shape shape, const Geometry& geometry, uint64_t direct_bytes,
                std::span<const uint8_t> dictionary, std::string& out)
    : _shape{shape}, _geometry{geometry}, _out{out}, _metas(geometry.frames) {
    _out.clear();
    if (direct_bytes != 0) {
      _out.reserve(geometry.off_data + direct_bytes);
    }
    _out.resize(geometry.off_data);
    if (!dictionary.empty()) {
      std::memcpy(_out.data() + DictionaryOffset(), dictionary.data(),
                  dictionary.size());
    }
  }

  uint32_t DictionaryOffset() const noexcept {
    return FrameTableEnd(_shape, _geometry);
  }

  NumericFrameMeta& Open(uint32_t frame, uint32_t first_entry) noexcept {
    auto& m = _metas[frame];
    m.frame.first_entry = first_entry;
    m.frame.comp_off = static_cast<uint32_t>(_out.size() - _geometry.off_data);
    return m;
  }

  void Close(NumericFrameMeta& m) const noexcept {
    m.frame.raw_len = static_cast<uint32_t>(_out.size() - _geometry.off_data -
                                            m.frame.comp_off);
    m.frame.comp_len = m.frame.raw_len;
  }

  template<typename Raw>
  void Leaf(NumericFrameMeta& m, uint32_t frame, uint32_t raw_len,
            ColumnDataCheckpointData& data, FrameStore store,
            const LeafSamples* samples, Raw&& raw) {
    bool compressible = true;
    if (samples && _sample < samples->frames.size() &&
        samples->frames[_sample] == frame) {
      const auto& stored = samples->bytes[_sample++];
      if (!stored.empty()) {
        m.frame.raw_len = raw_len;
        _out.append(stored);
        m.frame.comp_len = static_cast<uint32_t>(stored.size());
        return;
      }
      compressible = false;
    }
    const std::span<const uint8_t> bytes = raw();
    m.frame.raw_len = static_cast<uint32_t>(bytes.size());
    if (compressible) {
      const auto stored = store.compress(data, bytes);
      if (stored.size() < bytes.size()) {
        _out.append(stored.data(), stored.size());
        m.frame.comp_len = static_cast<uint32_t>(stored.size());
        return;
      }
    }
    _out.append(reinterpret_cast<const char*>(bytes.data()), bytes.size());
    m.frame.comp_len = m.frame.raw_len;
  }

  template<typename Own>
  void Finish(FrameHeader h, const Own& own, const Values<T>& values) {
    const auto rows = values.Get();
    h.width = sizeof(T);
    h.frame_log2 = _geometry.log2;
    h.row_count = static_cast<uint32_t>(rows.size());
    h.frame_count = _geometry.frames;
    h.off_frames = HeaderSize(_shape);
    h.off_data = static_cast<uint32_t>(_geometry.off_data);
    h.scale = values.Scale() > 1 ? values.Scale() : 0;
    for (uint32_t f = 0; f < h.frame_count; ++f) {
      auto& m = _metas[f];
      const uint32_t end =
        f + 1 < h.frame_count ? _metas[f + 1].frame.first_entry : h.row_count;
      U lo = rows[m.frame.first_entry];
      U hi = lo;
      for (auto r = m.frame.first_entry + 1; r < end; ++r) {
        if (Values<T>::Less(rows[r], lo)) {
          lo = rows[r];
        }
        if (Values<T>::Less(hi, rows[r])) {
          hi = rows[r];
        }
      }
      m.min = lo;
      m.max = hi;
    }
    SDB_ENSURE(_out.size() <= std::numeric_limits<uint32_t>::max(),
               "numeric codec: segment too large");
    h.data_size = static_cast<uint32_t>(_out.size() - h.off_data);
    auto* head = reinterpret_cast<duckdb::data_ptr_t>(_out.data());
    duckdb::Store(h, head);
    duckdb::Store(own, head + sizeof(FrameHeader));
    for (uint32_t f = 0; f < h.frame_count; ++f) {
      duckdb::Store(_metas[f], head + h.off_frames + f * kNumericFrameMetaSize);
    }
  }

 private:
  Shape _shape;
  Geometry _geometry;
  std::string& _out;
  std::vector<NumericFrameMeta> _metas;
  size_t _sample = 0;
};

inline bool MayWrite(const ColumnDataCheckpointData& data, bool analyzed,
                     ByteCodec leaf) noexcept {
  return analyzed || leaf != ByteCodec::None || data.preferences->hold_leafless;
}

template<typename T>
bool FlushNumeric(ColumnDataCheckpointData& data, Shape shape, ByteCodec leaf,
                  bool analyzed) {
  const auto& values = data.Get<Values<T>>();
  const auto& out = data.Get<NumericScratch>().out;
  if (!analyzed && data.preferences->contest_grown &&
      data.tuning.reuse.Grew(static_cast<double>(out.size()),
                             static_cast<double>(RawInput(data)))) {
    return false;
  }
  data.FlushSegment(CodecOf(shape, leaf), values.Stats(data.type),
                    values.Get().size(), out);
  return true;
}

struct Items {
  Shape shape = Shape::Plain;
  FrameStore store;
  uint8_t stored = 0;
  uint8_t run_width = 0;
  uint32_t count = 0;
  std::span<const uint8_t> dictionary;
};

inline bool Offered(const Items& items) noexcept {
  return items.stored != 0 &&
         !(items.store.shuffled && (items.stored == 1 || items.run_width != 0));
}

inline Geometry GeometryOf(const Items& items) noexcept {
  return BytesGeometry(items.shape, items.store.leaf, items.count,
                       uint32_t{items.stored} + items.run_width,
                       items.dictionary.size());
}

template<typename Bytes>
auto Slices(uint8_t stored, Bytes&& bytes) {
  return [stored, bytes = std::forward<Bytes>(bytes)](
           uint32_t begin, uint32_t count,
           ScratchVector<uint8_t>&) -> std::span<const uint8_t> {
    return {bytes() + size_t{begin} * stored, size_t{count} * stored};
  };
}

struct SameFrame {
  void operator()(NumericFrameMeta&, uint32_t) const noexcept {}
};

template<typename T, typename Frame>
double AnalyzeFrames(ColumnDataCheckpointData& data, const Items& items,
                     Analysis& analysis, double bound, Frame&& frame) {
  if (!Offered(items)) {
    return kNoScore;
  }
  analysis.done = true;
  const auto leaf = items.store.leaf;
  const auto rows = data.Get<Values<T>>().Get().size();
  const uint32_t item = uint32_t{items.stored} + items.run_width;
  const auto geometry = GeometryOf(items);
  const auto price = data.preferences->leaf_price[std::to_underlying(leaf)];
  if (leaf == ByteCodec::None) {
    const auto score =
      ScoreOf(geometry.off_data + uint64_t{items.count} * item, rows, price);
    return score < bound ? score : kNoScore;
  }
  auto& samples = data.Get<NumericScratch>().samples;
  const auto size = EstimateLeaf(
    geometry, items.count, item, data, items.store, samples,
    [&](uint32_t sample, uint32_t f) -> std::span<const uint8_t> {
      auto& copy = analysis.copies[sample];
      if (sample < analysis.copied) {
        return copy;
      }
      const auto begin = f * geometry.per;
      const auto count = std::min(items.count - begin, geometry.per);
      const std::span<const uint8_t> raw = frame(begin, count, copy);
      if (items.store.shuffled) {
        SDB_ASSERT(raw.data() != copy.data());
        copy.resize(raw.size());
        numeric::Shuffle(raw.data(), count, items.stored, copy.data());
      } else if (raw.data() != copy.data()) {
        return raw;
      }
      SDB_ASSERT(sample == analysis.copied);
      analysis.copied = sample + 1;
      return copy;
    },
    [&](uint64_t least) { return ScoreOf(least, rows, price) >= bound; });
  const auto score = ScoreOf(size, rows, price);
  if (!(score < bound)) {
    return kNoScore;
  }
  std::swap(samples, analysis.samples);
  analysis.leaf = leaf;
  return score;
}

template<typename T, typename Own, typename Frame, typename Start = SameFrame>
bool CompressFrames(ColumnDataCheckpointData& data, const Items& items,
                    const Analysis& analysis, const Own& own, Frame&& frame,
                    Start&& start = {}) {
  const auto leaf = items.store.leaf;
  if (!Offered(items) || !MayWrite(data, analysis.done, leaf)) {
    return false;
  }
  auto& scratch = data.Get<NumericScratch>();
  const uint32_t item = uint32_t{items.stored} + items.run_width;
  const auto geometry = GeometryOf(items);
  const uint64_t raw_bytes = uint64_t{items.count} * item;
  SegmentWriter<T> writer{items.shape, geometry,
                          leaf == ByteCodec::None ? raw_bytes : 0,
                          items.dictionary, scratch.out};
  const auto* samples = leaf != ByteCodec::None && analysis.leaf == leaf
                          ? &analysis.samples
                          : nullptr;
  for (uint32_t f = 0; f < geometry.frames; ++f) {
    const auto begin = f * geometry.per;
    const auto count = std::min(items.count - begin, geometry.per);
    auto& m = writer.Open(f, begin);
    start(m, begin);
    if (leaf == ByteCodec::None) {
      const std::span<const uint8_t> raw = frame(begin, count, scratch.frame);
      scratch.out.append(reinterpret_cast<const char*>(raw.data()), raw.size());
      writer.Close(m);
      continue;
    }
    writer.Leaf(m, f, count * item, data, items.store, samples,
                [&]() -> std::span<const uint8_t> {
                  const std::span<const uint8_t> raw =
                    frame(begin, count, scratch.frame);
                  if (!items.store.shuffled) {
                    return raw;
                  }
                  scratch.shuffled.resize(raw.size());
                  numeric::Shuffle(raw.data(), count, items.stored,
                                   scratch.shuffled.data());
                  return scratch.shuffled;
                });
  }
  writer.Finish(
    {
      .stored = items.stored,
      .level = leaf == ByteCodec::None ? uint8_t{0} : LeafLevel(data),
      .flags = items.store.shuffled ? kNumericShuffled : uint8_t{0},
      .raw_bytes = raw_bytes,
    },
    own, data.Get<Values<T>>());
  return FlushNumeric<T>(data, items.shape, leaf, analysis.done);
}

template<auto kAnalyze, auto kCompress, ByteCodec kLeaf, bool kShuffled>
inline constexpr CompressionFunction kStoredFrames{
  .analyze =
    [](ColumnDataCheckpointData& data, double bound) {
      return kAnalyze(data, bound,
                      FrameStore{kLeaf, kShuffled, kCompressLeaf<kLeaf>});
    },
  .compress =
    [](ColumnDataCheckpointData& data) {
      return kCompress(data,
                       FrameStore{kLeaf, kShuffled, kCompressLeaf<kLeaf>});
    },
};

template<typename T>
double AnalyzeBlocks(ColumnDataCheckpointData& data, Shape shape,
                     uint32_t items, uint64_t packed, size_t dictionary,
                     double bound) {
  const auto score =
    ScoreOf(PackedGeometry(shape, items, dictionary).off_data + packed,
            data.Get<Values<T>>().Get().size(),
            data.preferences->leaf_price[std::to_underlying(ByteCodec::None)]);
  return score < bound ? score : kNoScore;
}

template<typename T, typename V, typename Own>
bool CompressBlocks(ColumnDataCheckpointData& data, Shape shape,
                    FforBlocks<V>& blocks, std::span<const V> values,
                    std::span<const uint8_t> dictionary, bool analyzed,
                    const Own& own) {
  auto& out = data.Get<NumericScratch>().out;
  const auto n = static_cast<uint32_t>(values.size());
  const auto geometry = PackedGeometry(shape, n, dictionary.size());
  SegmentWriter<T> writer{shape, geometry, blocks.Bytes(geometry.per),
                          dictionary, out};
  for (uint32_t f = 0; f < geometry.frames; ++f) {
    const auto begin = f * geometry.per;
    const auto count = std::min(n - begin, geometry.per);
    auto& m = writer.Open(f, begin);
    blocks.Append(
      values, begin / numeric::kBlockValues,
      (size_t{count} + numeric::kBlockValues - 1) / numeric::kBlockValues, out);
    writer.Close(m);
  }
  writer.Finish(
    {
      .stored = sizeof(T),
      .flags = kNumericPacked,
      .raw_bytes = uint64_t{n} * sizeof(T),
    },
    own, data.Get<Values<T>>());
  return FlushNumeric<T>(data, shape, ByteCodec::None, analyzed);
}

}  // namespace irs::codecs
