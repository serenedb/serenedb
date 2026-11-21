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

#include <absl/numeric/int128.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/helper.hpp>
#include <limits>
#include <span>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/numeric/bitpacking_blocks.hpp"
#include "iresearch/formats/column/codecs/numeric/leaves.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_kernels.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_layout.hpp"
#include "iresearch/formats/column/codecs/numeric/numeric_values.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/default_init_allocator.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {

inline constexpr size_t kSampleFrames = 4;
inline constexpr uint32_t kSampleOrder[kSampleFrames] = {0, 2, 1, 3};
inline constexpr double kLeafPrice[kNumericLeafCount] = {1.0, 1.05, 1.15};
inline constexpr double kDeltaBlocksPrice = 1.05;
inline constexpr double kUnlikelyMargin = 0.5;
inline constexpr uint32_t kLeastFramesToStop = 2;

struct Geometry {
  uint8_t log2 = 0;
  uint32_t per = 0;
  uint32_t frames = 0;
  uint32_t meta = kNumericFrameMetaSize;
  uint32_t off_frames = 0;
  uint64_t off_data = 0;
};

constexpr uint32_t FramesOffset(Shape shape, bool packed,
                                bool scaled) noexcept {
  return OwnHeaderOffset(scaled ? kNumericScaled : uint8_t{0}) +
         OwnHeaderSize(shape, packed);
}

inline Geometry BytesGeometry(Shape shape, ByteCodec leaf, uint32_t items,
                              uint32_t item_bytes, size_t dictionary,
                              bool scaled) noexcept {
  const uint8_t log2 =
    leaf == ByteCodec::None ? kNumericFrameLog2 : kNumericLeafFrameLog2;
  const uint32_t per = (uint32_t{1} << log2) / item_bytes;
  const uint32_t frames = (items + per - 1) / per;
  const auto off_frames = FramesOffset(shape, false, scaled);
  return {
    .log2 = log2,
    .per = per,
    .frames = frames,
    .off_frames = off_frames,
    .off_data =
      off_frames + uint64_t{frames} * kNumericFrameMetaSize + dictionary,
  };
}

inline Geometry PackedGeometry(Shape shape, uint32_t items, size_t dictionary,
                               size_t width, bool scaled) noexcept {
  const uint32_t per =
    shape == Shape::Runs ? kRleBitpackingFrameRuns : kBitpackingFrameRows;
  const uint32_t frames = (items + per - 1) / per;
  const auto meta = static_cast<uint32_t>(FrameMetaSize(shape, true, width));
  const auto off_frames = FramesOffset(shape, true, scaled);
  return {
    .log2 = kBitpackingFrameLog2,
    .per = per,
    .frames = frames,
    .meta = meta,
    .off_frames = off_frames,
    .off_data = duckdb::AlignValue<uint64_t, 8>(
      off_frames + uint64_t{frames} * meta + dictionary),
  };
}

inline bool Packed(const Geometry& geometry) noexcept {
  return geometry.meta != kNumericFrameMetaSize;
}

inline uint32_t FrameTableEnd(const Geometry& geometry) noexcept {
  return geometry.off_frames + geometry.frames * geometry.meta;
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
};

template<typename Raw, typename Unlikely>
uint64_t EstimateLeaf(const Geometry& geometry, uint32_t items,
                      uint32_t item_bytes, ColumnDataCheckpointData& data,
                      FrameStore store, LeafSamples& samples, Raw&& raw,
                      Unlikely&& unlikely) {
  const uint64_t total = uint64_t{items} * item_bytes;
  const auto count = std::min<uint32_t>(geometry.frames, kSampleFrames);
  samples.frames.resize(count);
  samples.bytes.resize(count);
  for (uint32_t k = 0; k < count; ++k) {
    samples.frames[k] =
      static_cast<uint32_t>(uint64_t{k} * geometry.frames / count);
  }
  uint64_t compressed = 0;
  uint64_t read = 0;
  uint64_t projected = 0;
  uint32_t done = 0;
  for (const auto k : kSampleOrder) {
    if (k >= count) {
      continue;
    }
    const auto frame = samples.frames[k];
    const std::span<const uint8_t> bytes = raw(k, frame);
    SDB_ASSERT(bytes.size() ==
               uint64_t{std::min(items - frame * geometry.per, geometry.per)} *
                 item_bytes);
    const auto stored = CompressLeaf(store.leaf, data, bytes);
    compressed += std::min(stored.size(), bytes.size());
    read += bytes.size();
    if (stored.size() < bytes.size()) {
      samples.bytes[k].assign(stored.data(), stored.size());
    } else {
      samples.bytes[k].clear();
    }
    projected = geometry.off_data + total * compressed / read;
    if (++done < count && unlikely(projected, done)) {
      break;
    }
  }
  return projected;
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
    return FrameTableEnd(_geometry);
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
      const auto stored = CompressLeaf(store.leaf, data, bytes);
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
    Finish(h, own, values, [&](uint32_t, uint32_t first, uint32_t end) {
      return values.Bounds(first, end);
    });
  }

  template<typename Own, typename FrameBounds>
  void Finish(FrameHeader h, const Own& own, const Values<T>& values,
              FrameBounds&& frame_bounds) {
    h.width = sizeof(T);
    h.frame_log2 = _geometry.log2;
    h.row_count = static_cast<uint32_t>(values.Size());
    h.frame_count = _geometry.frames;
    h.off_frames = _geometry.off_frames;
    h.off_data = static_cast<uint32_t>(_geometry.off_data);
    if (values.Scale() > 1) {
      h.scale = values.Scale();
      h.flags |= kNumericScaled;
    }
    const auto own_offset = OwnHeaderOffset(h.flags);
    SDB_ASSERT(h.off_frames ==
               own_offset + OwnHeaderSize(_shape, Packed(_geometry)));
    for (uint32_t f = 0; f < h.frame_count; ++f) {
      auto& m = _metas[f];
      const uint32_t end =
        f + 1 < h.frame_count ? _metas[f + 1].frame.first_entry : h.row_count;
      const auto bounds = frame_bounds(f, m.frame.first_entry, end);
      m.min = bounds.lo;
      m.max = bounds.hi;
    }
    SDB_ENSURE(_out.size() <= std::numeric_limits<uint32_t>::max(),
               "numeric codec: segment too large");
    h.data_size = static_cast<uint32_t>(_out.size() - h.off_data);
    auto* head = reinterpret_cast<duckdb::data_ptr_t>(_out.data());
    std::memcpy(head, &h, kNumericHeaderSize);
    if (h.scale != 0) {
      duckdb::Store(h.scale, head + kNumericHeaderSize);
    }
    if (h.off_frames != own_offset) {
      duckdb::Store(own, head + own_offset);
    }
    for (uint32_t f = 0; f < h.frame_count; ++f) {
      const auto& m = _metas[f];
      auto* at = head + h.off_frames + f * _geometry.meta;
      if (!Packed(_geometry)) {
        duckdb::Store(m, at);
        continue;
      }
      duckdb::Store(m.frame.comp_off, at);
      at += sizeof(uint32_t);
      if (_shape == Shape::Runs) {
        duckdb::Store(m.frame.first_entry, at);
        duckdb::Store(static_cast<uint16_t>(m.base), at + sizeof(uint32_t));
        at += sizeof(uint32_t) + sizeof(uint16_t);
      } else {
        duckdb::Store(static_cast<U>(m.base), at);
        at += sizeof(U);
      }
      duckdb::Store(static_cast<U>(m.min), at);
      duckdb::Store(static_cast<U>(m.max), at + sizeof(U));
    }
  }

 private:
  Shape _shape;
  Geometry _geometry;
  std::string& _out;
  std::vector<NumericFrameMeta> _metas;
  size_t _sample = 0;
};

template<typename T>
void FlushNumeric(const Codec& codec, ColumnDataCheckpointData& data) {
  const auto& values = data.Get<Values<T>>();
  data.flush(codec, values.Stats(data.type), values.Size(),
             data.Get<NumericScratch>().out, {});
}

template<typename T>
struct NumericFacts final : State {
  static constexpr StateId kId = StateId::NumericFacts;

  explicit NumericFacts(ColumnDataCheckpointData& data) {
    const auto& values = data.Get<Values<T>>();
    if (!values.Usable()) {
      return;
    }
    uint64_t bits = 0;
    const auto bounds = values.Bounds();
    for (const auto& block : bounds) {
      bits += std::bit_width(
        static_cast<numeric::Bits<sizeof(T)>>(block.hi - block.lo));
    }
    facts = {
      static_cast<float>(bits) / static_cast<float>(bounds.size()),
      static_cast<float>(values.RunCount()) / static_cast<float>(values.Size()),
      static_cast<float>(values.GetReference().stored),
    };
  }

  Facts facts{};
};

template<typename T>
const Facts& FactsOf(ColumnDataCheckpointData& data) {
  return data.Get<NumericFacts<T>>().facts;
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

inline Geometry GeometryOf(const Items& items, bool scaled) noexcept {
  return BytesGeometry(items.shape, items.store.leaf, items.count,
                       uint32_t{items.stored} + items.run_width,
                       items.dictionary.size(), scaled);
}

template<typename T>
bool Scaled(ColumnDataCheckpointData& data) {
  return data.Get<Values<T>>().Scale() > 1;
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
  const auto leaf = items.store.leaf;
  const auto rows = data.Get<Values<T>>().Size();
  const uint32_t item = uint32_t{items.stored} + items.run_width;
  const auto geometry = GeometryOf(items, Scaled<T>(data));
  const auto price =
    data.contested ? kLeafPrice[std::to_underlying(leaf)] : 1.0;
  const uint64_t stream = uint64_t{items.count} * item;
  if (leaf == ByteCodec::None) {
    return ScoreOf(geometry.off_data + stream, rows, price);
  }
  auto& samples = data.Get<NumericScratch>().samples;
  const auto size = EstimateLeaf(
    geometry, items.count, item, data, items.store, samples,
    [&](uint32_t sample, uint32_t f) -> std::span<const uint8_t> {
      auto& copy = analysis.copies[sample];
      if ((analysis.copied >> sample & 1) != 0) {
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
      analysis.copied |= 1U << sample;
      return copy;
    },
    [&](uint64_t projected, uint32_t frames) {
      return frames >= kLeastFramesToStop &&
             ScoreOf(projected, rows, price) >=
               bound * (1.0 + kUnlikelyMargin / frames);
    });
  const auto score = ScoreOf(size, rows, price);
  if (score < bound) {
    std::swap(samples, analysis.samples);
    analysis.leaf = leaf;
  }
  return score;
}

template<typename T, typename Own, typename Frame, typename Start = SameFrame>
bool CompressFrames(const Codec& codec, ColumnDataCheckpointData& data,
                    const Items& items, const Analysis& analysis,
                    const Own& own, Frame&& frame, Start&& start = {}) {
  const auto leaf = items.store.leaf;
  if (!Offered(items)) {
    return false;
  }
  auto& scratch = data.Get<NumericScratch>();
  const uint32_t item = uint32_t{items.stored} + items.run_width;
  const auto geometry = GeometryOf(items, Scaled<T>(data));
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
    },
    own, data.Get<Values<T>>());
  FlushNumeric<T>(codec, data);
  return true;
}

template<auto kAnalyze, auto kCompress, ByteCodec kLeaf, bool kShuffled>
inline constexpr CompressionFunction kStoredFrames{
  .analyze =
    [](ColumnDataCheckpointData& data, double bound) {
      return kAnalyze(data, bound, FrameStore{kLeaf, kShuffled});
    },
  .compress =
    [](const Codec& codec, ColumnDataCheckpointData& data) {
      return kCompress(codec, data, FrameStore{kLeaf, kShuffled});
    },
};

template<typename T>
double AnalyzeBlocks(ColumnDataCheckpointData& data, Shape shape,
                     uint32_t items, uint64_t packed, size_t dictionary) {
  return ScoreOf(
    PackedGeometry(shape, items, dictionary, sizeof(T), Scaled<T>(data))
        .off_data +
      packed,
    data.Get<Values<T>>().Size(), 1.0);
}

template<typename T, typename V, typename Own>
void CompressBlocks(const Codec& codec, ColumnDataCheckpointData& data,
                    Shape shape, BitpackingBlocks<V>& blocks,
                    std::span<const V> values,
                    std::span<const uint8_t> dictionary, const Own& own) {
  auto& out = data.Get<NumericScratch>().out;
  const auto n = static_cast<uint32_t>(values.size());
  const auto geometry =
    PackedGeometry(shape, n, dictionary.size(), sizeof(T), Scaled<T>(data));
  const BlockBases<V> stored{
    .bytes =
      numeric::BytesFor(static_cast<V>(dictionary.size() / sizeof(T) - 1)),
  };
  SegmentWriter<T> writer{
    shape, geometry, blocks.Bytes(geometry.per, stored.bytes), dictionary, out};
  for (uint32_t f = 0; f < geometry.frames; ++f) {
    const auto begin = f * geometry.per;
    const auto count = std::min(n - begin, geometry.per);
    auto& m = writer.Open(f, begin);
    blocks.Append(
      BlockRows<V>{values}, begin / numeric::kBlockValues,
      (size_t{count} + numeric::kBlockValues - 1) / numeric::kBlockValues,
      stored, out);
    writer.Close(m);
  }
  writer.Finish(
    {
      .stored = sizeof(T),
      .flags = kNumericPacked,
    },
    own, data.Get<Values<T>>());
  FlushNumeric<T>(codec, data);
}

template<typename T>
bool ArithmeticFrame(const BitpackingBlocks<numeric::Bits<sizeof(T)>>& blocks,
                     const Values<T>& values, uint32_t begin, uint32_t count) {
  using U = numeric::Bits<sizeof(T)>;
  const size_t first = begin / numeric::kBlockValues;
  if (!blocks.EqualSteps(
        first, (count + numeric::kBlockValues - 1) / numeric::kBlockValues)) {
    return false;
  }
  if (values.Sequence()) {
    return true;
  }
  const auto rows = values.Rows();
  const auto wide = [](U v) {
    return std::is_signed_v<T>
             ? absl::int128{static_cast<std::make_signed_t<U>>(v)}
             : absl::int128{v};
  };
  const absl::int128 step{
    static_cast<std::make_signed_t<U>>(blocks.FrameStep(first))};
  return wide(rows[begin + count - 1]) - wide(rows[begin]) ==
         step * (count - 1);
}

template<typename T>
BlockBases<numeric::Bits<sizeof(T)>> FrameBases(const Values<T>& values,
                                                uint32_t begin,
                                                uint32_t count) {
  using U = numeric::Bits<sizeof(T)>;
  const auto bounds = values.Bounds(begin, begin + count);
  return {
    .origin = bounds.lo,
    .bytes = numeric::BytesFor(static_cast<U>(bounds.hi - bounds.lo)),
  };
}

template<typename T>
uint64_t SteppedBytes(const BitpackingBlocks<numeric::Bits<sizeof(T)>>& blocks,
                      const Values<T>& values) {
  uint64_t bytes = 0;
  const auto n = static_cast<uint32_t>(values.Size());
  for (uint32_t begin = 0; begin < n; begin += kBitpackingFrameRows) {
    const auto count = std::min(n - begin, kBitpackingFrameRows);
    if (!ArithmeticFrame<T>(blocks, values, begin, count)) {
      bytes += blocks.Bytes(
        begin / numeric::kBlockValues,
        (count + numeric::kBlockValues - 1) / numeric::kBlockValues,
        FrameBases(values, begin, count).bytes);
    }
  }
  return bytes;
}

template<typename T, typename Own>
void CompressStepped(const Codec& codec, ColumnDataCheckpointData& data,
                     Shape shape,
                     BitpackingBlocks<numeric::Bits<sizeof(T)>>& blocks,
                     const Own& own) {
  const auto& values = data.Get<Values<T>>();
  auto& out = data.Get<NumericScratch>().out;
  const auto n = static_cast<uint32_t>(values.Size());
  const auto geometry = PackedGeometry(shape, n, 0, sizeof(T), Scaled<T>(data));
  SegmentWriter<T> writer{
    shape, geometry, SteppedBytes<T>(blocks, values), {}, out};
  for (uint32_t f = 0; f < geometry.frames; ++f) {
    const auto begin = f * geometry.per;
    const auto count = std::min(n - begin, geometry.per);
    const size_t first = begin / numeric::kBlockValues;
    auto& m = writer.Open(f, begin);
    m.base = blocks.FrameStep(first);
    if (!ArithmeticFrame<T>(blocks, values, begin, count)) {
      blocks.Append(
        values.Rows(), first,
        (size_t{count} + numeric::kBlockValues - 1) / numeric::kBlockValues,
        FrameBases(values, begin, count), out);
    }
    writer.Close(m);
  }
  writer.Finish(
    {
      .stored = sizeof(T),
      .flags = kNumericPacked,
    },
    own, values);
  FlushNumeric<T>(codec, data);
}

}  // namespace irs::codecs
