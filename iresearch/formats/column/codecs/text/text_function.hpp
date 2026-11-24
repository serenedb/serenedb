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
#include <duckdb/common/helper.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <limits>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/codecs/compression_function.hpp"
#include "iresearch/formats/column/codecs/text/dedup_scratch.hpp"
#include "iresearch/formats/column/codecs/text/segment.hpp"
#include "iresearch/formats/column/codecs/text/segment_rows.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/text/values.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr double kOtherShapeWinsBelow = 0.9;
inline constexpr double kOtherShapeMinSaving = 4096;

inline double ShapeCost(Shape shape, bool repeats, double bytes) noexcept {
  return shape == (repeats ? Shape::Dictionary : Shape::Plain)
           ? bytes
           : std::max(bytes / kOtherShapeWinsBelow,
                      bytes + kOtherShapeMinSaving);
}

inline uint64_t PlainStorageBytes(const Segment& seg) noexcept {
  return kHeaderSize + seg.input + seg.rows * sizeof(int32_t);
}

inline Config NamedConfig(Shape shape, ByteCodec leaf,
                          const ColCodecParams& params) noexcept {
  return {
    .shape = shape,
    .leaf = leaf,
    .level = params.compression_level,
    .layout = params.tier == WriteTier::Flush ? FrameDictionary::FirstFrame
                                              : FrameDictionary::Local,
  };
}

struct Window final : State {
  static constexpr StateId kId = StateId::TextWindow;

  explicit Window(ColumnDataCheckpointData&) {}

  uint64_t rows = 0;
  uint64_t input = 0;
  uint64_t nulls = 0;
  uint32_t max_len = 0;
};

inline double Score(const Values& values, const Window& window, Shape shape,
                    uint64_t bytes) {
  return ShapeCost(shape, values.Repeats(), static_cast<double>(bytes)) /
         static_cast<double>(std::max<uint64_t>(window.input, 1));
}

inline bool Contends(const Values& values, const CodecSet& disabled,
                     Shape shape) noexcept {
  if (values.Rows() == 0) {
    return false;
  }
  return shape == Shape::Dictionary || !values.Repeats() ||
         (disabled.test(
            std::to_underlying(CodecOf(Shape::Dictionary, ByteCodec::Fsst))) &&
          disabled.test(
            std::to_underlying(CodecOf(Shape::Dictionary, ByteCodec::Lz4))));
}

template<typename Compression>
struct CodecState final : State {
  static constexpr StateId kId =
    TextState(Compression::kShape, Compression::kCodec);

  explicit CodecState(ColumnDataCheckpointData& data)
    : buffers{data.ctx->BufferManager()},
      type{data.type},
      builder{data, dedup},
      segment{buffers, type} {}

  duckdb::BufferManager& buffers;
  const duckdb::LogicalType& type;
  DedupScratch dedup;
  Compression builder;
  Segment segment;
  std::vector<std::unique_ptr<Segment>> trials;
};

template<typename Compression>
uint64_t SegmentEnd(uint64_t begin, uint64_t row) noexcept {
  if constexpr (Compression::kShape == Shape::Dictionary) {
    if (row - begin >= kVectorAlignedRows) {
      return duckdb::AlignValue<uint64_t, STANDARD_VECTOR_SIZE>(row);
    }
  }
  return row;
}

inline constexpr uint32_t kNoCut = std::numeric_limits<uint32_t>::max();

template<typename Compression>
IRS_NO_INLINE void CutRows(Compression& builder, const Config& config,
                           const Values& values, uint64_t begin, uint64_t end,
                           uint32_t target, Segment& seg) {
  const std::span<const uint32_t> codes = values.codes;
  const std::span<const duckdb::string_t* const> distinct = values.distinct;
  builder.InitCompression(config, begin, distinct.size(), seg);
  for (auto row = begin; row < end;) {
    builder.Compress(codes[row++], distinct);
    if (target != kNoCut && builder.Full(target)) {
      end = std::min(end, SegmentEnd<Compression>(begin, row));
      target = kNoCut;
    }
  }
  builder.FinalizeCompress();
}

inline void Flush(const Codec& codec, ColumnDataCheckpointData& data,
                  Segment& seg) {
  data.flush(codec, std::move(seg.stats), seg.rows, seg.bytes.View(),
             seg.shared);
}

template<typename Compression>
IRS_NO_INLINE void SealRows(Compression& builder, const Config& config,
                            const Codec& codec, ColumnDataCheckpointData& data,
                            Segment& seg) {
  const auto target = data.params.segment_target;
  const auto seal = [&] {
    builder.FinalizeCompress();
    Flush(codec, data, seg);
  };
  uint64_t row = 0;
  uint64_t first = 0;
  builder.InitCompression(config, first, 0, seg);
  for (const auto& chunk : data.chunks) {
    for (const auto entry : chunk.data.Values<duckdb::string_t>()) {
      builder.Compress(entry.IsValid() ? &entry.GetValueUnsafe() : nullptr);
      ++row;
      if (builder.Full(target)) {
        seal();
        first = row;
        builder.InitCompression(config, first, 0, seg);
      }
    }
  }
  if (row != first) {
    seal();
  }
}

template<typename Compression>
IRS_NO_INLINE void CompressRows(Compression& builder, const Config& config,
                                const Values& values, uint64_t begin,
                                Segment& seg, const Codec& codec,
                                ColumnDataCheckpointData& data) {
  const auto target = data.params.segment_target;
  for (auto row = begin; row < values.Rows(); row = seg.begin + seg.rows) {
    CutRows(builder, config, values, row, values.Rows(), target, seg);
    Flush(codec, data, seg);
  }
}

template<typename Compression>
Segment* Find(CodecState<Compression>& state, const Config& config) noexcept {
  for (const auto& trial : state.trials) {
    if (trial->config == config) {
      return trial.get();
    }
  }
  return nullptr;
}

template<typename Compression>
Segment& Trial(CodecState<Compression>& state, const Config& config,
               const Values& values, Window& window, uint32_t target) {
  if (auto* found = Find(state, config)) {
    return *found;
  }
  auto& seg = *state.trials.emplace_back(
    std::make_unique<Segment>(state.buffers, state.type));
  if (window.rows != 0) {
    CutRows(state.builder, config, values, 0, window.rows, kNoCut, seg);
  } else {
    CutRows(state.builder, config, values, 0, values.Rows(), target, seg);
    window.rows = seg.rows;
    window.input = seg.input;
    window.nulls = seg.nulls;
    window.max_len = seg.max_len;
  }
  return seg;
}

template<typename Compression>
inline constexpr Config kTrialConfig{
  .shape = Compression::kShape,
  .leaf = Compression::kCodec,
};

template<typename Compression>
double AnalyzeTrial(ColumnDataCheckpointData& data, double) {
  constexpr auto kShape = Compression::kShape;
  const auto& values = data.Get<Values>();
  if (!Contends(values, data.disabled, kShape)) {
    return kNoScore;
  }
  auto& state = data.Get<CodecState<Compression>>();
  auto& window = data.Get<Window>();
  const auto bytes = Trial(state, kTrialConfig<Compression>, values, window,
                           data.params.segment_target)
                       .Size();
  return Score(values, window, kShape, bytes);
}

template<typename Compression>
void CompressAs(const Config& config, const Codec& codec,
                ColumnDataCheckpointData& data) {
  auto& state = data.Get<CodecState<Compression>>();
  if constexpr (Compression::kShape == Shape::Plain) {
    SealRows(state.builder, config, codec, data, state.segment);
  } else {
    const auto& values = data.Get<Values>();
    uint64_t begin = 0;
    if (Find(state, config)) {
      auto& first = Trial(state, config, values, data.Get<Window>(),
                          data.params.segment_target);
      Flush(codec, data, first);
      begin = first.begin + first.rows;
    }
    CompressRows(state.builder, config, values, begin, state.segment, codec,
                 data);
  }
}

template<typename Compression>
bool CompressContested(const Config& config, const Codec& codec,
                       ColumnDataCheckpointData& data) {
  auto& state = data.Get<CodecState<Compression>>();
  const auto& values = data.Get<Values>();
  auto& window = data.Get<Window>();
  SDB_ASSERT(window.rows != 0);
  auto& first =
    Trial(state, config, values, window, data.params.segment_target);
  if (first.Size() > PlainStorageBytes(first)) {
    return false;
  }
  const auto rest = first.begin + first.rows;
  Flush(codec, data, first);
  CompressRows(state.builder, config, values, rest, first, codec, data);
  return true;
}

template<typename Compression>
bool CompressText(const Codec& codec, ColumnDataCheckpointData& data) {
  if (data.contested) {
    return CompressContested<Compression>(kTrialConfig<Compression>, codec,
                                          data);
  }
  CompressAs<Compression>(
    NamedConfig(Compression::kShape, Compression::kCodec, data.params), codec,
    data);
  return true;
}

template<typename Compression>
inline constexpr CompressionFunction kTextFunction{
  .analyze = AnalyzeTrial<Compression>,
  .compress = CompressText<Compression>,
};

}  // namespace irs::codecs::text
