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
#include <duckdb/common/vector/vector_iterator.hpp>
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
inline constexpr double kLeastTextShift = 1.5;
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

struct Contest final : State {
  static constexpr StateId kId = StateId::TextContest;

  struct Window {
    uint64_t rows = 0;
    uint64_t input = 0;
    uint64_t nulls = 0;
    uint32_t max_len = 0;
  };

  explicit Contest(ColumnDataCheckpointData&) {}

  Window trial;
  uint64_t built[kTextShapeCount] = {};
  bool shapes_measured = false;
  bool contends[kTextShapeCount] = {};
};

inline double Score(const Values& values, const Contest& contest, Shape shape,
                    double price, uint64_t bytes) {
  return ShapeCost(shape, values.Repeats(), static_cast<double>(bytes)) *
         price /
         static_cast<double>(std::max<uint64_t>(contest.trial.input, 1));
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
  Config config;
};

template<typename Compression>
bool EndsSegment(uint64_t begin, uint64_t end) noexcept {
  if constexpr (Compression::kShape == Shape::Dictionary) {
    return end % STANDARD_VECTOR_SIZE == 0 || end - begin < kVectorAlignedRows;
  } else {
    return true;
  }
}

template<typename Compression>
IRS_NO_INLINE void CutRows(Compression& builder, const Config& config,
                           const Values& values, uint64_t begin,
                           uint32_t target, Segment& seg) {
  const std::span<const uint32_t> codes = values.codes;
  const std::span<const duckdb::string_t* const> distinct = values.distinct;
  builder.InitCompression(config, begin, distinct.size(), seg);
  bool full = false;
  for (auto row = begin; row < codes.size();) {
    builder.Compress(codes[row++], distinct);
    full = full || builder.Full(target);
    if (full && EndsSegment<Compression>(begin, row)) {
      break;
    }
  }
  builder.FinalizeCompress();
}

template<typename Compression>
IRS_NO_INLINE void CompressRange(Compression& builder, const Config& config,
                                 const Values& values, uint64_t begin,
                                 uint64_t end, Segment& seg) {
  const std::span<const uint32_t> codes = values.codes;
  const std::span<const duckdb::string_t* const> distinct = values.distinct;
  builder.InitCompression(config, begin, distinct.size(), seg);
  for (auto row = begin; row < end; ++row) {
    builder.Compress(codes[row], distinct);
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
    CutRows(builder, config, values, row, target, seg);
    Flush(codec, data, seg);
  }
}

template<typename Compression>
const Segment* Find(const CodecState<Compression>& state,
                    const Config& config) noexcept {
  for (const auto& trial : state.trials) {
    if (trial->config == config) {
      return trial.get();
    }
  }
  return nullptr;
}

template<typename Compression>
Segment& Trial(CodecState<Compression>& state, const Config& config,
               const Values& values, Contest& contest, uint32_t target) {
  for (const auto& trial : state.trials) {
    if (trial->config == config) {
      return *trial;
    }
  }
  auto& seg = *state.trials.emplace_back(
    std::make_unique<Segment>(state.buffers, state.type));
  if (contest.trial.rows != 0) {
    CompressRange(state.builder, config, values, 0, contest.trial.rows, seg);
  } else {
    CutRows(state.builder, config, values, 0, target, seg);
    contest.trial = {
      .rows = seg.rows,
      .input = seg.input,
      .nulls = seg.nulls,
      .max_len = seg.max_len,
    };
  }
  auto& built = contest.built[std::to_underlying(Compression::kShape)];
  built = built == 0 ? seg.Size() : std::min<uint64_t>(built, seg.Size());
  return seg;
}

bool Contends(ColumnDataCheckpointData& data, Shape shape);

template<typename Compression>
bool Contests(CodecState<Compression>& state, ColumnDataCheckpointData& data,
              bool moved) {
  constexpr auto kShape = Compression::kShape;
  constexpr auto kLeaf = Compression::kCodec;
  const auto& values = data.Get<Values>();
  if (values.Rows() == 0) {
    return false;
  }
  auto& contest = data.Get<Contest>();
  state.config = {.shape = kShape, .leaf = kLeaf};
  const auto& tuned = data.tuning.strings.config;
  if (contest.trial.rows == 0 && tuned && tuned->shape == kShape &&
      tuned->leaf == kLeaf) {
    Trial(state, *tuned, values, contest, data.params.segment_target);
  }
  return (!moved && contest.trial.rows != 0) || Contends(data, kShape);
}

template<typename Compression, typename Measure>
double AnalyzeHinted(ColumnDataCheckpointData& data, double bound,
                     Measure&& measure) {
  const auto& values = data.Get<Values>();
  const auto codec = CodecOf(Compression::kShape, Compression::kCodec);
  return Hinted(data.tuning.hints[std::to_underlying(codec)], bound,
                values.Statistics(), kLeastTextShift, [&](bool moved) {
                  return measure(data.Get<CodecState<Compression>>(), values,
                                 moved);
                });
}

template<typename Compression>
double AnalyzeTrial(ColumnDataCheckpointData& data, double bound) {
  return AnalyzeHinted<Compression>(
    data, bound,
    [&](CodecState<Compression>& state, const Values& values, bool moved) {
      if (!Contests(state, data, moved)) {
        return kNoScore;
      }
      auto& contest = data.Get<Contest>();
      const auto bytes =
        Trial(state, state.config, values, contest, data.params.segment_target)
          .Size();
      return Score(values, contest, Compression::kShape, 1.0, bytes);
    });
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
      auto& first = Trial(state, config, values, data.Get<Contest>(),
                          data.params.segment_target);
      Flush(codec, data, first);
      begin = first.begin + first.rows;
    }
    CompressRows(state.builder, config, values, begin, state.segment, codec,
                 data);
  }
}

template<typename Compression>
bool CompressContested(const Codec& codec, ColumnDataCheckpointData& data) {
  auto& state = data.Get<CodecState<Compression>>();
  const auto& values = data.Get<Values>();
  auto& contest = data.Get<Contest>();
  SDB_ASSERT(contest.trial.rows != 0);
  data.tuning.strings.config = state.config;
  auto& first =
    Trial(state, state.config, values, contest, data.params.segment_target);
  if (first.Size() > PlainStorageBytes(first)) {
    return false;
  }
  const auto rest = first.begin + first.rows;
  Flush(codec, data, first);
  CompressRows(state.builder, state.config, values, rest, first, codec, data);
  return true;
}

template<typename Compression>
bool CompressText(const Codec& codec, ColumnDataCheckpointData& data) {
  if (data.contested) {
    return CompressContested<Compression>(codec, data);
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
