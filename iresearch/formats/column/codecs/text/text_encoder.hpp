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
#include "iresearch/formats/column/codecs/text/shape.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/formats/column/codecs/text/string_tuning.hpp"
#include "iresearch/formats/column/codecs/text/values.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/shared.hpp"

namespace irs::codecs::text {

inline constexpr double kContestMove = 2.0;

inline double Score(Values& values, double price, uint64_t bytes) {
  return static_cast<double>(bytes) * price /
         static_cast<double>(std::max<uint64_t>(values.trial.input, 1));
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

template<typename Codec>
class TextEncoder final : public State {
 public:
  static constexpr StateId kId = TextState(Codec::kCodec);
  static constexpr ByteCodec kLeaf = Codec::kCodec;

  explicit TextEncoder(ColumnDataCheckpointData& data)
    : _params{data.params},
      _tuning{data.tuning.strings},
      _buffers{data.ctx->BufferManager()},
      _codec{Make(data.type, _dedup, _tuning, data.params.segment_target)},
      _segment{_buffers} {}

  double Analyze(ColumnDataCheckpointData& data, double bound, Shape shape) {
    auto& values = data.Get<Values>();
    if (values.Rows() == 0) {
      return kNoScore;
    }
    const auto tier = _params.tier;
    auto& config = _config[std::to_underlying(shape)];
    config = {.shape = shape, .leaf = kLeaf};
    if (values.trial.rows == 0 && _tuning.config &&
        _tuning.config->shape == shape && _tuning.config->leaf == kLeaf) {
      Trial(*_tuning.config, values);
    }
    if (ContestedShape(data) != shape) {
      return kNoScore;
    }
    const auto price =
      kLeaf == ByteCodec::Lz4
        ? data.preferences->text_lz4_price[std::to_underlying(shape)]
        : 1.0;
    const auto score = [&](uint64_t bytes) {
      return Score(values, price, bytes);
    };
    uint64_t bytes = 0;
    if constexpr (requires {
                    _codec.Measure(*this, values, shape, tier, bound, score);
                  }) {
      const auto measured =
        _codec.Measure(*this, values, shape, tier, bound, score);
      config = measured.config;
      bytes = measured.bytes;
    } else {
      bytes = Trial(config, values).Size();
    }
    return score(bytes);
  }

  double Analyze(ColumnDataCheckpointData& data, double bound, Shape shape,
                 FrameDictionary frames) {
    auto& measured = _measured[std::to_underlying(shape)];
    if (!measured.done) {
      measured = {.done = true, .score = Analyze(data, bound, shape)};
    }
    return _config[std::to_underlying(shape)].layout == frames ? measured.score
                                                               : kNoScore;
  }

  bool Compress(ColumnDataCheckpointData& data, Shape shape,
                FrameDictionary frames) {
    const auto* config = data.Get<Values>().trial.rows != 0
                           ? &_config[std::to_underlying(shape)]
                           : (_tuning.config ? &*_tuning.config : nullptr);
    return config && config->layout == frames && Compress(data, shape);
  }

  void CompressAs(ColumnDataCheckpointData& data, const Config& config) {
    const auto target = _params.segment_target;
    if (config.shape == Shape::Plain) {
      SealRows(_codec, config, data, target, _segment);
    } else {
      CompressRows(_codec, config, data.Get<Values>(), 0, target, _segment,
                   data);
    }
  }

  bool Compress(ColumnDataCheckpointData& data, Shape shape) {
    const auto target = _params.segment_target;
    auto& values = data.Get<Values>();
    Config config;
    if (values.trial.rows != 0) {
      config = _config[std::to_underlying(shape)];
      _tuning.config = config;
      _tuning.contested_on = values.Statistics();
    } else {
      if (!_tuning.config || values.Rows() == 0 ||
          Moved(values.Statistics(), _tuning.contested_on, kContestMove)) {
        return false;
      }
      config = *_tuning.config;
      SDB_ASSERT(config.shape == shape && config.leaf == kLeaf);
    }
    auto& first = Trial(config, values);
    if (first.Size() > PlainStorageBytes(first)) {
      return false;
    }
    const auto rest = first.begin + first.rows;
    data.FlushSegment(CodecOf(shape, kLeaf), std::move(*first.stats),
                      first.rows, first.bytes.View());
    CompressRows(_codec, config, values, rest, target, first, data);
    return true;
  }

  uint64_t Size(const Config& config, Values& values) {
    return _codec.Size(*this, values, config);
  }

  const Segment* Find(const Config& config) const noexcept {
    for (const auto& trial : _trials) {
      if (trial->config == config) {
        return trial.get();
      }
    }
    return nullptr;
  }

  Segment& Trial(const Config& config, Values& values) {
    for (const auto& trial : _trials) {
      if (trial->config == config) {
        return *trial;
      }
    }
    auto& seg = *_trials.emplace_back(std::make_unique<Segment>(_buffers));
    if (values.trial.rows != 0) {
      CompressRange(_codec, config, values, 0, values.trial.rows, seg);
      return seg;
    }
    CutRows(_codec, config, values, 0, _params.segment_target, seg);
    values.trial = {
      .rows = seg.rows,
      .input = seg.input,
      .nulls = seg.nulls,
      .max_len = seg.max_len,
    };
    return seg;
  }

 private:
  static Codec Make(const duckdb::LogicalType& type, DedupScratch& dedup,
                    StringTuning& tuning, uint32_t target) {
    if constexpr (Sorted(Codec::kCodec)) {
      return Codec{type, dedup, tuning, target};
    } else {
      return Codec{type, dedup, tuning};
    }
  }

  template<typename State>
  IRS_NO_INLINE static void CutRows(State& state, const Config& config,
                                    const Values& values, uint64_t begin,
                                    uint32_t target, Segment& seg) {
    const std::span<const uint32_t> codes = values.codes;
    const std::span<const duckdb::string_t* const> distinct = values.distinct;
    state.InitCompression(config, begin, distinct.size(), seg);
    for (auto row = begin; row < codes.size();) {
      state.Compress(codes[row++], distinct);
      if (state.Full(target)) {
        break;
      }
    }
    state.FinalizeCompress();
  }

  template<typename State>
  IRS_NO_INLINE static void SealRows(State& state, const Config& config,
                                     ColumnDataCheckpointData& data,
                                     uint32_t target, Segment& seg) {
    const auto seal = [&] {
      state.FinalizeCompress();
      data.FlushSegment(CodecOf(seg.config.shape, seg.config.leaf),
                        std::move(*seg.stats), seg.rows, seg.bytes.View());
    };
    uint64_t row = 0;
    uint64_t first = 0;
    state.InitCompression(config, first, 0, seg);
    for (const auto& chunk : data.chunks) {
      for (const auto entry : chunk.data.Values<duckdb::string_t>()) {
        state.Compress(entry.IsValid() ? &entry.GetValueUnsafe() : nullptr);
        ++row;
        if (state.Full(target)) {
          seal();
          first = row;
          state.InitCompression(config, first, 0, seg);
        }
      }
    }
    if (row != first) {
      seal();
    }
  }

  template<typename State>
  IRS_NO_INLINE static void CompressRows(State& state, const Config& config,
                                         const Values& values, uint64_t begin,
                                         uint32_t target, Segment& seg,
                                         ColumnDataCheckpointData& data) {
    for (auto row = begin; row < values.Rows(); row = seg.begin + seg.rows) {
      CutRows(state, config, values, row, target, seg);
      data.FlushSegment(CodecOf(seg.config.shape, seg.config.leaf),
                        std::move(*seg.stats), seg.rows, seg.bytes.View());
    }
  }

  template<typename State>
  IRS_NO_INLINE static void CompressRange(State& state, const Config& config,
                                          const Values& values, uint64_t begin,
                                          uint64_t end, Segment& seg) {
    const std::span<const uint32_t> codes = values.codes;
    const std::span<const duckdb::string_t* const> distinct = values.distinct;
    state.InitCompression(config, begin, distinct.size(), seg);
    for (auto row = begin; row < end; ++row) {
      state.Compress(codes[row], distinct);
    }
    state.FinalizeCompress();
  }

  struct Measured {
    bool done = false;
    double score = kNoScore;
  };

  const ColCodecParams& _params;
  StringTuning& _tuning;
  duckdb::BufferManager& _buffers;
  DedupScratch _dedup;
  Codec _codec;
  Segment _segment;
  std::vector<std::unique_ptr<Segment>> _trials;
  Config _config[kTextShapeCount];
  Measured _measured[kTextShapeCount];
};

template<typename Codec, Shape kShape>
inline constexpr CompressionFunction kTextFunction{
  .analyze =
    [](ColumnDataCheckpointData& data, double bound) {
      return data.Get<TextEncoder<Codec>>().Analyze(data, bound, kShape);
    },
  .compress =
    [](ColumnDataCheckpointData& data) {
      return data.Get<TextEncoder<Codec>>().Compress(data, kShape);
    },
  .codec = CodecOf(kShape, Codec::kCodec),
};

template<typename Codec, Shape kShape>
inline constexpr CompressionFunction kNamedTextFunction{
  .compress =
    [](ColumnDataCheckpointData& data) {
      data.Get<TextEncoder<Codec>>().CompressAs(
        data, NamedConfig(kShape, Codec::kCodec, data.params));
      return true;
    },
  .codec = CodecOf(kShape, Codec::kCodec),
};

template<typename Codec, Shape kShape, FrameDictionary kFrames>
inline constexpr CompressionFunction kFramedTextFunction{
  .analyze =
    [](ColumnDataCheckpointData& data, double bound) {
      return data.Get<TextEncoder<Codec>>().Analyze(data, bound, kShape,
                                                    kFrames);
    },
  .compress =
    [](ColumnDataCheckpointData& data) {
      return data.Get<TextEncoder<Codec>>().Compress(data, kShape, kFrames);
    },
  .codec = CodecOf(kShape, Codec::kCodec),
};

}  // namespace irs::codecs::text
