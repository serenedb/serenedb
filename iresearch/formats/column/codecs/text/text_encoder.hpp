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
inline constexpr double kOtherShapeWinsBelow = 0.9;
inline constexpr double kOtherShapeMinSaving = 4096;

inline double ShapeCost(Shape shape, bool repeats, double bytes) noexcept {
  return shape == (repeats ? Shape::Dictionary : Shape::Plain)
           ? bytes
           : std::max(bytes / kOtherShapeWinsBelow,
                      bytes + kOtherShapeMinSaving);
}

inline double Score(Values& values, Shape shape, double price, uint64_t bytes) {
  return ShapeCost(shape, values.Repeats(), static_cast<double>(bytes)) *
         price / static_cast<double>(std::max<uint64_t>(values.trial.input, 1));
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

template<typename Compression>
class TextEncoder final : public State {
 public:
  static constexpr ByteCodec kLeaf = Compression::kCodec;
  static constexpr Shape kShape = Compression::kShape;
  static constexpr StateId kId = TextState(kShape, kLeaf);

  explicit TextEncoder(ColumnDataCheckpointData& data)
    : _params{data.params},
      _tuning{data.tuning.strings},
      _buffers{data.ctx->BufferManager()},
      _codec{data, _dedup},
      _segment{_buffers} {}

  Compression& Codec() noexcept { return _codec; }

  bool Contests(ColumnDataCheckpointData& data) {
    auto& values = data.Get<Values>();
    if (values.Rows() == 0) {
      return false;
    }
    _config = {.shape = kShape, .leaf = kLeaf};
    if (values.trial.rows == 0 && _tuning.config &&
        _tuning.config->shape == kShape && _tuning.config->leaf == kLeaf) {
      Trial(*_tuning.config, values);
    }
    return Contends(data, kShape);
  }

  void Contest(const Config& config) noexcept { _config = config; }

  const Config& Contested() const noexcept { return _config; }

  const Config* Chosen(const Values& values) const noexcept {
    if (values.trial.rows != 0) {
      return &_config;
    }
    return _tuning.config ? &*_tuning.config : nullptr;
  }

  void CompressAs(ColumnDataCheckpointData& data, const Config& config) {
    const auto target = _params.segment_target;
    if constexpr (kShape == Shape::Plain) {
      SealRows(_codec, config, data, target, _segment);
    } else {
      CompressRows(_codec, config, data.Get<Values>(), 0, target, _segment,
                   data);
    }
  }

  bool Compress(ColumnDataCheckpointData& data) {
    const auto target = _params.segment_target;
    auto& values = data.Get<Values>();
    Config config;
    if (values.trial.rows != 0) {
      config = _config;
      _tuning.config = config;
      _tuning.contested_on = values.Statistics();
    } else {
      if (!_tuning.config || values.Rows() == 0 ||
          Moved(values.Statistics(), _tuning.contested_on, kContestMove)) {
        return false;
      }
      config = *_tuning.config;
      SDB_ASSERT(config.shape == kShape && config.leaf == kLeaf);
    }
    auto& first = Trial(config, values);
    if (first.Size() > PlainStorageBytes(first)) {
      return false;
    }
    const auto rest = first.begin + first.rows;
    data.FlushSegment(CodecOf(kShape, kLeaf), std::move(*first.stats),
                      first.rows, first.bytes.View(), first.shared);
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
  IRS_NO_INLINE static void CutRows(Compression& state, const Config& config,
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

  IRS_NO_INLINE static void SealRows(Compression& state, const Config& config,
                                     ColumnDataCheckpointData& data,
                                     uint32_t target, Segment& seg) {
    const auto seal = [&] {
      state.FinalizeCompress();
      data.FlushSegment(CodecOf(seg.config.shape, seg.config.leaf),
                        std::move(*seg.stats), seg.rows, seg.bytes.View(),
                        seg.shared);
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

  IRS_NO_INLINE static void CompressRows(Compression& state,
                                         const Config& config,
                                         const Values& values, uint64_t begin,
                                         uint32_t target, Segment& seg,
                                         ColumnDataCheckpointData& data) {
    for (auto row = begin; row < values.Rows(); row = seg.begin + seg.rows) {
      CutRows(state, config, values, row, target, seg);
      data.FlushSegment(CodecOf(seg.config.shape, seg.config.leaf),
                        std::move(*seg.stats), seg.rows, seg.bytes.View(),
                        seg.shared);
    }
  }

  IRS_NO_INLINE static void CompressRange(Compression& state,
                                          const Config& config,
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

  const ColCodecParams& _params;
  StringTuning& _tuning;
  duckdb::BufferManager& _buffers;
  DedupScratch _dedup;
  Compression _codec;
  Segment _segment;
  std::vector<std::unique_ptr<Segment>> _trials;
  Config _config;
};

template<typename Compression>
double AnalyzeTrial(ColumnDataCheckpointData& data, double) {
  auto& encoder = data.Get<TextEncoder<Compression>>();
  if (!encoder.Contests(data)) {
    return kNoScore;
  }
  auto& values = data.Get<Values>();
  return Score(values, Compression::kShape, 1.0,
               encoder.Trial(encoder.Contested(), values).Size());
}

template<typename Compression>
bool CompressContested(ColumnDataCheckpointData& data) {
  return data.Get<TextEncoder<Compression>>().Compress(data);
}

template<typename Compression>
bool CompressNamed(ColumnDataCheckpointData& data) {
  data.Get<TextEncoder<Compression>>().CompressAs(
    data, NamedConfig(Compression::kShape, Compression::kCodec, data.params));
  return true;
}

template<typename Compression>
inline constexpr CompressionFunction kNamedTextFunction{
  .compress = CompressNamed<Compression>,
  .codec = CodecOf(Compression::kShape, Compression::kCodec),
};

}  // namespace irs::codecs::text
