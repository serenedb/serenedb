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

#include <cstdint>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <span>
#include <utility>

#include "iresearch/formats/column/codecs/string_compression_function.hpp"
#include "iresearch/formats/column/codecs/string_compressor.hpp"

namespace irs::codecs::text {

template<typename State>
IRS_NO_INLINE void CutRows(State& state, const Config& config,
                           const StringAccumulator& acc, uint64_t begin,
                           uint32_t target, Segment& seg) {
  const std::span<const uint32_t> codes = acc.codes;
  const std::span<const duckdb::string_t* const> values = acc.values;
  state.InitCompression(config, begin, values.size(), seg);
  for (auto row = begin; row < codes.size();) {
    state.Compress(codes[row++], values);
    if (state.Full(target)) {
      break;
    }
  }
  state.FinalizeCompress();
}

template<typename State>
IRS_NO_INLINE void SealRows(State& state, const Config& config,
                            std::span<const WriteChunk> chunks, uint32_t target,
                            Segment& seg, SegmentSink sink) {
  const auto seal = [&] {
    state.FinalizeCompress();
    sink(seg.config.choice, std::move(*seg.stats), seg.rows, seg.bytes.View());
  };
  uint64_t row = 0;
  uint64_t first = 0;
  state.InitCompression(config, first, 0, seg);
  for (const auto& chunk : chunks) {
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
IRS_NO_INLINE void CompressRows(State& state, const Config& config,
                                const StringAccumulator& acc, uint64_t begin,
                                uint32_t target, Segment& seg,
                                SegmentSink sink) {
  for (auto row = begin; row < acc.Rows(); row = seg.begin + seg.rows) {
    CutRows(state, config, acc, row, target, seg);
    sink(seg.config.choice, std::move(*seg.stats), seg.rows, seg.bytes.View());
  }
}

template<typename State>
IRS_NO_INLINE void CompressRange(State& state, const Config& config,
                                 const StringAccumulator& acc, uint64_t begin,
                                 uint64_t end, Segment& seg) {
  const std::span<const uint32_t> codes = acc.codes;
  const std::span<const duckdb::string_t* const> values = acc.values;
  state.InitCompression(config, begin, values.size(), seg);
  for (auto row = begin; row < end; ++row) {
    state.Compress(codes[row], values);
  }
  state.FinalizeCompress();
}

template<typename State>
PooledSegment CompressSegmentWith(StringCompressor& compressor,
                                  const Config& config,
                                  const StringAccumulator& acc,
                                  uint64_t begin) {
  auto seg = compressor.Acquire();
  CutRows(compressor.StateOf<State>(), config, acc, begin, compressor.Target(),
          *seg);
  return seg;
}

template<typename State>
void CompressWith(StringCompressor& compressor, const Config& config,
                  const StringAccumulator& acc, uint64_t begin,
                  SegmentSink sink) {
  if (begin >= acc.Rows()) {
    return;
  }
  const auto seg = compressor.Acquire();
  CompressRows(compressor.StateOf<State>(), config, acc, begin,
               compressor.Target(), *seg, sink);
}

template<typename State>
PooledSegment TrialCompressWith(StringCompressor& compressor,
                                const Config& config,
                                const StringAccumulator& acc, uint64_t begin,
                                uint64_t end) {
  auto seg = compressor.Acquire();
  CompressRange(compressor.StateOf<State>(), config, acc, begin, end, *seg);
  return seg;
}

template<typename State>
void CompressChunksWith(StringCompressor& compressor, const Config& config,
                        std::span<const WriteChunk> chunks, SegmentSink sink) {
  const auto seg = compressor.Acquire();
  SealRows(compressor.StateOf<State>(), config, chunks, compressor.Target(),
           *seg, sink);
}

template<typename State>
struct CompressionFunctionOf final : StringCompressionFunction {
  constexpr CompressionFunctionOf() = default;

  PooledSegment CompressSegment(StringCompressor& compressor,
                                const Config& config,
                                const StringAccumulator& acc,
                                uint64_t begin) const final {
    return CompressSegmentWith<State>(compressor, config, acc, begin);
  }

  void Compress(StringCompressor& compressor, const Config& config,
                const StringAccumulator& acc, uint64_t begin,
                SegmentSink sink) const final {
    CompressWith<State>(compressor, config, acc, begin, sink);
  }

  PooledSegment TrialCompress(StringCompressor& compressor,
                              const Config& config,
                              const StringAccumulator& acc, uint64_t begin,
                              uint64_t end) const final {
    return TrialCompressWith<State>(compressor, config, acc, begin, end);
  }

  void CompressChunks(StringCompressor& compressor, const Config& config,
                      std::span<const WriteChunk> chunks,
                      SegmentSink sink) const final {
    CompressChunksWith<State>(compressor, config, chunks, sink);
  }
};

}  // namespace irs::codecs::text
