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

#include "iresearch/formats/column/codecs/text/string_compressor.hpp"

#include <duckdb/common/vector/vector_iterator.hpp>
#include <utility>

#include "iresearch/utils/assert.hpp"

namespace irs::codecs::text {
namespace {

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
                            ColumnDataCheckpointData& data, uint32_t target,
                            Segment& seg) {
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
IRS_NO_INLINE void CompressRows(State& state, const Config& config,
                                const StringAccumulator& acc, uint64_t begin,
                                uint32_t target, Segment& seg,
                                ColumnDataCheckpointData& data) {
  for (auto row = begin; row < acc.Rows(); row = seg.begin + seg.rows) {
    CutRows(state, config, acc, row, target, seg);
    data.FlushSegment(CodecOf(seg.config.shape, seg.config.leaf),
                      std::move(*seg.stats), seg.rows, seg.bytes.View());
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

}  // namespace

template<typename Visit>
void StringCompressor::WithState(const Config& config, Visit&& visit) {
  switch (config.leaf) {
    case ByteCodec::Lz4:
      if (EffectiveLevel<Lz4>(config.level) > 1) {
        visit(StateOf<blob::CompressionState<Lz4Hc>>());
      } else {
        visit(StateOf<blob::CompressionState<Lz4>>());
      }
      return;
    case ByteCodec::Zstd:
      visit(StateOf<blob::CompressionState<Zstd>>());
      return;
    case ByteCodec::Zxc:
      visit(StateOf<blob::CompressionState<Zxc>>());
      return;
    case ByteCodec::Fsst:
      visit(StateOf<fsst::CompressionState>());
      return;
    case ByteCodec::FsstPlus:
      visit(StateOf<fsst_plus::CompressionState>());
      return;
    case ByteCodec::OnPair:
      visit(StateOf<onpair::CompressionState>());
      return;
    case ByteCodec::Fsst12:
      visit(StateOf<fsst12::CompressionState>());
      return;
    case ByteCodec::None:
      break;
  }
  SDB_UNREACHABLE();
}

PooledSegment StringCompressor::CompressSegment(const Config& config,
                                                const StringAccumulator& acc,
                                                uint64_t begin) {
  auto seg = Acquire();
  WithState(config, [&](auto& state) {
    CutRows(state, config, acc, begin, _params.segment_target, *seg);
  });
  return seg;
}

PooledSegment StringCompressor::TrialCompress(const Config& config,
                                              const StringAccumulator& acc,
                                              uint64_t begin, uint64_t end) {
  auto seg = Acquire();
  WithState(config, [&](auto& state) {
    CompressRange(state, config, acc, begin, end, *seg);
  });
  return seg;
}

void StringCompressor::Compress(const Config& config,
                                const StringAccumulator& acc, uint64_t begin,
                                ColumnDataCheckpointData& data) {
  if (begin >= acc.Rows()) {
    return;
  }
  const auto seg = Acquire();
  WithState(config, [&](auto& state) {
    CompressRows(state, config, acc, begin, _params.segment_target, *seg, data);
  });
}

void StringCompressor::CompressChunks(const Config& config,
                                      ColumnDataCheckpointData& data) {
  const auto seg = Acquire();
  WithState(config, [&](auto& state) {
    SealRows(state, config, data, _params.segment_target, *seg);
  });
}

PooledSegment StringCompressor::Acquire() {
  if (_free.empty()) {
    auto* seg = _pool.emplace_back(std::make_unique<Segment>(_buffers)).get();
    _free.reserve(_pool.size());
    return {seg, {&_free}};
  }
  auto* seg = _free.back();
  _free.pop_back();
  return {seg, {&_free}};
}

}  // namespace irs::codecs::text
