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

#include "iresearch/formats/column/codecs/text/string_seal.hpp"

#include <algorithm>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <string_view>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/text/string_auto.hpp"
#include "iresearch/formats/column/codecs/text/string_compressor.hpp"
#include "iresearch/formats/column/codecs/text/string_layout.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::string_t;

uint64_t PlainStorageBytes(const text::Segment& seg) noexcept {
  return kHeaderSize + seg.input + seg.rows * sizeof(int32_t);
}

text::Config NamedConfig(Shape shape, ByteCodec leaf,
                         const ColCodecParams& params) noexcept {
  return {
    .shape = shape,
    .leaf = leaf,
    .level = params.compression_level,
    .layout = params.tier == WriteTier::Flush ? FrameLayout::FirstFrame
                                              : FrameLayout::Local,
  };
}

void Accumulate(StringAccumulator& acc, std::span<const WriteChunk> chunks,
                StringTuning& tuning) {
  acc.Reserve(CountRows(chunks), tuning.last_distinct);
  for (const auto& chunk : chunks) {
    acc.Add(chunk.data);
  }
  tuning.last_distinct = acc.values.size();
}

auto FlushTo(ColumnDataCheckpointData& data) {
  return [&data](CodecType type, duckdb::BaseStatistics stats, uint64_t rows,
                 std::string_view bytes) {
    data.function = GetCodec(type, duckdb::PhysicalType::VARCHAR);
    data.FlushSegment(std::move(stats), rows, bytes);
  };
}

bool SealSegments(const StringAccumulator& acc, const ColCodecParams& params,
                  const duckdb::LogicalType& type, StringTuning& tuning,
                  duckdb::BufferManager& buffers, const CodecSet& disabled,
                  SegmentSink sink) {
  text::StringCompressor compressor{params, type, tuning.estimators, buffers};
  auto first =
    text::CompressFirstSegment(acc, params, tuning, disabled, compressor);
  if (!first) {
    return acc.Rows() == 0;
  }
  if (first->Size() > PlainStorageBytes(*first)) {
    return false;
  }
  const auto config = first->config;
  const auto rest = first->begin + first->rows;
  sink(TextCodec(config.shape, config.leaf), std::move(*first->stats),
       first->rows, first->bytes.View());
  first.reset();
  compressor.Compress(config, acc, rest, sink);
  return true;
}

}  // namespace

void StringAccumulator::Reserve(uint64_t rows, uint64_t distinct) {
  codes.reserve(rows);
  const auto expected = distinct == 0 ? rows : std::min(rows, distinct);
  values.reserve(expected);
  _distinct.reserve(expected);
}

void StringAccumulator::Add(const duckdb::Vector& input) {
  const auto base = codes.size();
  codes.resize(base + input.size());
  auto* out = codes.data() + base;
  for (const auto entry : input.Values<string_t>()) {
    auto& code = out[entry.GetIndex()];
    if (!entry.IsValid()) {
      code = 0;
      ++null_count;
      continue;
    }
    const auto& value = entry.GetValueUnsafe();
    if (_last && value == *_last) {
      code = _last_code;
      continue;
    }
    code = *_distinct.lazy_emplace(value, [&](const auto& construct) noexcept {
      construct(static_cast<uint32_t>(values.size() + 1));
    });
    if (code > values.size()) {
      values.push_back(&value);
    }
    _last = &value;
    _last_code = code;
  }
}

void CompressStrings(Shape shape, ByteCodec leaf,
                     ColumnDataCheckpointData& data,
                     std::span<const WriteChunk> chunks) {
  SDB_ASSERT(data.tuning);
  auto& tuning = data.tuning->strings;
  text::StringCompressor compressor{data.params, data.type, tuning.estimators,
                                    data.ctx.BufferManager()};
  const auto config = NamedConfig(shape, leaf, data.params);
  const auto sink = FlushTo(data);
  if (shape == Shape::Plain) {
    compressor.CompressChunks(config, chunks, sink);
    return;
  }
  StringAccumulator acc;
  Accumulate(acc, chunks, tuning);
  compressor.Compress(config, acc, 0, sink);
}

void CompressAutoStrings(ColumnDataCheckpointData& data,
                         std::span<const WriteChunk> chunks) {
  SDB_ASSERT(data.tuning);
  auto& tuning = data.tuning->strings;
  StringAccumulator acc;
  Accumulate(acc, chunks, tuning);
  if (SealSegments(acc, data.params, data.type, tuning,
                   data.ctx.BufferManager(), data.disabled, FlushTo(data))) {
    return;
  }
  data.function =
    GetCodec(CodecType::Uncompressed, duckdb::PhysicalType::VARCHAR);
  data.function->compress(data, chunks);
}

}  // namespace irs::codecs
