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

auto FlushTo(ColumnDataCheckpointData& data) {
  return [&data](CodecType type, duckdb::BaseStatistics stats, uint64_t rows,
                 std::string_view bytes) {
    data.codec = GetCodec(type, duckdb::PhysicalType::VARCHAR);
    data.FlushSegment(std::move(stats), rows, bytes);
  };
}

struct Strings final : State {
  explicit Strings(ColumnDataCheckpointData& data)
    : tuning{data.tuning->strings},
      compressor{data.params, data.type, tuning.estimators,
                 data.ctx->BufferManager()} {}

  const StringAccumulator& Accumulated(std::span<const WriteChunk> chunks) {
    if (!accumulated) {
      accumulated = true;
      acc.Reserve(CountRows(chunks), tuning.last_distinct);
      for (const auto& chunk : chunks) {
        acc.Add(chunk.data);
      }
      tuning.last_distinct = acc.values.size();
    }
    return acc;
  }

  StringTuning& tuning;
  text::StringCompressor compressor;
  StringAccumulator acc;
  text::PooledSegment first;
  bool accumulated = false;
};

Estimate Analyze(ColumnDataCheckpointData& data, double) {
  auto& strings = data.Get<Strings>();
  strings.first = text::DetectBestCompressionMethod(
    strings.Accumulated(data.chunks), data.params, strings.tuning,
    data.disabled, strings.compressor, std::move(strings.first));
  if (!strings.first) {
    return {};
  }
  const auto& seg = *strings.first;
  return {
    .codec = TextCodec(seg.config.shape, seg.config.leaf),
    .ratio = static_cast<double>(seg.Size()) /
             static_cast<double>(std::max<uint64_t>(seg.input, 1)),
  };
}

Encoded Encode(ColumnDataCheckpointData& data, const Estimate* pick) {
  if (data.only != CodecType::Auto) {
    return {.streams = true};
  }
  auto& strings = data.Get<Strings>();
  if (!pick) {
    const auto& config = strings.tuning.config;
    const auto& acc = strings.Accumulated(data.chunks);
    if (!config || !text::Enabled(data.disabled, *config) || acc.Rows() == 0) {
      return {};
    }
    strings.first = strings.compressor.CompressSegment(*config, acc, 0);
  }
  const auto& seg = *strings.first;
  if (seg.Size() > PlainStorageBytes(seg)) {
    return {};
  }
  return {.bytes = seg.Size(), .input = seg.input, .buffered = true};
}

void Flush(ColumnDataCheckpointData& data) {
  auto& strings = data.Get<Strings>();
  const auto sink = FlushTo(data);
  if (data.only != CodecType::Auto) {
    const auto config =
      NamedConfig(ShapeOf(data.only), LeafOf(data.only), data.params);
    if (config.shape == Shape::Plain) {
      strings.compressor.CompressChunks(config, data.chunks, sink);
    } else {
      strings.compressor.Compress(config, strings.Accumulated(data.chunks), 0,
                                  sink);
    }
    return;
  }
  auto& first = strings.first;
  const auto config = first->config;
  const auto rest = first->begin + first->rows;
  sink(TextCodec(config.shape, config.leaf), std::move(*first->stats),
       first->rows, first->bytes.View());
  first.reset();
  strings.compressor.Compress(config, strings.acc, rest, sink);
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

const CompressionFunction kStringsFunction{
  .analyze = Analyze,
  .encode = Encode,
  .flush = Flush,
};

}  // namespace irs::codecs
