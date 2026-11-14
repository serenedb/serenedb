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

#include "iresearch/formats/column/codecs/string_seal.hpp"

#include <algorithm>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <string_view>
#include <utility>

#include "iresearch/formats/column/codecs/string_compression_function.hpp"
#include "iresearch/formats/column/codecs/string_compressor.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"
#include "iresearch/formats/column/codecs/string_picker.hpp"

namespace irs::codecs {
namespace {

using duckdb::string_t;

uint64_t PlainStorageBytes(const text::Segment& seg) noexcept {
  return kHeaderSize + seg.input + seg.rows * sizeof(int32_t);
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

bool SealSegments(const StringAccumulator& acc, const StringChoice* named,
                  const ColCodecParams& params, const duckdb::LogicalType& type,
                  StringTuning& tuning, duckdb::BufferManager& buffers,
                  SegmentSink sink) {
  text::StringCompressor compressor{params, type, tuning.estimators, buffers};
  if (named) {
    const auto config = text::NamedConfig(*named, params);
    text::GetStringCompressionFunction(config).Compress(compressor, config, acc,
                                                        0, sink);
    return true;
  }
  text::StringPicker picker{acc, params, tuning, compressor};
  auto first = picker.First();
  if (!first) {
    return true;
  }
  if (first->Size() > PlainStorageBytes(*first)) {
    return false;
  }
  const auto config = first->config;
  const auto rest = first->begin + first->rows;
  sink(config.choice, std::move(*first->stats), first->rows,
       first->bytes.View());
  first.reset();
  text::GetStringCompressionFunction(config).Compress(compressor, config, acc,
                                                      rest, sink);
  return true;
}

void SealSegments(std::span<const WriteChunk> chunks, const StringChoice& named,
                  const ColCodecParams& params, const duckdb::LogicalType& type,
                  StringTuning& tuning, duckdb::BufferManager& buffers,
                  SegmentSink sink) {
  text::StringCompressor compressor{params, type, tuning.estimators, buffers};
  const auto config = text::NamedConfig(named, params);
  text::GetStringCompressionFunction(config).CompressChunks(compressor, config,
                                                            chunks, sink);
}

}  // namespace irs::codecs
