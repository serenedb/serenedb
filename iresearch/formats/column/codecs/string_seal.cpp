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
    code = *_distinct.lazy_emplace(value, [&](const auto& construct) {
      values.push_back(&value);
      construct(static_cast<uint32_t>(values.size()));
    });
    _last = &value;
    _last_code = code;
  }
}

SealOutcome SealSegments(const StringAccumulator& acc,
                         const StringChoice* named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         duckdb::BufferManager& buffers, SegmentSink sink) {
  text::StringCompressor compressor{params, type, tuning.estimators, buffers};
  SealOutcome outcome{.sealed = true};
  const auto emit = [&](StringChoice choice, duckdb::BaseStatistics stats,
                        uint64_t rows, std::string_view bytes) {
    outcome.all_dedup = outcome.all_dedup && choice.shape == Shape::Dedup;
    sink(choice, std::move(stats), rows, bytes);
  };
  if (named) {
    compressor.Compress(text::NamedConfig(*named, params), acc, 0, emit);
    return outcome;
  }
  text::StringPicker picker{acc, params, tuning, compressor};
  auto first = picker.First();
  if (!first) {
    return outcome;
  }
  if (first->Size() > PlainStorageBytes(*first)) {
    return {};
  }
  const auto config = first->config;
  const auto rest = first->begin + first->rows;
  emit(config.choice, std::move(*first->stats), first->rows,
       first->bytes.View());
  first.reset();
  compressor.Compress(config, acc, rest, emit);
  return outcome;
}

SealOutcome SealSegments(std::span<const WriteChunk> chunks,
                         const StringChoice& named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         duckdb::BufferManager& buffers, SegmentSink sink) {
  text::StringCompressor compressor{params, type, tuning.estimators, buffers};
  SealOutcome outcome{.sealed = true};
  compressor.SealChunks(text::NamedConfig(named, params), chunks,
                        [&](StringChoice choice, duckdb::BaseStatistics stats,
                            uint64_t rows, std::string_view bytes) {
                          outcome.all_dedup =
                            outcome.all_dedup && choice.shape == Shape::Dedup;
                          sink(choice, std::move(stats), rows, bytes);
                        });
  return outcome;
}

}  // namespace irs::codecs
