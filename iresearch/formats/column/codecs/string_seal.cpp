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

#include "iresearch/formats/column/codecs/calibration.hpp"
#include "iresearch/formats/column/codecs/segment_encoder.hpp"
#include "iresearch/formats/column/codecs/string_layout.hpp"

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
                         SegmentSink sink) {
  text::SegmentEncoder encoder{params, type, tuning};
  text::Calibration calibration{acc, named, params, tuning, encoder};
  SealOutcome outcome{.sealed = true};
  uint64_t row = 0;
  bool first = true;
  const bool due = !named && (!tuning.choice || tuning.clock.Due());
  while (row < acc.Rows()) {
    auto* seg = encoder.Cut(calibration.CutConfig(), acc, row);
    const auto end = seg->begin + seg->rows;
    if (!named) {
      const bool scheduled = first && due;
      const bool drift =
        !scheduled && calibration.Drifted(*seg, first || end < acc.Rows());
      const bool retune = drift && !first;
      if (retune) {
        encoder.Retrain();
      }
      if (scheduled || drift) {
        seg = calibration.Calibrate(seg, !tuning.levels_tuned || retune, drift);
      }
      if (first && seg->Size() > PlainStorageBytes(*seg)) {
        encoder.Release(seg);
        return {};
      }
    }
    outcome.all_dedup =
      outcome.all_dedup && seg->config.choice.shape == Shape::Dedup;
    const std::string_view parts[] = {seg->head, seg->data};
    sink(seg->config.choice, std::move(*seg->stats), seg->rows, parts);
    encoder.Release(seg);
    row = end;
    first = false;
  }
  return outcome;
}

SealOutcome SealSegments(std::span<const WriteChunk> chunks,
                         const StringChoice& named,
                         const ColCodecParams& params,
                         const duckdb::LogicalType& type, StringTuning& tuning,
                         SegmentSink sink) {
  text::SegmentEncoder encoder{params, type, tuning};
  SealOutcome outcome{.sealed = true};
  encoder.SealChunks(
    text::NamedConfig(named, params), chunks,
    [&](StringChoice choice, duckdb::BaseStatistics stats, uint64_t rows,
        std::span<const std::string_view> parts) {
      outcome.all_dedup = outcome.all_dedup && choice.shape == Shape::Dedup;
      sink(choice, std::move(stats), rows, parts);
    });
  return outcome;
}

}  // namespace irs::codecs
