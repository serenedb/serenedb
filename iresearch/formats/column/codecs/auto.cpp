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

#include <absl/algorithm/container.h>
#include <absl/container/inlined_vector.h>

#include <duckdb/common/types.hpp>
#include <utility>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"

namespace irs::codecs {
namespace {

constexpr size_t kMostCandidates = 16;

bool CompressAuto(ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  data.params.compression_level = 0;
  data.contested = true;
  absl::InlinedVector<const Codec*, kMostCandidates> candidates;
  for (const auto type : Candidates(physical, data.params.tier)) {
    if (data.disabled.test(std::to_underlying(type))) {
      continue;
    }
    if (const auto* codec = GetCodec(type, physical);
        codec && codec->compression) {
      candidates.push_back(codec);
    }
  }
  const auto remembered = [&](const Codec* codec) {
    const auto& hint = data.tuning.hints[std::to_underlying(codec->type)];
    return hint.known ? hint.score : 0.0;
  };
  absl::c_stable_sort(candidates, [&](const Codec* a, const Codec* b) {
    return remembered(a) < remembered(b);
  });
  auto best = kNoScore;
  const CompressionFunction* winner = nullptr;
  for (const auto* codec : candidates) {
    if (const auto score = codec->compression->analyze(data, best);
        score < best) {
      best = score;
      winner = codec->compression;
      if (best == 0) {
        break;
      }
    }
  }
  if (!winner || !winner->compress(data)) {
    UncompressedCompression(physical).compress(data);
  }
  return true;
}

constexpr CompressionFunction kAuto{.compress = CompressAuto};

constexpr Codec kAutoCodec{.type = CodecType::Auto, .compression = &kAuto};

}  // namespace

const Codec* AutoCodec(duckdb::PhysicalType) { return &kAutoCodec; }

}  // namespace irs::codecs
