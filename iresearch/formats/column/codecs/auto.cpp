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

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"

namespace irs::codecs {
namespace {

constexpr size_t kMostCandidates = 16;

struct Asked {
  const Candidate* row;
  const Codec* codec;
};

}  // namespace

bool CompressAuto(ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  data.params.compression_level = 0;
  data.contested = true;
  const auto& type = CandidatesOf(physical);
  absl::InlinedVector<Asked, kMostCandidates> candidates;
  bool remembers = false;
  for (const auto& row :
       data.params.tier == WriteTier::Flush ? type.flush : type.merge) {
    if (data.disabled.test(std::to_underlying(row.codec))) {
      continue;
    }
    if (const auto* codec = GetCodec(row.codec, physical)) {
      candidates.emplace_back(&row, codec);
      remembers = remembers || !row.always;
    }
  }
  auto& hints = data.tuning.hints;
  const auto now = data.tuning.row_group;
  const auto remembered = [&](const Asked& candidate) {
    const auto& hint = hints[std::to_underlying(candidate.row->codec)];
    return candidate.row->always || hint.forced == 0 ? 0.0 : hint.score;
  };
  absl::c_stable_sort(candidates, [&](const Asked& a, const Asked& b) {
    return remembered(a) < remembered(b);
  });
  const auto features =
    remembers && type.features ? type.features(data) : Features{};
  auto best = kNoScore;
  const Asked* winner = nullptr;
  for (const auto& candidate : candidates) {
    auto& hint = hints[std::to_underlying(candidate.row->codec)];
    const bool forced = candidate.row->always || Forgotten(hint, now);
    if (!forced && !Measures(hint, best, features, type.least_shift)) {
      continue;
    }
    data.price = candidate.row->price;
    const auto score = candidate.codec->compression.analyze(data, best);
    hint.features = features;
    hint.score = score;
    if (forced) {
      hint.forced = now;
    }
    if (score < best) {
      best = score;
      winner = &candidate;
      if (best == 0) {
        break;
      }
    }
  }
  if (winner) {
    data.price = winner->row->price;
  }
  if (!winner || !winner->codec->compression.compress(*winner->codec, data)) {
    CompressUncompressed(data);
  }
  return true;
}

}  // namespace irs::codecs
