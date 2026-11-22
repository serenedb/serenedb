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

#include <algorithm>
#include <array>
#include <duckdb/common/types.hpp>
#include <magic_enum/magic_enum.hpp>
#include <span>
#include <utility>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

struct Candidate {
  CodecType codec = CodecType::Uncompressed;
  double price = 1.0;
};

struct Candidates {
  std::span<const Candidate> flush;
  std::span<const Candidate> merge;
  Features (*features)(ColumnDataCheckpointData& data) = nullptr;
  double least_shift = 1.0;
};

constexpr double kLz4Price = 1.05;
constexpr double kZstdPrice = 1.15;
constexpr double kRoaringPrice = 2.0;
constexpr double kLeastTextShift = 1.5;

constexpr Candidate kLightweightRows[] = {
  {CodecType::Uncompressed},
  {CodecType::Roaring, kRoaringPrice},
};

constexpr Candidate kBoolRows[] = {
  {CodecType::Uncompressed},
  {CodecType::Bitpacking},
  {CodecType::Rle},
  {CodecType::Roaring, kRoaringPrice},
};

constexpr Candidate kIntegerRows[] = {
  {CodecType::Uncompressed},
  {CodecType::Bitpacking},
  {CodecType::Rle},
  {CodecType::Dict},
  {CodecType::Lz4, kLz4Price},
  {CodecType::RleLz4, kLz4Price},
  {CodecType::DictLz4, kLz4Price},
  {CodecType::Zstd, kZstdPrice},
  {CodecType::RleZstd, kZstdPrice},
  {CodecType::DictZstd, kZstdPrice},
};

constexpr Candidate kFreshIntegerRows[] = {
  {CodecType::Uncompressed},
  {CodecType::Bitpacking},
  {CodecType::Rle},
  {CodecType::Dict},
  {CodecType::Lz4, kLz4Price},
  {CodecType::RleLz4, kLz4Price},
  {CodecType::DictLz4, kLz4Price},
};

constexpr Candidate kFloatRows[] = {
  {CodecType::Uncompressed}, {CodecType::Alp}, {CodecType::Alprd},
  {CodecType::Bitpacking},   {CodecType::Rle}, {CodecType::Dict},
};

constexpr Candidate kTextRows[] = {
  {CodecType::DictFsst},
  {CodecType::DictLz4},
  {CodecType::Fsst},
  {CodecType::Lz4, kLz4Price},
};

constexpr Candidate kFreshTextRows[] = {
  {CodecType::DictFsst},
  {CodecType::DictLz4, kLz4Price},
  {CodecType::Fsst},
  {CodecType::Lz4, kLz4Price},
};

constexpr Candidates kLightweight{
  .flush = kLightweightRows,
  .merge = kLightweightRows,
};

constexpr Candidates kBool{
  .flush = kBoolRows,
  .merge = kBoolRows,
};

constexpr Candidates kInteger{
  .flush = kFreshIntegerRows,
  .merge = kIntegerRows,
  .features = NumericFeatures,
};

constexpr Candidates kFloat{
  .flush = kFloatRows,
  .merge = kFloatRows,
  .features = NumericFeatures,
};

constexpr Candidates kText{
  .flush = kFreshTextRows,
  .merge = kTextRows,
  .features = TextFeatures,
  .least_shift = kLeastTextShift,
};

constexpr CodecSet kAlwaysAsked =
  CodecSet{}
    .set(std::to_underlying(CodecType::Uncompressed))
    .set(std::to_underlying(CodecType::Bitpacking))
    .set(std::to_underlying(CodecType::Rle))
    .set(std::to_underlying(CodecType::Delta));

constexpr CodecType kNamedOnly[] = {
  CodecType::Delta,
  CodecType::DeltaLz4,
  CodecType::DeltaZstd,
};

static_assert([] {
  std::array<bool, magic_enum::enum_count<CodecType>()> listed{};
  for (const std::span<const Candidate> rows :
       {std::span<const Candidate>{kLightweightRows},
        std::span<const Candidate>{kBoolRows},
        std::span<const Candidate>{kIntegerRows},
        std::span<const Candidate>{kFreshIntegerRows},
        std::span<const Candidate>{kFloatRows}}) {
    for (const auto& row : rows) {
      listed[std::to_underlying(row.codec)] = true;
    }
  }
  for (const auto type : kNamedOnly) {
    listed[std::to_underlying(type)] = true;
  }
  for (const auto type : magic_enum::enum_values<CodecType>()) {
    const bool numeric = type != CodecType::Auto && type != CodecType::Empty &&
                         type != CodecType::Constant &&
                         (!IsText(type) || IsNumeric(type));
    if (listed[std::to_underlying(type)] != numeric) {
      return false;
    }
  }
  return true;
}());

bool AlwaysAsked(CodecType type) noexcept {
  return kAlwaysAsked.test(std::to_underlying(type));
}

const Candidates& CandidatesOf(PhysicalType physical) noexcept {
  switch (physical) {
    case PhysicalType::VARCHAR:
      return kText;
    case PhysicalType::FLOAT:
    case PhysicalType::DOUBLE:
      return kFloat;
    case PhysicalType::BOOL:
      return kBool;
    case PhysicalType::INT8:
    case PhysicalType::INT16:
    case PhysicalType::INT32:
    case PhysicalType::INT64:
    case PhysicalType::UINT8:
    case PhysicalType::UINT16:
    case PhysicalType::UINT32:
    case PhysicalType::UINT64:
    case PhysicalType::INT128:
    case PhysicalType::UINT128:
    case PhysicalType::LIST:
      return kInteger;
    default:
      return kLightweight;
  }
}

constexpr size_t kMostCandidates = 16;

struct Asked {
  const Candidate* row;
  const Codec* codec;
};

double Analyze(const Codec& codec, ColumnDataCheckpointData& data,
               const Features& features, double bound) {
  auto& hint = data.tuning.hints[std::to_underlying(codec.type)];
  const auto functions = codec.compression;
  const auto now = data.tuning.row_group;
  const bool every = AlwaysAsked(codec.type) || hint.refreshed == 0 ||
                     now - hint.refreshed > kOldHint ||
                     features[kStoredWidth] != hint.features[kStoredWidth];
  const size_t last = hint.function < functions.size() ? hint.function : 0;
  auto best = bound;
  auto smallest = kNoScore;
  hint.function = static_cast<uint8_t>(last);
  const auto ask = [&](size_t i) {
    const auto score = functions[i].analyze(data, best);
    if (score < smallest) {
      smallest = score;
      hint.function = static_cast<uint8_t>(i);
    }
    best = std::min(best, score);
  };
  ask(last);
  for (size_t i = 0; every && i < functions.size(); ++i) {
    if (i != last) {
      ask(i);
    }
  }
  hint.features = features;
  hint.score = smallest;
  if (every) {
    hint.refreshed = now;
  }
  return smallest;
}

}  // namespace

bool CompressAs(const Codec& codec, ColumnDataCheckpointData& data) {
  const auto functions = codec.compression;
  if (functions.size() == 1) {
    return functions.front().compress(codec, data);
  }
  const auto& type = CandidatesOf(data.type.InternalType());
  const auto features = !AlwaysAsked(codec.type) && type.features
                          ? type.features(data)
                          : Features{};
  if (Analyze(codec, data, features, kNoScore) == kNoScore) {
    return false;
  }
  const auto function =
    data.tuning.hints[std::to_underlying(codec.type)].function;
  return functions[function].compress(codec, data);
}

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
      remembers = remembers || !AlwaysAsked(row.codec);
    }
  }
  auto& hints = data.tuning.hints;
  const auto now = data.tuning.row_group;
  const auto remembered = [&](const Asked& candidate) {
    const auto& hint = hints[std::to_underlying(candidate.row->codec)];
    return AlwaysAsked(candidate.row->codec) || hint.forced == 0 ? 0.0
                                                                 : hint.score;
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
    const bool forced =
      AlwaysAsked(candidate.row->codec) || Forgotten(hint, now);
    if (!forced && !Measures(hint, best, features, type.least_shift)) {
      continue;
    }
    data.price = candidate.row->price;
    const auto score = Analyze(*candidate.codec, data, features, best);
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
  if (!winner) {
    CompressUncompressed(data);
    return true;
  }
  data.price = winner->row->price;
  const auto& codec = *winner->codec;
  const auto function = hints[std::to_underlying(codec.type)].function;
  if (!codec.compression[function].compress(codec, data)) {
    CompressUncompressed(data);
  }
  return true;
}

}  // namespace irs::codecs
