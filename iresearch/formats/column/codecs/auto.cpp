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
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

struct Variant {
  uint8_t function = 0;
  uint8_t option = 0;
  uint8_t level = 0;
  double price = 1.0;
};

struct Candidate {
  CodecType codec = CodecType::Uncompressed;
  double price = 1.0;
  std::span<const Variant> variants = {};
};

struct Candidates {
  std::span<const Candidate> flush;
  std::span<const Candidate> merge;
  Features (*features)(ColumnDataCheckpointData& data) = nullptr;
  double least_shift = 1.0;
  uint64_t (*input)(ColumnDataCheckpointData& data) = nullptr;
};

constexpr double kLz4Price = 1.05;
constexpr double kZstdPrice = 1.15;
constexpr double kRoaringPrice = 2.0;
constexpr double kLeastTextShift = 1.5;
constexpr double kVariantPrice = 1.03;

constexpr Variant kFrames[] = {
  {.level = 4},
  {.level = 6, .price = kVariantPrice},
};

constexpr Variant kDictionaryFrames[] = {
  {.level = 4},
  {.option = text::kSorted, .level = 4},
  {.level = 6, .price = kVariantPrice},
  {.option = text::kSorted, .level = 6, .price = kVariantPrice},
};

constexpr Variant kFreshFrames[] = {
  {.level = 1},
  {.option = text::kWide, .level = 1, .price = kVariantPrice},
};

constexpr Variant kFreshDictionaryFrames[] = {
  {.level = 1},
  {.option = text::kSorted, .level = 1},
  {.option = text::kWide, .level = 1, .price = kVariantPrice},
  {.option = text::kSorted | text::kWide, .level = 1, .price = kVariantPrice},
};

constexpr Variant kNamedDictionaryFrames[] = {
  {},
  {.option = text::kSorted, .price = kVariantPrice},
};

constexpr double kCodeRunPrice = 1.5;

constexpr Variant kCodeRuns[] = {
  {.function = 0, .price = kCodeRunPrice},
  {.function = 1, .option = numeric::kLongRuns},
};

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
  {CodecType::DictLz4, 1.0, kDictionaryFrames},
  {CodecType::Fsst},
  {CodecType::Lz4, kLz4Price, kFrames},
};

constexpr Candidate kFreshTextRows[] = {
  {CodecType::DictFsst},
  {CodecType::DictLz4, kLz4Price, kFreshDictionaryFrames},
  {CodecType::Fsst},
  {CodecType::Lz4, kLz4Price, kFreshFrames},
};

constexpr Candidate kCodeRows[] = {
  {CodecType::Bitpacking},
  {CodecType::Rle, 1.0, kCodeRuns},
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
  .input = TextBytes,
};

constexpr Candidates kCodes{
  .flush = kCodeRows,
  .merge = kCodeRows,
  .features = NumericFeatures,
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

std::span<const Variant> NamedVariants(
  CodecType type, const ColumnDataCheckpointData& data) noexcept {
  if (data.type.InternalType() != PhysicalType::VARCHAR || data.rows == 0) {
    return {};
  }
  switch (type) {
    case CodecType::DictLz4:
    case CodecType::DictZstd:
    case CodecType::DictZxc:
      return kNamedDictionaryFrames;
    default:
      return {};
  }
}

constexpr size_t kMostCandidates = 16;

struct Asked {
  const Candidate* row;
  const Codec* codec;
};

size_t Variants(const Asked& asked) noexcept {
  return asked.row->variants.empty() ? asked.codec->compression.size()
                                     : asked.row->variants.size();
}

struct Handed {
  const CompressionFunction& function;
  double price;

  double Analyze(ColumnDataCheckpointData& data, double bound) const {
    return price * function.analyze(data, bound / price);
  }

  double Estimate(ColumnDataCheckpointData& data) const {
    return price * function.estimate(data);
  }
};

Handed Hand(const Asked& asked, size_t i, uint8_t level,
            ColumnDataCheckpointData& data) {
  const auto variants = asked.row->variants;
  const auto variant = variants.empty()
                         ? Variant{.function = static_cast<uint8_t>(i)}
                         : variants[i];
  data.option = variant.option;
  data.params.compression_level = variant.level != 0 ? variant.level : level;
  return {asked.codec->compression[variant.function],
          asked.row->price * variant.price};
}

size_t LeastEstimated(const Asked& asked, uint8_t level, size_t held,
                      ColumnDataCheckpointData& data) {
  auto at = held;
  auto least = Hand(asked, held, level, data).Estimate(data);
  for (size_t i = 0; i < Variants(asked); ++i) {
    if (i == held) {
      continue;
    }
    const auto estimate = Hand(asked, i, level, data).Estimate(data);
    if (estimate < least) {
      least = estimate;
      at = i;
    }
  }
  return at;
}

double Analyze(const Asked& asked, uint8_t level, const Candidates& type,
               ColumnDataCheckpointData& data, const Features& features,
               double bound) {
  const auto codec = asked.codec->type;
  auto& hint = data.tuning.hints[std::to_underlying(codec)];
  const auto count = Variants(asked);
  const auto now = data.tuning.row_group;
  const auto input = type.input ? type.input(data) : 0;
  const bool every =
    AlwaysAsked(codec) || hint.refreshed == 0 ||
    (type.input ? input >= kMoreInput * hint.input
                : now - hint.refreshed > kOldHint ||
                    features[kStoredWidth] != hint.features[kStoredWidth]);
  const bool estimated = data.contested && count > 1 &&
                         asked.codec->compression.front().estimate != nullptr;
  SDB_ASSERT(hint.variant < count);
  size_t last = hint.variant;
  if (every && estimated) {
    last = LeastEstimated(asked, level, last, data);
  }
  auto best = bound;
  auto smallest = kNoScore;
  hint.variant = static_cast<uint8_t>(last);
  const auto ask = [&](size_t i) {
    const auto score = Hand(asked, i, level, data).Analyze(data, best);
    if (score < smallest) {
      smallest = score;
      hint.variant = static_cast<uint8_t>(i);
    }
    best = std::min(best, score);
  };
  ask(last);
  for (size_t i = 0; every && !estimated && i < count; ++i) {
    if (i != last) {
      ask(i);
    }
  }
  hint.features = features;
  hint.score = smallest;
  if (every) {
    hint.refreshed = now;
    hint.input = input;
  }
  return smallest;
}

bool Compress(const Asked& asked, uint8_t level,
              ColumnDataCheckpointData& data) {
  const auto variant =
    data.tuning.hints[std::to_underlying(asked.codec->type)].variant;
  return Hand(asked, variant, level, data)
    .function.compress(*asked.codec, data);
}

void Contest(ColumnDataCheckpointData& data, const Candidates& type) {
  const auto physical = data.type.InternalType();
  data.params.compression_level = 0;
  data.contested = true;
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
    const auto score = Analyze(candidate, 0, type, data, features, best);
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
  if (!winner || !Compress(*winner, 0, data)) {
    CompressUncompressed(data);
  }
}

}  // namespace

bool CompressAs(const Codec& codec, ColumnDataCheckpointData& data) {
  const auto physical = data.type.InternalType();
  const Candidate row{
    .codec = codec.type,
    .variants = NamedVariants(codec.type, data),
  };
  const Asked asked{&row, &codec};
  if (Variants(asked) == 1) {
    return codec.compression.front().compress(codec, data);
  }
  const auto level = data.params.compression_level;
  const auto& type = CandidatesOf(physical);
  const auto features = !AlwaysAsked(codec.type) && type.features
                          ? type.features(data)
                          : Features{};
  if (Analyze(asked, level, type, data, features, kNoScore) == kNoScore) {
    return false;
  }
  return Compress(asked, level, data);
}

void CompressAuto(ColumnDataCheckpointData& data) {
  Contest(data, CandidatesOf(data.type.InternalType()));
}

void CompressAutoCodes(ColumnDataCheckpointData& data) {
  ++data.tuning.row_group;
  Contest(data, kCodes);
}

}  // namespace irs::codecs
