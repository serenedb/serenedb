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
#include <vector>

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

constexpr CodecType kOrder[] = {
  CodecType::Uncompressed, CodecType::Rle,       CodecType::Bitpacking,
  CodecType::Alp,          CodecType::Alprd,     CodecType::Roaring,
  CodecType::Ffor,         CodecType::DeltaFfor, CodecType::RleFfor,
  CodecType::DictFfor,     CodecType::Lz4,       CodecType::DeltaLz4,
  CodecType::RleLz4,       CodecType::DictLz4,   CodecType::Zstd,
  CodecType::DeltaZstd,    CodecType::RleZstd,   CodecType::DictZstd,
};

struct Candidate {
  const CompressionFunction* function;
  CodecSet codecs;
};

const std::vector<Candidate>& CandidatesOf(PhysicalType physical) {
  static const auto kCandidates = [] {
    std::array<std::vector<Candidate>, 256> all;
    for (size_t index = 0; index < all.size(); ++index) {
      auto& list = all[index];
      const auto add = [&](CodecType type) {
        const auto* codec = GetCodec(type, static_cast<PhysicalType>(index));
        if (type == CodecType::Auto || !codec || !codec->compression) {
          return;
        }
        auto it = absl::c_find_if(list, [&](const Candidate& candidate) {
          return candidate.function == codec->compression;
        });
        if (it == list.end()) {
          it = list.insert(list.end(), Candidate{codec->compression, {}});
        }
        it->codecs.set(std::to_underlying(type));
      };
      for (const auto type : kOrder) {
        add(type);
      }
      for (const auto type : magic_enum::enum_values<CodecType>()) {
        add(type);
      }
    }
    return all;
  }();
  return kCandidates[std::to_underlying(physical)];
}

const CompressionFunction* Held(
  const ColumnDataCheckpointData& data,
  std::span<const CompressionFunction* const> functions) {
  const auto pick = data.tuning->reuse.pick;
  if (data.disabled.test(std::to_underlying(pick))) {
    return nullptr;
  }
  const auto* codec = GetCodec(pick, data.type.InternalType());
  SDB_ASSERT(codec && codec->compression);
  return absl::c_linear_search(functions, codec->compression)
           ? codec->compression
           : nullptr;
}

bool CompressAuto(ColumnDataCheckpointData& data) {
  if (CompressConstant(data)) {
    return true;
  }
  absl::InlinedVector<const CompressionFunction*,
                      magic_enum::enum_count<CodecType>()>
    enabled;
  for (const auto& [function, codecs] : CandidatesOf(data.type.InternalType())) {
    if ((codecs & ~data.disabled).any()) {
      enabled.push_back(function);
    }
  }
  Contest(enabled, data);
  return true;
}

constexpr CompressionFunction kAuto{.compress = CompressAuto};

constexpr Codec kAutoCodec{.type = CodecType::Auto, .compression = &kAuto};

}  // namespace

void Contest(std::span<const CompressionFunction* const> functions,
             ColumnDataCheckpointData& data) {
  SDB_ASSERT(data.tuning);
  auto& reuse = data.tuning->reuse;
  const auto input = static_cast<double>(std::max<uint64_t>(data.rows, 1));
  if (const auto* held = reuse.Due() ? nullptr : Held(data, functions)) {
    const auto written = data.written;
    if (held->compress(data)) {
      if (reuse.Drifted(static_cast<double>(data.written - written), input,
                        true)) {
        reuse.clock.Restart();
      }
      return;
    }
  }
  Estimate best;
  const CompressionFunction* winner = nullptr;
  for (const auto* function : functions) {
    if (const auto estimate = function->analyze(data, best.Score());
        estimate.Score() < best.Score()) {
      best = estimate;
      winner = function;
    }
  }
  const auto written = data.written;
  if (!winner || !winner->compress(data)) {
    winner = &UncompressedCompression(data.type.InternalType());
    best = {.codec = CodecType::Uncompressed};
    winner->compress(data);
  }
  const auto bytes = data.written - written;
  if (bytes == 0) {
    return;
  }
  reuse.Settle(best.codec, reuse.pick == best.codec, static_cast<double>(bytes),
               input);
}

const Codec* AutoCodec(PhysicalType) { return &kAutoCodec; }

}  // namespace irs::codecs
