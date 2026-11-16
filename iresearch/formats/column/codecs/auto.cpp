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

#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/tuning.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

constexpr CodecType kOrder[] = {
  CodecType::Uncompressed, CodecType::Rle,          CodecType::Bitpacking,
  CodecType::Alp,          CodecType::Alprd,        CodecType::Roaring,
  CodecType::Ffor,         CodecType::DeltaFfor,    CodecType::RleFfor,
  CodecType::DictFfor,     CodecType::Lz4,          CodecType::DeltaLz4,
  CodecType::RleLz4,       CodecType::DictLz4,      CodecType::Zstd,
  CodecType::DeltaZstd,    CodecType::RleZstd,      CodecType::DictZstd,
  CodecType::Fsst,         CodecType::Zxc,          CodecType::FsstPlus,
  CodecType::Onpair,       CodecType::Fsst12,       CodecType::DictFsst,
  CodecType::DictZxc,      CodecType::DictFsstPlus, CodecType::DictOnpair,
  CodecType::DictFsst12,
};

static_assert([] {
  std::array<int, magic_enum::enum_count<CodecType>()> listed{};
  for (const auto type : kOrder) {
    ++listed[std::to_underlying(type)];
  }
  for (const auto type : magic_enum::enum_values<CodecType>()) {
    const bool contested = type != CodecType::Auto &&
                           type != CodecType::Constant &&
                           type != CodecType::Empty;
    if (listed[std::to_underlying(type)] != (contested ? 1 : 0)) {
      return false;
    }
  }
  return true;
}());

const CompressionFunction* Held(const ColumnDataCheckpointData& data,
                                std::span<const Contender> contenders) {
  const auto pick = std::to_underlying(data.tuning.reuse.pick);
  const auto it = absl::c_find_if(contenders, [&](const Contender& contender) {
    return contender.codecs.test(pick);
  });
  return it == contenders.end() ? nullptr : it->function;
}

bool CompressAuto(ColumnDataCheckpointData& data) {
  if (CompressConstant(data)) {
    return true;
  }
  const auto physical = data.type.InternalType();
  absl::InlinedVector<Contender, std::size(kOrder)> contenders;
  for (const auto type : kOrder) {
    if (data.disabled.test(std::to_underlying(type))) {
      continue;
    }
    const auto* codec = GetCodec(type, physical);
    if (!codec || !codec->compression) {
      continue;
    }
    auto it = absl::c_find_if(contenders, [&](const Contender& contender) {
      return contender.function == codec->compression;
    });
    if (it == contenders.end()) {
      it =
        contenders.insert(contenders.end(), Contender{codec->compression, {}});
    }
    it->codecs.set(std::to_underlying(type));
  }
  Contest(contenders, data);
  return true;
}

constexpr CompressionFunction kAuto{.compress = CompressAuto};

constexpr Codec kAutoCodec{.type = CodecType::Auto, .compression = &kAuto};

}  // namespace

void Contest(std::span<const Contender> contenders,
             ColumnDataCheckpointData& data) {
  auto& reuse = data.tuning.reuse;
  const auto input = static_cast<double>(RawInput(data));
  if (const auto* held = reuse.Due() ? nullptr : Held(data, contenders)) {
    const auto written = data.written;
    if (held->compress(data)) {
      if (reuse.Grew(static_cast<double>(data.written - written), input)) {
        reuse.Restart();
      }
      return;
    }
  }
  Estimate best;
  const CompressionFunction* winner = nullptr;
  for (const auto& [function, codecs] : contenders) {
    if (const auto estimate = function->analyze(data, best.score);
        estimate.score < best.score) {
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
  reuse.Settle(best.codec, static_cast<double>(bytes), input);
}

const Codec* AutoCodec(PhysicalType) { return &kAutoCodec; }

}  // namespace irs::codecs
