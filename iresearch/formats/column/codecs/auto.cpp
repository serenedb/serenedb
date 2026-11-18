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

constexpr CodecType kLightweight[] = {
  CodecType::Uncompressed,
  CodecType::Rle,
  CodecType::Bitpacking,
  CodecType::Roaring,
};

constexpr CodecType kIntegerMerge[] = {
  CodecType::Uncompressed, CodecType::Bitpacking, CodecType::Ffor,
  CodecType::RleFfor,      CodecType::DictFfor,   CodecType::Lz4,
  CodecType::RleLz4,       CodecType::DictLz4,    CodecType::Zstd,
  CodecType::RleZstd,      CodecType::DictZstd,
};

constexpr CodecType kIntegerFlush[] = {
  CodecType::Uncompressed, CodecType::Bitpacking, CodecType::Ffor,
  CodecType::RleFfor,      CodecType::DictFfor,   CodecType::Lz4,
  CodecType::RleLz4,       CodecType::DictLz4,
};

constexpr CodecType kFloat[] = {
  CodecType::Uncompressed, CodecType::Rle,  CodecType::Alp,
  CodecType::Alprd,        CodecType::Ffor, CodecType::RleFfor,
  CodecType::DictFfor,
};

constexpr CodecType kNamedOnly[] = {
  CodecType::DeltaFfor,
  CodecType::DeltaLz4,
  CodecType::DeltaZstd,
};

constexpr CodecType kText[] = {
  CodecType::Uncompressed, CodecType::DictFsst, CodecType::DictLz4,
  CodecType::Fsst,         CodecType::Lz4,
};

struct Orders {
  std::span<const CodecType> flush;
  std::span<const CodecType> merge;
};

constexpr Orders kLightweightOrders{.flush = kLightweight,
                                    .merge = kLightweight};
constexpr Orders kIntegerOrders{.flush = kIntegerFlush, .merge = kIntegerMerge};
constexpr Orders kFloatOrders{.flush = kFloat, .merge = kFloat};
constexpr Orders kTextOrders{.flush = kText, .merge = kText};

const Orders& OrdersOf(PhysicalType physical) noexcept {
  switch (physical) {
    case PhysicalType::VARCHAR:
      return kTextOrders;
    case PhysicalType::FLOAT:
    case PhysicalType::DOUBLE:
      return kFloatOrders;
    case PhysicalType::INT8:
    case PhysicalType::INT16:
    case PhysicalType::INT32:
    case PhysicalType::INT64:
    case PhysicalType::UINT8:
    case PhysicalType::UINT16:
    case PhysicalType::UINT32:
    case PhysicalType::UINT64:
      return kIntegerOrders;
    default:
      return kLightweightOrders;
  }
}

static_assert([] {
  std::array<bool, magic_enum::enum_count<CodecType>()> listed{};
  for (const auto order :
       {kLightweightOrders.merge, kIntegerOrders.merge, kFloatOrders.merge,
        std::span<const CodecType>{kNamedOnly}}) {
    for (const auto type : order) {
      listed[std::to_underlying(type)] = true;
    }
  }
  for (const auto type : magic_enum::enum_values<CodecType>()) {
    const bool contested =
      type != CodecType::Auto && type != CodecType::Constant &&
      type != CodecType::Empty && (!IsText(type) || IsNumeric(type));
    if (listed[std::to_underlying(type)] != contested) {
      return false;
    }
  }
  return true;
}());

const CompressionFunction* FunctionOf(std::span<const Contender> contenders,
                                      CodecType codec) {
  const auto it = absl::c_find_if(contenders, [&](const Contender& contender) {
    return contender.codecs.test(std::to_underlying(codec));
  });
  return it == contenders.end() ? nullptr : it->function;
}

bool PackingUndercutsHeld(ColumnDataCheckpointData& data) {
  const auto& reuse = data.tuning.reuse;
  const auto input = static_cast<double>(RawInput(data));
  const auto one_bit_per_row = static_cast<double>(data.rows) / 8;
  if (!reuse.Undercuts(one_bit_per_row, input)) {
    return false;
  }
  const auto packed = PackedBytes(data);
  return packed != 0 && reuse.Undercuts(static_cast<double>(packed), input);
}

bool CompressHeld(const CompressionFunction& held,
                  ColumnDataCheckpointData& data) {
  const auto written = data.written;
  if (!held.compress(data)) {
    return false;
  }
  auto& reuse = data.tuning.reuse;
  if (reuse.Grew(static_cast<double>(data.written - written),
                 static_cast<double>(RawInput(data)))) {
    reuse.Restart();
  }
  return true;
}

void Decide(std::span<const Contender> contenders,
            ColumnDataCheckpointData& data) {
  Estimate best;
  const CompressionFunction* winner = nullptr;
  const auto analyze = [&](const CompressionFunction* function) {
    if (const auto estimate = function->analyze(data, best.score);
        estimate.score < best.score) {
      best = estimate;
      winner = function;
    }
  };
  const auto* held = FunctionOf(contenders, data.tuning.reuse.pick);
  if (held) {
    analyze(held);
  }
  for (const auto& [function, codecs] : contenders) {
    if (function != held) {
      analyze(function);
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
  data.tuning.reuse.Settle(best.codec, static_cast<double>(bytes),
                           static_cast<double>(RawInput(data)));
}

bool CompressAuto(ColumnDataCheckpointData& data) {
  if (CompressConstant(data)) {
    return true;
  }
  const auto physical = data.type.InternalType();
  absl::InlinedVector<Contender, std::size(kIntegerMerge)> contenders;
  const auto& orders = OrdersOf(physical);
  for (const auto type :
       data.params.tier == WriteTier::Flush ? orders.flush : orders.merge) {
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
  const auto* held = reuse.Due() ? nullptr : FunctionOf(contenders, reuse.pick);
  if (held && !PackingUndercutsHeld(data) && CompressHeld(*held, data)) {
    return;
  }
  Decide(contenders, data);
}

const Codec* AutoCodec(PhysicalType) { return &kAutoCodec; }

}  // namespace irs::codecs
