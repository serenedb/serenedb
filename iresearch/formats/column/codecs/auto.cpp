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
  CodecType::Uncompressed, CodecType::Ffor,     CodecType::DeltaFfor,
  CodecType::RleFfor,      CodecType::DictFfor, CodecType::Lz4,
  CodecType::RleLz4,       CodecType::DictLz4,  CodecType::Zstd,
  CodecType::RleZstd,      CodecType::DictZstd,
};

constexpr CodecType kIntegerFlush[] = {
  CodecType::Uncompressed, CodecType::Ffor,     CodecType::DeltaFfor,
  CodecType::RleFfor,      CodecType::DictFfor, CodecType::Lz4,
  CodecType::RleLz4,       CodecType::DictLz4,
};

constexpr CodecType kFloat[] = {
  CodecType::Uncompressed, CodecType::Rle,  CodecType::Alp,
  CodecType::Alprd,        CodecType::Ffor, CodecType::RleFfor,
  CodecType::DictFfor,
};

constexpr CodecType kNamedOnly[] = {
  CodecType::DeltaLz4,
  CodecType::DeltaZstd,
};

constexpr CodecType kText[] = {
  CodecType::Uncompressed, CodecType::DictFsst, CodecType::DictLz4,
  CodecType::Fsst,         CodecType::Lz4,
};

constexpr size_t kMostContenders = 40;

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

const CompressionFunction* Held(CompressionFunctions contenders,
                                const ReusePolicy& reuse) {
  return absl::c_linear_search(contenders, reuse.held) ? reuse.held : nullptr;
}

bool PackingUndercutsHeld(CompressionFunctions contenders,
                          ColumnDataCheckpointData& data) {
  if (absl::c_none_of(contenders, [](const CompressionFunction* contender) {
        return contender->codec == CodecType::Ffor;
      })) {
    return false;
  }
  const auto& reuse = data.tuning.reuse;
  const auto input = static_cast<double>(RawInput(data));
  const auto one_bit_per_row = static_cast<double>(data.rows) / 8;
  if (!reuse.Undercuts(one_bit_per_row, input)) {
    return false;
  }
  const auto packed = PackedBytes(data);
  return packed != 0 && reuse.Undercuts(static_cast<double>(packed), input);
}

bool Lz4UndercutsHeld(CompressionFunctions contenders,
                      const CompressionFunction& held,
                      ColumnDataCheckpointData& data) {
  if (!IsNumeric(held.codec) || LeafOf(held.codec) != ByteCodec::None ||
      absl::c_none_of(contenders, [](const CompressionFunction* contender) {
        return contender->codec == CodecType::Lz4;
      })) {
    return false;
  }
  const auto price =
    data.preferences->leaf_price[std::to_underlying(ByteCodec::Lz4)];
  const auto sampled = static_cast<double>(SampledLz4Bytes(data)) * price;
  return sampled != 0 && data.tuning.reuse.Undercuts(
                           sampled, static_cast<double>(RawInput(data)));
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

bool OneCodec(CompressionFunctions contenders) {
  return absl::c_all_of(contenders, [&](const CompressionFunction* contender) {
    return contender->codec == contenders.front()->codec;
  });
}

void Decide(CompressionFunctions contenders, bool timed,
            ColumnDataCheckpointData& data) {
  auto& reuse = data.tuning.reuse;
  const auto* held = Held(contenders, reuse);
  auto best = kNoScore;
  const CompressionFunction* winner = nullptr;
  const auto analyze = [&](const CompressionFunction* contender) {
    if (const auto score = contender->analyze(data, best); score < best) {
      best = score;
      winner = contender;
    }
  };
  for (const auto* contender : contenders) {
    if (held && contender->codec == held->codec) {
      analyze(contender);
    }
  }
  for (const auto* contender : contenders) {
    if (!held || contender->codec != held->codec) {
      analyze(contender);
    }
  }
  const auto written = data.written;
  if (!winner || !winner->compress(data)) {
    winner = &UncompressedCompression(data.type.InternalType());
    winner->compress(data);
  }
  const auto bytes = data.written - written;
  if (bytes == 0) {
    return;
  }
  reuse.Settle(
    winner,
    !timed || winner == reuse.held || (held && held->codec == winner->codec),
    static_cast<double>(bytes), static_cast<double>(RawInput(data)));
}

bool CompressAuto(ColumnDataCheckpointData& data) {
  if (CompressConstant(data)) {
    return true;
  }
  const auto physical = data.type.InternalType();
  const bool flush = data.params.tier == WriteTier::Flush;
  data.preferences = flush ? &kFlushPreferences : &kMergePreferences;
  data.params.compression_level = 0;
  absl::InlinedVector<const CompressionFunction*, kMostContenders> contenders;
  const auto& orders = OrdersOf(physical);
  for (const auto type : flush ? orders.flush : orders.merge) {
    if (data.disabled.test(std::to_underlying(type))) {
      continue;
    }
    const auto* codec = GetCodec(type, physical);
    if (!codec || !codec->compression) {
      continue;
    }
    if (const auto own = Contenders(type, physical); !own.empty()) {
      contenders.insert(contenders.end(), own.begin(), own.end());
      continue;
    }
    SDB_ASSERT(codec->compression->analyze);
    SDB_ASSERT(codec->compression->codec == type);
    SDB_ASSERT(!absl::c_linear_search(contenders, codec->compression));
    contenders.push_back(codec->compression);
  }
  Contest(contenders, data);
  return true;
}

constexpr CompressionFunction kAuto{.compress = CompressAuto};

constexpr Codec kAutoCodec{.type = CodecType::Auto, .compression = &kAuto};

}  // namespace

void Contest(CompressionFunctions contenders, ColumnDataCheckpointData& data) {
  auto& reuse = data.tuning.reuse;
  const bool timed = !OneCodec(contenders);
  const bool due = reuse.Due() && (timed || reuse.gap == 1);
  const auto* held = due ? nullptr : Held(contenders, reuse);
  bool probed = false;
  if (held && !PackingUndercutsHeld(contenders, data)) {
    probed = !reuse.probed && Lz4UndercutsHeld(contenders, *held, data);
    if (!probed && CompressHeld(*held, data)) {
      return;
    }
  }
  Decide(contenders, timed, data);
  reuse.probed = probed && reuse.held == held;
}

const Codec* AutoCodec(PhysicalType) { return &kAutoCodec; }

}  // namespace irs::codecs
