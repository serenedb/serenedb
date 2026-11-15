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

#include <algorithm>
#include <duckdb/common/types.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <span>
#include <utility>

#include "iresearch/formats/column/codecs/numeric/numeric.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/formats/column/codecs/text/string_seal.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::PhysicalType;

struct Analyzed {
  const CompressionFunction* function = nullptr;
  duckdb::idx_t bytes = kNotAnalyzed;
};

Analyzed Cheapest(const ColumnDataCheckpointData& data,
                  std::span<const WriteChunk> chunks) {
  Analyzed best;
  const auto physical = data.type.InternalType();
  for (const auto& codec : AnalyzedCodecs()) {
    const auto* candidate = EnabledCodec(codec.type, physical, data.disabled);
    if (!candidate) {
      continue;
    }
    const auto bytes = candidate->analyze(chunks);
    if (bytes < best.bytes) {
      best = {candidate, bytes};
    }
  }
  SDB_ENSURE(best.function,
             "column writer: no codec accepted the row group for ",
             data.type.ToString());
  return best;
}

uint64_t Compress(const CompressionFunction& function,
                  ColumnDataCheckpointData& data,
                  std::span<const WriteChunk> chunks) {
  const auto written = data.written;
  data.function = &function;
  function.compress(data, chunks);
  return data.written - written;
}

void CompressCheapest(ColumnDataCheckpointData& data,
                      std::span<const WriteChunk> chunks) {
  Compress(*Cheapest(data, chunks).function, data, chunks);
}

void CompressNumbers(ColumnDataCheckpointData& data,
                     std::span<const WriteChunk> chunks) {
  if (data.disabled.test(std::to_underlying(CodecType::Numeric))) {
    CompressCheapest(data, chunks);
    return;
  }
  SDB_ASSERT(data.tuning);
  const auto& type = data.type;
  const auto physical = type.InternalType();
  const auto row_count = CountRows(chunks);
  auto& tuning = data.tuning->numeric;
  auto& reuse = tuning.reuse;
  const bool merge = data.params.tier == WriteTier::Merge;
  const bool due = reuse.Due();
  auto stats = duckdb::BaseStatistics::CreateEmpty(type);
  if ((merge || !due) && absl::c_all_of(chunks, [&](const WriteChunk& c) {
        return UpdateConstant(c.data, stats);
      })) {
    if (merge) {
      reuse.clock.Restart();
    }
    data.function = GetCodec(CodecType::Constant, physical);
    data.FlushSegment(std::move(stats), row_count, {});
    return;
  }
  const auto analyze = [&] {
    const auto cheapest = Cheapest(data, chunks);
    tuning.analyzed = cheapest.function->type;
    return cheapest;
  };

  Analyzed analyzed;
  bool search = due;
  if (const bool reused = !due && !reuse.pick; reused) {
    const auto rows = static_cast<double>(std::max<uint64_t>(row_count, 1));
    const auto* held =
      merge || tuning.analyzed == CodecType::Auto
        ? nullptr
        : EnabledCodec(tuning.analyzed, physical, data.disabled);
    if (held && Accepts(*held, chunks)) {
      const auto bytes = Compress(*held, data, chunks);
      if (reuse.Grew(static_cast<double>(bytes), rows)) {
        reuse.clock.Restart();
      }
      return;
    }
    const auto kept = tuning.analyzed;
    analyzed = analyze();
    if (merge && analyzed.function->type == kept) {
      if (reuse.Grew(static_cast<double>(analyzed.bytes), rows)) {
        reuse.clock.Restart();
      }
      Compress(*analyzed.function, data, chunks);
      return;
    }
    search = true;
  } else if (due || merge) {
    analyzed = analyze();
  }
  auto segment = SealNumeric(type, chunks, row_count, data.params, tuning,
                             search, analyzed.bytes, [&] {
                               analyzed = analyze();
                               return analyzed.bytes;
                             });
  if (segment.bytes.empty()) {
    Compress(*analyzed.function, data, chunks);
    return;
  }
  data.function = GetCodec(CodecType::Numeric, physical);
  data.FlushSegment(std::move(segment.stats), row_count, segment.bytes);
}

void CompressValidity(ColumnDataCheckpointData& data,
                      std::span<const WriteChunk> chunks) {
  const auto physical = data.type.InternalType();
  const auto* roaring =
    EnabledCodec(CodecType::Roaring, physical, data.disabled);
  if (!roaring) {
    CompressCheapest(data, chunks);
    return;
  }
  const auto* bitmap = GetCodec(CodecType::Uncompressed, physical);
  SDB_ASSERT(bitmap);
  data.function = roaring;
  if (!CompressRoaringValidity(data, chunks, bitmap->analyze(chunks))) {
    Compress(*bitmap, data, chunks);
  }
}

constexpr CompressionFunction kCheapest{
  .type = CodecType::Auto,
  .compress = CompressCheapest,
};

constexpr CompressionFunction kNumbers{
  .type = CodecType::Auto,
  .compress = CompressNumbers,
};

constexpr CompressionFunction kValidity{
  .type = CodecType::Auto,
  .compress = CompressValidity,
};

constexpr CompressionFunction kStrings{
  .type = CodecType::Auto,
  .compress = CompressAutoStrings,
};

}  // namespace

bool Accepts(const CompressionFunction& function,
             std::span<const WriteChunk> chunks) {
  return !function.accepts || function.accepts(chunks);
}

const CompressionFunction* EnabledCodec(CodecType type, PhysicalType physical,
                                        const CodecSet& disabled) {
  if (disabled.test(std::to_underlying(type))) {
    return nullptr;
  }
  const auto* function = GetCodec(type, physical);
  return function && function->analyze ? function : nullptr;
}

const CompressionFunction& CheapestFunction() noexcept { return kCheapest; }

const CompressionFunction* AutoFunction(PhysicalType physical) {
  switch (physical) {
    case PhysicalType::VARCHAR:
      return &kStrings;
    case PhysicalType::BIT:
      return &kValidity;
    default:
      return NumericFunction(physical) ? &kNumbers : &kCheapest;
  }
}

}  // namespace irs::codecs
