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
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/compression/roaring/roaring.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <duckdb/storage/statistics/stats_writer.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <type_traits>

#include "iresearch/formats/column/codecs/codecs.hpp"
#include "iresearch/formats/column/codecs/numeric_axis.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;

template<typename T>
inline constexpr bool kStored = std::is_same_v<T, duckdb::string_t>;

struct Stored final : SegmentScanState {
  explicit Stored(duckdb::BufferHandle pinned) : bytes{std::move(pinned)} {}

  duckdb::BufferHandle bytes;
};

std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment& segment) {
  if (segment.size == 0) {
    return nullptr;
  }
  return std::make_unique<Stored>(segment.Pin());
}

const duckdb::BufferHandle* Bytes(ColumnScanState& state) noexcept {
  return state.scan_state ? &state.scan_state->Cast<Stored>().bytes : nullptr;
}

template<typename T>
T ValueOf(const ColumnSegment& segment, const duckdb::BufferHandle* bytes,
          duckdb::Vector& result) {
  if constexpr (kStored<T>) {
    if (!bytes) {
      return T{};
    }
    const T value{duckdb::const_char_ptr_cast(bytes->Ptr()),
                  static_cast<uint32_t>(segment.size)};
    return value.IsInlined()
             ? value
             : duckdb::StringVector::AddStringOrBlob(result, value);
  } else {
    return duckdb::NumericStats::GetMinUnsafe<T>(segment.stats);
  }
}

template<typename T>
void Scan(const ColumnSegment& segment, ColumnScanState& state,
          idx_t scan_count, duckdb::Vector& result, idx_t result_offset,
          duckdb::ScanVectorType scan_type) {
  const auto value = ValueOf<T>(segment, Bytes(state), result);
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  if (scan_type != duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    std::fill_n(data + result_offset, scan_count, value);
    return;
  }
  data[0] = value;
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t(scan_count));
}

template<typename T>
void Select(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result, idx_t result_offset,
            const duckdb::SelectionVector&, idx_t sel_count,
            duckdb::ScanVectorType scan_type) {
  Scan<T>(segment, state,
          scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR ? vector_count
                                                                  : sel_count,
          result, result_offset, scan_type);
}

template<typename T>
void FetchRows(const ColumnSegment& segment, ColumnFetchState& state,
               const idx_t*, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  const auto value = ValueOf<T>(
    segment, segment.size == 0 ? nullptr : &state.GetOrInsertHandle(segment),
    result);
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  for (idx_t i = 0; i < count; ++i) {
    data[result_offset + result_sel.get_index(i)] = value;
  }
}

template<typename T>
void Filter(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result,
            duckdb::SelectionVector&, idx_t& sel_count,
            const duckdb::TableFilter&,
            duckdb::TableFilterState& filter_state) {
  Scan<T>(segment, state, vector_count, result, 0,
          duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR);
  duckdb::SelectionVector accepted;
  idx_t accepted_count = 1;
  duckdb::ColumnSegment::FilterSelection(accepted, result, filter_state, 1,
                                         accepted_count);
  if (accepted_count == 0) {
    sel_count = 0;
  }
}

template<typename T>
constexpr DecompressionFunction kDecompression{
  .init_scan = InitScan,
  .scan = Scan<T>,
  .select = Select<T>,
  .filter = Filter<T>,
  .fetch_rows = FetchRows<T>,
};

void FillValidity(const ColumnSegment& segment, duckdb::Vector& result,
                  idx_t start, idx_t count) {
  SDB_ASSERT(segment.stats.CanHaveNull());
  if (count != 0) {
    duckdb::roaring::SetInvalidRange(
      duckdb::FlatVector::ValidityMutable(result), start, start + count);
  }
}

void ScanValidity(const ColumnSegment& segment, ColumnScanState&,
                  idx_t scan_count, duckdb::Vector& result, idx_t result_offset,
                  duckdb::ScanVectorType scan_type) {
  if (scan_type != duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    FillValidity(segment, result, result_offset, scan_count);
    return;
  }
  SDB_ASSERT(segment.stats.CanHaveNull());
  if (result.GetType().InternalType() == duckdb::PhysicalType::STRUCT ||
      result.GetVectorType() == duckdb::VectorType::CONSTANT_VECTOR) {
    duckdb::ConstantVector::SetNull(result, duckdb::count_t(scan_count));
    return;
  }
  result.Flatten();
  FillValidity(segment, result, 0, scan_count);
}

void SelectValidity(const ColumnSegment& segment, ColumnScanState& state, idx_t,
                    duckdb::Vector& result, idx_t result_offset,
                    const duckdb::SelectionVector&, idx_t sel_count,
                    duckdb::ScanVectorType scan_type) {
  if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    ScanValidity(segment, state, sel_count, result, 0, scan_type);
    return;
  }
  FillValidity(segment, result, result_offset, sel_count);
}

void FetchRowsValidity(const ColumnSegment& segment, ColumnFetchState&,
                       const idx_t*, idx_t count, duckdb::Vector& result,
                       const duckdb::SelectionVector& result_sel,
                       idx_t result_offset) {
  SDB_ASSERT(segment.stats.CanHaveNull());
  auto& mask = duckdb::FlatVector::ValidityMutable(result);
  for (idx_t i = 0; i < count; ++i) {
    mask.SetInvalid(result_offset + result_sel.get_index(i));
  }
}

constexpr DecompressionFunction kValidityDecompression{
  .init_scan = InitScan,
  .scan = ScanValidity,
  .select = SelectValidity,
  .fetch_rows = FetchRowsValidity,
};

bool CompressValidity(const Codec& codec, ColumnDataCheckpointData& data) {
  if (data.rows == 0 ||
      absl::c_any_of(data.chunks, [](const WriteChunk& chunk) {
        return duckdb::FlatVector::Validity(chunk.data)
                 .CountValid(chunk.data.size()) != 0;
      })) {
    return false;
  }
  auto stats = duckdb::BaseStatistics::CreateEmpty(data.type);
  stats.SetHasNullFast();
  data.flush(codec, std::move(stats), data.rows, {}, {});
  return true;
}

constexpr CompressionFunction kValidityFunction{.compress = CompressValidity};

constexpr Codec kValidity{
  .type = CodecType::Constant,
  .compression = kValidityFunction,
  .decompression = kValidityDecompression,
};

template<typename T>
bool EveryValueIsEqual(std::span<const WriteChunk> chunks,
                       duckdb::BaseStatistics& stats, std::string_view& bytes) {
  if constexpr (kStored<T>) {
    duckdb::StatsWriter<T> writer{stats.GetType()};
    const T* first = nullptr;
    for (const auto& chunk : chunks) {
      for (const auto entry : chunk.data.Values<T>()) {
        if (!entry.IsValid()) {
          return false;
        }
        const auto& value = entry.GetValueUnsafe();
        if (!first) {
          first = &value;
          writer.Update(value);
        } else if (value == *first) {
          writer.UpdateRepeated(value);
        } else {
          return false;
        }
      }
    }
    SDB_ASSERT(first);
    writer.Merge(stats);
    bytes = {first->GetData(), first->GetSize()};
    return true;
  } else {
    if (stats.GetStatsType() != duckdb::StatisticsType::NUMERIC_STATS) {
      return false;
    }
    for (const auto& chunk : chunks) {
      for (const auto value : chunk.data.Values<T>()) {
        if (!value.IsValid()) {
          stats.SetHasNullFast();
          continue;
        }
        stats.SetHasNoNullFast();
        stats.UpdateNumericStats<T>(value.GetValue());
      }
      if (!duckdb::NumericStats::IsConstant(stats)) {
        return false;
      }
    }
    return true;
  }
}

bool CompressRows(const Codec& codec, ColumnDataCheckpointData& data) {
  if (data.rows == 0) {
    return false;
  }
  auto stats = duckdb::BaseStatistics::CreateEmpty(data.type);
  std::string_view bytes;
  const auto physical = data.type.InternalType();
  const bool constant =
    physical == duckdb::PhysicalType::VARCHAR
      ? EveryValueIsEqual<duckdb::string_t>(data.chunks, stats, bytes)
      : Resolve<NumericAxis>(physical, [&]<typename T>() {
          return EveryValueIsEqual<T>(data.chunks, stats, bytes);
        });
  if (!constant) {
    return false;
  }
  data.flush(codec, std::move(stats), data.rows, bytes, {});
  return true;
}

constexpr CompressionFunction kFunction{.compress = CompressRows};

template<typename T>
constexpr Codec kCodec{
  .type = CodecType::Constant,
  .compression = kFunction,
  .decompression = kDecompression<T>,
};

}  // namespace

const Codec* ConstantCodec(duckdb::PhysicalType physical) {
  switch (physical) {
    case duckdb::PhysicalType::BIT:
      return &kValidity;
    case duckdb::PhysicalType::VARCHAR:
      return &kCodec<duckdb::string_t>;
    default:
      return Resolve<NumericAxis>(
        physical, [&]<typename T>() -> const Codec* { return &kCodec<T>; });
  }
}

}  // namespace irs::codecs
