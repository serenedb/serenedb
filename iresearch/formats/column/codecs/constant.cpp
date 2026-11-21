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

#include <algorithm>
#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/storage/compression/roaring/roaring.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>

#include "iresearch/formats/column/codecs/numeric_axis.hpp"
#include "iresearch/formats/column/codecs/registry.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;

void FillValidity(const ColumnSegment& segment, duckdb::Vector& result,
                  idx_t start, idx_t count) {
  SDB_ASSERT(segment.stats.CanHaveNull());
  if (count != 0) {
    duckdb::roaring::SetInvalidRange(
      duckdb::FlatVector::ValidityMutable(result), start, start + count);
  }
}

template<typename T>
void Fill(const ColumnSegment& segment, duckdb::Vector& result, idx_t start,
          idx_t count) {
  std::fill_n(duckdb::FlatVector::GetDataMutable<T>(result) + start, count,
              duckdb::NumericStats::GetMinUnsafe<T>(segment.stats));
}

std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment&) {
  return nullptr;
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

template<typename T>
void Scan(const ColumnSegment& segment, ColumnScanState&, idx_t scan_count,
          duckdb::Vector& result, idx_t result_offset,
          duckdb::ScanVectorType scan_type) {
  if (scan_type != duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    Fill<T>(segment, result, result_offset, scan_count);
    return;
  }
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  data[0] = duckdb::NumericStats::GetMinUnsafe<T>(segment.stats);
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t(scan_count));
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

template<typename T>
void FetchRows(const ColumnSegment& segment, ColumnFetchState&, const idx_t*,
               idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  const auto value = duckdb::NumericStats::GetMinUnsafe<T>(segment.stats);
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  for (idx_t i = 0; i < count; ++i) {
    data[result_offset + result_sel.get_index(i)] = value;
  }
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

template<typename T>
void Select(const ColumnSegment& segment, ColumnScanState& state,
            idx_t vector_count, duckdb::Vector& result, idx_t result_offset,
            const duckdb::SelectionVector&, idx_t sel_count,
            duckdb::ScanVectorType scan_type) {
  if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    Scan<T>(segment, state, vector_count, result, 0, scan_type);
    return;
  }
  Fill<T>(segment, result, result_offset, sel_count);
}

constexpr DecompressionFunction kValidityDecompression{
  .init_scan = InitScan,
  .scan = ScanValidity,
  .select = SelectValidity,
  .fetch_rows = FetchRowsValidity,
};

constexpr Codec kValidity{
  .type = CodecType::Constant,
  .decompression = &kValidityDecompression,
};

template<typename T>
constexpr DecompressionFunction kDecompression{
  .init_scan = InitScan,
  .scan = Scan<T>,
  .select = Select<T>,
  .fetch_rows = FetchRows<T>,
};

struct Rows final : State {
  static constexpr StateId kId = StateId::Constant;

  explicit Rows(ColumnDataCheckpointData& data)
    : stats{duckdb::BaseStatistics::CreateEmpty(data.type)} {
    if (data.rows == 0 ||
        stats.GetStatsType() != duckdb::StatisticsType::NUMERIC_STATS) {
      return;
    }
    constant =
      Resolve<NumericAxis>(data.type.InternalType(), [&]<typename T>() {
        for (const auto& chunk : data.chunks) {
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
      });
  }

  duckdb::BaseStatistics stats;
  bool constant = false;
};

constexpr CompressionFunction kFunction{
  .analyze = [](ColumnDataCheckpointData& data,
                double) { return data.Get<Rows>().constant ? 0.0 : kNoScore; },
  .compress =
    [](ColumnDataCheckpointData& data) {
      auto& rows = data.Get<Rows>();
      if (!rows.constant) {
        return false;
      }
      data.FlushSegment(CodecType::Constant, std::move(rows.stats), data.rows,
                        {});
      return true;
    },
};

template<typename T>
constexpr Codec kCodec{
  .type = CodecType::Constant,
  .compression = &kFunction,
  .decompression = &kDecompression<T>,
};

}  // namespace

const Codec* ConstantCodec(duckdb::PhysicalType physical) {
  if (physical == duckdb::PhysicalType::BIT) {
    return &kValidity;
  }
  if (physical == duckdb::PhysicalType::LIST) {
    return nullptr;
  }
  return Resolve<NumericAxis>(
    physical, [&]<typename T>() -> const Codec* { return &kCodec<T>; });
}

}  // namespace irs::codecs
