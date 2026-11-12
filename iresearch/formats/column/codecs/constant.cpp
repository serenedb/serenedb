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

#include <duckdb/common/types/hugeint.hpp>
#include <duckdb/common/types/uhugeint.hpp>
#include <duckdb/common/vector/constant_vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/function/compression/compression.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>

#include "iresearch/formats/column/codecs/registry.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;

void FillValidity(const ColumnSegment& segment, duckdb::Vector& result,
                  idx_t start, idx_t count) {
  if (!segment.stats.CanHaveNull()) {
    return;
  }
  auto& mask = duckdb::FlatVector::ValidityMutable(result);
  for (idx_t i = 0; i < count; ++i) {
    mask.SetInvalid(start + i);
  }
}

template<typename T>
void Fill(const ColumnSegment& segment, duckdb::Vector& result, idx_t start,
          idx_t count) {
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  const auto value = duckdb::NumericStats::GetMin<T>(segment.stats);
  for (idx_t i = 0; i < count; ++i) {
    data[start + i] = value;
  }
}

std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment&) {
  return nullptr;
}

void ScanPartialValidity(const ColumnSegment& segment, ColumnScanState&,
                         idx_t scan_count, duckdb::Vector& result,
                         idx_t result_offset) {
  FillValidity(segment, result, result_offset, scan_count);
}

template<typename T>
void ScanPartial(const ColumnSegment& segment, ColumnScanState&,
                 idx_t scan_count, duckdb::Vector& result,
                 idx_t result_offset) {
  Fill<T>(segment, result, result_offset, scan_count);
}

void ScanValidity(const ColumnSegment& segment, ColumnScanState&,
                  idx_t scan_count, duckdb::Vector& result) {
  if (!segment.stats.CanHaveNull()) {
    return;
  }
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
          duckdb::Vector& result) {
  auto* data = duckdb::FlatVector::GetDataMutable<T>(result);
  data[0] = duckdb::NumericStats::GetMin<T>(segment.stats);
  result.SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
  duckdb::FlatVector::SetSize(result, duckdb::count_t(scan_count));
}

void FetchRowsValidity(const ColumnSegment& segment, ColumnFetchState&,
                       const idx_t*, idx_t count, duckdb::Vector& result,
                       const duckdb::SelectionVector& result_sel,
                       idx_t result_offset) {
  for (idx_t i = 0; i < count; ++i) {
    FillValidity(segment, result, result_offset + result_sel.get_index(i), 1);
  }
}

template<typename T>
void FetchRows(const ColumnSegment& segment, ColumnFetchState&, const idx_t*,
               idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  for (idx_t i = 0; i < count; ++i) {
    Fill<T>(segment, result, result_offset + result_sel.get_index(i), 1);
  }
}

void SelectValidity(const ColumnSegment& segment, ColumnScanState& state, idx_t,
                    duckdb::Vector& result, idx_t result_offset,
                    const duckdb::SelectionVector&, idx_t sel_count,
                    duckdb::ScanVectorType scan_type) {
  if (scan_type == duckdb::ScanVectorType::SCAN_ENTIRE_VECTOR) {
    ScanValidity(segment, state, sel_count, result);
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
    Scan<T>(segment, state, vector_count, result);
    return;
  }
  Fill<T>(segment, result, result_offset, sel_count);
}

void FilterValidity(const ColumnSegment& segment, ColumnScanState& state,
                    idx_t vector_count, duckdb::Vector& result,
                    duckdb::SelectionVector&, idx_t& sel_count,
                    const duckdb::TableFilter& filter,
                    duckdb::TableFilterState& filter_state) {
  bool filters_nulls = false;
  bool filters_valid_values = false;
  duckdb::ConstantFun::FiltersNullValues(result.GetType(), filter,
                                         filters_nulls, filters_valid_values,
                                         filter_state);
  if (segment.stats.CanHaveNull() ? filters_nulls : filters_valid_values) {
    sel_count = 0;
    return;
  }
  ScanValidity(segment, state, vector_count, result);
}

constexpr CompressionFunction kValidity{
  .type = CodecType::Constant,
  .init_scan = InitScan,
  .scan_vector = ScanValidity,
  .scan_partial = ScanPartialValidity,
  .select = SelectValidity,
  .filter = FilterValidity,
  .fetch_rows = FetchRowsValidity,
  .skip = EmptySkip,
};

template<typename T, duckdb::PhysicalType kPhysical>
constexpr CompressionFunction kFunction{
  .type = CodecType::Constant,
  .init_scan = InitScan,
  .scan_vector = Scan<T>,
  .scan_partial = ScanPartial<T>,
  .select = Select<T>,
  .fetch_rows = FetchRows<T>,
  .skip = EmptySkip,
};

}  // namespace

const CompressionFunction* ConstantFunction(duckdb::PhysicalType physical) {
  using enum duckdb::PhysicalType;
  switch (physical) {
    case BIT:
      return &kValidity;
    case BOOL:
      return &kFunction<int8_t, BOOL>;
    case INT8:
      return &kFunction<int8_t, INT8>;
    case INT16:
      return &kFunction<int16_t, INT16>;
    case INT32:
      return &kFunction<int32_t, INT32>;
    case INT64:
      return &kFunction<int64_t, INT64>;
    case UINT8:
      return &kFunction<uint8_t, UINT8>;
    case UINT16:
      return &kFunction<uint16_t, UINT16>;
    case UINT32:
      return &kFunction<uint32_t, UINT32>;
    case UINT64:
      return &kFunction<uint64_t, UINT64>;
    case INT128:
      return &kFunction<duckdb::hugeint_t, INT128>;
    case UINT128:
      return &kFunction<duckdb::uhugeint_t, UINT128>;
    case FLOAT:
      return &kFunction<float, FLOAT>;
    case DOUBLE:
      return &kFunction<double, DOUBLE>;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
