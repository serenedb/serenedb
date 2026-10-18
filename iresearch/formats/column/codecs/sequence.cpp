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

#include <duckdb/common/helper.hpp>
#include <duckdb/storage/buffer_manager.hpp>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <duckdb/storage/table/column_segment.hpp>
#include <duckdb/storage/table/scan_state.hpp>

#include "iresearch/formats/column/codecs/compression.hpp"

/*
Data layout per segment, the list codes of a LIST column's row group when
every row holds a list not seen before:
+------------------------------------------------------+
|                     First value                      |
|   +----------------------------------------------+   |
|   |   uint64_t first                             |   |
|   +----------------------------------------------+   |
|   row i holds first + i                              |
+------------------------------------------------------+
*/

namespace irs::codecs {
namespace {

using duckdb::idx_t;

uint64_t LoadFirst(duckdb::ColumnSegment& segment) {
  auto& buffer_manager =
    duckdb::BufferManager::GetBufferManager(segment.GetDatabase());
  auto handle = buffer_manager.Pin(segment.GetBlockHandle());
  return duckdb::Load<uint64_t>(handle.Ptr() + segment.GetBlockOffset());
}

struct ScanState final : duckdb::SegmentScanState {
  explicit ScanState(uint64_t first) noexcept : first{first} {}

  uint64_t first;
};

uint64_t First(duckdb::ColumnScanState& state) {
  return state.scan_state->Cast<ScanState>().first;
}

duckdb::unique_ptr<duckdb::SegmentScanState> InitScan(
  const duckdb::QueryContext&, duckdb::ColumnSegment& segment) {
  return duckdb::make_uniq<ScanState>(LoadFirst(segment));
}

void ScanPartial(duckdb::ColumnSegment&, duckdb::ColumnScanState& state,
                 idx_t scan_count, duckdb::Vector& result,
                 idx_t result_offset) {
  const uint64_t start = First(state) + state.GetPositionInSegment();
  auto* out = duckdb::FlatVector::GetDataMutable<uint64_t>(result);
  for (idx_t i = 0; i < scan_count; ++i) {
    out[result_offset + i] = start + i;
  }
}

void ScanVector(duckdb::ColumnSegment& segment, duckdb::ColumnScanState& state,
                idx_t scan_count, duckdb::Vector& result) {
  ScanPartial(segment, state, scan_count, result, 0);
}

void Select(duckdb::ColumnSegment&, duckdb::ColumnScanState& state, idx_t,
            duckdb::Vector& result, const duckdb::SelectionVector& sel,
            idx_t sel_count) {
  const uint64_t start = First(state) + state.GetPositionInSegment();
  auto* out = duckdb::FlatVector::GetDataMutable<uint64_t>(result);
  for (idx_t i = 0; i < sel_count; ++i) {
    out[i] = start + sel.get_index(i);
  }
}

void FetchRows(duckdb::ColumnSegment& segment, duckdb::ColumnFetchState& state,
               const idx_t* rows, idx_t count, duckdb::Vector& result,
               const duckdb::SelectionVector& result_sel, idx_t result_offset) {
  auto& handle = state.GetOrInsertHandle(segment);
  const auto first =
    duckdb::Load<uint64_t>(handle.Ptr() + segment.GetBlockOffset());
  auto* data = duckdb::FlatVector::GetDataMutable<uint64_t>(result);
  for (idx_t i = 0; i < count; ++i) {
    data[result_offset + result_sel.get_index(i)] =
      first + static_cast<uint64_t>(rows[i]);
  }
}

duckdb::CompressionFunction MakeFunction(duckdb::PhysicalType physical) {
  duckdb::CompressionFunction f{
    duckdb::CompressionType::COMPRESSION_COL_SEQUENCE,
    physical,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    nullptr,
    InitScan,
    ScanVector,
    ScanPartial,
    FetchRows,
    duckdb::UncompressedFunctions::EmptySkip,
  };
  f.select = Select;
  return f;
}

}  // namespace

const duckdb::CompressionFunction* SequenceFunction(
  duckdb::PhysicalType physical) {
  static const auto list = MakeFunction(duckdb::PhysicalType::LIST);
  static const auto ubigint = MakeFunction(duckdb::PhysicalType::UINT64);
  switch (physical) {
    case duckdb::PhysicalType::LIST:
      return &list;
    case duckdb::PhysicalType::UINT64:
      return &ubigint;
    default:
      return nullptr;
  }
}

}  // namespace irs::codecs
