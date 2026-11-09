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

#include "iresearch/formats/column/codecs/registry.hpp"

namespace irs::codecs {
namespace {

using duckdb::idx_t;

struct EmptyScan final : SegmentScanState {};

std::unique_ptr<SegmentScanState> InitScan(const ColumnSegment&) {
  return std::make_unique<EmptyScan>();
}

void Scan(const ColumnSegment&, ColumnScanState&, idx_t, duckdb::Vector&) {}

void ScanPartial(const ColumnSegment&, ColumnScanState&, idx_t, duckdb::Vector&,
                 idx_t) {}

void Select(const ColumnSegment&, ColumnScanState&, idx_t, duckdb::Vector&,
            const duckdb::SelectionVector&, idx_t) {}

void Filter(const ColumnSegment&, ColumnScanState&, idx_t, duckdb::Vector&,
            duckdb::SelectionVector&, idx_t&, const duckdb::TableFilter&,
            duckdb::TableFilterState&) {}

void FetchRows(const ColumnSegment&, ColumnFetchState&, const idx_t*, idx_t,
               duckdb::Vector&, const duckdb::SelectionVector&, idx_t) {}

constexpr CompressionFunction kFunction{
  .type = duckdb::CompressionType::COMPRESSION_EMPTY,
  .data_type = duckdb::PhysicalType::BIT,
  .init_scan = InitScan,
  .scan_vector = Scan,
  .scan_partial = ScanPartial,
  .select = Select,
  .filter = Filter,
  .fetch_rows = FetchRows,
  .skip = EmptySkip,
};

}  // namespace

const CompressionFunction& EmptyValidityFunction() { return kFunction; }

}  // namespace irs::codecs
