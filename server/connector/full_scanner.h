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

#pragma once

#include <absl/functional/function_ref.h>

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <duckdb/storage/statistics/base_statistics.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/column_extract.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/table_filter_iterator.hpp>
#include <memory>
#include <span>
#include <vector>

namespace sdb::connector {

struct MultiColumnCheck {
  const duckdb::ExpressionFilter* filter;
  std::vector<const irs::ColumnReader*> readers;
};

class FullScanner {
 public:
  FullScanner(const irs::ColReader& reader,
              std::span<const irs::ColumnstoreProjection> projections,
              std::span<const irs::ColFilterSpec> filters,
              std::span<const MultiColumnCheck> multi,
              duckdb::ClientContext* context, irs::ColFilterStateCache& states);

  FullScanner(const FullScanner&) = delete;
  FullScanner& operator=(const FullScanner&) = delete;

  // The exclusive end row this scanner has advanced to (max start_row + count
  // over Filter calls). The column cursors only move forward, so a Filter
  // below this row is invalid -- the caller rebuilds the scanner instead (see
  // ColScanLocalState::StartUnit, where a scan order can hand out units of one
  // segment out of row order).
  uint64_t ScannedEnd() const noexcept { return _scanned_end; }

  // Zonemap skip for the bulk loop: no row before the returned end can pass
  // the pushed filters (0 = no skip), so the caller jumps the scan cursor.
  uint64_t DeadUntil(uint64_t row) {
    const auto dead_end = _filters.Empty() ? 0 : _filters.DeadUntil(row);
    return _multi.empty() ? dead_end : std::max(dead_end, MultiDeadUntil(row));
  }

  duckdb::idx_t Filter(uint64_t start_row, duckdb::idx_t count,
                       duckdb::DataChunk& output,
                       const duckdb::SelectionVector* live = nullptr,
                       duckdb::idx_t live_count = 0);

  void Project(uint64_t start_row, duckdb::idx_t count, duckdb::idx_t survivors,
               duckdb::DataChunk& output);
  duckdb::idx_t Scan(uint64_t start_row, duckdb::idx_t count,
                     duckdb::DataChunk& output,
                     const duckdb::SelectionVector* live = nullptr,
                     duckdb::idx_t live_count = 0);

  const duckdb::SelectionVector* Selection() const noexcept {
    return _selection;
  }

 private:
  void FillAbsent(duckdb::DataChunk& output) const;
  uint64_t MultiDeadUntil(uint64_t row);

  struct MultiFilter {
    const duckdb::ExpressionFilter* filter;
    std::vector<const irs::ColumnReader*> readers;
    std::vector<irs::BlockWindow> windows;
    uint64_t live_begin = 0;
    uint64_t live_end = 0;
  };

  struct Binding {
    const irs::ColumnReader* reader = nullptr;
    duckdb::idx_t output_slot = 0;
    bool is_list_like = false;
    std::unique_ptr<irs::ColumnReader::ScanState> state;
    std::unique_ptr<irs::ExtractBinding> extract;
  };

  irs::ReadContext _ctx;
  std::vector<Binding> _bound;
  std::vector<duckdb::idx_t> _absent;
  irs::ColFilterChain _filters;
  std::vector<MultiFilter> _multi;
  duckdb::vector<duckdb::BaseStatistics> _multi_stats;
  duckdb::ClientContext* _context;
  duckdb::buffer_ptr<duckdb::SelectionData> _sel_data;
  duckdb::SelectionVector _sel;
  const duckdb::SelectionVector* _selection = nullptr;
  uint64_t _scanned_end = 0;
};

void ScanLiveRows(duckdb::ClientContext& context, const irs::SubReader& segment,
                  uint64_t begin, uint64_t end,
                  std::span<const irs::ColumnstoreProjection> projections,
                  irs::ColFilterStateCache& filter_states,
                  duckdb::SelectionVector& live, duckdb::DataChunk& chunk,
                  absl::FunctionRef<bool(duckdb::DataChunk&, uint64_t,
                                         std::span<const duckdb::sel_t>)>
                    visit);

}  // namespace sdb::connector
