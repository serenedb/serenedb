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

#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/selection_vector.hpp>
#include <duckdb/planner/table_filter.hpp>
#include <duckdb/planner/table_filter_state.hpp>
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/column_extract.hpp"
#include "iresearch/index/table_filter_iterator.hpp"

namespace sdb::connector {

class FullScanner {
 public:
  FullScanner(const irs::ColReader& reader,
              std::span<const ColumnstoreProjection> projections,
              std::span<const TableFilterDocIterator::FilterSpec> filters,
              duckdb::ClientContext* context);

  FullScanner(const FullScanner&) = delete;
  FullScanner& operator=(const FullScanner&) = delete;

  bool HasAny() const noexcept { return !_bound.empty() || !_filters.empty(); }

  // Scans the contiguous rows [start_row, start_row+count)
  // RowGroup::Scan-style: the pushed `.col` filters narrow the row selection
  // in-scan (codec Filter + zonemap, decoded once into the projected output
  // vector then Sliced), and the remaining projected columns materialize only
  // the survivors. Returns the number of rows written to `output` (== count
  // when there are no filters).
  duckdb::idx_t Scan(uint64_t start_row, duckdb::idx_t count,
                     duckdb::DataChunk& output);

 private:
  struct Binding {
    const irs::ColumnReader* reader = nullptr;
    duckdb::idx_t output_slot = 0;
    bool is_list_like = false;
    std::unique_ptr<irs::ColumnReader::ScanState> state;
    std::unique_ptr<ExtractBinding> extract;
  };

  // A pushed `.col` filter. `output_slots` are the projected slots the filter
  // column also outputs to (a column can be projected more than once, e.g. an
  // UPDATE emits its key column twice): the codec decode doubles as the
  // materialization of the first slot (Sliced), and the rest Reference it
  // zero-copy. Empty when the column is filter-only, in which case it decodes
  // into `scratch` purely to evaluate the predicate.
  struct FilterCol {
    const irs::ColumnReader* reader = nullptr;
    irs::field_id field = 0;
    const duckdb::TableFilter* filter = nullptr;
    std::vector<duckdb::idx_t> output_slots;
    std::unique_ptr<duckdb::TableFilterState> state;
    std::unique_ptr<irs::ColumnReader::ScanState> scan;
    // Codec Scan/Filter may morph the vector (e.g. dict_fsst emits a
    // DICTIONARY view over codec-owned buffers), so every use goes through
    // VectorScratch::Reset() -- never reuse it dirty.
    std::unique_ptr<irs::ColumnReader::VectorScratch> scratch;
  };

  irs::ReadContext _ctx;
  std::vector<Binding> _bound;
  std::vector<FilterCol> _filters;
  duckdb::buffer_ptr<duckdb::SelectionData> _sel_data;
  duckdb::SelectionVector _sel;
};

}  // namespace sdb::connector
