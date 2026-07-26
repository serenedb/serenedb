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

#include "connector/full_scanner.h"

#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/list_vector.hpp>
#include <duckdb/planner/filter/expression_filter.hpp>
#include <duckdb/planner/filter/table_filter_functions.hpp>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

FullScanner::FullScanner(
  const irs::ColReader& reader,
  std::span<const ColumnstoreProjection> projections,
  std::span<const TableFilterDocIterator::FilterSpec> filters,
  duckdb::ClientContext* context, ColFilterStateCache& states)
  : _ctx{reader} {
  _sel_data = duckdb::make_buffer<duckdb::SelectionData>(STANDARD_VECTOR_SIZE);
  _sel.Initialize(_sel_data);

  // `.col` filters (the score is computed, not stored -- never a bulk filter).
  _filters.Bind(reader, _ctx, filters, *context, states);

  _bound.reserve(projections.size());
  for (const auto& projection : projections) {
    const auto* column_reader =
      reader.Column(static_cast<irs::field_id>(projection.column_id));
    if (!column_reader) {
      continue;
    }
    // A projected column that is also a filter column materializes as part of
    // its filter step (decode once into this slot, then Slice) -- record the
    // slot on the filter and don't scan it again below.
    if (!projection.IsExtract() &&
        _filters.AttachOutputSlot(
          static_cast<irs::field_id>(projection.column_id),
          projection.output_slot)) {
      continue;
    }
    auto& b = _bound.emplace_back();
    b.reader = column_reader;
    b.output_slot = projection.output_slot;
    if (projection.IsExtract()) {
      b.extract = std::make_unique<ExtractBinding>();
      b.extract->Bind(*column_reader, _ctx, projection.extract_path,
                      projection.extract_scan_type, context);
      continue;
    }
    const auto type_id = column_reader->Type().id();
    b.is_list_like = type_id == duckdb::LogicalTypeId::LIST ||
                     type_id == duckdb::LogicalTypeId::MAP;
    b.state = std::make_unique<irs::ColumnReader::ScanState>(
      column_reader->InitScan(_ctx));
  }

  _filters.FinishBind();
}

duckdb::idx_t FullScanner::Scan(uint64_t start_row, duckdb::idx_t count,
                                duckdb::DataChunk& output) {
  if (count == 0) {
    return 0;
  }
  _scanned_end = std::max<uint64_t>(_scanned_end, start_row + count);
  SDB_IF_FAILURE("SearchIncludeFetchFault") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }

  if (_filters.Empty()) {
    for (auto& b : _bound) {
      auto& out = output.data[b.output_slot];
      if (b.extract) {
        b.extract->MaterializeContiguous(start_row, count, out);
        continue;
      }
      if (b.is_list_like) {
        duckdb::ListVector::SetListSize(out, 0);
      }
      const auto cursor = b.reader->GatherCursor(*b.state);
      if (cursor != start_row) {
        SDB_ASSERT(cursor <= start_row);
        b.reader->Skip(*b.state, start_row - cursor);
      }
      SDB_ASSERT(
        duckdb::FlatVector::Validity(out).CheckAllValid(count, 0),
        "columnstore Scan requires an all-valid target; validity codec "
        "AND-combines into out_vec");
      b.reader->Scan(*b.state, out, count);
    }
    return count;
  }

  // RowGroup::Scan-style: narrow `_sel` with the codec filters (each filter
  // column decoded once, straight into its projected output vector), then
  // materialize only the survivors of the remaining projected columns.
  _sel.Initialize(_sel_data);
  for (duckdb::idx_t i = 0; i < count; ++i) {
    _sel.set_index(i, i);
  }
  const duckdb::idx_t survivors =
    _filters.FilterWindow(start_row, count, _sel, count, &output);
  if (survivors == 0) {
    // Every row filtered out. Column scan cursors that didn't advance this
    // vector re-position to the next anchor on the following call.
    return 0;
  }
  _filters.FinishOutputs(start_row, count, _sel, survivors, output);
  // Materialize the survivors of the non-filter projected columns.
  for (auto& b : _bound) {
    auto& out = output.data[b.output_slot];
    if (b.extract) {
      if (survivors == count) {
        b.extract->MaterializeContiguous(start_row, count, out);
      } else {
        b.extract->MaterializeSelected(start_row, _sel, survivors, out);
      }
      continue;
    }
    if (b.is_list_like) {
      duckdb::ListVector::SetListSize(out, 0);
    }
    b.reader->GatherDense(*b.state, start_row, _sel, survivors, count, out);
  }
  return survivors;
}

}  // namespace sdb::connector
