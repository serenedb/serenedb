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

#include "basics/assert.h"
#include "basics/debugging.h"
#include "pg/sql_exception_macro.h"

namespace sdb::connector {

FullScanner::FullScanner(const irs::ColReader& reader,
                         std::span<const ColumnstoreProjection> projections,
                         duckdb::ClientContext* context)
  : _ctx{reader} {
  _bound.reserve(projections.size());
  for (const auto& projection : projections) {
    const auto* column_reader =
      reader.Column(static_cast<irs::field_id>(projection.column_id));
    if (!column_reader) {
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
}

void FullScanner::Scan(uint64_t start_row, duckdb::idx_t count,
                       duckdb::DataChunk& output) {
  if (_bound.empty() || count == 0) {
    return;
  }
  SDB_IF_FAILURE("SearchIncludeFetchFault") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
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
    SDB_ASSERT(duckdb::FlatVector::Validity(out).CheckAllValid(count, 0),
               "columnstore Scan requires an all-valid target; validity codec "
               "AND-combines into out_vec");
    b.reader->Scan(*b.state, out, count);
  }
}

}  // namespace sdb::connector
