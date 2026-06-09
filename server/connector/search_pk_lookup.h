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

#include <duckdb/common/types/vector.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/index/iterators.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <limits>
#include <memory>
#include <span>

#include "basics/assert.h"
#include "basics/debugging.h"
#include "basics/duckdb_engine.h"
#include "basics/exceptions.h"
#include "catalog/table_options.h"

namespace sdb::connector {

inline std::pair<const irs::ColReader*, const irs::ColumnReader*>
SegmentPkColumn(const irs::IndexReader& reader, size_t seg_idx) noexcept {
  if (seg_idx >= reader.size()) {
    return {nullptr, nullptr};
  }
  const auto* cs_reader = reader[seg_idx].CsReader();
  if (!cs_reader) {
    return {nullptr, nullptr};
  }
  const auto* pk_col = cs_reader->Column(
    static_cast<irs::field_id>(catalog::Column::kGeneratedPKId));
  if (!pk_col) {
    return {nullptr, nullptr};
  }
  return {cs_reader, pk_col};
}

class SegmentPkSequentialFetcher {
 public:
  SegmentPkSequentialFetcher(const irs::ColReader& cs_reader,
                             const irs::ColumnReader& pk_col)
    : _ctx{cs_reader}, _pk_col{&pk_col} {}

  SegmentPkSequentialFetcher(const SegmentPkSequentialFetcher&) = delete;
  SegmentPkSequentialFetcher& operator=(const SegmentPkSequentialFetcher&) =
    delete;

  void Reset(const irs::ColReader& cs_reader, const irs::ColumnReader& pk_col) {
    _ctx.Reset(cs_reader);
    _pk_col = &pk_col;
  }

  template<typename DocIds>
  void Fetch(const DocIds& docs, duckdb::Vector& out) {
    if (docs.size() == 0) {
      return;
    }
    SDB_IF_FAILURE("SearchPkFetchFault") { SDB_THROW(ERROR_DEBUG); }
    SDB_ASSERT(docs.IsSorted());
    irs::ColumnReader::RangeScan range{*_pk_col, _ctx};
    irs::ColumnReader::ScanRowsBatched(range, docs, out, 0);
  }

 private:
  irs::ReadContext _ctx;
  const irs::ColumnReader* _pk_col;
};

struct PkLookupBuffers {
  std::unique_ptr<duckdb::Vector> seg_pk_vec;
  std::unique_ptr<SegmentPkSequentialFetcher> fetcher;
  uint32_t last_seg_idx = std::numeric_limits<uint32_t>::max();
};

}  // namespace sdb::connector
