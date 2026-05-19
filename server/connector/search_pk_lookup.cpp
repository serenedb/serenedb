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

#include "connector/search_pk_lookup.h"

#include <duckdb/common/vector/flat_vector.hpp>

#include "basics/debugging.h"
#include "basics/exceptions.h"

namespace sdb::connector {

void SegmentPkSequentialFetcher::Fetch(
  std::span<const irs::doc_id_t> sorted_docs, duckdb::Vector& out,
  duckdb::idx_t out_start) {
  if (sorted_docs.empty()) {
    return;
  }
  SDB_IF_FAILURE("SearchPkFetchFault") { SDB_THROW(ERROR_DEBUG); }
  SDB_ASSERT(absl::c_is_sorted(sorted_docs));

  struct RowView {
    std::span<const irs::doc_id_t> docs;
    size_t size() const noexcept { return docs.size(); }
    uint64_t operator[](size_t i) const noexcept {
      return static_cast<uint64_t>(docs[i]) -
             static_cast<uint64_t>(irs::doc_limits::min());
    }
  };
  irs::columnstore::ColumnReader::RangeScan range{*_pk_col, _ctx};
  irs::columnstore::ColumnReader::ScanRowsBatched(range, RowView{sorted_docs},
                                                  out, out_start);
}

}  // namespace sdb::connector
