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

#include <duckdb.hpp>
#include <iresearch/search/filter.hpp>

#include "connector/duckdb_scan_base.hpp"

namespace sdb::connector {

// Computes the matching doc count from iresearch up-front, then emits
// zero-column DataChunks capped at STANDARD_VECTOR_SIZE until drained.
// The aggregate above (count_star) sums chunk cardinalities.
struct SearchCountScanGlobalState : public CommonScanGlobalState {
  // Per-segment prepared filter queries. Empty when CountScan.stored_filter
  // is null (match-all short-circuit via IndexReader::live_docs_count()).
  // Otherwise built once in SearchCountScanInitGlobal -- the only prepare
  // site for CountScan. Indexed by segment ordinal; entries may be null.
  irs::PrepareCollector::ptr collector;
  std::vector<irs::QueryBuilder::ptr> queries;

  uint64_t total = 0;
  uint64_t emitted = 0;
  bool counted = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchCountScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SearchCountScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output);

}  // namespace sdb::connector
