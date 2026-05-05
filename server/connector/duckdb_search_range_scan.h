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
#include <string>
#include <vector>

#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"

namespace sdb::connector {

// Global state for SearchRangeScan (HNSW bounded-radius range search).
// Identical structure to SearchAnnScanGlobalState: results are collected
// lazily on the first call and streamed in batches.
struct SearchRangeScanGlobalState : public CommonScanGlobalState {
  // TODO(codeworse): Make batch-processing of range scan
  // Currently, it evaluates once and stores the whole result
  // Range scan probably supports streaming the result, so we need to
  // search only one batch per request

  const RangeSearchScan* scan = nullptr;
  PrimaryKeyBatch pk_batch;
  size_t total_results = 0;
  size_t current_idx = 0;
  bool results_ready = false;
  std::unique_ptr<ANNFilter> filter;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchRangeScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SearchRangeScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output);

}  // namespace sdb::connector
