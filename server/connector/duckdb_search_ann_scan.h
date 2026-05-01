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
#include <duckdb/execution/expression_executor.hpp>
#include <string>
#include <vector>

#include "connector/duckdb_ann_filter.h"
#include "connector/duckdb_scan_base.hpp"
#include "connector/duckdb_table_function.h"
#include "connector/index_source.h"

namespace sdb::connector {

// Global state for SearchAnnScan (HNSW top-k).
// HNSW search is lazy: all results are collected on the first scan call and
// then streamed in STANDARD_VECTOR_SIZE batches via ann_current_idx.
struct SearchAnnScanGlobalState : public CommonScanGlobalState {
  const ANNScan* scan = nullptr;
  std::unique_ptr<ANNFilter> filter;
  // HNSW result PKs collected once in score-sorted order; per-call scan
  // slices [current_idx, current_idx + batch_size). Default-constructed to
  // std::monostate; switched on first ANNSearchImpl call.
  PrimaryKeyBatch pk_batch;
  size_t total_results = 0;
  size_t current_idx = 0;
  bool results_ready = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

}  // namespace sdb::connector
