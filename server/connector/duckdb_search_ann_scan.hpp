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

#include "connector/duckdb_scan_base.hpp"

namespace sdb::connector {

// Global state for SearchAnnScan (HNSW top-k).
// HNSW search is lazy: all results are collected on the first scan call and
// then streamed in STANDARD_VECTOR_SIZE batches via ann_current_idx.
struct SearchAnnScanGlobalState : public CommonScanGlobalState {
  std::vector<std::string> ann_pk_bytes;
  size_t ann_current_idx = 0;
  bool ann_search_done = false;
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchAnnScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

void SearchAnnScanFunction(duckdb::ClientContext& context,
                           duckdb::TableFunctionInput& data,
                           duckdb::DataChunk& output);

}  // namespace sdb::connector
