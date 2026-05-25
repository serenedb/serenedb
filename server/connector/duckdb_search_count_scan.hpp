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

#include <atomic>
#include <duckdb.hpp>
#include <iresearch/search/filter.hpp>

#include "connector/duckdb_scan_base.hpp"

namespace sdb::connector {

struct SearchCountScanGlobalState;

// Per-thread per-segment counting. Each thread emits its own
// local_count as zero-column rows; count_star above sums cardinalities.
// Match-all (no filter) short-circuits via reader.live_docs_count().
struct SearchCountScanLocalState : public CommonScanLocalState {
  uint64_t local_count = 0;
  uint64_t local_emitted = 0;

  void OnSegment(duckdb::ClientContext& ctx, const irs::SubReader& seg,
                 uint32_t seg_idx, SearchCountScanGlobalState& g);
  EmitOutput EmitNextChunk(duckdb::ClientContext& ctx,
                           SearchCountScanGlobalState& g,
                           duckdb::DataChunk& output);
};

struct SearchCountScanGlobalState : public CommonScanGlobalState {
  // Null when match-all (no stored_filter).
  irs::Filter::Query::ptr query;

  // Match-all: live_docs_count() precomputed; OnSegment loop skipped.
  bool match_all_shortcut = false;
  uint64_t match_all_total = 0;

  duckdb::idx_t MaxThreads() const override {
    if (match_all_shortcut) {
      return 1;
    }
    return std::max<duckdb::idx_t>(1, total_segments);
  }
};

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchCountScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input);

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchCountScanInitLocal(
  duckdb::ExecutionContext& context, duckdb::TableFunctionInitInput& input,
  duckdb::GlobalTableFunctionState* global_state);

void SearchCountScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output);

}  // namespace sdb::connector
