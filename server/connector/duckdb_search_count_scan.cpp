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

#include "connector/duckdb_search_count_scan.hpp"

#include <algorithm>
#include <duckdb/common/types/data_chunk.hpp>
#include <iresearch/index/index_reader.hpp>

#include "connector/duckdb_table_function.h"

namespace sdb::connector {

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> SearchCountScanInitGlobal(
  duckdb::ClientContext& context, duckdb::TableFunctionInitInput& input) {
  auto& bind_data = input.bind_data->Cast<SereneDBScanBindData>();
  auto state = duckdb::make_uniq<SearchCountScanGlobalState>();
  // Reuse the common init for transaction isolation checks. The scan emits
  // zero columns, so projected_columns / projected_types stay empty.
  InitCommonState(*state, context, bind_data, input);

  // Single prepare site for CountScan. When stored_filter is null the
  // scan short-circuits to live_docs_count() and skips iteration.
  const auto& count_scan = bind_data.scan_source->Cast<CountScan>();
  if (count_scan.stored_filter) {
    SDB_ASSERT(count_scan.snapshot);
    auto& reader = count_scan.snapshot->reader;
    state->collector = count_scan.stored_filter->MakeCollector(nullptr);
    state->queries.reserve(reader.size());
    for (size_t i = 0; i < reader.size(); ++i) {
      state->queries.emplace_back(count_scan.stored_filter->PrepareSegment(
        reader[i], {.collector = state->collector.get()}));
    }
  }
  return state;
}

void SearchCountScanFunction(duckdb::ClientContext& /*context*/,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchCountScanGlobalState>();
  auto& bind_data = data.bind_data->Cast<SereneDBScanBindData>();
  auto& count_scan = bind_data.scan_source->Cast<CountScan>();

  if (gstate.finished) {
    output.SetCardinality(0);
    return;
  }

  // First call: compute the total match count from iresearch.
  // Mirrors server/connector/search_count_data_source.cpp:54-74.
  if (!gstate.counted) {
    SDB_ASSERT(count_scan.snapshot);
    auto& reader = count_scan.snapshot->reader;
    if (!gstate.collector) {
      gstate.total = reader.live_docs_count();
    } else {
      uint64_t count = 0;
      for (size_t i = 0; i < reader.size(); ++i) {
        auto& segment = reader[i];
        auto& query = gstate.queries[i];
        if (!query) {
          continue;
        }
        auto doc = segment.mask(query->Execute({}));
        count += doc->count();
      }
      gstate.total = count;
    }
    gstate.counted = true;
  }

  const uint64_t remaining = gstate.total - gstate.emitted;
  if (remaining == 0) {
    gstate.finished = true;
    output.SetCardinality(0);
    return;
  }
  const duckdb::idx_t batch = static_cast<duckdb::idx_t>(
    std::min<uint64_t>(remaining, STANDARD_VECTOR_SIZE));
  output.SetChildCardinality(batch);
  gstate.emitted += batch;
  gstate.produced_rows.fetch_add(batch, std::memory_order_relaxed);
}

}  // namespace sdb::connector
