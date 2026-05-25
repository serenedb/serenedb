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
  // Zero-column scan; projected_columns / types stay empty.
  InitCommonState(*state, context, bind_data, input);

  const auto& count_scan = bind_data.scan_source->Cast<CountScan>();
  SDB_ASSERT(count_scan.snapshot);
  state->reader = &count_scan.snapshot->reader;
  state->total_segments = count_scan.snapshot->reader.size();

  if (count_scan.stored_filter) {
    state->query = count_scan.stored_filter->prepare({
      .index = count_scan.snapshot->reader,
    });
  } else {
    state->match_all_shortcut = true;
    state->match_all_total = count_scan.snapshot->reader.live_docs_count();
  }
  return state;
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> SearchCountScanInitLocal(
  duckdb::ExecutionContext& /*context*/,
  duckdb::TableFunctionInitInput& /*input*/,
  duckdb::GlobalTableFunctionState* state) {
  auto& gstate = state->Cast<SearchCountScanGlobalState>();
  auto lstate = duckdb::make_uniq<SearchCountScanLocalState>();
  if (gstate.match_all_shortcut) {
    // MaxThreads=1 here; this lone lstate owns the precomputed total.
    lstate->local_count = gstate.match_all_total;
    lstate->segments_exhausted = true;
  }
  return lstate;
}

void SearchCountScanLocalState::OnSegment(duckdb::ClientContext& /*ctx*/,
                                          const irs::SubReader& seg,
                                          uint32_t /*seg_idx*/,
                                          SearchCountScanGlobalState& g) {
  if (!g.query) {
    return;  // match_all_shortcut path; lstate.local_count seeded in InitLocal
  }
  auto doc = seg.mask(g.query->execute({.segment = seg}));
  local_count += doc->count();
}

EmitOutput SearchCountScanLocalState::EmitNextChunk(
  duckdb::ClientContext& /*ctx*/, SearchCountScanGlobalState& g,
  duckdb::DataChunk& output) {
  if (!segments_exhausted) {
    return EmitOutput::NeedsSegment;
  }
  if (local_emitted >= local_count) {
    return EmitOutput::Finished;
  }
  const auto batch =
    std::min<duckdb::idx_t>(local_count - local_emitted, STANDARD_VECTOR_SIZE);
  output.SetCardinality(batch);
  g.produced_rows.fetch_add(batch, std::memory_order_relaxed);
  local_emitted += batch;
  return EmitOutput::Chunk;
}

void SearchCountScanFunction(duckdb::ClientContext& context,
                             duckdb::TableFunctionInput& data,
                             duckdb::DataChunk& output) {
  auto& gstate = data.global_state->Cast<SearchCountScanGlobalState>();
  auto& lstate = data.local_state->Cast<SearchCountScanLocalState>();
  RunParallelInvertedIndexScan(context, gstate, lstate, output);
}

}  // namespace sdb::connector
