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

#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/count/make.hpp>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {

void RunCountScan(duckdb::TableFunctionInput& /*input*/, ScanGlobalState& g,
                  CountLocalState& l, duckdb::DataChunk& output) {
  while (ClaimUnit(g, l)) {
    const auto& unit = l.unit;
    const auto& sub = (*g.reader)[unit.seg];
    l.Classify(g, unit.seg);
    if (l.seg_cls.segment_dead) {
      UnitDone(g, l);
      continue;
    }
    if (unit.whole && l.seg_cls.active.empty() && !g.Bind().search.filter &&
        !g.vector_scorer) {
      l.local_count += sub.live_docs_count();
      UnitDone(g, l);
      continue;
    }
    const auto& seg_query = EnsureSegmentQuery(g, l, unit.seg);
    auto* table = BeginVerify(l.col_verify, sub, g, l);
    auto plan = irs::count::MakeRoot(
      seg_query, {.table = table, .range = g.RangeOf(unit)});
    EnsurePlanned(plan != nullptr);
    l.local_count += plan->Run();
    UnitDone(g, l);
  }
  if (l.local_emitted >= l.local_count) {
    output.SetChildCardinality(0);
    return;
  }
  const auto batch = std::min<duckdb::idx_t>(l.local_count - l.local_emitted,
                                             STANDARD_VECTOR_SIZE);
  output.SetChildCardinality(batch);
  g.produced_rows.fetch_add(batch, std::memory_order_relaxed);
  l.local_emitted += batch;
}

}  // namespace sdb::connector
