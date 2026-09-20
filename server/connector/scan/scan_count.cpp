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

#include <algorithm>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/count/make.hpp>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {

void RunCountScan(duckdb::TableFunctionInput&, ScanGlobalState& g,
                  CountLocalState& l, duckdb::DataChunk& output) {
  while (NextLiveUnit(g, l)) {
    const auto& unit = l.unit;
    const auto& sub = (*g.reader)[unit.seg];
    if (unit.whole && l.seg_cls.active.empty() && !g.Bind().search.filter &&
        !g.vector_scorer) {
      l.local_count += sub.live_docs_count();
    } else {
      if (l.root_seg != unit.seg) {
        const auto& seg_query = EnsureSegmentQuery(g, l, unit.seg);
        auto* table = BeginVerify(l.col_verify, sub, g, l);
        auto plan = irs::count::MakeRoot(
          seg_query, {.table = table,
                      .span = unit.whole || unit.rg_begin == 0
                                ? irs::doc_id_t{0}
                                : static_cast<irs::doc_id_t>(g.rg_size)});
        EnsurePlanned(plan != nullptr);
        l.root = std::move(plan);
        l.root_seg = unit.seg;
      }
      const auto range = g.RangeOf(unit);
      l.local_count += irs::utils::downCast<irs::count::Root>(l.root.get())
                         ->Run(range.begin, range.end);
    }
    if (FinishUnit(g, l)) {
      FinishSegments(g, 1);
    }
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
