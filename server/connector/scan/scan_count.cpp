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
namespace {

void FinishRoot(CountLocalState& l) {
  if (l.root) {
    l.local_count +=
      irs::utils::downCast<irs::count::Root>(l.root.get())->Finish();
    l.root.reset();
    l.root_seg = std::numeric_limits<uint32_t>::max();
  }
}

void Emit(ScanGlobalState& g, CountLocalState& l, duckdb::DataChunk& output) {
  const auto batch = std::min<uint64_t>(l.local_count, STANDARD_VECTOR_SIZE);
  l.local_count -= batch;
  if (batch != 0) {
    WriteVirtualColumns(g, l.empty_filled, batch, nullptr, output);
  }
  output.SetChildCardinality(batch);
}

}  // namespace

void RunCountScan(duckdb::TableFunctionInput&, ScanGlobalState& g,
                  CountLocalState& l, duckdb::DataChunk& output) {
  for (;;) {
    if (l.whole_backlog && l.local_count >= STANDARD_VECTOR_SIZE) {
      Emit(g, l, output);
      return;
    }
    if (!NextLiveUnit(g, l)) {
      break;
    }
    const auto& unit = l.unit;
    l.whole_backlog = unit.whole;
    const auto& sub = (*g.reader)[unit.seg];
    if (unit.whole && l.seg_cls.active.empty() && !g.ClaimedWhere() &&
        !g.vector_scorer) {
      l.local_count += sub.live_docs_count();
    } else {
      const auto range = g.RangeOf(unit);
      if (l.root_seg != unit.seg || range.begin < l.root_end) {
        FinishRoot(l);
        const auto& seg_query = EnsureSegmentQuery(g, &l, unit.seg);
        auto* table = BeginVerify(l.col_verify, sub, g, l);
        auto plan = irs::count::MakeRoot(
          seg_query,
          {.table = table, .span = g.UnitSpan(unit), .partial = true});
        EnsurePlanned(plan != nullptr);
        l.root = std::move(plan);
        l.root_seg = unit.seg;
      }
      auto& root = *irs::utils::downCast<irs::count::Root>(l.root.get());
      l.local_count += root.Run(range.begin, range.end);
      l.local_count += root.Finish();
      l.root_end = range.end;
    }
    FinishUnit(g, l);
  }
  FinishRoot(l);
  Emit(g, l, output);
}

}  // namespace sdb::connector
