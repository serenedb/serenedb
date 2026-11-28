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
#include <utility>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

constexpr uint64_t kEmitSlice = 64 * STANDARD_VECTOR_SIZE;

void FinishRoot(CountLocalState& l) {
  if (l.root) {
    l.local_count += l.root->Finish();
    l.root.reset();
    l.root_seg = std::numeric_limits<uint32_t>::max();
  }
}

uint64_t ClaimCounted(ScanGlobalState& g) {
  auto counted = g.counted.load(std::memory_order_relaxed);
  uint64_t take = 0;
  do {
    take = std::min(counted, std::max(kEmitSlice, counted / g.workers));
  } while (take != 0 && !g.counted.compare_exchange_weak(
                          counted, counted - take, std::memory_order_relaxed));
  return take;
}

}  // namespace

void RunCountScan(duckdb::TableFunctionInput&, ScanGlobalState& g,
                  CountLocalState& l, duckdb::DataChunk& output) {
  while (NextLiveUnit(g, l)) {
    const auto& unit = l.unit;
    const auto& sub = (*g.reader)[unit.seg];
    if (unit.whole && l.seg_cls.active.empty() && !g.ClaimedWhere() &&
        !g.vector_scorer) {
      l.local_count += sub.live_docs_count();
    } else {
      const auto range = g.RangeOf(unit);
      if (l.root_seg != unit.seg || range.begin < l.root_end) {
        FinishRoot(l);
        l.plan_memory.Rewind();
        const auto& seg_query = EnsureSegmentQuery(g, &l, unit.seg);
        auto* table = BeginVerify(l.col_verify, sub, g, l);
        auto plan = irs::count::MakeRoot(seg_query, {.memory = l.plan_memory,
                                                     .table = table,
                                                     .span = g.UnitSpan(unit),
                                                     .partial = true});
        EnsurePlanned(plan != nullptr);
        l.root = std::move(plan);
        l.root_seg = unit.seg;
      }
      l.local_count += l.root->Run(range.begin, range.end);
      l.local_count += l.root->Finish();
      l.root_end = range.end;
    }
    FinishUnit(g, l);
  }
  FinishRoot(l);
  if (l.local_count != 0) {
    g.counted.fetch_add(std::exchange(l.local_count, 0),
                        std::memory_order_relaxed);
  }
  if (l.to_emit == 0) {
    l.to_emit = ClaimCounted(g);
  }
  const auto batch = std::min<uint64_t>(l.to_emit, STANDARD_VECTOR_SIZE);
  l.to_emit -= batch;
  if (batch != 0) {
    WriteVirtualColumns(g, batch, nullptr, output);
  }
  output.SetChildCardinality(batch);
}

}  // namespace sdb::connector
