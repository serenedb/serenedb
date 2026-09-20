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
#include <iresearch/search/docs/make.hpp>
#include <iresearch/search/hits/make.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {
namespace {

bool StartUnit(ScanGlobalState& g, StreamLocalState& l) {
  const auto seg_idx = l.unit.seg;
  const auto& seg = (*g.reader)[seg_idx];
  if (g.needs_lookup && !SegmentPkColumn(*g.reader, seg_idx).second) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("inverted-index segment has no stored PK column but the query "
              "needs to map hits back to source rows"));
  }
  l.EnsureHitBatcher(g);
  const auto first_row =
    l.unit.whole ? 0 : uint64_t{l.unit.rg_begin} * g.rg_size;
  const bool resume =
    l.hit_batcher->ResumeSegment(seg_idx) && l.root && first_row >= l.stop_row;
  if (!resume) {
    if (!l.hit_batcher->Empty()) {
      return false;
    }
    l.hit_batcher->BeginSegment(seg_idx, seg.GetColReader(), g.client_context,
                                &l.filter_states, l.seg_cls.active);
  }
  l.defer_flush = !l.unit.whole;
  const auto seg_rows = static_cast<uint64_t>(seg.docs_count());
  l.next_row = first_row;
  l.stop_row = l.unit.whole ? seg_rows
                            : std::min<uint64_t>(
                                uint64_t{l.unit.rg_end} * g.rg_size, seg_rows);
  l.root_exhausted = false;
  if (resume) {
    return true;
  }
  const auto& seg_query = EnsureSegmentQuery(g, l, seg_idx);
  l.scored = g.ScanScore();
  if (l.scored) {
    SDB_ENSURE(g.scorer_obj != nullptr,
               "a scan that emits a score has a scorer to compute it with");
    l.score_fetcher.Clear();
    auto root = irs::hits::MakeRoot(
      seg_query, {.scorer = *g.scorer_obj, .fetcher = l.score_fetcher});
    EnsurePlanned(root != nullptr);
    l.root = std::move(root);
  } else {
    auto root = irs::docs::MakeRoot(seg_query, {});
    EnsurePlanned(root != nullptr);
    l.root = std::move(root);
  }
  return true;
}

void Exhaust(StreamLocalState& l) { l.root_exhausted = true; }

void PushHits(StreamLocalState& l) {
  auto& batcher = *l.hit_batcher;
  for (;;) {
    if (l.root_exhausted) {
      if (!l.defer_flush && !batcher.Ready() && !batcher.Empty()) {
        batcher.Finalize();
      }
      return;
    }
    auto row = l.next_row;
    if (!batcher.Filters().Empty()) {
      if (const auto dead = batcher.Filters().DeadUntil(row); dead != 0) {
        row = dead;
      }
    }
    if (row >= l.stop_row) {
      l.next_row = row;
      Exhaust(l);
      continue;
    }
    const auto span = batcher.OpenWindow(row);
    if (span == 0) {
      l.next_row = row;
      return;
    }
    const auto width =
      static_cast<irs::doc_id_t>(std::min<uint64_t>(span, l.stop_row - row));
    const auto min = irs::doc_limits::min() + static_cast<irs::doc_id_t>(row);
    const auto max = min + width;
    const auto n =
      l.scored ? irs::utils::downCast<irs::hits::Root>(l.root.get())
                   ->Run(min, max, batcher.WindowHead(), batcher.ScoreHead())
               : irs::utils::downCast<irs::docs::Root>(l.root.get())
                   ->Run(min, max, batcher.WindowHead());
    batcher.CommitWindow(n);
    l.next_row = row + width;
  }
}

bool Streaming(const StreamLocalState& l) noexcept { return !l.root_exhausted; }

duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx, ScanGlobalState& g,
                        StreamLocalState& l, duckdb::DataChunk& output) {
  for (;;) {
    if (!l.hit_batcher || (!Streaming(l) && l.hit_batcher->Empty())) {
      return 0;
    }
    if (!l.hit_batcher->Ready()) {
      PushHits(l);
      if (!l.hit_batcher->Ready()) {
        return 0;
      }
    }
    SDB_IF_FAILURE("SearchLookupFault") {
      if (g.needs_lookup) {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
    }
    const auto produced = EmitReadyBatch(ctx, g, l, output);
    if (produced != 0) {
      return produced;
    }
    output.Reset();
  }
}

}  // namespace

void RunStreamScan(duckdb::ClientContext& ctx,
                   duckdb::TableFunctionInput& input, ScanGlobalState& g,
                   StreamLocalState& l, duckdb::DataChunk& output) {
  if (!RunPrepareStage(input, g, l)) {
    return;
  }
  for (;;) {
    if (l.has_unit) {
      const auto added = EmitChunk(ctx, g, l, output);
      SDB_ASSERT(added <= STANDARD_VECTOR_SIZE);
      if (added != 0) {
        const auto kept = FinalizeBatch(ctx, g, l, output, added);
        if (kept != 0) {
          output.SetChildCardinality(kept);
          return;
        }
        output.Reset();
        continue;
      }
      if (l.needs_start) {
        l.needs_start = false;
        StartUnit(g, l);
        continue;
      }
      if (FinishUnit(g, l)) {
        FinishSegments(g, 1);
      }
    }
    if (!NextLiveUnit(g, l)) {
      if (l.hit_batcher && !l.hit_batcher->Empty()) {
        l.defer_flush = false;
        const auto added = EmitChunk(ctx, g, l, output);
        if (added != 0) {
          const auto kept = FinalizeBatch(ctx, g, l, output, added);
          if (kept != 0) {
            output.SetChildCardinality(kept);
            return;
          }
        }
      }
      break;
    }
    if (!StartUnit(g, l)) {
      l.defer_flush = false;
      l.needs_start = true;
    }
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
