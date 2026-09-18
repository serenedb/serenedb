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

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {
namespace {

constexpr uint32_t kStageDocs = STANDARD_VECTOR_SIZE;

void StartUnit(ScanGlobalState& g, StreamLocalState& l) {
  const auto seg_idx = l.unit.seg;
  const auto& seg = (*g.reader)[seg_idx];
  l.stage_at = 0;
  l.stage_len = 0;
  if (g.needs_lookup && !SegmentPkColumn(*g.reader, seg_idx).second) {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("inverted-index segment has no stored PK column but the query "
              "needs to map hits back to source rows"));
  }
  l.EnsureHitBatcher(g);
  l.hit_batcher->BeginSegment(seg_idx, seg.GetColReader(), g.client_context,
                              &l.filter_states, l.seg_cls.active);
  const auto& seg_query = EnsureSegmentQuery(g, l, seg_idx);
  irs::detail::DeadRuns* const table = l.hit_batcher->Skipper();
  const auto range = g.RangeOf(l.unit);
  l.scored = g.ScanScore();
  if (l.scored) {
    SDB_ENSURE(g.scorer_obj != nullptr,
               "a scan that emits a score has a scorer to compute it with");
    l.score_fetcher.Clear();
    auto root = irs::hits::MakeRoot(seg_query, {.scorer = *g.scorer_obj,
                                                .fetcher = l.score_fetcher,
                                                .table = table,
                                                .range = range});
    EnsurePlanned(root != nullptr);
    l.root = std::move(root);
  } else {
    auto root =
      irs::docs::MakeRoot(seg_query, {.table = table, .range = range});
    EnsurePlanned(root != nullptr);
    l.root = std::move(root);
  }
  l.root_exhausted = false;
}

void Exhaust(StreamLocalState& l) {
  l.root_exhausted = true;
  l.root.reset();
}

bool Refill(StreamLocalState& l) {
  SDB_ASSERT(l.stage_at == l.stage_len);
  l.stage_at = 0;
  l.stage_len = 0;
  if (l.root_exhausted) {
    return false;
  }
  const auto n =
    l.scored ? irs::utils::downCast<irs::hits::Root>(l.root.get())
                 ->Run(l.stage_docs.data(), l.stage_scores.data(), kStageDocs)
             : irs::utils::downCast<irs::docs::Root>(l.root.get())
                 ->Run(l.stage_docs.data(), kStageDocs);
  if (n == 0) {
    Exhaust(l);
    return false;
  }
  l.stage_len = n;
  return true;
}

void PushHits(StreamLocalState& l) {
  auto& batcher = *l.hit_batcher;
  for (;;) {
    if (l.stage_at == l.stage_len && !Refill(l)) {
      if (!batcher.Ready() && !batcher.Empty()) {
        batcher.Finalize();
      }
      return;
    }
    if (!batcher.Filters().Empty()) {
      const auto rows = batcher.SegmentRowCount();
      auto row = static_cast<uint64_t>(l.stage_docs[l.stage_at] -
                                       irs::doc_limits::min());
      auto dead = batcher.Filters().DeadUntil(row);
      while (dead != 0 && row < rows) {
        row = dead;
        while (l.stage_at != l.stage_len &&
               static_cast<uint64_t>(l.stage_docs[l.stage_at] -
                                     irs::doc_limits::min()) < row) {
          ++l.stage_at;
        }
        if (l.stage_at == l.stage_len) {
          break;
        }
        row = static_cast<uint64_t>(l.stage_docs[l.stage_at] -
                                    irs::doc_limits::min());
        dead = batcher.Filters().DeadUntil(row);
      }
      if (l.stage_at == l.stage_len || row >= rows) {
        if (row >= rows) {
          l.stage_at = l.stage_len;
          Exhaust(l);
        }
        continue;
      }
    }
    const auto first = l.stage_docs[l.stage_at];
    const auto span = batcher.OpenWindow(first - irs::doc_limits::min());
    if (span == 0) {
      return;
    }
    const auto max = first + static_cast<irs::doc_id_t>(span);
    uint32_t n = 0;
    while (l.stage_at + n != l.stage_len &&
           l.stage_docs[l.stage_at + n] < max) {
      ++n;
    }
    SDB_ASSERT(n != 0);
    std::copy_n(l.stage_docs.data() + l.stage_at, n, batcher.WindowHead());
    if (l.scored) {
      std::copy_n(l.stage_scores.data() + l.stage_at, n, batcher.ScoreHead());
    }
    batcher.CommitWindow(n);
    l.stage_at += n;
  }
}

bool Streaming(const StreamLocalState& l) noexcept {
  return !l.root_exhausted || l.stage_at != l.stage_len;
}

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
      if (FinishUnit(g, l)) {
        FinishSegments(g, 1);
      }
    }
    if (!NextLiveUnit(g, l)) {
      break;
    }
    StartUnit(g, l);
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
