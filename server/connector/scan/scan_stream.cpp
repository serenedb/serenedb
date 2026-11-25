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
#include <duckdb/common/fault_points.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/docs/make.hpp>
#include <iresearch/search/hits/make.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"
#include "connector/search_pk_lookup.h"

namespace sdb::connector {
namespace {

bool Resumes(const StreamLocalState& l) noexcept {
  return l.hit_batcher->Bound(l.unit.seg) && l.piece.begin >= l.stop_row;
}

void StartUnit(ScanGlobalState& g, StreamLocalState& l) {
  const auto seg_idx = l.unit.seg;
  const auto& seg = (*g.reader)[seg_idx];
  const bool resume = Resumes(l);
  l.next_row = l.piece.begin;
  l.stop_row = l.piece.end;
  l.unit_done = false;
  l.started = true;
  if (!resume) {
    if (g.needs_lookup && !SegmentPkColumn(*g.reader, seg_idx).second) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INTERNAL_ERROR),
        ERR_MSG("inverted-index segment has no stored PK column but the "
                "query needs to map hits back to source rows"));
    }
    SDB_ASSERT(l.hit_batcher->Empty());
    l.hit_batcher->BeginSegment(seg_idx, seg.GetColReader(), g.client_context,
                                &l.filter_states, l.seg_cls.active);
  } else if (!g.remote) {
    l.unit_done = !l.root;
    return;
  }
  const auto& seg_query = EnsureSegmentQuery(g, &l, seg_idx);
  if (irs::QueryBuilder::IsEmpty(seg_query)) {
    l.root.reset();
    l.unit_done = true;
    return;
  }
  const auto span = g.UnitSpan(l.unit);
  l.scored = g.ScanScore();
  if (l.scored) {
    SDB_ENSURE(g.scorer_obj != nullptr,
               "a scan that emits a score has a scorer to compute it with");
    l.score_fetcher.Clear();
    auto root = irs::hits::MakeRoot(
      seg_query,
      {.scorer = *g.scorer_obj, .fetcher = l.score_fetcher, .span = span});
    EnsurePlanned(root != nullptr);
    l.root = std::move(root);
  } else {
    auto root = irs::docs::MakeRoot(seg_query, {.span = span});
    EnsurePlanned(root != nullptr);
    l.root = std::move(root);
  }
}

uint64_t SampledRow(const ScanGlobalState& g, StreamLocalState& l,
                    uint64_t row) {
  while (row < l.stop_row) {
    const auto vector = row / STANDARD_VECTOR_SIZE;
    const auto key = (uint64_t{l.unit.seg} << 32) | vector;
    if (key != l.sample_vector) {
      l.sample_vector = key;
      l.sample_keeps = g.sampling.KeepsVector(SampleRandom(g, l));
    }
    if (l.sample_keeps) {
      return row;
    }
    row = (vector + 1) * STANDARD_VECTOR_SIZE;
  }
  return row;
}

duckdb::idx_t KeepSampledHits(const ScanGlobalState& g, StreamLocalState& l,
                              duckdb::idx_t n) {
  auto& batcher = *l.hit_batcher;
  auto* docs = batcher.WindowHead();
  auto* scores = l.scored ? batcher.ScoreHead() : nullptr;
  duckdb::idx_t kept = 0;
  for (duckdb::idx_t i = 0; i != n; ++i) {
    if (g.sampling.KeepsRow(docs[i] - irs::doc_limits::min())) {
      docs[kept] = docs[i];
      if (scores) {
        scores[kept] = scores[i];
      }
      ++kept;
    }
  }
  return kept;
}

void PushHits(const ScanGlobalState& g, StreamLocalState& l) {
  auto& batcher = *l.hit_batcher;
  const auto& sampling = g.sampling;
  const bool sample_vectors =
    sampling.do_system_sample && sampling.is_percentage;
  const bool sample_rows = sampling.do_system_sample &&
                           !sampling.is_percentage && sampling.sample_rate < 1;
  while (!l.unit_done) {
    auto row = l.next_row;
    if (!batcher.Filters().Empty()) {
      if (const auto dead = batcher.Filters().DeadUntil(row); dead != 0) {
        row = dead;
      }
    }
    if (sample_vectors) {
      if (const auto sampled = SampledRow(g, l, row); sampled != row) {
        l.next_row = sampled;
        continue;
      }
    }
    if (row >= l.stop_row) {
      l.next_row = row;
      l.unit_done = true;
      return;
    }
    const auto span = batcher.OpenWindow(row);
    if (span == 0) {
      l.next_row = row;
      return;
    }
    const auto end =
      sample_vectors
        ? std::min<uint64_t>(
            l.stop_row, (row / STANDARD_VECTOR_SIZE + 1) * STANDARD_VECTOR_SIZE)
        : l.stop_row;
    const auto width =
      static_cast<irs::doc_id_t>(std::min<uint64_t>(span, end - row));
    const auto min = irs::doc_limits::min() + static_cast<irs::doc_id_t>(row);
    const auto max = min + width;
    auto n = l.scored
               ? irs::utils::downCast<irs::hits::Root>(l.root.get())
                   ->Run(min, max, batcher.WindowHead(), batcher.ScoreHead())
               : irs::utils::downCast<irs::docs::Root>(l.root.get())
                   ->Run(min, max, batcher.WindowHead());
    if (sample_rows) {
      n = KeepSampledHits(g, l, n);
    }
    batcher.CommitWindow(n);
    l.next_row = row + width;
  }
}

duckdb::idx_t EmitChunk(duckdb::ClientContext& ctx, ScanGlobalState& g,
                        StreamLocalState& l, duckdb::DataChunk& output,
                        bool flush) {
  auto& batcher = *l.hit_batcher;
  for (;;) {
    if (!batcher.Ready()) {
      PushHits(g, l);
      if (!batcher.Ready()) {
        if (!flush || batcher.Empty()) {
          return 0;
        }
        batcher.Finalize();
        if (!batcher.Ready()) {
          return 0;
        }
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

bool Deliver(duckdb::ClientContext& ctx, ScanGlobalState& g,
             StreamLocalState& l, duckdb::DataChunk& output,
             duckdb::idx_t added) {
  SDB_ASSERT(added <= STANDARD_VECTOR_SIZE);
  const auto kept = FinalizeBatch(ctx, g, l, output, added);
  if (kept != 0) {
    output.SetChildCardinality(kept);
    return true;
  }
  output.Reset();
  return false;
}

}  // namespace

void RunStreamScan(duckdb::ClientContext& ctx,
                   duckdb::TableFunctionInput& input, ScanGlobalState& g,
                   StreamLocalState& l, duckdb::DataChunk& output) {
  if (!RunPrepareStage(input, g, l)) {
    return;
  }
  l.EnsureHitBatcher(g);
  for (;;) {
    if (l.has_unit) {
      if (!l.started) {
        if (!Resumes(l)) {
          if (const auto added = EmitChunk(ctx, g, l, output, true);
              added != 0) {
            if (Deliver(ctx, g, l, output, added)) {
              return;
            }
            continue;
          }
        }
        StartUnit(g, l);
      }
      if (const auto added = EmitChunk(ctx, g, l, output, false); added != 0) {
        if (Deliver(ctx, g, l, output, added)) {
          return;
        }
        continue;
      }
      if (PreviousPiece(l)) {
        l.started = false;
        continue;
      }
      FinishUnit(g, l);
    }
    if (!NextLiveUnit(g, l)) {
      if (const auto added = EmitChunk(ctx, g, l, output, true); added != 0) {
        if (Deliver(ctx, g, l, output, added)) {
          return;
        }
        continue;
      }
      break;
    }
    l.started = false;
  }
  output.SetChildCardinality(0);
}

}  // namespace sdb::connector
