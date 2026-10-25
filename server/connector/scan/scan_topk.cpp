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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/top/make.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <numeric>

#include "connector/index_source_factory.h"
#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

void CollectUnit(ScanGlobalState& g, TopKLocalState& l) {
  const auto& unit = l.unit;
  if (!l.collector) {
    l.collector.emplace(g.topk.global_kth_score, l.hit_slice);
  }
  auto& collector = *l.collector;
  collector.SetSegment(unit.seg);
  const auto range = g.RangeOf(unit);
  if (l.root_seg != unit.seg || range.begin < l.root_end) {
    const auto& seg = (*g.reader)[unit.seg];
    l.score_fetcher.Clear();
    const auto& seg_query = EnsureSegmentQuery(g, l, unit.seg);
    auto* table = BeginVerify(l.col_verify, seg, g, l);
    SDB_ENSURE(g.scorer_obj != nullptr,
               "a scan that ranks by score has a scorer to rank with");
    auto plan = irs::top::MakeRoot(
      seg_query,
      {.scorer = *g.scorer_obj,
       .fetcher = l.score_fetcher,
       .table = table,
       .prune = g.prune_scorer != nullptr && g.stats_scorer == g.prune_scorer,
       .k = static_cast<uint32_t>(l.hit_slice.size()),
       .span = g.UnitSpan(unit)});
    EnsurePlanned(plan != nullptr);
    l.root = std::move(plan);
    l.root_seg = unit.seg;
  }
  irs::utils::downCast<irs::top::Root>(l.root.get())
    ->Run(range.begin, range.end, collector);
  l.root_end = range.end;
}

void PublishHits(ScanGlobalState& g, TopKLocalState& l) {
  auto& t = g.topk;
  auto hits = l.collector ? l.hit_slice.first(l.collector->AcceptedCount())
                          : l.hit_slice.first(0);
  if (hits.size() > t.limit) {
    std::nth_element(hits.begin(), hits.begin() + t.limit, hits.end(),
                     BetterScore{});
    hits = hits.first(t.limit);
  }
  t.accepted[l.worker].store(static_cast<uint32_t>(hits.size()),
                             std::memory_order_release);
}

void Merge(ScanGlobalState& g) {
  auto& t = g.topk;
  size_t total = 0;
  for (uint32_t w = 0; w < g.workers; ++w) {
    const auto n = t.accepted[w].load(std::memory_order_acquire);
    auto* const slice = t.hits.data() + size_t{w} * t.pool;
    SDB_ASSERT(total <= size_t{w} * t.pool);
    std::copy_n(slice, n, t.hits.data() + total);
    total += n;
  }
  std::span<irs::ScoreDoc> all{t.hits.data(), total};
  const auto want = static_cast<size_t>(t.limit);
  if (all.size() > want) {
    std::nth_element(all.begin(), all.begin() + want, all.end(), BetterScore{});
    all = all.first(want);
  }
  t.answer.assign(all.begin(), all.end());
  PlanFetch(g);
}

bool FetchedColumn(const ScanGlobalState& g, duckdb::idx_t col) noexcept {
  return !g.needs_lookup ||
         g.lookup_projected_columns[col] == duckdb::DConstants::INVALID_INDEX;
}

bool ProducedColumn(const ScanGlobalState& g, const FetchLocalState& f,
                    duckdb::idx_t col) noexcept {
  if (col == g.score_output_idx || col == g.tableoid_output_idx ||
      col == g.doc_address_output_idx) {
    return true;
  }
  if (absl::c_any_of(f.offsets_entries,
                     [col](const auto& e) { return e.output_idx == col; })) {
    return true;
  }
  return absl::c_any_of(g.cs_projections,
                        [col](const auto& p) { return p.output_slot == col; });
}

void CopyFetched(const ScanGlobalState& g, const FetchLocalState& f,
                 const duckdb::DataChunk& from, duckdb::DataChunk& into,
                 duckdb::idx_t count, duckdb::idx_t offset) {
  for (duckdb::idx_t c = 0; c < from.ColumnCount(); ++c) {
    if (!FetchedColumn(g, c)) {
      continue;
    }
    if (!ProducedColumn(g, f, c)) {
      into.data[c].SetVectorType(duckdb::VectorType::CONSTANT_VECTOR);
      duckdb::ConstantVector::SetNull(into.data[c], true);
      continue;
    }
    duckdb::VectorOperations::Copy(from.data[c], into.data[c], count, 0,
                                   offset);
  }
  into.SetChildCardinality(offset + count);
}

void AppendBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                 FetchLocalState& l, duckdb::DataChunk& tmp,
                 duckdb::DataChunk& into, std::unique_ptr<duckdb::Vector>& pk,
                 duckdb::idx_t capacity, duckdb::idx_t& appended) {
  const auto count = EmitReadyBatch(ctx, g, l, tmp);
  if (count == 0) {
    tmp.Reset();
    return;
  }
  SDB_ASSERT(appended + count <= capacity);
  CopyFetched(g, l, tmp, into, count, appended);
  if (g.needs_lookup) {
    SDB_ASSERT(l.pk_column != nullptr);
    if (!pk) {
      pk = std::make_unique<duckdb::Vector>(l.pk_column->GetType(), capacity);
    }
    duckdb::VectorOperations::Copy(*l.pk_column, *pk, count, 0, appended);
  }
  appended += count;
  tmp.Reset();
}

}  // namespace

FetchedHits FetchHits(duckdb::ClientContext& ctx, ScanGlobalState& g,
                      RankedLocalState& l, uint32_t seg,
                      std::span<const irs::ScoreDoc> hits, bool rebind) {
  const auto& t = g.topk;
  SDB_IF_FAILURE("TopKFetchBudget") {
    static std::atomic_uint64_t gFetchedRows{0};
    const auto fetched =
      gFetchedRows.fetch_add(hits.size(), std::memory_order_relaxed) +
      hits.size();
    if (fetched > t.limit) {
      THROW_SQL_ERROR(ERR_MSG("top-k read columns for ", fetched,
                              " rows against a limit of ", t.limit));
    }
  }
  auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
  chunk->Initialize(ctx, g.projected_types, hits.size());
  if (l.fetch_tmp.ColumnCount() == 0) {
    l.fetch_tmp.Initialize(ctx, g.projected_types);
  }
  auto& tmp = l.fetch_tmp;
  std::unique_ptr<duckdb::Vector> pk;
  duckdb::idx_t appended = 0;
  AppendHits(ctx, g, l, tmp, seg, hits, rebind, *chunk, pk, appended);
  SDB_ASSERT(appended == hits.size());
  return {.chunk = std::move(chunk), .pk = std::move(pk)};
}

void AppendHits(duckdb::ClientContext& ctx, ScanGlobalState& g,
                FetchLocalState& l, duckdb::DataChunk& tmp, uint32_t seg,
                std::span<const irs::ScoreDoc> hits, bool rebind,
                duckdb::DataChunk& chunk, std::unique_ptr<duckdb::Vector>& pk,
                duckdb::idx_t& appended) {
  l.EnsureHitBatcher(g);
  auto& batcher = *l.hit_batcher;
  if (rebind || !batcher.Bound(seg)) {
    batcher.BeginSegment(seg, (*g.reader)[seg].GetColReader(),
                         g.client_context);
  }
  const bool scored = g.ScanScore();
  size_t at = 0;
  bool done = false;
  while (!done) {
    while (!batcher.Ready()) {
      if (at == hits.size()) {
        if (batcher.Empty()) {
          done = true;
          break;
        }
        batcher.Finalize();
        if (!batcher.Ready()) {
          done = true;
        }
        break;
      }
      const auto row = hits[at].doc - irs::doc_limits::min();
      const auto span = batcher.OpenWindow(row);
      if (span == 0) {
        break;
      }
      auto* out_docs = batcher.WindowHead();
      auto* out_scores = scored ? batcher.ScoreHead() : nullptr;
      duckdb::idx_t n = 0;
      while (at != hits.size() &&
             (hits[at].doc - irs::doc_limits::min()) < row + span) {
        out_docs[n] = hits[at].doc;
        if (out_scores != nullptr) {
          out_scores[n] = hits[at].score;
        }
        ++n;
        ++at;
      }
      batcher.CommitWindow(n);
    }
    if (batcher.Ready()) {
      AppendBatch(ctx, g, l, tmp, chunk, pk, chunk.GetCapacity(), appended);
    }
  }
}

namespace {

void FetchUnit(duckdb::ClientContext& ctx, ScanGlobalState& g,
               RankedLocalState& l, uint32_t index) {
  auto& t = g.topk;
  const auto& fu = t.fetch_units[index];
  auto fetched =
    FetchHits(ctx, g, l, fu.seg, {t.answer.data() + fu.first, fu.count}, false);
  t.fetched[index] = std::move(fetched.chunk);
  t.fetched_pk[index] = std::move(fetched.pk);
}

}  // namespace

void EnsureIndexSource(duckdb::ClientContext& ctx, const ScanGlobalState& g,
                       FetchLocalState& l) {
  if (!l.index_source) {
    l.index_source =
      MakeIndexSource(ctx, g.Bind(), *g.snapshot, g.lookup_projected_columns,
                      g.projected_types, g.Bind().columns.ids,
                      const_cast<duckdb::TableFilterSet*>(g.pushed_filters));
  }
}

void MaterializeHits(duckdb::ClientContext& ctx, const ScanGlobalState& g,
                     RankedLocalState& l, const duckdb::DataChunk& fetched,
                     duckdb::Vector& pk, std::span<const irs::ScoreDoc> hits,
                     duckdb::DataChunk& batch,
                     std::vector<irs::ScoreDoc>& row_hits) {
  batch.Reset();
  CopyFetched(g, l, fetched, batch, hits.size(), 0);
  const auto rows = l.index_source->Materialize(ctx, pk, hits.size(), batch);
  const auto survivors = l.index_source->Survivors();
  for (duckdb::idx_t i = 0; i < rows; ++i) {
    row_hits.push_back(hits[survivors[i]]);
  }
  batch.SetChildCardinality(rows);
  l.answer_chunk->Append(batch, duckdb::VectorAppendMode::ALLOW_RESIZE);
}

namespace {

void BuildAnswer(duckdb::ClientContext& ctx, ScanGlobalState& g,
                 RankedLocalState& l) {
  auto& t = g.topk;
  const auto total = t.answer.size();
  l.answer_chunk = duckdb::make_uniq<duckdb::DataChunk>();
  l.answer_chunk->Initialize(ctx, g.projected_types,
                             std::max<size_t>(1, total));
  std::vector<irs::ScoreDoc> row_hits;
  if (!g.needs_lookup) {
    for (uint32_t u = 0; u < t.fetch_units.size(); ++u) {
      l.answer_chunk->Append(*t.fetched[u],
                             duckdb::VectorAppendMode::ALLOW_RESIZE);
    }
    row_hits = t.answer;
  } else {
    EnsureIndexSource(ctx, g, l);
    row_hits.reserve(total);
    duckdb::DataChunk batch;
    batch.Initialize(ctx, g.projected_types);
    for (uint32_t u = 0; u < t.fetch_units.size(); ++u) {
      const auto& fu = t.fetch_units[u];
      MaterializeHits(ctx, g, l, *t.fetched[u], *t.fetched_pk[u],
                      {t.answer.data() + fu.first, fu.count}, batch, row_hits);
    }
    if (g.has_lookup_filter && g.vector_scorer) {
      VerifyMore(ctx, g, l, row_hits);
    }
  }
  l.answer_order.resize(row_hits.size());
  std::iota(l.answer_order.begin(), l.answer_order.end(), 0);
  const auto better = [&](uint32_t a, uint32_t b) {
    return BetterScore{}(row_hits[a], row_hits[b]);
  };
  const auto want = static_cast<size_t>(t.limit);
  if (l.answer_order.size() > want) {
    std::nth_element(l.answer_order.begin(), l.answer_order.begin() + want,
                     l.answer_order.end(), better);
    l.answer_order.resize(want);
  }
  absl::c_sort(l.answer_order, better);
  if (t.offset != 0) {
    l.answer_order.erase(l.answer_order.begin(),
                         l.answer_order.begin() +
                           std::min<size_t>(t.offset, l.answer_order.size()));
  }
  l.emit_sel_data = duckdb::make_buffer<duckdb::SelectionData>(
    std::max<duckdb::idx_t>(1, l.answer_order.size()));
  l.emit_sel.Initialize(l.emit_sel_data);
}

void EmitNext(RankedLocalState& l, duckdb::DataChunk& output) {
  const auto remaining = l.answer_order.size() - l.emitted;
  const auto n = std::min<duckdb::idx_t>(remaining, STANDARD_VECTOR_SIZE);
  if (n == 0) {
    output.SetChildCardinality(0);
    return;
  }
  for (duckdb::idx_t i = 0; i < n; ++i) {
    l.emit_sel.set_index(i, l.answer_order[l.emitted + i]);
  }
  output.Slice(*l.answer_chunk, l.emit_sel, n);
  l.emitted += n;
}

}  // namespace

void SortByAddress(std::span<irs::ScoreDoc> hits) {
  absl::c_sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

std::vector<ScanGlobalState::TopKState::FetchUnit> FetchUnitsOf(
  std::span<const irs::ScoreDoc> hits, uint64_t rg_size) {
  std::vector<ScanGlobalState::TopKState::FetchUnit> units;
  for (uint32_t i = 0; i < hits.size();) {
    const auto seg = hits[i].segment_idx;
    const auto rg = (hits[i].doc - irs::doc_limits::min()) / rg_size;
    uint32_t j = i + 1;
    while (j < hits.size() && j - i < STANDARD_VECTOR_SIZE &&
           hits[j].segment_idx == seg &&
           (hits[j].doc - irs::doc_limits::min()) / rg_size == rg) {
      ++j;
    }
    units.push_back({.seg = seg, .first = i, .count = j - i});
    i = j;
  }
  return units;
}

void PlanFetch(ScanGlobalState& g) {
  auto& t = g.topk;
  SortByAddress(t.answer);
  t.fetch_units = FetchUnitsOf(t.answer, g.rg_size);
  t.fetched.resize(t.fetch_units.size());
  t.fetched_pk.resize(t.fetch_units.size());
}

void RunAnswerStages(duckdb::ClientContext& ctx,
                     duckdb::TableFunctionInput& input, ScanGlobalState& g,
                     RankedLocalState& l, duckdb::DataChunk& output,
                     absl::FunctionRef<void()> merge) {
  auto& t = g.topk;
  if (!t.merge_barrier.Released()) {
    const bool collected =
      g.done_segments.load(std::memory_order_acquire) == g.live_segments &&
      t.published.load(std::memory_order_acquire) ==
        g.worker_count.load(std::memory_order_acquire);
    if (collected && !t.merge_taken.exchange(true, std::memory_order_acq_rel)) {
      merge();
      t.merge_barrier.Release(input);
    } else {
      if (!t.shared_fetch &&
          duckdb::TaskScheduler::GetScheduler(ctx).GetNumberOfTasks() != 0) {
        output.SetChildCardinality(0);
        return;
      }
      if (t.merge_barrier.Park(input, ctx)) {
        l.parked_on = &t.merge_barrier;
        return;
      }
      if (!t.merge_barrier.Released()) {
        output.SetChildCardinality(0);
        return;
      }
    }
  }
  for (;;) {
    const auto i = t.next_fetch_unit.fetch_add(1, std::memory_order_relaxed);
    if (i >= t.fetch_units.size()) {
      break;
    }
    FetchUnit(ctx, g, l, i);
    if (t.fetch_done.fetch_add(1, std::memory_order_acq_rel) + 1 ==
        t.fetch_units.size()) {
      if (!t.emit_taken.exchange(true, std::memory_order_acq_rel)) {
        l.emitter = true;
        BuildAnswer(ctx, g, l);
      }
    }
  }
  if (!l.emitter) {
    output.SetChildCardinality(0);
    return;
  }
  EmitNext(l, output);
}

void InitTopKGlobal(ScanGlobalState& g, duckdb::ClientContext& /*context*/) {
  auto& t = g.topk;
  t.limit = *g.top_k;
  t.offset = g.top_offset;
  t.shared_fetch = t.limit > STANDARD_VECTOR_SIZE;
  const auto k = static_cast<double>(*g.top_k);
  t.pool = ClampCandidates(k, k, g.reader->live_docs_count());
  t.hits.resize(size_t{g.workers} * t.pool);
  t.accepted = std::make_unique<std::atomic_uint32_t[]>(g.workers);
  for (uint32_t w = 0; w < g.workers; ++w) {
    t.accepted[w].store(0, std::memory_order_relaxed);
  }
}

void InitTopKLocal(ScanGlobalState& g, TopKLocalState& l,
                   duckdb::TableFunctionInitInput& input) {
  SDB_ASSERT(l.worker < g.workers);
  l.hit_slice = std::span<irs::ScoreDoc>{g.topk.hits}.subspan(
    size_t{l.worker} * g.topk.pool, g.topk.pool);
  BuildOffsetsEntries(l, input, g.Bind());
}

void RunTopKScan(duckdb::ClientContext& ctx, duckdb::TableFunctionInput& input,
                 ScanGlobalState& g, TopKLocalState& l,
                 duckdb::DataChunk& output) {
  if (!RunPrepareStage(input, g, l)) {
    return;
  }
  if (!l.published) {
    while (NextLiveUnit(g, l)) {
      CollectUnit(g, l);
      FinishUnit(g, l);
    }
    PublishHits(g, l);
    l.published = true;
    g.topk.published.fetch_add(1, std::memory_order_acq_rel);
  }
  RunAnswerStages(ctx, input, g, l, output, [&] { Merge(g); });
}

}  // namespace sdb::connector
