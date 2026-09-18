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
#include <cmath>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/queries/vector_similarity_query.hpp>
#include <iresearch/search/top/make.hpp>
#include <iresearch/utils/debugging.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <numeric>

#include "connector/index_source_factory.h"
#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"
#include "query/config.h"

namespace sdb::connector {
namespace {

void SortByAddress(std::span<irs::ScoreDoc> hits) {
  absl::c_sort(hits, [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
  });
}

bool BetterScore(const irs::ScoreDoc& l, const irs::ScoreDoc& r) noexcept {
  if (l.score != r.score) {
    return l.score > r.score;
  }
  return std::pair{l.segment_idx, l.doc} < std::pair{r.segment_idx, r.doc};
}

void RerankHits(ScanGlobalState& g, std::span<irs::ScoreDoc> hits) {
  SDB_ASSERT(g.vector_scorer != nullptr);
  const auto& vs = *g.vector_scorer;
  const std::span<const float> query{vs.query_vector};
  const auto d = static_cast<uint32_t>(vs.query_vector.size());
  size_t i = 0;
  while (i < hits.size()) {
    const uint32_t seg = hits[i].segment_idx;
    size_t j = i + 1;
    while (j < hits.size() && hits[j].segment_idx == seg) {
      ++j;
    }
    const auto& sub = (*g.reader)[seg];
    if (const auto* vec_col = sub.Column(vs.field_id); vec_col != nullptr) {
      irs::RerankExactDistances(sub, *vec_col, d, query, vs.metric,
                                hits.subspan(i, j - i));
    }
    i = j;
  }
}

void CollectUnit(ScanGlobalState& g, TopKLocalState& l) {
  const auto& unit = l.unit;
  const auto& seg = (*g.reader)[unit.seg];
  l.Classify(g, unit.seg);
  if (l.seg_cls.segment_dead) {
    return;
  }
  if (!l.collector) {
    l.collector.emplace(l.local_threshold, l.hit_slice);
  }
  auto& collector = *l.collector;
  l.score_fetcher.Clear();
  collector.SetSegment(unit.seg);
  collector.RaiseScoreThreshold(
    g.topk.global_kth_score.load(std::memory_order_relaxed));

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
     .range = g.RangeOf(unit)});
  EnsurePlanned(plan != nullptr);
  plan->Run(collector);

  const irs::score_t kth = l.local_threshold;
  auto cur = g.topk.global_kth_score.load(std::memory_order_relaxed);
  while (kth > cur && !g.topk.global_kth_score.compare_exchange_weak(
                        cur, kth, std::memory_order_relaxed)) {
  }
}

void PublishHits(ScanGlobalState& g, TopKLocalState& l) {
  auto& t = g.topk;
  auto hits = l.collector ? l.hit_slice.first(l.collector->AcceptedCount())
                          : l.hit_slice.first(0);
  if (t.rerank_pool != 0 && g.vector_scorer != nullptr &&
      g.vector_scorer->quant != irs::VectorQuantization::None) {
    SortByAddress(hits);
    RerankHits(g, hits);
  }
  if (!g.has_lookup_filter && hits.size() > t.limit) {
    std::nth_element(hits.begin(), hits.begin() + t.limit, hits.end(),
                     BetterScore);
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
  if (!g.has_lookup_filter && all.size() > want) {
    std::nth_element(all.begin(), all.begin() + want, all.end(), BetterScore);
    all = all.first(want);
  }
  absl::c_sort(all, BetterScore);
  t.answer_rank.resize(all.size());
  std::iota(t.answer_rank.begin(), t.answer_rank.end(), 0);
  absl::c_sort(t.answer_rank, [&](uint32_t l, uint32_t r) {
    return std::pair{all[l].segment_idx, all[l].doc} <
           std::pair{all[r].segment_idx, all[r].doc};
  });
  t.answer.resize(all.size());
  for (uint32_t i = 0; i < t.answer_rank.size(); ++i) {
    t.answer[i] = all[t.answer_rank[i]];
  }
  for (uint32_t i = 0; i < t.answer.size();) {
    const auto seg = t.answer[i].segment_idx;
    const auto rg = (t.answer[i].doc - irs::doc_limits::min()) / g.rg_size;
    uint32_t j = i + 1;
    while (j < t.answer.size() && j - i < STANDARD_VECTOR_SIZE &&
           t.answer[j].segment_idx == seg &&
           (t.answer[j].doc - irs::doc_limits::min()) / g.rg_size == rg) {
      ++j;
    }
    t.fetch_units.push_back({.seg = seg, .first = i, .count = j - i});
    i = j;
  }
  t.fetched.resize(t.fetch_units.size());
  t.fetched_pk.resize(t.fetch_units.size());
}

bool FetchedColumn(const ScanGlobalState& g, duckdb::idx_t col) noexcept {
  return !g.needs_lookup ||
         g.lookup_projected_columns[col] == duckdb::DConstants::INVALID_INDEX;
}

void CopyFetched(const ScanGlobalState& g, const duckdb::DataChunk& from,
                 duckdb::DataChunk& into, duckdb::idx_t count,
                 duckdb::idx_t offset) {
  for (duckdb::idx_t c = 0; c < from.ColumnCount(); ++c) {
    if (FetchedColumn(g, c)) {
      duckdb::VectorOperations::Copy(from.data[c], into.data[c], count, 0,
                                     offset);
    }
  }
  into.SetChildCardinality(offset + count);
}

void AppendBatch(duckdb::ClientContext& ctx, ScanGlobalState& g,
                 TopKLocalState& l, duckdb::DataChunk& tmp,
                 duckdb::DataChunk& into, std::unique_ptr<duckdb::Vector>& pk,
                 duckdb::idx_t capacity, duckdb::idx_t& appended) {
  const auto count = EmitReadyBatch(ctx, g, l, tmp);
  if (count == 0) {
    tmp.Reset();
    return;
  }
  CopyFetched(g, tmp, into, count, appended);
  if (g.needs_lookup) {
    SDB_ASSERT(l.pk_column != nullptr);
    if (!pk) {
      pk = std::make_unique<duckdb::Vector>(l.pk_column->GetType(), capacity);
    }
    duckdb::VectorOperations::Copy(*l.pk_column, *pk, count, 0, appended);
  }
  appended += count;
  g.metrics.rows_fetched.fetch_add(count, std::memory_order_relaxed);
  tmp.Reset();
}

void FetchUnit(duckdb::ClientContext& ctx, ScanGlobalState& g,
               TopKLocalState& l, uint32_t index) {
  auto& t = g.topk;
  const auto& fu = t.fetch_units[index];
  SDB_IF_FAILURE("TopKFetchBudget") {
    static std::atomic_uint64_t gFetchedRows{0};
    const auto fetched =
      gFetchedRows.fetch_add(fu.count, std::memory_order_relaxed) + fu.count;
    if (fetched > t.limit) {
      THROW_SQL_ERROR(ERR_MSG("top-k read columns for ", fetched,
                              " rows against a limit of ", t.limit));
    }
  }
  auto chunk = duckdb::make_uniq<duckdb::DataChunk>();
  chunk->Initialize(ctx, g.projected_types, fu.count);
  if (l.fetch_tmp.ColumnCount() == 0) {
    l.fetch_tmp.Initialize(ctx, g.projected_types);
  }
  auto& tmp = l.fetch_tmp;
  std::unique_ptr<duckdb::Vector> pk;
  duckdb::idx_t appended = 0;

  l.EnsureHitBatcher(g);
  auto& batcher = *l.hit_batcher;
  batcher.BeginSegment(fu.seg, (*g.reader)[fu.seg].GetColReader(),
                       g.client_context);
  const std::span<const irs::ScoreDoc> hits{t.answer.data() + fu.first,
                                            fu.count};
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
      AppendBatch(ctx, g, l, tmp, *chunk, pk, fu.count, appended);
    }
  }
  SDB_ASSERT(appended == fu.count);
  t.fetched[index] = std::move(chunk);
  t.fetched_pk[index] = std::move(pk);
}

void BuildAnswer(duckdb::ClientContext& ctx, ScanGlobalState& g,
                 TopKLocalState& l) {
  auto& t = g.topk;
  const auto total = t.answer.size();
  l.answer_chunk = duckdb::make_uniq<duckdb::DataChunk>();
  l.answer_chunk->Initialize(ctx, g.projected_types,
                             std::max<size_t>(1, total));
  std::vector<uint32_t> row_answer;
  row_answer.reserve(total);
  if (!g.needs_lookup) {
    for (uint32_t u = 0; u < t.fetch_units.size(); ++u) {
      l.answer_chunk->Append(*t.fetched[u],
                             duckdb::VectorAppendMode::ALLOW_RESIZE);
      const auto& fu = t.fetch_units[u];
      for (uint32_t i = 0; i < fu.count; ++i) {
        row_answer.push_back(fu.first + i);
      }
    }
  } else {
    if (!l.index_source) {
      l.index_source =
        MakeIndexSource(ctx, g.Bind(), g.lookup_projected_columns,
                        g.projected_types, g.Bind().columns.ids,
                        const_cast<duckdb::TableFilterSet*>(g.pushed_filters));
    }
    duckdb::DataChunk batch;
    batch.Initialize(ctx, g.projected_types);
    for (uint32_t u = 0; u < t.fetch_units.size(); ++u) {
      const auto& fu = t.fetch_units[u];
      auto& fetched = *t.fetched[u];
      auto& pk = *t.fetched_pk[u];
      batch.Reset();
      CopyFetched(g, fetched, batch, fu.count, 0);
      const auto rows = l.index_source->Materialize(ctx, pk, fu.count, batch);
      g.metrics.rows_looked_up.fetch_add(fu.count, std::memory_order_relaxed);
      const auto survivors = l.index_source->Survivors();
      for (duckdb::idx_t i = 0; i < rows; ++i) {
        row_answer.push_back(fu.first + static_cast<uint32_t>(survivors[i]));
      }
      batch.SetChildCardinality(rows);
      l.answer_chunk->Append(batch, duckdb::VectorAppendMode::ALLOW_RESIZE);
    }
  }
  l.answer_order.resize(row_answer.size());
  std::iota(l.answer_order.begin(), l.answer_order.end(), 0);
  const auto rank_of = [&](uint32_t row) {
    return t.answer_rank[row_answer[row]];
  };
  const auto want = static_cast<size_t>(t.limit);
  if (l.answer_order.size() > want) {
    std::nth_element(l.answer_order.begin(), l.answer_order.begin() + want,
                     l.answer_order.end(), [&](uint32_t a, uint32_t b) {
                       return rank_of(a) < rank_of(b);
                     });
    l.answer_order.resize(want);
  }
  absl::c_sort(l.answer_order,
               [&](uint32_t a, uint32_t b) { return rank_of(a) < rank_of(b); });
  if (t.offset != 0) {
    l.answer_order.erase(l.answer_order.begin(),
                         l.answer_order.begin() +
                           std::min<size_t>(t.offset, l.answer_order.size()));
  }
  l.emit_sel_data = duckdb::make_buffer<duckdb::SelectionData>(
    std::max<duckdb::idx_t>(1, l.answer_order.size()));
  l.emit_sel.Initialize(l.emit_sel_data);
}

void EmitNext(TopKLocalState& l, duckdb::DataChunk& output) {
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

void InitTopKGlobal(ScanGlobalState& g, duckdb::ClientContext& context) {
  auto& t = g.topk;
  const auto& ss = g.Bind();
  t.limit = *ss.score.top_k;
  t.offset = ss.score.top_n_consumed ? ss.score.top_offset : 0;
  if (ss.score.vector &&
      (ss.score.vector->quant != irs::VectorQuantization::None ||
       g.has_lookup_filter)) {
    static constinit SettingRef gRerank{"sdb_rerank_factor"};
    const auto k = static_cast<double>(*ss.score.top_k);
    const double pool = std::ceil(gRerank.Double(context) * k);
    t.rerank_pool = pool == 0 ? 0 : static_cast<uint32_t>(std::max(pool, k));
  }
  t.pool =
    t.rerank_pool != 0 ? t.rerank_pool : static_cast<uint32_t>(*ss.score.top_k);
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
  auto& t = g.topk;
  if (!RunPrepareStage(input, g, l)) {
    return;
  }
  if (!l.published) {
    uint32_t finished = 0;
    while (!l.units_exhausted) {
      if (!ClaimUnit(g, l)) {
        break;
      }
      CollectUnit(g, l);
      finished += static_cast<uint32_t>(FinishUnit(g, l));
    }
    // The hits have to be visible to the merger before the segments they came
    // from are counted: the merge runs as soon as the count is complete.
    PublishHits(g, l);
    l.published = true;
    if (finished != 0) {
      FinishSegments(g, finished);
    }
  }
  if (!t.merge_barrier.Released()) {
    if (g.done_segments.load(std::memory_order_acquire) == g.live_segments &&
        !t.merge_taken.exchange(true, std::memory_order_acq_rel)) {
      Merge(g);
      t.merge_barrier.Release(input);
    } else {
      if (t.merge_barrier.Park(input)) {
        g.metrics.parked.fetch_add(1, std::memory_order_relaxed);
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

}  // namespace sdb::connector
