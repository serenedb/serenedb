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
#include <iresearch/index/index_reader.hpp>
#include <iresearch/search/ann/family.hpp>
#include <iresearch/search/queries/vector_similarity_query.hpp>
#include <iterator>

#include "connector/scan/scan_plan.h"
#include "connector/scan/scan_state.h"

namespace sdb::connector {
namespace {

irs::detail::RawVectorReader* SegmentRescorer(const ScanGlobalState& g,
                                              AnnLocalState& l, uint32_t seg) {
  SDB_ASSERT(g.vector_scorer != nullptr);
  const auto& vs = *g.vector_scorer;
  if (l.rescorers.size() <= seg) {
    l.rescorers.resize(g.reader->size());
  }
  auto& reader = l.rescorers[seg];
  if (!reader) {
    const auto& sub = (*g.reader)[seg];
    const auto* column = sub.Column(vs.field_id);
    const auto* columns = sub.GetColReader();
    if (!column || !columns) {
      return nullptr;
    }
    reader = std::make_unique<irs::detail::RawVectorReader>(
      *column, *columns, static_cast<uint32_t>(vs.query_vector.size()));
    reader->SetQuery(vs.query_vector, vs.metric);
  }
  return reader.get();
}

void RescoreUnit(const ScanGlobalState& g, AnnLocalState& l,
                 std::span<irs::ScoreDoc> hits) {
  auto* reader = SegmentRescorer(g, l, hits.front().segment_idx);
  if (!reader) {
    return;
  }
  l.rescore_docs.resize(hits.size());
  l.rescore_scores.resize(hits.size());
  for (size_t i = 0; i < hits.size(); ++i) {
    l.rescore_docs[i] = hits[i].doc;
  }
  reader->ComputeDistances(l.rescore_docs, l.rescore_scores);
  for (size_t i = 0; i < hits.size(); ++i) {
    hits[i].score = l.rescore_scores[i];
  }
}

irs::score_t RescoreBar(const ScanGlobalState::AnnState& a) noexcept {
  return std::max(a.rescore_bar.load(std::memory_order_acquire),
                  a.floor->Load());
}

void PushBest(ScanGlobalState& g, std::span<const irs::ScoreDoc> hits) {
  auto& a = g.ann;
  const auto limit = g.topk.limit;
  absl::MutexLock lock{&a.best_mutex};
  for (const auto& hit : hits) {
    if (hit.score <= g.score_static_floor) {
      continue;
    }
    if (a.best.size() < limit) {
      a.best.push_back(hit);
      absl::c_push_heap(a.best, BetterScore{});
    } else if (BetterScore{}(hit, a.best.front())) {
      absl::c_pop_heap(a.best, BetterScore{});
      a.best.back() = hit;
      absl::c_push_heap(a.best, BetterScore{});
    }
  }
  if (a.best.size() == limit &&
      a.best.front().score > a.rescore_bar.load(std::memory_order_relaxed)) {
    a.rescore_bar.store(a.best.front().score, std::memory_order_release);
  }
}

void RescoreWhileReachable(ScanGlobalState& g, AnnLocalState& l,
                           std::span<irs::ScoreDoc> pool) {
  auto& a = g.ann;
  const auto reachable =
    absl::c_partition(pool, [bar = RescoreBar(a)](const irs::ScoreDoc& hit) {
      return hit.score >= bar;
    });
  pool = pool.first(static_cast<size_t>(reachable - pool.begin()));
  const auto lower = [](const irs::ScoreDoc& l, const irs::ScoreDoc& r) {
    return l.score < r.score;
  };
  absl::c_make_heap(pool, lower);
  constexpr size_t kAhead = 8;
  constexpr size_t kNear = 2;
  constexpr size_t kPushBatch = 8;
  struct Row {
    irs::detail::RawVectorReader* reader = nullptr;
    const irs::byte_type* data = nullptr;
  };
  std::array<Row, kAhead + 1> rows;
  std::array<irs::ScoreDoc, kPushBatch> pending;
  size_t pending_size = 0;
  size_t heap_size = pool.size();
  for (size_t i = 0;; ++i) {
    const auto bar = RescoreBar(a);
    while (heap_size != 0 && pool.size() - heap_size <= i + kAhead &&
           pool.front().score >= bar) {
      auto heap = pool.first(heap_size--);
      absl::c_pop_heap(heap, lower);
      const auto& next = pool[heap_size];
      auto* reader = SegmentRescorer(g, l, next.segment_idx);
      rows[(pool.size() - 1 - heap_size) % rows.size()] = {
        .reader = reader,
        .data = reader ? reader->PrefetchRow<2>(next.doc) : nullptr,
      };
    }
    const auto popped = pool.size() - heap_size;
    if (i == popped) {
      break;
    }
    if (i + kNear < popped) {
      if (const auto& near = rows[(i + kNear) % rows.size()]; near.data) {
        near.reader->PromoteRow(near.data);
      }
    }
    auto& hit = pool[pool.size() - 1 - i];
    if (hit.score < bar) {
      break;
    }
    if (const auto& row = rows[i % rows.size()]; row.reader) {
      hit.score = row.reader->ComputeDistance(hit.doc, row.data);
    }
    if (hit.score >= a.rescore_bar.load(std::memory_order_relaxed)) {
      pending[pending_size++] = hit;
      if (pending_size == kPushBatch) {
        PushBest(g, pending);
        pending_size = 0;
      }
    }
  }
  if (pending_size != 0) {
    PushBest(g, std::span{pending}.first(pending_size));
  }
}

void FlushSegment(ScanGlobalState& g, AnnLocalState& l) {
  auto pool = l.collector->Flush();
  if (!pool.empty()) {
    for (auto& hit : pool) {
      hit.segment_idx = l.pool_seg;
    }
    if (g.has_lookup_filter) {
      const auto ranked = absl::c_partition(
        pool, [floor = l.segment_floor->Load()](const irs::ScoreDoc& hit) {
          return hit.score >= floor;
        });
      pool = pool.first(static_cast<size_t>(ranked - pool.begin()));
      if (g.ann.rescore && !pool.empty()) {
        SortByAddress(pool);
        RescoreUnit(g, l, pool);
      }
      for (const auto& hit : pool) {
        if (hit.score > g.score_static_floor) {
          l.rescored.push_back(hit);
        }
      }
      l.segment_floor->Reset(g.score_static_floor);
    } else if (g.ann.rescore) {
      absl::c_copy_if(pool, std::back_inserter(l.pending),
                      [bar = RescoreBar(g.ann)](const irs::ScoreDoc& hit) {
                        return hit.score >= bar;
                      });
    } else {
      PushBest(g, pool);
    }
  }
  l.collector->Clear();
}

void CollectUnit(ScanGlobalState& g, AnnLocalState& l) {
  const auto& unit = l.unit;
  if (l.pool_seg != unit.seg) {
    if (l.pool_seg != std::numeric_limits<uint32_t>::max()) {
      FlushSegment(g, l);
    }
    l.pool_seg = unit.seg;
  }
  if (l.root_seg != unit.seg) {
    const auto& seg = (*g.reader)[unit.seg];
    l.score_fetcher.Clear();
    const auto& seg_query = EnsureSegmentQuery(g, l, unit.seg);
    auto* table = BeginVerify(l.col_verify, seg, g, l);
    SDB_ENSURE(g.scorer_obj != nullptr,
               "a scan that ranks by score has a scorer to rank with");
    auto plan = irs::ann::MakeRoot(seg_query, {.scorer = *g.scorer_obj,
                                               .fetcher = l.score_fetcher,
                                               .table = table,
                                               .k = l.collector->K()});
    EnsurePlanned(plan != nullptr);
    l.root = std::move(plan);
    l.root_seg = unit.seg;
  }
  irs::utils::downCast<irs::ann::Root>(l.root.get())->Run(*l.collector);
}

void PublishHits(ScanGlobalState& g, AnnLocalState& l) {
  if (l.pool_seg != std::numeric_limits<uint32_t>::max()) {
    FlushSegment(g, l);
  }
  if (!g.has_lookup_filter) {
    RescoreWhileReachable(g, l, l.pending);
  } else if (!l.rescored.empty()) {
    absl::MutexLock lock{&g.ann.best_mutex};
    absl::c_copy(l.rescored, std::back_inserter(g.ann.rescored));
  }
}

void Merge(ScanGlobalState& g) {
  auto& t = g.topk;
  auto& a = g.ann;
  if (!g.has_lookup_filter) {
    {
      absl::MutexLock lock{&a.best_mutex};
      t.answer.assign(a.best.begin(), a.best.end());
    }
    PlanFetch(g);
    return;
  }
  {
    absl::MutexLock lock{&a.best_mutex};
    t.hits = std::move(a.rescored);
  }
  absl::c_sort(t.hits, BetterScore{});
  t.ranked = t.hits;
  t.answer.assign(
    t.hits.begin(),
    t.hits.begin() + std::min<size_t>(t.hits.size(), t.verify_first));
  PlanFetch(g);
}

std::vector<irs::ScoreDoc> RankExactly(ScanGlobalState& g, RankedLocalState& l,
                                       uint64_t per_segment) {
  auto vs = *g.vector_scorer;
  vs.exact = true;
  const auto filter =
    MakeVectorFilter(vs, g.ClaimedWhere(), vs.EffectiveRadius());
  std::vector<irs::ScoreDoc> ranked;
  for (const auto seg : g.segment_order) {
    l.Classify(g, seg);
    if (l.seg_cls.segment_dead) {
      continue;
    }
    const auto& segment = (*g.reader)[seg];
    const auto slots = std::min<uint64_t>(segment.docs_count(), per_segment);
    if (slots == 0) {
      continue;
    }
    const auto query = filter->PrepareSegment(segment, {});
    irs::ann::SharedFloor floor{static_cast<uint32_t>(slots)};
    irs::ann::Collector collector{floor, static_cast<uint32_t>(slots)};
    l.score_fetcher.Clear();
    auto root = irs::ann::MakeRoot(
      *query, {.scorer = *g.scorer_obj,
               .fetcher = l.score_fetcher,
               .table = BeginVerify(l.col_verify, segment, g, l),
               .k = static_cast<uint32_t>(slots)});
    EnsurePlanned(static_cast<bool>(root));
    root->Run(collector);
    auto hits = collector.Flush();
    if (hits.size() > slots) {
      std::nth_element(hits.begin(), hits.begin() + slots, hits.end(),
                       BetterScore{});
      hits = hits.first(slots);
    }
    for (auto& hit : hits) {
      if (hit.score > g.score_static_floor) {
        hit.segment_idx = seg;
        ranked.push_back(hit);
      }
    }
  }
  absl::c_sort(ranked, BetterScore{});
  return ranked;
}

void FillShortAnswer(ScanGlobalState& g, RankedLocalState& l) {
  auto& t = g.topk;
  const auto* vs = g.vector_scorer;
  if (t.answer.size() >= t.limit || g.has_lookup_filter ||
      vs->kind != irs::AnnKind::Ivf || vs->exact ||
      vs->radius != std::numeric_limits<float>::max()) {
    return;
  }
  t.answer = RankExactly(g, l, t.limit);
  t.answer.resize(std::min<size_t>(t.answer.size(), t.limit));
  PlanFetch(g);
}

}  // namespace

void VerifyMore(duckdb::ClientContext& ctx, ScanGlobalState& g,
                RankedLocalState& l, std::vector<irs::ScoreDoc>& row_hits) {
  const auto& t = g.topk;
  const size_t want = t.limit;
  auto ranked = t.ranked;
  size_t verified = t.answer.size();
  size_t step = std::max<size_t>(verified, want);
  bool widened =
    g.vector_scorer->exact || ranked.size() >= g.reader->live_docs_count();
  std::vector<irs::ScoreDoc> exact;
  std::vector<irs::ScoreDoc> hits;
  duckdb::DataChunk batch;
  batch.Initialize(ctx, g.projected_types);
  while (row_hits.size() < want) {
    if (verified == ranked.size()) {
      if (widened) {
        return;
      }
      widened = true;
      exact = RankExactly(g, l, std::numeric_limits<uint64_t>::max());
      ranked = exact;
      verified = 0;
      step = std::max<size_t>(step, want);
      row_hits.clear();
      l.answer_chunk->Reset();
      continue;
    }
    step *= 4;
    const auto take = std::min(ranked.size() - verified, step);
    hits.assign(ranked.begin() + verified, ranked.begin() + verified + take);
    verified += take;
    SortByAddress(hits);
    const auto units = FetchUnitsOf(hits, g.rg_size);
    for (size_t u = 0; u < units.size(); ++u) {
      const auto& unit = units[u];
      const std::span<const irs::ScoreDoc> part{hits.data() + unit.first,
                                                unit.count};
      auto fetched = FetchHits(ctx, g, l, unit.seg, part,
                               u == 0 || units[u - 1].seg != unit.seg);
      MaterializeHits(ctx, g, l, *fetched.chunk, *fetched.pk, part, batch,
                      row_hits);
    }
  }
}

void InitAnnGlobal(ScanGlobalState& g, duckdb::ClientContext& /*context*/) {
  auto& t = g.topk;
  auto& a = g.ann;
  t.limit = *g.top_k;
  t.offset = g.top_offset;
  t.shared_fetch = g.has_lookup_filter || t.limit > STANDARD_VECTOR_SIZE;
  const auto* vs = g.vector_scorer;
  SDB_ASSERT(vs);
  const auto k = static_cast<double>(*g.top_k);
  double pool = k;
  if (!vs->exact) {
    a.rescore =
      vs->rescore_width > 0.f ||
      (g.has_lookup_filter && vs->quant != irs::VectorQuantization::None);
    if (g.has_lookup_filter) {
      constexpr double kLookupFilterOverfetch = 4.0;
      pool = std::max(pool, vs->kind == irs::AnnKind::Hnsw
                              ? static_cast<double>(vs->ef_search)
                              : k * kLookupFilterOverfetch);
    }
  }
  auto ranked = ClampCandidates(pool, k, g.reader->live_docs_count());
  if (vs->exact && g.has_lookup_filter) {
    g.workers = 1;
    ranked = static_cast<uint32_t>(
      std::clamp<uint64_t>(g.reader->live_docs_count(), ranked,
                           std::numeric_limits<uint32_t>::max()));
    t.verify_first = static_cast<uint32_t>(std::min<uint64_t>(
      std::max<uint64_t>(uint64_t{4} * t.limit, STANDARD_VECTOR_SIZE),
      std::numeric_limits<uint32_t>::max()));
  }
  a.floor.emplace(ranked, g.score_static_floor);
}

void InitAnnLocal(ScanGlobalState& g, AnnLocalState& l,
                  duckdb::TableFunctionInitInput& input) {
  SDB_ASSERT(l.worker < g.workers);
  const auto k = g.ann.floor->K();
  if (g.has_lookup_filter) {
    l.segment_floor.emplace(k, g.score_static_floor);
    l.collector.emplace(*l.segment_floor, k);
  } else {
    l.collector.emplace(*g.ann.floor, k);
  }
  BuildOffsetsEntries(l, input, g.Bind());
}

void RunAnnScan(duckdb::ClientContext& ctx, duckdb::TableFunctionInput& input,
                ScanGlobalState& g, AnnLocalState& l,
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
  RunAnswerStages(ctx, input, g, l, output, [&] {
    Merge(g);
    FillShortAnswer(g, l);
  });
}

}  // namespace sdb::connector
