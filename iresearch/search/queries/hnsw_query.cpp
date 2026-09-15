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

#include "iresearch/search/queries/hnsw_query.hpp"

#include <algorithm>
#include <span>

#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs {
namespace {

template<VectorMetric M>
struct HnswQueryDist {
  const float* base;
  uint32_t d;
  const float* q;

  const float* Row(uint32_t id) const noexcept {
    return base + static_cast<size_t>(id) * d;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t = 0.f) const noexcept {
    HnswComputeDistances<M>(q, base, d, ids, out);
  }

  score_t One(uint32_t id) const noexcept {
    score_t s{};
    Batch({&id, 1}, &s);
    return s;
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }
};

struct HnswCodeDist {
  const byte_type* codes;
  uint32_t record_size;
  QuantizerReader* qr;

  const byte_type* Row(uint32_t id) const noexcept {
    return codes + static_cast<size_t>(id) * record_size;
  }

  score_t One(uint32_t id) {
    score_t out = .0f;
    Batch({&id, 1}, &out);
    return out;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t threshold = kHnswNoThreshold) {
    qr->ComputeGathered(codes, record_size, ids, threshold, out);
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }
};

template<typename Fn>
void WithHnswDist(const HnswData& data, std::span<const float> query,
                  const std::shared_ptr<const QuantizerCodebook>& codebook,
                  VectorMetric metric, uint32_t d, uint32_t record_size,
                  Fn&& fn) {
  if (codebook) {
    auto reader = MakeQuantizerReader(codebook);
    reader->StartCluster(data.centroid.empty() ? nullptr
                                               : data.centroid.data());
    HnswCodeDist dist{.codes = data.codes.data(),
                      .record_size = record_size,
                      .qr = reader.get()};
    fn(dist);
    return;
  }
  ResolveEnum<VectorMetric>(
    EffectiveQuantMetric(metric), [&]<VectorMetric M>() {
      HnswQueryDist<M> dist{
        .base = data.vectors.data(), .d = d, .q = query.data()};
      fn(dist);
    });
}

HnswSearchScratch& ThreadScratch() {
  static thread_local HnswSearchScratch scratch;
  return scratch;
}

std::vector<ScoreDoc> CollectHits(std::span<const HnswCandidate> found,
                                  const DocumentMask* mask) {
  std::vector<ScoreDoc> hits;
  hits.reserve(found.size());
  for (const auto& c : found) {
    const auto doc = static_cast<doc_id_t>(c.node) + doc_limits::min();
    if (mask != nullptr && mask->contains(doc)) {
      continue;
    }
    hits.push_back({.score = c.score, .doc = doc});
  }
  std::ranges::sort(
    hits, [](const ScoreDoc& l, const ScoreDoc& r) { return l.doc < r.doc; });
  return hits;
}

}  // namespace

namespace {

// The set the segment's predicates fold into for the graph walk: the inner
// query's docs (every doc, without one), narrowed by the table filter's
// column predicates. The walk asks about nodes in graph order, not doc order,
// so the set ends up filled to the last node touched; that is the cost of
// evaluating the predicates once, the same set ts_dict folds its WHERE into.
// Deleted docs are not dropped here: the hits are masked once, in CollectHits.
detail::LazyBitset MakeSet(const QueryBuilder* inner,
                           detail::TableFilter* table, doc_id_t docs_count) {
  if (inner == nullptr) {
    return detail::LazyBitset{docs_count, nullptr, table};
  }
  auto node = inner->PlanFill({}, ScoreMergeType::Noop);
  SDB_ASSERT(node);
  if (auto* folded = node->Folded(); folded != nullptr) {
    return detail::LazyBitset{std::move(*folded), nullptr, table};
  }
  return detail::LazyBitset{std::move(node), docs_count, nullptr, table};
}

// The walk computes on the order of `ef * m0` distances unfiltered; with a
// predicate admitting a share `p` of the graph it needs about `1 / p` times
// as many candidates before `ef` of them are admitted, but far fewer than
// that in distances, since most of a node's neighbours were already visited:
// `kWalkShare` is that observed fraction. Scanning the predicate's own docs
// costs one distance per match. The walk is taken where it is expected to be
// the cheaper of the two; a walk that then overspends its budget (the match
// count, what the scan would have cost) falls back to the scan.
inline constexpr long double kWalkShare = 0.25L;

bool HnswPreferScan(uint64_t matches, uint32_t ef, uint32_t m0,
                    uint64_t nodes) noexcept {
  if (matches == 0 || nodes == 0) {
    return true;
  }
  const long double walk = kWalkShare * static_cast<long double>(ef) * m0 *
                           nodes / static_cast<long double>(matches);
  return static_cast<long double>(matches) <= walk;
}

template<typename Dist>
void HnswAdmit(Dist& dist, uint32_t ef, HnswSearchScratch& s) {
  auto& nearest = s.nearest;
  s.scores.resize(s.batch.size());
  dist.Batch(s.batch, s.scores.data(),
             nearest.size() >= ef ? nearest.front().score : kHnswNoThreshold);
  for (size_t i = 0; i < s.batch.size(); ++i) {
    const HnswCandidate cand{s.scores[i], s.batch[i]};
    if (nearest.size() >= ef && cand.score <= nearest.front().score) {
      continue;
    }
    nearest.push_back(cand);
    std::push_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
    if (nearest.size() > ef) {
      std::pop_heap(nearest.begin(), nearest.end(), HnswNearestOrder{});
      nearest.pop_back();
    }
  }
  s.batch.clear();
}

// Top-`ef` over the set's docs, no graph: every match is scored once, in
// batches so the distance kernel and the quantizer's early exit apply.
template<typename Dist>
void HnswScanTopK(detail::LazyBitset& set, const HnswGraph& graph, Dist& dist,
                  uint32_t ef, HnswSearchScratch& s) {
  constexpr size_t kBatch = 256;
  s.nearest.clear();
  s.batch.clear();
  const auto size = graph.Size();
  for (auto doc = set.Probe(doc_limits::min()); !doc_limits::eof(doc);
       doc = set.Probe(doc + 1)) {
    const auto node = static_cast<uint32_t>(doc - doc_limits::min());
    if (node >= size) {
      break;
    }
    // A row without a vector owns a node id but no place in the graph.
    if (graph.LevelOf(node) == 0) {
      continue;
    }
    // No prefetch here: a batch of 256 codes outgrows L1, the distance kernel
    // fetches its own lookahead.
    s.batch.push_back(node);
    if (s.batch.size() == kBatch) {
      HnswAdmit(dist, ef, s);
    }
  }
  if (!s.batch.empty()) {
    HnswAdmit(dist, ef, s);
  }
}

}  // namespace

template<typename Dist>
void HnswQuery::RunFiltered(Dist& dist, detail::TableFilter* table,
                            HnswSearchScratch& scratch) const {
  SDB_ASSERT(_inner != nullptr || table != nullptr);
  const auto& graph = _data->graph;
  const auto docs_count = static_cast<doc_id_t>(_segment.docs_count());
  auto set = MakeSet(_inner.get(), table, docs_count);
  const auto admit = [&](uint32_t node) {
    return set.Contains(static_cast<doc_id_t>(node) + doc_limits::min());
  };
  if (_ef == 0) {
    ResolveBool(_inclusive, [&]<bool Inclusive>() {
      HnswSearchRadius<Inclusive>(graph, dist, _threshold, _max_results,
                                  scratch, admit);
    });
    return;
  }
  // An inner query knows its upper bound up front. A table's column predicates
  // are only known once evaluated, and evaluating them is a column scan, far
  // cheaper than the distances that hang on the answer: fold the whole set and
  // count it.
  const uint64_t matches =
    table != nullptr ? set.Count() : _inner->EstimateMax();
  auto mode = _filter_mode;
  if (mode == HnswFilterMode::Auto) {
    mode = HnswPreferScan(matches, _ef, graph.M0(), graph.Size())
             ? HnswFilterMode::Scan
             : HnswFilterMode::Walk;
  }
  // Every walk is capped at what the scan would have cost and falls back to
  // it, forced ones included: a mode is a preference, never a way to spend
  // more than the exact answer costs. A walk that could not fill its beam
  // although the predicate admits enough docs lost the admitted subgraph's
  // connectivity: the scan answers exactly.
  const auto budget = matches;
  const auto walked = [&](bool complete) {
    return complete &&
           (scratch.nearest.size() >= _ef || scratch.nearest.size() >= matches);
  };
  switch (mode) {
    case HnswFilterMode::Walk:
      if (walked(HnswSearchTopK<HnswWalk::Through>(graph, dist, _ef, scratch,
                                                   admit, budget))) {
        return;
      }
      break;
    case HnswFilterMode::Prune:
      if (walked(HnswSearchTopK<HnswWalk::Prune>(graph, dist, _ef, scratch,
                                                 admit, budget))) {
        return;
      }
      break;
    case HnswFilterMode::TwoHop:
      if (walked(HnswSearchTopK<HnswWalk::TwoHop>(graph, dist, _ef, scratch,
                                                  admit, budget))) {
        return;
      }
      break;
    case HnswFilterMode::Bridge:
      if (walked(HnswSearchTopK<HnswWalk::Bridge>(graph, dist, _ef, scratch,
                                                  admit, budget))) {
        return;
      }
      break;
    case HnswFilterMode::Scan:
    case HnswFilterMode::Auto:
      break;
  }
  HnswScanTopK(set, graph, dist, _ef, scratch);
}

std::vector<ScoreDoc> HnswQuery::RunSearch(detail::TableFilter* table) const {
  auto& scratch = ThreadScratch();
  if (table != nullptr && !table->Foldable()) {
    table = nullptr;
  }
  WithHnswDist(*_data, _query, _codebook, _metric, _d, _record_size,
               [&](auto& dist) {
                 if (_inner != nullptr || table != nullptr) {
                   RunFiltered(dist, table, scratch);
                   return;
                 }
                 if (_ef != 0) {
                   HnswSearchTopK(_data->graph, dist, _ef, scratch);
                   return;
                 }
                 ResolveBool(_inclusive, [&]<bool Inclusive>() {
                   HnswSearchRadius<Inclusive>(_data->graph, dist, _threshold,
                                               _max_results, scratch);
                 });
               });
  return CollectHits(scratch.nearest, _segment.docs_mask());
}

}  // namespace irs
