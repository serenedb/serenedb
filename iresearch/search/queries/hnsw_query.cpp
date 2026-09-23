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
#include <bit>
#include <numeric>
#include <optional>
#include <span>
#include <utility>

#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
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

std::vector<ScoreDoc> CollectHits(std::span<const HnswCandidate> found,
                                  const DocumentMask* mask, doc_id_t end) {
  std::vector<ScoreDoc> hits;
  hits.reserve(found.size());
  for (const auto& c : found) {
    const auto doc = static_cast<doc_id_t>(c.node) + doc_limits::min();
    // A hit names a row the caller will read columns for, so a node outside
    // the segment is a torn read rather than an answer.
    SDB_ENSURE(doc >= doc_limits::min() && doc < end,
               "an hnsw hit is outside its segment");
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

// A predicate only the columnstore answers, asked one hop at a time.
//
// The set above is filled as a prefix, so the first hop that lands near the end
// of the segment evaluates the predicate over everything below it: measured on
// sift at a million rows that is a flat 1.6 ms, against about 1.0 ms of actual
// search, and it does not move with the beam. The walk visits a few hundred
// nodes, so it is a million column reads to answer a few hundred questions.
//
// This answers the questions instead. A hop's nodes are sorted and asked in one
// call, because the column scans require rows to ascend; the next hop starts
// anywhere, so the scans are rewound between hops. Every answer is remembered,
// so a node revisited on a later hop costs nothing, and the walk asks about
// each node at most once however often it is reached.
class WalkFilter {
 public:
  WalkFilter(detail::TableFilter& table, doc_id_t docs_count,
             HnswWalkFilterScratch& s)
    : _table{&table},
      _s{s},
      _docs_count{docs_count},
      _points{table.PointReads() != detail::PointRead::None} {
    _s.Grow((size_t{docs_count} + 63) / 64);
  }

  ~WalkFilter() { _s.Clear(); }

  WalkFilter(const WalkFilter&) = delete;
  WalkFilter& operator=(const WalkFilter&) = delete;

  // The hop, before anything is asked about a single node of it.
  void Prepare(std::span<const uint32_t> batch) const {
    if (_points) {
      return;
    }
    auto& ask = _s.ask;
    ask.clear();
    for (const auto node : batch) {
      if (node < _docs_count && !Bit(_s.known, node)) {
        ask.push_back(node + doc_limits::min());
      }
    }
    if (ask.empty()) {
      return;
    }
    std::sort(ask.begin(), ask.end());
    ask.erase(std::unique(ask.begin(), ask.end()), ask.end());
    for (const auto doc : ask) {
      Mark(doc - doc_limits::min());
    }
    // The previous hop left the scans wherever it ended; this one starts
    // anywhere, and the readers only go forwards.
    _table->Rewind();
    const auto kept =
      _table->Narrow(ask.data(), nullptr, static_cast<uint32_t>(ask.size()));
    for (uint32_t i = 0; i < kept; ++i) {
      Set(_s.pass, ask[i] - doc_limits::min());
    }
  }

  bool operator()(uint32_t node) const {
    if (node >= _docs_count) {
      return false;
    }
    if (!Bit(_s.known, node)) {
      // Reached without a hop of its own: the seed of the walk, or a node a
      // mode admits outside the batch. One question, still ascending on its own.
      Mark(node);
      if (_table->Admits(node + doc_limits::min())) {
        Set(_s.pass, node);
      }
    }
    return Bit(_s.pass, node);
  }

 private:
  static bool Bit(const std::vector<uint64_t>& w, uint32_t i) noexcept {
    return ((w[i / 64] >> (i % 64)) & 1U) != 0;
  }
  static void Set(std::vector<uint64_t>& w, uint32_t i) noexcept {
    w[i / 64] |= uint64_t{1} << (i % 64);
  }
  // A word is recorded the first time anything in it is set, so the clear at
  // the end touches only the words the walk reached.
  void Mark(uint32_t node) const {
    const auto w = node / 64;
    if (_s.known[w] == 0) {
      _s.dirty.push_back(w);
    }
    _s.known[w] |= uint64_t{1} << (node % 64);
  }

  detail::TableFilter* _table;
  HnswWalkFilterScratch& _s;
  doc_id_t _docs_count;
  bool _points;
};

// A predicate the term index answers, conjoined with one only the columnstore answers.
//
// Building the set with both applies the column predicate to every doc the postings admit, in the
// set's own constructor, before anything has decided how the query will be answered: on sift at a
// million rows the conjunction row costs 1.53 ms more than the equality row of the same final
// selectivity, which is a hundred thousand scattered column reads for a walk that reaches a few
// hundred nodes. The set is built from the postings alone here, and the column predicate is asked
// only about the nodes the walk reaches that the postings already admit.
class InnerAndTableFilter {
 public:
  InnerAndTableFilter(detail::LazyBitset& inner, const WalkFilter& table,
                      HnswWalkFilterScratch& s) noexcept
    : _inner{&inner}, _table{&table}, _s{s} {}

  void Prepare(std::span<const uint32_t> batch) const {
    auto& keep = _s.keep;
    keep.clear();
    for (const auto node : batch) {
      if (_inner->Contains(node + doc_limits::min())) {
        keep.push_back(node);
      }
    }
    _table->Prepare(keep);
  }

  bool operator()(uint32_t node) const {
    return _inner->Contains(node + doc_limits::min()) && (*_table)(node);
  }

 private:
  detail::LazyBitset* _inner;
  const WalkFilter* _table;
  HnswWalkFilterScratch& _s;
};

// The hop hook is found by shape, so a signature that drifts apart would leave
// the walk asking one question per node with nothing to say it had stopped
// batching. This is what that would cost: a positioned column read per node.
static_assert(requires(const WalkFilter& f) {
  f.Prepare(std::span<const uint32_t>{});
}, "WalkFilter::Prepare must match the hop hook HnswExpandLevel looks for");
static_assert(requires(const InnerAndTableFilter& f) {
  f.Prepare(std::span<const uint32_t>{});
}, "InnerAndTableFilter::Prepare must match it too");

// The walk computes on the order of `ef * m0` distances unfiltered; with a
// predicate admitting a share `p` of the graph it needs about `1 / p` times
// as many candidates before `ef` of them are admitted, but far fewer than
// that in distances, since most of a node's neighbours were already visited:
// `kWalkShare` is that observed fraction. Scanning the predicate's own docs
// costs one distance per match. The walk is taken where it is expected to be
// the cheaper of the two; a walk that then overspends its budget (the match
// count, what the scan would have cost) falls back to the scan.
inline constexpr long double kWalkShare = 0.25L;

// The nodes a walk is expected to touch, in the units `HnswPreferScan`
// compares: `kWalkShare * ef * m0` candidates per admitted node, scaled by how
// much of the graph the predicate rejects.
long double HnswWalkNodes(uint64_t matches, uint32_t ef, uint32_t m0,
                          uint64_t nodes) noexcept {
  if (matches == 0) {
    return static_cast<long double>(nodes);
  }
  return kWalkShare * static_cast<long double>(ef) * m0 * nodes /
         static_cast<long double>(matches);
}

// Asking the columnstore about the nodes a walk reaches, instead of folding the
// predicate over the segment once, is only worth it while the walk stays small.
// The two are not the same kind of work: a fold is a vectorised compare per row
// (about a nanosecond), a probe is a positioned read that locates a block,
// checks its zonemap and decodes it for one row (microseconds), and the walk
// re-decodes the same block every time it comes back to it. Measured on a
// million-row search table the probe came out near four thousand times the
// per-row fold, which is the ratio here: below it the walk touches few enough
// nodes to beat reading the column, above it -- which is every beam over a
// segment of any size -- folding wins, by 4x at a tenth selectivity.
//
// This is the columnstore's price alone. A predicate the term index answers
// costs a bit test per node and never reaches this decision.
inline constexpr long double kProbeFoldRatio = 4096;

inline constexpr long double kPointReadFoldRatio = 32;

inline constexpr long double kVectorPointReadFoldRatio = 456;

// Windows of the set filled to estimate its size before a plan is chosen: about
// thirty thousand docs, a twentieth of a millisecond on a million-row segment.
inline constexpr uint32_t kCountSampleWindows = 8;

// What one scanned row costs against one walked candidate. They are not the
// same work, and the difference is not a constant. Both read one row's codes,
// so both carry `record_size` bytes; on top of that a walked candidate arrives
// through a random read, loads its links, and pays the visited set and two
// heaps, while a scanned row arrives in a batch off a stream the distance
// kernel prefetches. That fixed extra is what this measures, in bytes of code
// read, so the ratio is `record / (record + kWalkCandidateOverhead)`: near
// zero for short codes, where the overhead is everything, and approaching one
// for long ones, where both plans are just reading vectors.
//
// Calibrated by forcing both plans and reading off where they cross, over
// clustered corpora, sq8, one thread, six predicates (term index and
// columnstore, 1% to 20% selectivity) and four beams. Writing the decision as
// `walk / matches`, the crossover bracket came out:
//
//   d=128,  1M rows, 3 segments   walk wins <= 0.24    scan wins >= 0.60
//   d=1024, 300k rows, 2 segments walk wins <= 0.53    scan wins >= 1.33
//
// One overhead fits both: 0.40 lands inside the first bracket and 0.84 inside
// the second. Getting this wrong is worth 10% to 31% -- the flat 1.0 this
// replaces gave that up on seven of twenty-eight cells at d=128, and a flat
// 0.4 would give up 10% on two of twenty-four at d=1024.
inline constexpr long double kWalkCandidateOverhead = 192;

long double ScanCandidateCost(uint32_t record_size) noexcept {
  const auto r = static_cast<long double>(record_size);
  return r / (r + kWalkCandidateOverhead);
}

inline constexpr uint64_t kTwoHopAdmittedNeighbours = 2;

bool HnswBridgeRejected(uint32_t record_size, uint32_t m0) noexcept {
  return static_cast<long double>(record_size) >
         static_cast<long double>(m0) * sizeof(uint32_t) +
           kWalkCandidateOverhead;
}

long double HnswTwoHopShare(uint64_t matches, uint64_t nodes, uint32_t m0,
                            uint32_t record_size) noexcept {
  const auto p = std::min<long double>(
    1, static_cast<long double>(matches) / static_cast<long double>(nodes));
  const auto crossed = static_cast<long double>(m0) * sizeof(uint32_t) +
                       kWalkCandidateOverhead;
  const auto scored =
    static_cast<long double>(record_size) + kWalkCandidateOverhead;
  return p + (1 - p) * crossed / scored;
}

bool HnswPreferScan(uint64_t matches, uint32_t ef, uint32_t m0, uint64_t nodes,
                    uint32_t record_size, bool two_hop) noexcept {
  if (matches == 0 || nodes == 0) {
    return true;
  }
  long double walk = HnswWalkNodes(matches, ef, m0, nodes);
  if (two_hop && matches * m0 >= kTwoHopAdmittedNeighbours * nodes) {
    walk *= HnswTwoHopShare(matches, nodes, m0, record_size);
  }
  const long double scan =
    static_cast<long double>(matches) * ScanCandidateCost(record_size);
  return scan <= walk;
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
  // Which plan answers this decides how the set is built, so the count comes first and costs
  // nothing: a bounded sample of the set stands in, and folding to decide costs more than either
  // plan does.
  std::optional<detail::LazyBitset> probe;
  uint64_t matches = 0;
  bool bounded = true;
  if (table != nullptr || _inner->Kind() == QueryKind::Boolean) {
    probe.emplace(MakeSet(_inner.get(), table, docs_count));
    matches = probe->EstimateCount(kCountSampleWindows);
    bounded = probe->Filled() >= probe->End();
  } else {
    matches = _inner->EstimateMax();
  }
  auto mode = _filter_mode;
  const bool two_hop = _filter_mode == HnswFilterMode::Auto &&
                       HnswBridgeRejected(_record_size, graph.M0());
  if (_ef != 0 && mode == HnswFilterMode::Auto) {
    mode = HnswPreferScan(matches, _ef, graph.M0(), graph.Size(), _record_size,
                          two_hop)
             ? HnswFilterMode::Scan
             : HnswFilterMode::Walk;
  }
  // A walk asks the columnstore about the nodes it reaches; only a scan needs the predicate applied
  // to the whole segment up front, which is what building the set with the table does.
  // Where the walk is wide enough that probing costs more than the fold it
  // saves, the walk still runs -- against the folded set, one bit test a node.
  const auto point_read = table == nullptr || two_hop
                            ? detail::PointRead::None
                            : table->PointReads();
  const auto probe_ratio = point_read == detail::PointRead::Row
                             ? kPointReadFoldRatio
                           : point_read == detail::PointRead::Vector
                             ? kVectorPointReadFoldRatio
                             : kProbeFoldRatio;
  const bool ask_per_hop =
    table != nullptr && _ef != 0 && mode == HnswFilterMode::Walk &&
    (_column_filter == HnswColumnFilter::Read ||
     (_column_filter == HnswColumnFilter::Auto &&
      HnswWalkNodes(matches, _ef, graph.M0(), graph.Size()) * probe_ratio <=
        static_cast<long double>(graph.Size())));
  if (ask_per_hop) {
    // The walk asks the columnstore itself, so the sampled set -- which has the
    // predicate folded into it -- is not the set it walks against.
    probe.reset();
  }
  auto set = probe ? std::move(*probe)
                   : MakeSet(_inner.get(), ask_per_hop ? nullptr : table,
                             docs_count);
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
  // A walk over a predicate only the columnstore answers asks about the nodes
  // it reaches instead of folding the segment; anything with an inner query
  // already has its docs from the postings, and a scan needs the whole set.
  // The acceptor is passed through, not wrapped: a wrapper would hide the hop
  // hook that makes a hop one question instead of thirty-two.
  // Every walk is capped at what the scan would have cost and falls back to it,
  // forced ones included: a mode is a preference, never a way to spend more
  // than the exact answer costs. A walk that could not fill its beam although
  // the predicate admits enough docs lost the admitted subgraph's connectivity:
  // the scan answers exactly.
  const auto budget = matches;
  if (two_hop && mode == HnswFilterMode::Walk && !ask_per_hop) {
    mode = HnswFilterMode::TwoHop;
  }
  const auto walked = [&](bool complete) {
    return complete && (scratch.nearest.size() >= _ef ||
                        (bounded && scratch.nearest.size() >= matches));
  };
  const auto run = [&](const auto& acc) {
    switch (mode) {
      case HnswFilterMode::Walk:
        return walked(HnswSearchTopK<HnswWalk::Through>(graph, dist, _ef,
                                                        scratch, acc, budget));
      case HnswFilterMode::Prune:
        return walked(HnswSearchTopK<HnswWalk::Prune>(graph, dist, _ef, scratch,
                                                      acc, budget));
      case HnswFilterMode::TwoHop:
        return walked(HnswSearchTopK<HnswWalk::TwoHop>(graph, dist, _ef,
                                                       scratch, acc, budget));
      case HnswFilterMode::Bridge:
        return walked(HnswSearchTopK<HnswWalk::Bridge>(graph, dist, _ef,
                                                       scratch, acc, budget));
      case HnswFilterMode::Scan:
      case HnswFilterMode::Auto:
        return false;
    }
    return false;
  };
  // Only the Through walk is asked at hop granularity. The others consult the
  // acceptor while they are still collecting a hop, before the hop exists, so
  // they would take one positioned read per node: worse than the fold this is
  // replacing. They keep the set.
  if (ask_per_hop) {
    const WalkFilter walk_filter{*table, docs_count, scratch.walk_filter};
    const bool done =
      _inner == nullptr
        ? run(walk_filter)
        : run(InnerAndTableFilter{set, walk_filter, scratch.walk_filter});
    if (done) {
      return;
    }
    // The walk gave up, so the scan answers exactly. It needs the predicate over the whole set,
    // which this set was built without, and the scans are wherever the last hop left them.
    table->Rewind();
    set = MakeSet(_inner.get(), table, docs_count);
  } else if (run(admit)) {
    return;
  }
  HnswScanTopK(set, graph, dist, _ef, scratch);
}

std::vector<ScoreDoc> HnswQuery::RunSearch(detail::TableFilter* table) const {
  HnswSearchScratch scratch;
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
  return CollectHits(
    scratch.nearest, _segment.docs_mask(),
    doc_limits::min() + static_cast<doc_id_t>(_segment.docs_count()));
}

}  // namespace irs
