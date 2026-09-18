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
#include <numeric>
#include <optional>
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

  uint32_t PrefixDims() const noexcept { return 0; }

  void BatchPrefix(std::span<const uint32_t> ids, score_t* out) const noexcept {
    Batch(ids, out);
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

  uint32_t PrefixDims() const noexcept { return qr->PrefixDims(); }

  void BatchPrefix(std::span<const uint32_t> ids, score_t* out) {
    qr->ComputeGatheredPrefix(codes, record_size, ids, out);
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
// Buffers for the walk's answers, kept per thread. Sizing them per query would
// be two bitmaps of the segment: at ten million rows that is 2.5 MB allocated
// and zeroed for a walk that asks about a few hundred nodes. They are grown
// once and cleared by the words actually touched.
struct WalkFilterScratch {
  std::vector<uint64_t> known;
  std::vector<uint64_t> pass;
  std::vector<uint32_t> dirty;
  std::vector<doc_id_t> ask;
  std::vector<uint32_t> keep;

  void Grow(size_t words) {
    if (known.size() < words) {
      known.resize(words, 0);
      pass.resize(words, 0);
    }
  }

  void Clear() {
    for (const auto w : dirty) {
      known[w] = 0;
      pass[w] = 0;
    }
    dirty.clear();
  }
};

WalkFilterScratch& ThreadWalkScratch() {
  static thread_local WalkFilterScratch scratch;
  return scratch;
}

class WalkFilter {
 public:
  WalkFilter(detail::TableFilter& table, doc_id_t docs_count)
    : _table{&table}, _s{ThreadWalkScratch()}, _docs_count{docs_count} {
    _s.Grow((size_t{docs_count} + 63) / 64);
  }

  ~WalkFilter() { _s.Clear(); }

  WalkFilter(const WalkFilter&) = delete;
  WalkFilter& operator=(const WalkFilter&) = delete;

  // The hop, before anything is asked about a single node of it.
  void Prepare(std::span<const uint32_t> batch) const {
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
      doc_id_t doc = node + doc_limits::min();
      Mark(node);
      _table->Rewind();
      if (_table->Narrow(&doc, nullptr, 1) == 1) {
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
  WalkFilterScratch& _s;
  doc_id_t _docs_count;
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
  InnerAndTableFilter(detail::LazyBitset& inner, const WalkFilter& table) noexcept
    : _inner{&inner}, _table{&table}, _s{ThreadWalkScratch()} {}

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
  WalkFilterScratch& _s;
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

// Windows of the set filled to estimate its size before a plan is chosen: about
// thirty thousand docs, a twentieth of a millisecond on a million-row segment.
inline constexpr uint32_t kCountSampleWindows = 8;

// The rows a two-pass scan scores in full, as a multiple of the beam: the
// `keep` of HnswScanWords.
inline constexpr long double kScanPrefixPool = 8;

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

bool HnswPreferScan(uint64_t matches, uint32_t ef, uint32_t m0, uint64_t nodes,
                    uint32_t record_size, uint32_t parallel = 1,
                    bool prefix = false) noexcept {
  if (matches == 0 || nodes == 0) {
    return true;
  }
  const long double walk = HnswWalkNodes(matches, ef, m0, nodes);
  // A scan splits across `parallel` workers; the walk is one thread moving
  // through the graph, so only the scan's side of the comparison shrinks. A
  // scan that ranks on a prefix of each code reads a quarter of the rows it
  // scores, and scores a pool of them in full on top.
  long double scan =
    static_cast<long double>(matches) * ScanCandidateCost(record_size);
  if (prefix) {
    scan = scan / 4 + kScanPrefixPool * ef;
  }
  scan /= std::max<uint32_t>(parallel, 1);
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

// Top-`ef` over the set's docs, no graph: every match is scored once, in
// batches so the distance kernel and the quantizer's early exit apply.
// The rows a split scan's part reads, gathered from the folded set.
inline void HnswCollectRange(std::span<const uint64_t> words,
                             const HnswGraph& graph, doc_id_t first,
                             doc_id_t last, std::vector<uint32_t>& out) {
  constexpr auto kBits = detail::LazyBitset::kBits;
  constexpr auto kMin = detail::LazyBitset::kMin;
  out.clear();
  const auto size = graph.Size();
  const auto stop = std::min<uint64_t>(last - kMin, words.size() * kBits);
  for (auto bit = static_cast<uint64_t>(first - kMin); bit < stop;) {
    const auto word_idx = bit / kBits;
    auto word = words[word_idx] & (~uint64_t{0} << (bit % kBits));
    const auto word_end = (word_idx + 1) * kBits;
    if (word_end > stop) {
      const auto keep = kBits - (word_end - stop);
      word &= keep == kBits ? ~uint64_t{0} : ((uint64_t{1} << keep) - 1);
    }
    while (word != 0) {
      const auto node =
        static_cast<uint32_t>(word_idx * kBits + std::countr_zero(word));
      word &= word - 1;
      if (node >= size) {
        return;
      }
      // A row without a vector owns a node id but no place in the graph.
      if (graph.LevelOf(node) != 0) {
        out.push_back(node);
      }
    }
    bit = word_end;
  }
}

// Two passes over the part's rows when the codes are long enough that reading
// a quarter of one ranks it well: the first scores every row on that prefix
// and keeps the best `keep`, the second scores those in full. The answer is
// the exact top-`ef` of what the first pass kept.
template<typename Dist>
void HnswScanPrefix(std::span<const uint32_t> rows, Dist& dist, uint32_t ef,
                    HnswSearchScratch& s, uint32_t keep) {
  constexpr size_t kBatch = 256;
  auto& partial = s.prefix_scores;
  partial.resize(rows.size());
  for (size_t i = 0; i < rows.size(); i += kBatch) {
    const auto n = std::min<size_t>(kBatch, rows.size() - i);
    dist.BatchPrefix(rows.subspan(i, n), partial.data() + i);
  }
  auto& order = s.prefix_order;
  order.resize(rows.size());
  std::iota(order.begin(), order.end(), uint32_t{0});
  std::nth_element(
    order.begin(), order.begin() + keep, order.end(),
    [&](uint32_t l, uint32_t r) { return partial[l] > partial[r]; });
  s.nearest.clear();
  s.batch.clear();
  for (uint32_t i = 0; i < keep; ++i) {
    s.batch.push_back(rows[order[i]]);
    if (s.batch.size() == kBatch) {
      HnswAdmit(dist, ef, s);
    }
  }
  if (!s.batch.empty()) {
    HnswAdmit(dist, ef, s);
  }
}

// Top-`ef` over the docs a folded set names within [first, last): the same
// scan, reading bits that are already there rather than filling them.
template<typename Dist>
void HnswScanWords(std::span<const uint64_t> words, const HnswGraph& graph,
                   Dist& dist, uint32_t ef, HnswSearchScratch& s,
                   doc_id_t first, doc_id_t last) {
  // A pool wide enough that the rows the prefix pass drops were never
  // contenders, and enough rows for its second read of the best of them to be
  // worth the first read of all of them.
  const auto keep = std::max<uint32_t>(8 * ef, 128);
  if (dist.PrefixDims() != 0) {
    HnswCollectRange(words, graph, first, last, s.rows);
    if (s.rows.size() > 2 * static_cast<size_t>(keep)) {
      HnswScanPrefix(std::span<const uint32_t>{s.rows}, dist, ef, s, keep);
      return;
    }
  }
  constexpr size_t kBatch = 256;
  constexpr auto kBits = detail::LazyBitset::kBits;
  constexpr auto kMin = detail::LazyBitset::kMin;
  s.nearest.clear();
  s.batch.clear();
  const auto size = graph.Size();
  const auto stop = std::min<uint64_t>(last - kMin, words.size() * kBits);
  for (auto bit = static_cast<uint64_t>(first - kMin); bit < stop;) {
    const auto word_idx = bit / kBits;
    auto word = words[word_idx] & (~uint64_t{0} << (bit % kBits));
    const auto word_end = (word_idx + 1) * kBits;
    if (word_end > stop) {
      const auto keep = kBits - (word_end - stop);
      word &= keep == kBits ? ~uint64_t{0} : ((uint64_t{1} << keep) - 1);
    }
    while (word != 0) {
      const auto node =
        static_cast<uint32_t>(word_idx * kBits + std::countr_zero(word));
      word &= word - 1;
      if (node >= size) {
        bit = stop;
        word = 0;
        break;
      }
      // A row without a vector owns a node id but no place in the graph.
      if (graph.LevelOf(node) == 0) {
        continue;
      }
      s.batch.push_back(node);
      if (s.batch.size() == kBatch) {
        HnswAdmit(dist, ef, s);
      }
    }
    bit = word_end;
  }
  if (!s.batch.empty()) {
    HnswAdmit(dist, ef, s);
  }
}

template<typename Dist>
void HnswScanTopK(detail::LazyBitset& set, const HnswGraph& graph, Dist& dist,
                  uint32_t ef, HnswSearchScratch& s, doc_id_t first,
                  doc_id_t last) {
  constexpr size_t kBatch = 256;
  s.nearest.clear();
  s.batch.clear();
  const auto size = graph.Size();
  for (auto doc = set.Probe(first); !doc_limits::eof(doc) && doc < last;
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
                            HnswSearchScratch& scratch, uint32_t part,
                            uint32_t parts) const {
  SDB_ASSERT(_inner != nullptr || table != nullptr);
  const auto& graph = _data->graph;
  const auto docs_count = static_cast<doc_id_t>(_segment.docs_count());
  // One part of a split query: its rows are the part's doc range, and a split
  // is only asked for where the answer is a scan (a walk moves through the
  // whole graph, so it cannot be cut into doc ranges).
  const auto per = (docs_count + parts - 1) / std::max<uint32_t>(parts, 1);
  const auto first =
    doc_limits::min() + std::min<doc_id_t>(docs_count, per * part);
  const auto last =
    doc_limits::min() + std::min<doc_id_t>(docs_count, per * (part + 1));
  if (parts > 1) {
    HnswScanWords(FoldOnce(table, docs_count), graph, dist, _ef, scratch, first,
                  last);
    return;
  }
  // Which plan answers this decides how the set is built, so the count comes first and costs
  // nothing: a bounded sample of the set stands in, and folding to decide costs more than either
  // plan does.
  //
  // An inner query alone knows its own upper bound, so it answers for itself. A columnstore
  // conjunct does not: `WHERE cat10 = 3 AND num BETWEEN ...` is as selective as both together,
  // and taking the term's bound for the pair reads a hundredth of the segment as a tenth. The
  // walk then runs against a set ten times sparser than it was planned for, which is ten times
  // the candidates before the beam fills. Sample the set that will actually be walked.
  std::optional<detail::LazyBitset> probe;
  uint64_t matches = 0;
  if (table != nullptr) {
    probe.emplace(MakeSet(_inner.get(), table, docs_count));
    matches = probe->EstimateCount(kCountSampleWindows);
  } else {
    matches = _inner->EstimateMax();
  }
  auto mode = _filter_mode;
  if (_ef != 0 && mode == HnswFilterMode::Auto) {
    mode = HnswPreferScan(matches, _ef, graph.M0(), graph.Size(), _record_size)
             ? HnswFilterMode::Scan
             : HnswFilterMode::Walk;
  }
  // A walk asks the columnstore about the nodes it reaches; only a scan needs the predicate applied
  // to the whole segment up front, which is what building the set with the table does.
  // Where the walk is wide enough that probing costs more than the fold it
  // saves, the walk still runs -- against the folded set, one bit test a node.
  const bool ask_per_hop =
    table != nullptr && _ef != 0 && mode == HnswFilterMode::Walk &&
    HnswWalkNodes(matches, _ef, graph.M0(), graph.Size()) * kProbeFoldRatio <=
      static_cast<long double>(graph.Size());
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
  const auto walked = [&](bool complete) {
    return complete &&
           (scratch.nearest.size() >= _ef || scratch.nearest.size() >= matches);
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
    const WalkFilter walk_filter{*table, docs_count};
    const bool done = _inner == nullptr
                        ? run(walk_filter)
                        : run(InnerAndTableFilter{set, walk_filter});
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
  HnswScanTopK(set, graph, dist, _ef, scratch, first, last);
}

std::span<const uint64_t> HnswQuery::FoldOnce(detail::TableFilter* table,
                                              doc_id_t docs_count) const {
  std::lock_guard<std::mutex> lock{_fold_lock};
  if (!_folded_done) {
    auto set = MakeSet(_inner.get(), table, docs_count);
    set.Reach(set.End());
    const auto words =
      (set.End() - detail::LazyBitset::kMin + detail::LazyBitset::kBits - 1) /
      detail::LazyBitset::kBits;
    _folded.assign(set.Words(), set.Words() + words);
    _folded_done = true;
  }
  return _folded;
}

std::optional<uint64_t> HnswQuery::ScanCandidates(
  uint32_t parallel, std::optional<uint64_t> table_rows) const {
  // The same rule RunFiltered uses, from what is known without evaluating the
  // predicate: an inner query's own upper bound, and whatever bound the caller
  // has for its table filter. Both are upper bounds, and a scan only gets
  // cheaper as the true count falls, so preferring it here still holds.
  if (_ef == 0 || _filter_mode == HnswFilterMode::Walk ||
      _filter_mode == HnswFilterMode::Prune ||
      _filter_mode == HnswFilterMode::TwoHop ||
      _filter_mode == HnswFilterMode::Bridge) {
    return std::nullopt;
  }
  const auto& graph = _data->graph;
  uint64_t matches = 0;
  if (_inner != nullptr) {
    matches = _inner->EstimateMax();
    if (table_rows) {
      matches = std::min<uint64_t>(matches, *table_rows);
    }
  } else if (table_rows) {
    matches = *table_rows;
  } else {
    return std::nullopt;
  }
  // The two-pass scan applies to a quantized index whose codes are long
  // enough for a quarter of one to be fewer cache lines (HnswScanWords).
  const bool prefix = _codebook != nullptr && _d >= 512;
  if (_filter_mode != HnswFilterMode::Scan &&
      !HnswPreferScan(matches, _ef, graph.M0(), graph.Size(), _record_size,
                      parallel, prefix)) {
    return std::nullopt;
  }
  return matches;
}

std::vector<ScoreDoc> HnswQuery::RunSearch(detail::TableFilter* table,
                                           uint32_t part,
                                           uint32_t parts) const {
  auto& scratch = ThreadScratch();
  if (table != nullptr && !table->Foldable()) {
    table = nullptr;
  }
  if (parts > 1 && _inner == nullptr && table == nullptr) {
    // Nothing to split by doc range after all (the table filter turned out not
    // to fold): the whole search is one part's, the rest answer nothing.
    if (part != 0) {
      return {};
    }
    parts = 1;
  }
  WithHnswDist(*_data, _query, _codebook, _metric, _d, _record_size,
               [&](auto& dist) {
                 if (_inner != nullptr || table != nullptr) {
                   RunFiltered(dist, table, scratch, part, parts);
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
