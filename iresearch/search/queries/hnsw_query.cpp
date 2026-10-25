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

#include <absl/algorithm/container.h>

#include <algorithm>
#include <bit>
#include <numeric>
#include <optional>
#include <span>
#include <utility>

#include "iresearch/formats/hnsw/hnsw_bases.hpp"
#include "iresearch/search/detail/lazy_bitset.hpp"
#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/queries/vector_similarity_query.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/debugging.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

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
  QuantizerLevelBound bound;

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
    if (bound.rows &&
        qr->ComputeGatheredBounded(
          codes, record_size, ids, bound,
          threshold == kHnswNoThreshold
            ? threshold
            : threshold - kHnswPanoramaMargin * (1.f + std::abs(threshold)),
          out)) {
      return;
    }
    qr->ComputeGathered(codes, record_size, ids, threshold, out);
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }
};

struct HnswColumnDist {
  detail::RawVectorReader* reader;
  std::vector<doc_id_t>* docs;

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t = kHnswNoThreshold) {
    docs->clear();
    for (const auto id : ids) {
      docs->push_back(static_cast<doc_id_t>(id) + doc_limits::min());
    }
    reader->ComputeDistances(*docs, {out, ids.size()});
  }
};

template<typename Fn>
void WithHnswDist(const HnswData& data, std::span<const float> query,
                  const std::shared_ptr<const QuantizerCodebook>& codebook,
                  const PanoramaQuery* panorama, VectorMetric metric,
                  uint32_t d, uint32_t record_size, Fn&& fn) {
  if (codebook) {
    auto reader = MakeQuantizerReader(codebook);
    reader->StartCluster(data.centroid.empty() ? nullptr
                                               : data.centroid.data());
    HnswCodeDist dist{.codes = data.codes.data(),
                      .record_size = record_size,
                      .qr = reader.get()};
    if (panorama && !data.tails.empty()) {
      dist.bound = {.width = kHnswCodeLevel,
                    .query_step = kHnswPanoramaLevel,
                    .query = panorama->tails,
                    .rows = data.tails.data(),
                    .row_stride = HnswCodeTails(d)};
    }
    fn(dist);
    return;
  }
  ResolveEnum<VectorMetric>(
    EffectiveQuantMetric(metric), [&]<VectorMetric M>() {
      if constexpr (M == VectorMetric::L2Sqr ||
                    M == VectorMetric::InnerProduct) {
        if (panorama) {
          HnswPanoramaDist<M> dist{.base = data.vectors.data(),
                                   .d = d,
                                   .q = panorama->rotated.data(),
                                   .tails = panorama->tails.data(),
                                   .norms = data.norms.data()};
          fn(dist);
          return;
        }
      }
      HnswQueryDist<M> dist{
        .base = data.vectors.data(), .d = d, .q = query.data()};
      fn(dist);
    });
}

std::vector<ScoreDoc> CollectHits(std::span<const HnswCandidate> found,
                                  const SubReader& segment) {
  const auto it_mask = segment.MaskedDocs();
  const auto end =
    doc_limits::min() + static_cast<doc_id_t>(segment.docs_count());
  std::vector<ScoreDoc> hits;
  hits.reserve(found.size());
  for (const auto& c : found) {
    const auto doc = static_cast<doc_id_t>(c.node) + doc_limits::min();
    SDB_ENSURE(doc >= doc_limits::min() && doc < end,
               "an hnsw hit is outside its segment");
    if (it_mask.Contains(doc)) {
      continue;
    }
    hits.push_back({.score = c.score, .doc = doc});
  }
  absl::c_sort(
    hits, [](const ScoreDoc& l, const ScoreDoc& r) { return l.doc < r.doc; });
  return hits;
}

std::vector<ScoreDoc> CollectHits(std::span<const HnswCandidate> found,
                                  const SubReader& segment,
                                  std::span<const score_t> spreads,
                                  std::vector<score_t>& spread_out) {
  SDB_ASSERT(spreads.size() == found.size());
  std::vector<uint32_t> order(found.size());
  std::iota(order.begin(), order.end(), 0);
  absl::c_sort(order, [&](uint32_t l, uint32_t r) {
    return found[l].node < found[r].node;
  });
  const auto it_mask = segment.MaskedDocs();
  const auto end =
    doc_limits::min() + static_cast<doc_id_t>(segment.docs_count());
  std::vector<ScoreDoc> hits;
  hits.reserve(found.size());
  spread_out.clear();
  spread_out.reserve(found.size());
  for (const auto i : order) {
    const auto doc = static_cast<doc_id_t>(found[i].node) + doc_limits::min();
    SDB_ENSURE(doc >= doc_limits::min() && doc < end,
               "an hnsw hit is outside its segment");
    if (it_mask.Contains(doc)) {
      continue;
    }
    hits.push_back({.score = found[i].score, .doc = doc});
    spread_out.push_back(spreads[i]);
  }
  return hits;
}

struct WalkFilterScratch {
  HnswVisited known;
  HnswVisited pass;
  std::vector<doc_id_t> ask;
  std::vector<uint32_t> keep;
};

class WalkFilter {
 public:
  WalkFilter(detail::TableFilter& table, doc_id_t docs_count,
             WalkFilterScratch& s)
    : _table{&table},
      _s{s},
      _docs_count{docs_count},
      _points{table.PointReads() != detail::PointRead::None} {
    _s.known.Reset(docs_count);
    _s.pass.Reset(docs_count);
  }

  WalkFilter(const WalkFilter&) = delete;
  WalkFilter& operator=(const WalkFilter&) = delete;

  void Prepare(std::span<const uint32_t> batch) const {
    if (_points) {
      return;
    }
    auto& ask = _s.ask;
    ask.clear();
    for (const auto node : batch) {
      if (node < _docs_count && !_s.known.Test(node)) {
        ask.push_back(node + doc_limits::min());
      }
    }
    if (ask.empty()) {
      return;
    }
    absl::c_sort(ask);
    ask.erase(std::unique(ask.begin(), ask.end()), ask.end());
    for (const auto doc : ask) {
      _s.known.Set(doc - doc_limits::min());
    }
    _table->Rewind();
    const auto kept =
      _table->Narrow(ask.data(), nullptr, static_cast<uint32_t>(ask.size()));
    for (uint32_t i = 0; i < kept; ++i) {
      _s.pass.Set(ask[i] - doc_limits::min());
    }
  }

  bool operator()(uint32_t node) const {
    if (node >= _docs_count) {
      return false;
    }
    if (!_s.known.TestAndSet(node)) {
      if (_table->Admits(node + doc_limits::min())) {
        _s.pass.Set(node);
      }
    }
    return _s.pass.Test(node);
  }

 private:
  detail::TableFilter* _table;
  WalkFilterScratch& _s;
  doc_id_t _docs_count;
  bool _points;
};

class InnerAndTableFilter {
 public:
  InnerAndTableFilter(detail::LazyBitset& inner, const WalkFilter& table,
                      WalkFilterScratch& s) noexcept
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
  WalkFilterScratch& _s;
};

static_assert(
  requires(const WalkFilter& f) { f.Prepare(std::span<const uint32_t>{}); },
  "WalkFilter::Prepare must match the hop hook HnswExpandLevel looks for");
static_assert(
  requires(const InnerAndTableFilter& f) {
    f.Prepare(std::span<const uint32_t>{});
  }, "InnerAndTableFilter::Prepare must match it too");

inline constexpr long double kWalkShare = 0.25L;

long double HnswWalkNodes(uint64_t matches, uint32_t ef, uint32_t m0,
                          uint64_t nodes) noexcept {
  if (matches == 0) {
    return static_cast<long double>(nodes);
  }
  return kWalkShare * static_cast<long double>(ef) * m0 * nodes /
         static_cast<long double>(matches);
}

inline constexpr long double kProbeFoldRatio = 4096;

inline constexpr long double kPointReadFoldRatio = 32;

inline constexpr long double kVectorPointReadFoldRatio = 456;

inline constexpr uint32_t kCountSampleWindows = 8;

inline constexpr uint64_t kHnswColumnScanShare = 16;

inline constexpr long double kWalkCandidateOverhead = 192;

long double ScanCandidateCost(uint32_t record_size) noexcept {
  const auto r = static_cast<long double>(record_size);
  return r / (r + kWalkCandidateOverhead);
}

uint32_t HnswCandidateBytes(uint32_t record_size, uint32_t d,
                            const QuantizerStats* stats) noexcept {
  if (record_size == 0) {
    return d * static_cast<uint32_t>(sizeof(float));
  }
  return stats && stats->Kind() == VectorQuantization::PQ
           ? record_size * static_cast<uint32_t>(1 + sizeof(float))
           : record_size;
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
  const auto crossed =
    static_cast<long double>(m0) * sizeof(uint32_t) + kWalkCandidateOverhead;
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

struct HnswFilterPlan {
  bool walks = false;
  HnswWalk walk = HnswWalk::Through;
  bool two_hop = false;
  bool ask_per_hop = false;
};

HnswFilterPlan PlanHnswFilter(HnswFilterMode forced,
                              HnswColumnFilter column_filter,
                              const HnswGraph& graph, uint32_t record,
                              uint32_t ef, uint64_t matches,
                              const detail::TableFilter* table) {
  HnswFilterPlan plan;
  plan.two_hop =
    forced == HnswFilterMode::Auto && HnswBridgeRejected(record, graph.M0());
  auto mode = forced;
  if (mode == HnswFilterMode::Auto) {
    mode = HnswPreferScan(matches, ef, graph.M0(), graph.Size(), record,
                          plan.two_hop)
             ? HnswFilterMode::Scan
             : HnswFilterMode::Walk;
  }
  const auto point_read =
    !table || plan.two_hop ? detail::PointRead::None : table->PointReads();
  const auto probe_ratio =
    point_read == detail::PointRead::Row      ? kPointReadFoldRatio
    : point_read == detail::PointRead::Vector ? kVectorPointReadFoldRatio
                                              : kProbeFoldRatio;
  plan.ask_per_hop =
    table && mode == HnswFilterMode::Walk &&
    (column_filter == HnswColumnFilter::Read ||
     (column_filter == HnswColumnFilter::Auto &&
      HnswWalkNodes(matches, ef, graph.M0(), graph.Size()) * probe_ratio <=
        static_cast<long double>(graph.Size())));
  switch (mode) {
    case HnswFilterMode::Walk:
      plan.walk = plan.two_hop && !plan.ask_per_hop ? HnswWalk::TwoHop
                  : forced == HnswFilterMode::Auto && !plan.ask_per_hop
                    ? HnswWalk::Adaptive
                    : HnswWalk::Through;
      plan.walks = true;
      break;
    case HnswFilterMode::Prune:
      plan.walk = HnswWalk::Prune;
      plan.walks = true;
      break;
    case HnswFilterMode::TwoHop:
      plan.walk = HnswWalk::TwoHop;
      plan.walks = true;
      break;
    case HnswFilterMode::Bridge:
      plan.walk = HnswWalk::Bridge;
      plan.walks = true;
      break;
    case HnswFilterMode::Scan:
    case HnswFilterMode::Auto:
      break;
  }
  return plan;
}

auto AdmitIn(detail::LazyBitset& set) {
  return [&set](uint32_t node) {
    return set.Contains(static_cast<doc_id_t>(node) + doc_limits::min());
  };
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
    HnswKeepNearest(nearest, cand, ef);
  }
  s.batch.clear();
}

std::vector<uint32_t> AdmittedSeeds(detail::LazyBitset& set,
                                    const HnswGraph& graph, uint32_t want) {
  std::vector<uint32_t> seeds;
  const auto size = static_cast<uint64_t>(graph.Size());
  for (uint32_t k = 0; k < want; ++k) {
    const auto from =
      doc_limits::min() + static_cast<doc_id_t>(size * k / want);
    for (auto doc = set.Probe(from); !doc_limits::eof(doc);
         doc = set.Probe(doc + 1)) {
      const auto node = static_cast<uint32_t>(doc - doc_limits::min());
      if (node >= size) {
        break;
      }
      if (graph.LevelOf(node) != 0) {
        if (seeds.empty() || seeds.back() != node) {
          seeds.push_back(node);
        }
        break;
      }
    }
  }
  return seeds;
}

template<typename Dist>
void HnswScanTopK(detail::LazyBitset& set, const HnswGraph& graph, Dist& dist,
                  uint32_t ef, HnswSearchScratch& s) {
  constexpr size_t kBatch = 256;
  s.nearest.clear();
  s.batch.clear();
  const auto size = graph.Size();
  size_t limit = std::min<size_t>(kBatch, ef);
  for (auto doc = set.Probe(doc_limits::min()); !doc_limits::eof(doc);
       doc = set.Probe(doc + 1)) {
    const auto node = static_cast<uint32_t>(doc - doc_limits::min());
    if (node >= size) {
      break;
    }
    if (graph.LevelOf(node) == 0) {
      continue;
    }
    s.batch.push_back(node);
    if (s.batch.size() == limit) {
      HnswAdmit(dist, ef, s);
      limit = std::min(kBatch, 2 * limit);
    }
  }
  if (!s.batch.empty()) {
    HnswAdmit(dist, ef, s);
  }
}

}  // namespace

HnswQuery::HnswQuery(const SubReader& segment,
                     std::shared_ptr<const HnswData> data,
                     const VectorFilterOptions& opts, uint32_t record_size,
                     HnswSearchLimits limits, score_t boost,
                     QueryBuilder::ptr&& inner, AnnQueryCache* cache)
  : QueryBuilderImpl{segment, static_cast<uint32_t>(segment.docs_count()),
                     QueryKind::Ann},
    _data{std::move(data)},
    _cache{cache},
    _inner{std::move(inner)},
    _exact{segment.Column(opts.centroids_id)},
    _query{opts.query.size(),
           [&](float& slot, size_t i) noexcept { slot = opts.query[i]; }},
    _metric{opts.metric},
    _d{static_cast<uint32_t>(opts.query.size())},
    _record_size{record_size},
    _ef{limits.ef},
    _top_k{opts.top_k},
    _rescore_width{opts.rescore_width},
    _threshold{limits.threshold},
    _max_results{limits.max_results},
    _boost{boost},
    _filter_mode{opts.hnsw_filter_mode},
    _column_filter{opts.hnsw_column_filter},
    _inclusive{limits.inclusive} {
  if (_metric == VectorMetric::Cosine) {
    duckdb::L2NormalizeOp::Operation(_query.data(), _query.data(), _d);
  }
}

template<typename Dist>
void HnswQuery::RunRadius(Dist& dist, detail::TableFilter* table,
                          HnswSearchScratch& scratch) const {
  ResolveBool(_inclusive, [&]<bool Inclusive>() {
    if (!_inner && !table) {
      HnswSearchRadius<Inclusive>(_data->graph, dist, _threshold, _max_results,
                                  scratch);
      return;
    }
    auto set = detail::MakeDocSet(
      _inner.get(), static_cast<doc_id_t>(_segment.docs_count()), table);
    HnswSearchRadius<Inclusive>(_data->graph, dist, _threshold, _max_results,
                                scratch, AdmitIn(set));
  });
}

template<typename Dist>
void HnswQuery::RunFiltered(Dist& dist, detail::TableFilter* table,
                            HnswSearchScratch& scratch) const {
  SDB_ASSERT(_ef != 0 && (_inner || table));
  const auto& graph = _data->graph;
  const auto docs_count = static_cast<doc_id_t>(_segment.docs_count());
  std::optional<detail::LazyBitset> probe;
  uint64_t matches = 0;
  bool bounded = true;
  if (table || _inner->Kind() == QueryKind::Boolean) {
    probe.emplace(detail::MakeDocSet(_inner.get(), docs_count, table));
    matches =
      table ? probe->EstimateCount(kCountSampleWindows) : probe->Count();
    bounded = probe->Filled() >= probe->End();
  } else {
    matches = _inner->EstimateMax();
  }
  const auto plan =
    PlanHnswFilter(_filter_mode, _column_filter, graph,
                   HnswCandidateBytes(_record_size, _d, _data->stats.get()),
                   _ef, matches, table);
  if (plan.ask_per_hop) {
    probe.reset();
  }
  auto set = probe ? std::move(*probe)
                   : detail::MakeDocSet(_inner.get(), docs_count,
                                        plan.ask_per_hop ? nullptr : table);
  const auto admit = AdmitIn(set);
  uint64_t budget = matches;
  scratch.admit_share =
    std::min(1.0, static_cast<double>(matches) /
                    static_cast<double>(std::max<uint64_t>(1, graph.Size())));
  const auto walked = [&](bool complete) {
    return complete && (scratch.nearest.size() >= _ef ||
                        (bounded && scratch.nearest.size() >= matches));
  };
  const auto run = [&](const auto& acc) {
    bool done = false;
    if (plan.walks) {
      ResolveEnum<HnswWalk>(plan.walk, [&]<HnswWalk Walk>() {
        done =
          walked(HnswSearchTopK<Walk>(graph, dist, _ef, scratch, acc, budget));
      });
    }
    return done;
  };
  if (plan.ask_per_hop) {
    WalkFilterScratch walk_scratch;
    const WalkFilter walk_filter{*table, docs_count, walk_scratch};
    const bool done =
      _inner ? run(InnerAndTableFilter{set, walk_filter, walk_scratch})
             : run(walk_filter);
    if (done) {
      return;
    }
    table->Rewind();
    set = detail::MakeDocSet(_inner.get(), docs_count, table);
  } else if (run(admit)) {
    return;
  } else if (plan.two_hop && plan.walk == HnswWalk::TwoHop &&
             scratch.scored < budget) {
    budget -= scratch.scored;
    const auto seeds = AdmittedSeeds(set, graph, 2 * _ef);
    if (walked(HnswSearchTopKFrom<HnswWalk::TwoHop>(graph, dist, _ef, scratch,
                                                    seeds, admit, budget))) {
      return;
    }
  }
  SDB_IF_FAILURE("irs::HnswFilteredScan") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  HnswScanTopK(set, graph, dist, _ef, scratch);
}

void HnswQuery::PrepareDistance() const {
  absl::call_once(_distance_prepared, [&] {
    std::span<const float> query = _query;
    if (_data->rotation) {
      _panorama =
        RotateQuery(_cache, _data->rotation, _data->byte_rotation, _query);
      query = _panorama->rotated;
    }
    if (_data->stats) {
      _codebook = _ef == 0 ? _data->stats->MakeRangeCodebook(query)
                           : QueryCodebook(_cache, *_data->stats, query);
      SDB_ASSERT(_codebook);
    }
  });
}

bool HnswQuery::ScansByColumn() const {
  if (_ef == 0 || !_exact || !_data->rotation || !_inner ||
      _inner->Kind() == QueryKind::Boolean || !_segment.GetColReader()) {
    return false;
  }
  const auto matches = _inner->EstimateMax();
  const uint64_t share =
    _data->byte_rotation ? 2 * kHnswColumnScanShare : kHnswColumnScanShare;
  if (matches * share > _d) {
    return false;
  }
  if (_filter_mode != HnswFilterMode::Auto) {
    return _filter_mode == HnswFilterMode::Scan;
  }
  return !PlanHnswFilter(
            _filter_mode, _column_filter, _data->graph,
            HnswCandidateBytes(_record_size, _d, _data->stats.get()), _ef,
            matches, nullptr)
            .walks;
}

void HnswQuery::ScanColumn(HnswSearchScratch& s) const {
  auto set = detail::MakeDocSet(_inner.get(),
                                static_cast<doc_id_t>(_segment.docs_count()));
  detail::RawVectorReader reader{*_exact, *_segment.GetColReader(), _d};
  reader.SetQuery(_query, _metric);
  std::vector<doc_id_t> docs;
  HnswColumnDist dist{.reader = &reader, .docs = &docs};
  HnswScanTopK(set, _data->graph, dist, _ef, s);
}

void HnswQuery::Widen(QuantizerReader& reader, HnswSearchScratch& scratch,
                      std::vector<score_t>& spreads) const {
  auto& nearest = scratch.nearest;
  const size_t n = nearest.size();
  if (n == 0) {
    return;
  }
  auto& ids = scratch.batch;
  auto& scores = scratch.scores;
  ids.resize(n);
  scores.resize(3 * n);
  for (size_t i = 0; i < n; ++i) {
    ids[i] = nearest[i].node;
    scores[i] = nearest[i].score;
  }
  const std::span<score_t> estimates{scores.data(), n};
  score_t* errors = scores.data() + n;
  reader.ScoreErrors(_data->codes.data(), _record_size, ids, estimates, errors);
  const auto z = _rescore_width;
  auto floor = std::numeric_limits<score_t>::lowest();
  if (_top_k != 0 && n > _top_k) {
    const std::span<score_t> lower{scores.data() + 2 * n, n};
    for (size_t i = 0; i < n; ++i) {
      lower[i] = estimates[i] - z * errors[i];
    }
    const auto kth = lower.begin() + (_top_k - 1);
    absl::c_nth_element(lower, kth, std::greater{});
    floor = *kth;
  }
  size_t kept = 0;
  spreads.clear();
  for (size_t i = 0; i < n; ++i) {
    const auto spread = z * errors[i];
    if (estimates[i] + spread < floor) {
      continue;
    }
    nearest[kept++] = {.score = estimates[i], .node = ids[i]};
    spreads.push_back(spread);
  }
  nearest.resize(kept);
}

std::vector<ScoreDoc> HnswQuery::RunSearch(
  detail::TableFilter* table, std::vector<score_t>* spreads) const {
  HnswSearchScratch scratch;
  std::vector<score_t> widened;
  bool wide = false;
  if (table && !table->Foldable()) {
    table = nullptr;
  }
  if (!table && ScansByColumn()) {
    ScanColumn(scratch);
    return CollectHits(scratch.nearest, _segment);
  }
  PrepareDistance();
  WithHnswDist(*_data, _query, _codebook, _panorama.get(), _metric, _d,
               _record_size, [&](auto& dist) {
                 if (_ef == 0) {
                   RunRadius(dist, table, scratch);
                 } else if (_inner || table) {
                   RunFiltered(dist, table, scratch);
                 } else {
                   HnswSearchTopK(_data->graph, dist, _ef, scratch);
                 }
                 if constexpr (requires { dist.qr; }) {
                   if (spreads && _ef != 0 && _rescore_width > 0.f) {
                     Widen(*dist.qr, scratch, widened);
                     wide = true;
                   }
                 }
               });
  auto hits = wide ? CollectHits(scratch.nearest, _segment, widened, *spreads)
                   : CollectHits(scratch.nearest, _segment);
  if (_ef == 0 && _data->stats && _exact && !hits.empty()) {
    RerankExactDistances(_segment, *_exact, _d, _query, _metric, hits);
    std::erase_if(hits, [&](const ScoreDoc& hit) {
      return _inclusive ? hit.score < _threshold : hit.score <= _threshold;
    });
  }
  return hits;
}

}  // namespace irs
