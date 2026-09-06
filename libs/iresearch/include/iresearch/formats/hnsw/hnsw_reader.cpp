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

#include "iresearch/formats/hnsw/hnsw_reader.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <span>
#include <utility>

#include "basics/assert.h"
#include "basics/memory.hpp"
#include "basics/misc.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/store/data_input.hpp"

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
             score_t /*threshold*/ = 0.f) const noexcept {
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

class HnswTopRoot : public top::Root {
 public:
  HnswTopRoot(std::vector<ScoreDoc>&& hits, const SubReader& segment,
              ColumnArgsFetcher& fetcher, const search::ScoreArgs& args)
    : _hits{std::move(hits)}, _fetcher{fetcher} {
    SDB_ASSERT(args.scorer != nullptr);
    _provider.attr.value = _block;
    _score = args.scorer->PrepareScorer({
      .segment = segment,
      .field = search::NoField(),
      .doc_attrs = _provider,
      .fetcher = &fetcher,
      .stats = args.stats,
      .boost = args.boost,
    });
  }

  void Run(LoserScoreCollector& collector) final {
    for (size_t i = 0, total = _hits.size(); i < total;) {
      const auto n =
        static_cast<uint32_t>(std::min<size_t>(kScoreBlock, total - i));
      for (uint32_t j = 0; j < n; ++j) {
        _block[j] = _hits[i + j].score;
        _docs[j] = _hits[i + j].doc;
      }
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, n});
      _score.Score(_scores, static_cast<scores_size_t>(n));
      collector.AddDocs(_docs, n, _scores);
      i += n;
    }
  }

 private:
  struct Provider final : AttributeProvider {
    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      return type == irs::Type<BoostBlockAttr>::id() ? &attr : nullptr;
    }

    BoostBlockAttr attr;
  };

  std::vector<ScoreDoc> _hits;
  Provider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  score_t _block[kScoreBlock];
  score_t _scores[kScoreBlock];
  doc_id_t _docs[kScoreBlock];
};

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

class HnswQuery : public QueryBuilder {
 public:
  HnswQuery(const SubReader& segment, std::shared_ptr<const HnswData> data,
            std::shared_ptr<const QuantizerCodebook> codebook,
            std::vector<float> query, VectorMetric metric, uint32_t d,
            uint32_t record_size, uint32_t ef, score_t threshold,
            size_t max_results, bool inclusive, score_t boost)
    : QueryBuilder{segment},
      _data{std::move(data)},
      _codebook{std::move(codebook)},
      _query{std::move(query)},
      _metric{metric},
      _d{d},
      _record_size{record_size},
      _ef{ef},
      _threshold{threshold},
      _max_results{max_results},
      _boost{boost},
      _inclusive{inclusive} {}

  top::Root::ptr PlanTop(const top::Context& ctx) const final {
    auto hits = RunSearch();
    if (hits.empty()) {
      return {};
    }
    const auto record = Stats(top::ScoredOf(ctx));
    const search::ScoreArgs args{.scorer = record.scorer,
                                 .stats = record.stats,
                                 .fetcher = &ctx.fetcher,
                                 .boost = _boost};
    return memory::make_managed<HnswTopRoot>(std::move(hits), _segment,
                                             ctx.fetcher, args);
  }

  count::Root::ptr PlanCount(const count::Context&) const final { return {}; }

  docs::Root::ptr PlanDocs(const docs::Context&) const final { return {}; }

  scored::Root::ptr PlanScored(const scored::Context&) const final {
    return {};
  }

  lead::Node::ptr PlanLead(const search::ScoredCtx&) const final { return {}; }

  probe::Node::ptr PlanProbe(const search::ScoredCtx&, uint64_t) const final {
    return {};
  }

  fill::Node::ptr PlanFill(const search::ScoredCtx&,
                           ScoreMergeType) const final {
    return {};
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  std::vector<ScoreDoc> RunSearch() const {
    auto& scratch = ThreadScratch();
    WithHnswDist(*_data, _query, _codebook, _metric, _d, _record_size,
                 [&](auto& dist) {
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

  std::shared_ptr<const HnswData> _data;
  std::shared_ptr<const QuantizerCodebook> _codebook;
  std::vector<float> _query;
  VectorMetric _metric;
  uint32_t _d;
  uint32_t _record_size;
  uint32_t _ef;
  score_t _threshold;
  size_t _max_results;
  score_t _boost;
  bool _inclusive;
};

std::vector<float> NormalizedQuery(const VectorFilterOptions& opts,
                                   uint32_t d) {
  std::vector<float> q{opts.query.begin(), opts.query.end()};
  if (opts.metric == VectorMetric::Cosine) {
    std::vector<float> normalized(q.size());
    vector::L2Space<float, float, float>::Normalize(
      reinterpret_cast<const byte_type*>(q.data()), static_cast<uint16_t>(d),
      normalized.data());
    q = std::move(normalized);
  }
  return q;
}

}  // namespace

HnswHeader HnswIndex::ReadHeader(IndexInput& in) {
  HnswHeader h;
  h.version = static_cast<uint32_t>(in.ReadI32());
  h.d = static_cast<uint32_t>(in.ReadI32());
  h.metric = static_cast<VectorMetric>(in.ReadI32());
  h.quant = static_cast<VectorQuantization>(in.ReadI32());
  h.ef_construction = static_cast<uint32_t>(in.ReadI32());
  h.record_size = static_cast<uint32_t>(in.ReadI32());
  h.rows = static_cast<uint64_t>(in.ReadI64());
  return h;
}

std::shared_ptr<const HnswData> HnswIndex::Load(
  const SubReader& segment) const {
  std::call_once(_once, [&] {
    auto in = segment.ReopenAnn();
    if (!in) {
      return;
    }
    in->Seek(_meta.offset);
    IRS_IGNORE(ReadHeader(*in));
    auto data = std::make_shared<HnswData>();
    data->graph = HnswGraph::Deserialize(*in);
    if (_header.quant == VectorQuantization::None) {
      data->vectors.resize(static_cast<size_t>(_header.rows) * _header.d);
      if (!data->vectors.empty()) {
        in->ReadData(reinterpret_cast<byte_type*>(data->vectors.data()),
                     data->vectors.size() * sizeof(float));
      }
    } else {
      const auto stats_size = static_cast<size_t>(in->ReadI64());
      bstring stats;
      stats.resize(stats_size);
      if (stats_size != 0) {
        in->ReadData(stats.data(), stats_size);
      }
      data->stats = MakeQuantizerStats(_header.quant, _header.d, stats,
                                       EffectiveQuantMetric(_header.metric),
                                       /*row_major=*/true);
      if (!data->stats) {
        return;
      }
      if (QuantizerNeedsCentroid(_header.quant)) {
        data->centroid.resize(_header.d);
        in->ReadData(reinterpret_cast<byte_type*>(data->centroid.data()),
                     data->centroid.size() * sizeof(float));
      }
      data->codes.resize(static_cast<size_t>(_header.rows) *
                         _header.record_size);
      if (!data->codes.empty()) {
        in->ReadData(data->codes.data(), data->codes.size());
      }
    }
    _data = std::move(data);
  });
  return _data;
}

QueryBuilder::ptr HnswIndex::PrepareKnn(const SubReader& segment,
                                        const PrepareContext& ctx,
                                        const VectorFilterOptions& opts,
                                        uint32_t /*effort*/) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  auto query = NormalizedQuery(opts, _header.d);
  auto codebook = data->stats ? data->stats->MakeCodebook(query) : nullptr;
  SDB_ASSERT(!data->stats || codebook);
  SDB_ASSERT(opts.ef_search != 0);
  const auto ef = std::max(opts.ef_search, opts.min_ef);
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), std::move(codebook), std::move(query),
    opts.metric, _header.d, _header.record_size, ef, kHnswNoThreshold,
    /*max_results=*/0, /*inclusive=*/false, ctx.boost);
  built->SetStats(ctx.Record());
  return built;
}

QueryBuilder::ptr HnswIndex::PrepareRange(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const VectorFilterOptions& opts,
                                          float radius, bool inclusive,
                                          uint32_t /*effort*/) const {
  SDB_ASSERT(opts.query.size() == _header.d);
  if (Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  auto query = NormalizedQuery(opts, _header.d);
  auto codebook = data->stats ? data->stats->MakeCodebook(query) : nullptr;
  SDB_ASSERT(!data->stats || codebook);
  const bool angular = opts.metric == VectorMetric::InnerProduct ||
                       opts.metric == VectorMetric::Cosine;
  const score_t threshold = angular ? radius : -radius;
  auto built = memory::make_tracked<HnswQuery>(
    ctx.memory, segment, std::move(data), std::move(codebook), std::move(query),
    opts.metric, _header.d, _header.record_size, /*ef=*/0, threshold,
    static_cast<size_t>(_header.rows), inclusive, ctx.boost);
  built->SetStats(ctx.Record());
  return built;
}

}  // namespace irs
