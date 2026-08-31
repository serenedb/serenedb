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
#include "iresearch/search/cost.hpp"
#include "iresearch/search/scorer.hpp"
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
    if constexpr (EffectiveQuantMetric(M) == VectorMetric::L2Sqr) {
      HnswBatchL2Sqr(q, base, d, ids, out);
    } else if constexpr (EffectiveQuantMetric(M) ==
                         VectorMetric::InnerProduct) {
      HnswBatchIp(q, base, d, ids, out);
    } else {
      for (size_t i = 0; i < ids.size(); ++i) {
        out[i] = ComputeDistance<EffectiveQuantMetric(M)>(
          q, Row(ids[i]), static_cast<uint16_t>(d));
      }
    }
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
  std::vector<byte_type> gather;

  const byte_type* Row(uint32_t id) const noexcept {
    return codes + static_cast<size_t>(id) * record_size;
  }

  score_t One(uint32_t id) {
    score_t out = .0f;
    qr->ComputeBlock({Row(id), record_size}, kHnswNoThreshold, &out);
    return out;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t threshold = kHnswNoThreshold) {
    gather.resize(ids.size() * static_cast<size_t>(record_size));
    for (size_t i = 0; i < ids.size(); ++i) {
      std::memcpy(gather.data() + i * record_size, Row(ids[i]), record_size);
    }
    qr->ComputeBlock(gather, threshold, out);
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }
};

template<typename Fn>
void WithHnswDist(const HnswData& data, std::span<const float> query,
                  VectorMetric metric, uint32_t d, uint32_t record_size,
                  Fn&& fn) {
  if (data.stats) {
    auto codebook = data.stats->MakeCodebook(query);
    if (!codebook) {
      return;
    }
    auto reader = MakeQuantizerReader(codebook);
    if (!reader) {
      return;
    }
    reader->StartCluster(data.centroid.empty() ? nullptr
                                               : data.centroid.data());
    HnswCodeDist dist{.codes = data.codes.data(),
                      .record_size = record_size,
                      .qr = reader.get()};
    fn(dist);
    return;
  }
  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric M>() {
    HnswQueryDist<M> dist{
      .base = data.vectors.data(), .d = d, .q = query.data()};
    fn(dist);
  });
}

class HnswTopKIterator : public DocIterator {
 public:
  HnswTopKIterator(std::vector<ScoreDoc>&& hits, score_t boost)
    : _hits{std::move(hits)}, _boost{boost}, _cost{_hits.size()} {
    _boosts.value = _scores.data();
  }

  doc_id_t advance() final {
    if (_pos >= _hits.size()) {
      _cur = .0f;
      return _doc = doc_limits::eof();
    }
    _cur = _hits[_pos].score;
    return _doc = _hits[_pos++].doc;
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) {
      return _doc;
    }
    while (_pos < _hits.size() && _hits[_pos].doc < target) {
      ++_pos;
    }
    return advance();
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = {},
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = nullptr,
      .boost = _boost,
    });
  }

  void FetchScoreArgs(uint16_t index) final {
    SDB_ASSERT(index < _scores.size());
    _scores[index] = _cur;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<CostAttr>::id()) {
      return &_cost;
    }
    if (type == irs::Type<BoostBlockAttr>::id()) {
      return &_boosts;
    }
    return nullptr;
  }

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  std::vector<ScoreDoc> _hits;
  size_t _pos = 0;
  score_t _boost;
  CostAttr _cost;
  BoostBlockAttr _boosts;
  std::array<score_t, kScoreBlock> _scores;
  score_t _cur = .0f;
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

class HnswVectorQuery : public QueryBuilder {
 public:
  HnswVectorQuery(const SubReader& segment,
                  std::shared_ptr<const HnswData> data,
                  std::vector<float> query, VectorMetric metric, uint32_t d,
                  uint32_t record_size, uint32_t ef, score_t boost)
    : QueryBuilder{segment},
      _data{std::move(data)},
      _query{std::move(query)},
      _metric{metric},
      _d{d},
      _record_size{record_size},
      _ef{ef},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& /*stats*/) const final {
    HnswSearchScratch scratch;
    WithHnswDist(*_data, _query, _metric, _d, _record_size, [&](auto& dist) {
      HnswSearchTopK(_data->graph, dist, _ef, scratch);
    });

    auto hits = CollectHits(scratch.nearest, _segment.docs_mask());
    if (hits.empty()) {
      return DocIterator::empty();
    }
    return memory::make_tracked<HnswTopKIterator>(ctx.memory, std::move(hits),
                                                  _boost);
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  std::shared_ptr<const HnswData> _data;
  std::vector<float> _query;
  VectorMetric _metric;
  uint32_t _d;
  uint32_t _record_size;
  uint32_t _ef;
  score_t _boost;
};

class HnswRangeQuery : public QueryBuilder {
 public:
  HnswRangeQuery(const SubReader& segment, std::shared_ptr<const HnswData> data,
                 std::vector<float> query, VectorMetric metric, uint32_t d,
                 uint32_t record_size, score_t threshold, size_t max_results,
                 score_t boost)
    : QueryBuilder{segment},
      _data{std::move(data)},
      _query{std::move(query)},
      _metric{metric},
      _d{d},
      _record_size{record_size},
      _threshold{threshold},
      _max_results{max_results},
      _boost{boost} {}

  DocIterator::ptr Execute(const ExecutionContext& ctx,
                           const StatsBuffer& /*stats*/) const final {
    HnswSearchScratch scratch;
    WithHnswDist(*_data, _query, _metric, _d, _record_size, [&](auto& dist) {
      HnswSearchRadius(_data->graph, dist, _threshold, _max_results, scratch);
    });

    auto hits = CollectHits(scratch.nearest, _segment.docs_mask());
    if (hits.empty()) {
      return DocIterator::empty();
    }
    return memory::make_tracked<HnswTopKIterator>(ctx.memory, std::move(hits),
                                                  _boost);
  }

  void Visit(PreparedStateVisitor&, score_t) const final {}

  score_t Boost() const noexcept final { return _boost; }

 private:
  std::shared_ptr<const HnswData> _data;
  std::vector<float> _query;
  VectorMetric _metric;
  uint32_t _d;
  uint32_t _record_size;
  score_t _threshold;
  size_t _max_results;
  score_t _boost;
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
                                        uint32_t effort) const {
  if (opts.query.size() != _header.d || Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  const auto ef = std::max(effort, kHnswDefaultEfSearch);
  return memory::make_tracked<HnswVectorQuery>(
    ctx.memory, segment, std::move(data), NormalizedQuery(opts, _header.d),
    opts.metric, _header.d, _header.record_size, ef, ctx.boost);
}

QueryBuilder::ptr HnswIndex::PrepareRange(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const VectorFilterOptions& opts,
                                          float radius, bool /*inclusive*/,
                                          uint32_t /*effort*/) const {
  if (opts.query.size() != _header.d || Empty()) {
    return QueryBuilder::Empty();
  }
  auto data = Load(segment);
  if (!data || data->graph.Empty()) {
    return QueryBuilder::Empty();
  }
  const bool angular = opts.metric == VectorMetric::InnerProduct ||
                       opts.metric == VectorMetric::Cosine;
  const score_t threshold = angular ? radius : -radius;
  return memory::make_tracked<HnswRangeQuery>(
    ctx.memory, segment, std::move(data), NormalizedQuery(opts, _header.d),
    opts.metric, _header.d, _header.record_size, threshold,
    static_cast<size_t>(_header.rows), ctx.boost);
}

}  // namespace irs
