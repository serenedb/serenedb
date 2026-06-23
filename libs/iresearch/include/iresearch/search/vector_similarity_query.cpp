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

#include "iresearch/search/vector_similarity_query.hpp"

#include <cmath>
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {
namespace {

// Wraps the cluster-union disjunction and scores every candidate by its exact
// distance to the query vector. Per-doc scores are published through a
// BoostBlockAttr that VectorSimilarityScorer reads back (mirrors how
// NGramSimilarityDocIterator surfaces its filter boost).
class VectorSimilarityDocIterator : public DocIterator {
 public:
  VectorSimilarityDocIterator(DocIterator::ptr&& approx,
                              const ColReader& col_reader,
                              const ColumnReader& vector_column,
                              std::span<const float> query, VectorMetric metric,
                              float radius, bool inclusive,
                              CostAttr::Type estimation, score_t boost)
    : _approx{std::move(approx)},
      _read_ctx{col_reader},
      _vreader{vector_column, _read_ctx},
      _query{query},
      _dist{ResolveVectorDistance(metric)},
      _radius{radius},
      _gated{std::isfinite(radius)},
      _inclusive{inclusive},
      _nearest_is_largest{VectorMetricNearestIsLargest(metric)},
      _boost{boost},
      _cost{estimation} {
    SDB_ASSERT(_approx);
    SDB_ASSERT(_query.size() == _vreader.Dimension());
  }

  ~VectorSimilarityDocIterator() {
    if (_block) {
      std::allocator<score_t>{}.deallocate(_block, kScoreBlock);
    }
  }

  doc_id_t advance() final {
    if (!_gated) {
      return _doc = _approx->advance();
    }
    for (;;) {
      const auto doc = _approx->advance();
      if (doc_limits::eof(doc) || Gate(doc)) {
        return _doc = doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (!_gated) {
      return _doc = _approx->seek(target);
    }
    const auto doc = _approx->seek(target);
    if (doc_limits::eof(doc) || Gate(doc)) {
      return _doc = doc;
    }
    return advance();
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
    if (!_block) {
      _block = std::allocator<score_t>{}.allocate(kScoreBlock);
      _boosts.value = _block;
    }
    return ctx.scorer->PrepareScorer({
      .segment = *ctx.segment,
      .field = _field,
      .doc_attrs = *this,
      .fetcher = ctx.fetcher,
      .stats = nullptr,
      .boost = _boost,
    });
  }

  void FetchScoreArgs(uint16_t index) final {
    SDB_ASSERT(_block);
    _block[index] = _gated ? _cached_score : Score(value());
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<CostAttr>::id()) {
      return &_cost;
    }
    if (type == irs::Type<BoostBlockAttr>::id()) {
      return _block ? &_boosts : nullptr;
    }
    return _approx->GetMutable(type);
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    CollectImpl(*this, scorer, fetcher, collector);
  }

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      FillBlockScoreContext score,
                                      FillBlockMatchContext match) final {
    return FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  float Distance(doc_id_t doc) {
    const float* v = _vreader.ReadDoc(doc);
    return _dist(reinterpret_cast<const byte_type*>(_query.data()),
                 reinterpret_cast<const byte_type*>(v),
                 static_cast<uint16_t>(_query.size()));
  }

  score_t Score(doc_id_t doc) { return Distance(doc); }

  // Caches the (light) distance of `doc` and reports whether it is within the
  // radius. The connector supplies `radius` in light space; for metrics where
  // nearer means a larger light value (inner product, cosine similarity) the
  // ball is `light >= radius`, otherwise `light <= radius`.
  bool Gate(doc_id_t doc) {
    const float dist = Distance(doc);
    _cached_score = dist;
    if (_nearest_is_largest) {
      return _inclusive ? dist >= _radius : dist > _radius;
    }
    return _inclusive ? dist <= _radius : dist < _radius;
  }

  DocIterator::ptr _approx;
  ReadContext _read_ctx;
  IvfVectorReader _vreader;
  std::span<const float> _query;
  VectorDistanceFn _dist;
  float _radius;
  bool _gated;
  bool _inclusive;
  bool _nearest_is_largest;
  score_t _boost;
  FieldProperties _field;
  CostAttr _cost;
  BoostBlockAttr _boosts;
  score_t* _block = nullptr;
  score_t _cached_score = 0.f;
};

}  // namespace

DocIterator::ptr VectorSimilarityQuery::execute(
  const ExecutionContext& ctx) const {
  const auto& segment = ctx.segment;
  const auto* state = _states.find(segment);
  if (!state || state->cookies.empty()) {
    return DocIterator::empty();
  }
  SDB_ASSERT(state->reader);
  SDB_ASSERT(state->vector_column);

  const auto* col_reader = segment.GetColReader();
  if (!col_reader) {
    return DocIterator::empty();
  }

  std::vector<PostingCookie> cookies;
  cookies.reserve(state->cookies.size());
  for (const auto& cookie : state->cookies) {
    SDB_ASSERT(cookie);
    cookies.push_back({.cookie = cookie.get(), .field = state->reader->meta()});
  }

  auto approx =
    state->reader->Iterator(IndexFeatures::None, cookies, ctx.wand,
                            /*min_match=*/1, ScoreMergeType::Noop);
  if (!approx) {
    return DocIterator::empty();
  }

  // Hybrid search: intersect the cluster-union candidates with the inner
  // predicate (e.g. a text filter) before reranking. Keeps the published
  // BoostBlockAttr on the wrapping VectorSimilarityDocIterator.
  if (_inner) {
    auto inner_it = _inner->execute(ctx);
    if (!inner_it) {
      return DocIterator::empty();
    }
    ScoreAdapters itrs;
    itrs.reserve(2);
    itrs.emplace_back(std::move(approx));
    itrs.emplace_back(std::move(inner_it));
    approx = MakeConjunction(ScoreMergeType::Noop, ctx.wand,
                             segment.docs_count(), std::move(itrs));
    if (!approx) {
      return DocIterator::empty();
    }
  }

  return memory::make_managed<VectorSimilarityDocIterator>(
    std::move(approx), *col_reader, *state->vector_column, std::span{_query},
    _metric, _radius, _inclusive, state->estimation, _boost);
}

}  // namespace irs
