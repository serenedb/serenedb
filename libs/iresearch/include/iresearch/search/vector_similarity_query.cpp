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

#include <array>
#include <cmath>
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/containers/bitset.hpp"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
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

class VectorBlockScanDocIterator : public DocIterator {
 public:
  static constexpr size_t kBlock = doc_limits::kBlockSize;  // 128

  VectorBlockScanDocIterator(const TermReader& reader,
                             std::vector<PostingCookie>&& cookies,
                             std::vector<uint64_t>&& pay_starts,
                             std::vector<uint32_t>&& cluster_counts,
                             std::unique_ptr<VectorBlockReader> vr,
                             bitset&& inner_bits, bool has_inner, score_t boost,
                             CostAttr::Type estimation)
    : _reader{reader},
      _cookies{std::move(cookies)},
      _pay_starts{std::move(pay_starts)},
      _cluster_counts{std::move(cluster_counts)},
      _vr{std::move(vr)},
      _inner_bits{std::move(inner_bits)},
      _has_inner{has_inner},
      _boost{boost},
      _cost{estimation} {
    SDB_ASSERT(_vr);
  }

  doc_id_t advance() final {
    for (;;) {
      if (!_cur) {
        if (_cluster >= _cookies.size()) {
          return _doc = doc_limits::eof();
        }
        _cur = _reader.Iterator(IndexFeatures::None, _cookies[_cluster++]);
        if (!_cur) {
          continue;
        }
      }
      const auto doc = _cur->advance();
      if (!doc_limits::eof(doc)) {
        return _doc = doc;
      }
      _cur = nullptr;
    }
  }

  doc_id_t seek(doc_id_t target) final {
    while (_doc < target) {
      if (doc_limits::eof(advance())) {
        break;
      }
    }
    return _doc;
  }

  void Collect(const ScoreFunction&, ColumnArgsFetcher&,
               ScoreCollector& collector) final {
    for (size_t c = 0; c < _cookies.size(); ++c) {
      auto it = _reader.Iterator(IndexFeatures::None, _cookies[c]);
      if (!it) {
        continue;
      }
      _vr->StartCluster(_pay_starts.empty() ? 0 : _pay_starts[c],
                        _cluster_counts.empty() ? 0 : _cluster_counts[c]);
      uint32_t base = 0;
      for (;;) {
        size_t got = 0;
        for (; got < kBlock; ++got) {
          const auto doc = it->advance();
          if (doc_limits::eof(doc)) {
            break;
          }
          _docs[got] = doc;
        }
        if (got == 0) {
          break;
        }
        _vr->ComputeBlock(_docs.data(), base, got, _boost, _dist.data());
        base += static_cast<uint32_t>(got);

        if (!_has_inner) {
          collector.AddDocs(_docs.data(), got, _dist.data());
          continue;
        }
        size_t k = 0;
        for (size_t i = 0; i < got; ++i) {
          if (_inner_bits.test(_docs[i])) {
            _keep_docs[k] = _docs[i];
            _keep_dist[k] = _dist[i];
            ++k;
          }
        }
        if (k != 0) {
          collector.AddDocs(_keep_docs.data(), k, _keep_dist.data());
        }
      }
    }
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return type == irs::Type<CostAttr>::id() ? &_cost : nullptr;
  }

 private:
  const TermReader& _reader;
  std::vector<PostingCookie> _cookies;
  std::vector<uint64_t> _pay_starts;
  std::vector<uint32_t> _cluster_counts;
  std::unique_ptr<VectorBlockReader> _vr;
  bitset _inner_bits;
  bool _has_inner;
  score_t _boost;
  CostAttr _cost;
  std::array<doc_id_t, kBlock> _docs;
  std::array<score_t, kBlock> _dist;
  std::array<doc_id_t, kBlock> _keep_docs;
  std::array<score_t, kBlock> _keep_dist;
  DocIterator::ptr _cur;
  size_t _cluster = 0;
};

}  // namespace

DocIterator::ptr VectorSimilarityQuery::Execute(
  const ExecutionContext& ctx, const StatsBuffer& stats) const {
  if (_state.cookies.empty()) {
    return DocIterator::empty();
  }
  SDB_ASSERT(_state.reader);
  SDB_ASSERT(_state.vector_column);

  const auto* col_reader = _segment.GetColReader();
  if (!col_reader) {
    return DocIterator::empty();
  }

  std::vector<PostingCookie> cookies;
  cookies.reserve(_state.cookies.size());
  for (const auto& cookie : _state.cookies) {
    SDB_ASSERT(cookie);
    cookies.push_back({.cookie = cookie.get(), .field = _state.reader->meta()});
  }

  if (std::isinf(_radius)) {
    std::unique_ptr<VectorBlockReader> vr;
    if (_state.quant != VectorQuantization::None) {
      if (auto pay_in = _state.reader->ReopenPayload()) {
        vr = MakeQuantizerReader(_state.quant, std::move(pay_in), _state.d);
      }
    }
    if (!vr) {
      vr = MakeRawVectorReader(*_state.vector_column, *col_reader, _state.d);
    }
    vr->SetQuery(std::span{_query}, _metric);

    bitset inner_bits;
    bool has_inner = false;
    if (_inner) {
      auto inner_it = _inner->Execute(ctx, stats);
      if (!inner_it) {
        return DocIterator::empty();
      }
      inner_bits.reset(_segment.docs_count() + doc_limits::min());
      for (auto d = inner_it->advance(); !doc_limits::eof(d);
           d = inner_it->advance()) {
        inner_bits.set(d);
      }
      has_inner = true;
    }

    return memory::make_managed<VectorBlockScanDocIterator>(
      *_state.reader, std::move(cookies),
      std::vector<uint64_t>{_state.pay_starts.begin(), _state.pay_starts.end()},
      std::vector<uint32_t>{_state.cluster_counts.begin(),
                            _state.cluster_counts.end()},
      std::move(vr), std::move(inner_bits), has_inner, _boost,
      _state.estimation);
  }

  auto approx = _state.reader->Iterator(IndexFeatures::None, cookies, ctx.wand,
                                        /*min_match=*/1, ScoreMergeType::Noop);
  if (!approx) {
    return DocIterator::empty();
  }

  // Hybrid search: intersect the cluster-union candidates with the inner
  // predicate (e.g. a text filter) before reranking. Keeps the published
  // BoostBlockAttr on the wrapping VectorSimilarityDocIterator.
  if (_inner) {
    auto inner_it = _inner->Execute(ctx, stats);
    if (!inner_it) {
      return DocIterator::empty();
    }
    ScoreAdapters itrs;
    itrs.reserve(2);
    itrs.emplace_back(std::move(approx));
    itrs.emplace_back(std::move(inner_it));
    approx = MakeConjunction(ScoreMergeType::Noop, ctx.wand,
                             _segment.docs_count(), std::move(itrs));
    if (!approx) {
      return DocIterator::empty();
    }
  }

  return memory::make_managed<VectorSimilarityDocIterator>(
    std::move(approx), *col_reader, *_state.vector_column, std::span{_query},
    _metric, _radius, _inclusive, _state.estimation, _boost);
}

}  // namespace irs
