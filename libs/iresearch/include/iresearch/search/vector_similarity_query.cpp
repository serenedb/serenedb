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
#include <memory>
#include <span>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/posting/iterator_doc.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

class RawVectorReader {
 public:
  RawVectorReader(const ColumnReader& vector_column,
                  const ColReader& col_reader, uint32_t d)
    : _read_ctx{col_reader}, _vreader{vector_column, _read_ctx}, _d{d} {}

  void SetQuery(std::span<const float> query, VectorMetric metric) {
    _query.assign(query.begin(), query.end());
    _dist = ResolveVectorDistance(metric);
  }

  void ComputeDistance(doc_id_t doc, score_t boost, score_t& out) {
    SDB_ASSERT(_dist);

    const auto* q = reinterpret_cast<const byte_type*>(_query.data());
    const auto d = static_cast<uint16_t>(_d);
    const float* v = _vreader.ReadDoc(doc);
    out = _dist(q, reinterpret_cast<const byte_type*>(v), d) * boost;
  }

 private:
  ReadContext _read_ctx;
  IvfVectorReader _vreader;
  VectorDistanceFn _dist = nullptr;
  std::vector<float> _query;
  uint32_t _d;
};

class VectorDistanceIterator : public DocIterator {
 public:
  VectorDistanceIterator(DocIterator::ptr&& src, score_t boost,
                         CostAttr::Type estimation)
    : _src{std::move(src)}, _boost{boost}, _cost{estimation} {
    SDB_ASSERT(_src);
    _boosts.value = _scores.data();
  }

  score_t Distance() const noexcept { return _cur_dist; }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    SDB_ASSERT(ctx.scorer);
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
    SDB_ASSERT(index < _scores.size());
    _scores[index] = _cur_dist;
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == irs::Type<CostAttr>::id()) {
      return &_cost;
    }
    if (type == irs::Type<BoostBlockAttr>::id()) {
      return &_boosts;
    }
    return _src->GetMutable(type);
  }

 protected:
  DocIterator::ptr _src;
  score_t _boost;
  FieldProperties _field;
  CostAttr _cost;
  BoostBlockAttr _boosts;
  std::array<score_t, kScoreBlock> _scores;
  score_t _cur_dist = .0f;
};

class RawVectorIterator : public VectorDistanceIterator {
 public:
  RawVectorIterator(DocIterator::ptr&& src, const ColumnReader& vector_column,
                    const ColReader& col_reader, uint32_t d,
                    std::span<const float> query, VectorMetric metric,
                    score_t boost, CostAttr::Type estimation)
    : VectorDistanceIterator{std::move(src), boost, estimation},
      _reader{vector_column, col_reader, d} {
    _reader.SetQuery(query, metric);
  }

  doc_id_t advance() final {
    const auto doc = _src->advance();
    if (doc_limits::eof(doc)) {
      _cur_dist = .0f;
      return _doc = doc;
    }
    _reader.ComputeDistance(doc, kNoBoost, _cur_dist);
    return _doc = doc;
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) {
      return _doc;
    }
    const auto doc = _src->seek(target);
    if (doc_limits::eof(doc)) {
      _cur_dist = .0f;
      return _doc = doc;
    }
    _reader.ComputeDistance(doc, kNoBoost, _cur_dist);
    return _doc = doc;
  }

 private:
  RawVectorReader _reader;
};

class QVectorIterator : public VectorDistanceIterator {
 public:
  QVectorIterator(DocIterator::ptr&& src, std::unique_ptr<QuantizerReader> qr,
                  score_t boost, CostAttr::Type estimation)
    : VectorDistanceIterator{std::move(src), boost, estimation},
      _qr{std::move(qr)},
      _total{estimation} {
    SDB_ASSERT(_qr);
    _posting = sdb::basics::downCast<PostingIteratorBase>(_src.get());
  }

  doc_id_t advance() final {
    if (_pos == _len) {
      _len = 0;
      for (; _len < kPostingBlock; ++_len) {
        const auto doc = _src->advance();
        if (doc_limits::eof(doc)) {
          break;
        }
        _docs[_len] = doc;
      }
      if (_len == 0) {
        _cur_dist = .0f;
        return _doc = doc_limits::eof();
      }
      FillDistancesBlock();
      _pos = 0;
    }
    _cur_dist = _dist[_pos];
    _doc = _docs[_pos];
    ++_pos;
    return _doc;
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) {
      return _doc;
    }
    const auto doc = _src->seek(target);
    _pos = _len = 0;
    if (doc_limits::eof(doc)) {
      _cur_dist = .0f;
      return _doc = doc;
    }
    const uint32_t remaining = _posting->RemainingDocs();
    SDB_ASSERT(remaining < _total);
    _base = static_cast<uint32_t>(_total - 1 - remaining);
    _qr->ComputeBlock(_base, 1, kNoBoost, &_cur_dist);
    ++_base;
    return _doc = doc;
  }

 private:
  void FillDistancesBlock() {
    SDB_ASSERT(_len > 0);
    SDB_ASSERT(_len <= _dist.size());
    _qr->ComputeBlock(_base, _len, kNoBoost, _dist.data());
    _base += _len;
  }

  std::unique_ptr<QuantizerReader> _qr;
  PostingIteratorBase* _posting = nullptr;
  CostAttr::Type _total;
  std::array<doc_id_t, kPostingBlock> _docs;
  std::array<score_t, kPostingBlock> _dist;
  uint32_t _base = 0;
  uint16_t _len = 0;
  uint16_t _pos = 0;
};

class VectorRangeIterator : public DocIterator {
 public:
  VectorRangeIterator(memory::managed_ptr<VectorDistanceIterator>&& inner,
                      VectorMetric metric, float radius, bool inclusive)
    : _inner{std::move(inner)},
      _radius{radius},
      _inclusive{inclusive},
      _nearest_is_largest{VectorMetricNearestIsLargest(metric)} {
    SDB_ASSERT(_inner);
  }

  doc_id_t advance() final {
    for (;;) {
      const auto doc = _inner->advance();
      if (doc_limits::eof(doc) || Inside(_inner->Distance())) {
        return _doc = doc;
      }
    }
  }

  doc_id_t seek(doc_id_t target) final {
    if (target <= _doc) {
      return _doc;
    }
    const auto doc = _inner->seek(target);
    if (doc_limits::eof(doc) || Inside(_inner->Distance())) {
      return _doc = doc;
    }
    return advance();
  }

  doc_id_t LazySeek(doc_id_t target) final { return seek(target); }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    return _inner->PrepareScore(ctx);
  }

  void FetchScoreArgs(uint16_t index) final { _inner->FetchScoreArgs(index); }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _inner->GetMutable(type);
  }

 private:
  bool Inside(score_t dist) const noexcept {
    if (_nearest_is_largest) {
      return _inclusive ? dist >= _radius : dist > _radius;
    }
    return _inclusive ? dist <= _radius : dist < _radius;
  }

  memory::managed_ptr<VectorDistanceIterator> _inner;
  float _radius;
  bool _inclusive;
  bool _nearest_is_largest;
};

class FilterIterator : public DocIterator {
 public:
  explicit FilterIterator(DocIterator::ptr&& it) noexcept : _it{std::move(it)} {
    SDB_ASSERT(_it);
  }

  doc_id_t advance() final { return _doc = _it->advance(); }

  doc_id_t seek(doc_id_t target) final { return _doc = _it->seek(target); }

  doc_id_t LazySeek(doc_id_t target) final {
    return _doc = _it->LazySeek(target);
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return _it->GetMutable(type);
  }

 private:
  DocIterator::ptr _it;
};

std::vector<PostingCookie> MakeCookies(const VectorState& state) {
  std::vector<PostingCookie> cookies;
  cookies.reserve(state.cookies.size());
  for (const auto& cookie : state.cookies) {
    SDB_ASSERT(cookie);
    cookies.push_back({.cookie = cookie.get(), .field = state.reader->meta()});
  }
  return cookies;
}

memory::managed_ptr<VectorDistanceIterator> MakeRawReranker(
  const SubReader& segment, const VectorState& state,
  std::span<const float> query, VectorMetric metric, score_t boost,
  const QueryBuilder* inner, const ExecutionContext& ctx,
  const StatsBuffer& stats) {
  const auto* col_reader = segment.GetColReader();
  if (!col_reader) {
    return nullptr;
  }

  auto cookies = MakeCookies(state);
  DocIterator::ptr src =
    state.reader->Iterator(IndexFeatures::None, cookies, WandContext{},
                           /*min_match=*/1, ScoreMergeType::Noop);
  if (!src) {
    return nullptr;
  }

  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (inner) {
    auto inner_it = inner->Execute(ctx, stats);
    if (!inner_it) {
      return nullptr;
    }
    ScoreAdapters itrs;
    itrs.reserve(2);
    itrs.emplace_back(std::move(src));
    itrs.emplace_back(std::move(inner_it));
    src = MakeConjunction(ScoreMergeType::Noop, WandContext{}, docs_count,
                          std::move(itrs));
    if (!src) {
      return nullptr;
    }
  }

  const auto d = static_cast<uint32_t>(state.vector_column->ArraySize());
  return memory::make_managed<RawVectorIterator>(
    std::move(src), *state.vector_column, *col_reader, d, query, metric, boost,
    state.estimation);
}

}  // namespace

DocIterator::ptr KnnVectorQuery::Execute(const ExecutionContext& ctx,
                                         const StatsBuffer& stats) const {
  if (_state.cookies.empty()) {
    return DocIterator::empty();
  }
  SDB_ASSERT(_state.reader);
  SDB_ASSERT(_state.vector_column);

  const std::span<const float> query{_query};
  const auto docs_count = static_cast<doc_id_t>(_segment.docs_count());

  if (_state.quant != VectorQuantization::None) {
    SDB_ASSERT(_state.pay_starts.size() == _state.cookies.size());
    SDB_ASSERT(_state.cluster_counts.size() == _state.cookies.size());

    ScoreAdapters children;
    children.reserve(_state.cookies.size());
    bool ok = true;
    for (size_t c = 0; c < _state.cookies.size(); ++c) {
      auto pay_in = _state.reader->ReopenPayload();
      auto vr =
        pay_in ? MakeQuantizerReader(_state.quant, std::move(pay_in), _state.d)
               : nullptr;
      if (!vr) {
        ok = false;
        break;
      }
      vr->SetQuery(query, _metric);
      vr->StartCluster(_state.pay_starts[c], _state.cluster_counts[c]);

      const PostingCookie cookie{.cookie = _state.cookies[c].get(),
                                 .field = _state.reader->meta()};
      auto postings = _state.reader->Iterator(IndexFeatures::None, cookie);
      if (!postings) {
        continue;
      }

      children.emplace_back(memory::make_managed<QVectorIterator>(
        std::move(postings), std::move(vr), _boost, _state.cluster_counts[c]));
    }
    if (ok && !children.empty()) {
      using Disjunction =
        DisjunctionIterator<ScoreAdapter, ScoreMergeType::Sum>;
      auto v = MakeDisjunction<Disjunction>(WandContext{}, docs_count,
                                            std::move(children));
      if (!_inner) {
        return v;
      }
      auto inner_it = _inner->Execute(ctx, stats);
      if (!inner_it) {
        return DocIterator::empty();
      }
      ScoreAdapters itrs;
      itrs.reserve(2);
      itrs.emplace_back(std::move(v));
      itrs.emplace_back(
        memory::make_managed<FilterIterator>(std::move(inner_it)));
      return MakeConjunction(ScoreMergeType::Sum, WandContext{}, docs_count,
                             std::move(itrs));
    }
  }

  auto it = MakeRawReranker(_segment, _state, query, _metric, _boost,
                            _inner.get(), ctx, stats);
  return it ? DocIterator::ptr{std::move(it)} : DocIterator::empty();
}

DocIterator::ptr RangeVectorQuery::Execute(const ExecutionContext& ctx,
                                           const StatsBuffer& stats) const {
  if (_state.cookies.empty()) {
    return DocIterator::empty();
  }
  SDB_ASSERT(_state.reader);
  SDB_ASSERT(_state.vector_column);

  auto it = MakeRawReranker(_segment, _state, std::span<const float>{_query},
                            _metric, _boost, _inner.get(), ctx, stats);
  if (!it) {
    return DocIterator::empty();
  }
  return memory::make_managed<VectorRangeIterator>(std::move(it), _metric,
                                                   _radius, _inclusive);
}

}  // namespace irs
