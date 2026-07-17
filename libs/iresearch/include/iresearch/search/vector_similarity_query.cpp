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
#include <optional>
#include <span>
#include <tuple>
#include <type_traits>
#include <vector>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/formats/posting/common.hpp"
#include "iresearch/formats/posting/format_block_128.hpp"
#include "iresearch/formats/posting/iterator_doc.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/search/make_disjunction.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attribute_helper.hpp"

namespace irs {
namespace {

class RawVectorReader {
 public:
  RawVectorReader(const ColumnReader& vector_column,
                  const ColReader& col_reader, uint32_t d)
    : _read_ctx{col_reader}, _vreader{vector_column, _read_ctx}, _d{d} {}

  void SetQuery(std::span<const float> query, VectorMetric metric) {
    _query.assign(query.begin(), query.end());
    _dist = ResolveScoringDistance(metric);
  }

  void ComputeDistance(doc_id_t doc, score_t boost, score_t& out) {
    ComputeDistanceRun(doc, 1, boost, std::span<score_t>{&out, 1});
  }

  void ComputeDistanceRun(doc_id_t first_doc, size_t run, score_t boost,
                          std::span<score_t> out) {
    SDB_ASSERT(_dist);
    SDB_ASSERT(out.size() >= run);
    const auto* q = reinterpret_cast<const byte_type*>(_query.data());
    const auto d = static_cast<uint16_t>(_d);
    const float* base = _vreader.ReadDocBatch(first_doc, run);
    for (size_t k = 0; k < run; ++k) {
      out[k] =
        _dist(q, reinterpret_cast<const byte_type*>(base + k * _d), d) * boost;
    }
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
      .field = {},
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

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  RawVectorReader _reader;
};

using QVectorPosting =
  PostingIteratorBase<IteratorTraitsImpl<FormatTraits128, false, false, false>>;

class QVectorIterator : public VectorDistanceIterator {
 public:
  QVectorIterator(DocIterator::ptr&& src, std::unique_ptr<QuantizerReader> qr,
                  score_t boost, CostAttr::Type estimation)
    : VectorDistanceIterator{std::move(src), boost, estimation},
      _qr{std::move(qr)},
      _total{estimation} {
    SDB_ASSERT(_qr);
    _posting = sdb::basics::downCast<QVectorPosting>(_src.get());
  }

  doc_id_t advance() final {
    if (_pos == _len) {
      FillDocsBlock();
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
    _qr->ComputeBlock(_base, 1, &_cur_dist);
    ++_base;
    return _doc = doc;
  }

  std::span<const doc_id_t> GetDocsBlock() const noexcept {
    return std::span{_docs}.subspan(_pos, _len);
  }
  std::span<const float> GetDistBlock() const noexcept {
    return std::span{_dist}.subspan(_pos, _len);
  }

  void AdvanceBlock() {
    FillDocsBlock();
    if (_len == 0) {
      _cur_dist = .0f;
      return;
    }
    FillDistancesBlock();
    _pos = 0;
  }

  void Collect(const ScoreFunction& /*scorer*/, ColumnArgsFetcher& /*fetcher*/,
               ScoreCollector& collector) final {
    for (;;) {
      AdvanceBlock();
      const auto docs = GetDocsBlock();
      if (docs.empty()) {
        break;
      }
      const auto dist = GetDistBlock();
      SDB_ASSERT(docs.size() == dist.size());
      if (_boost == kNoBoost) {
        collector.AddDocs(docs.data(), docs.size(), dist.data());
      } else {
        std::array<score_t, kPostingBlock> boosted;
        for (size_t i = 0; i < dist.size(); ++i) {
          boosted[i] = dist[i] * _boost;
        }
        collector.AddDocs(docs.data(), docs.size(), boosted.data());
      }
    }
    _doc = doc_limits::eof();
  }

  uint32_t count() final { return irs::DocIterator::CountImpl(*this); }

  IRS_DOC_ITERATOR_EMIT_DEFAULTS

  std::pair<doc_id_t, bool> FillBlock(doc_id_t min, doc_id_t max,
                                      uint64_t* mask,
                                      irs::FillBlockScoreContext score,
                                      irs::FillBlockMatchContext match) final {
    return irs::DocIterator::FillBlockImpl(*this, min, max, mask, score, match);
  }

 private:
  void FillDistancesBlock() {
    SDB_ASSERT(_len > 0);
    SDB_ASSERT(_len <= _dist.size());
    _qr->ComputeBlock(_base, _len, _dist.data());
    _base += _len;
  }

  void FillDocsBlock() {
    _docs = _posting->NextLeafBlock();
    _len = static_cast<uint16_t>(_docs.size());
  }

  std::unique_ptr<QuantizerReader> _qr;
  QVectorPosting* _posting = nullptr;
  CostAttr::Type _total;
  std::span<const doc_id_t> _docs;
  std::array<score_t, kPostingBlock> _dist;
  uint32_t _base = 0;
  uint16_t _len = 0;
  uint16_t _pos = 0;
};

template<bool Inclusive>
class VectorRangeIterator : public DocIterator {
 public:
  VectorRangeIterator(memory::managed_ptr<VectorDistanceIterator>&& inner,
                      float radius)
    : _inner{std::move(inner)}, _radius{radius} {
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

  IRS_DOC_ITERATOR_DEFAULTS

 private:
  bool Inside(score_t dist) const noexcept {
    // The scoring distance is "larger = nearer" for every metric, so a doc is
    // within the (scoring-space) radius iff its score clears the threshold.
    bool res = dist > _radius;
    if constexpr (Inclusive) {
      res |= dist == _radius;
    }
    return res;
  }

  memory::managed_ptr<VectorDistanceIterator> _inner;
  float _radius;
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

  IRS_DOC_ITERATOR_DEFAULTS

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

template<typename Primary, typename Inner>
DocIterator::ptr MergeWithInner(Primary&& primary, Inner&& inner,
                                doc_id_t docs_count,
                                ScoreMergeType merge_type) {
  ScoreAdapters itrs;
  itrs.reserve(2);
  itrs.emplace_back(std::forward<Primary>(primary));
  itrs.emplace_back(std::forward<Inner>(inner));
  return MakeConjunction(merge_type, WandContext{}, docs_count,
                         std::move(itrs));
}

struct ClusterInputs {
  DocIterator::ptr postings;
  std::unique_ptr<QuantizerReader> vr;
};

std::optional<ClusterInputs> MakeClusterIterator(const VectorState& state,
                                                 size_t c, bool has_centroids,
                                                 IndexInput& pay_root) {
  const PostingCookie cookie{.cookie = state.cookies[c].get(),
                             .field = state.reader->meta()};
  auto postings = state.reader->Iterator(IndexFeatures::None, cookie);
  if (!postings) {
    return std::nullopt;
  }
  auto vr = MakeQuantizerReader(state.codebook, pay_root.Dup());
  SDB_ASSERT(vr);
  const float* centroid =
    has_centroids ? state.cluster_centroids.data() + c * state.d : nullptr;
  vr->StartCluster(state.pay_starts[c], state.cluster_counts[c], centroid);
  return ClusterInputs{std::move(postings), std::move(vr)};
}

using QVectorIterators = std::vector<memory::managed_ptr<QVectorIterator>>;

template<typename Out>
bool BuildClusterIterators(const VectorState& state, score_t boost, Out& out) {
  auto pay_root = state.reader->ReopenPayload();
  if (!pay_root) {
    return false;
  }
  out.reserve(state.cookies.size());
  const bool has_centroids =
    state.cluster_centroids.size() == state.cookies.size() * state.d;
  for (size_t c = 0; c < state.cookies.size(); ++c) {
    auto ci = MakeClusterIterator(state, c, has_centroids, *pay_root);
    if (!ci) {
      continue;
    }
    auto qit = memory::make_managed<QVectorIterator>(std::move(ci->postings),
                                                     std::move(ci->vr), boost,
                                                     state.cluster_counts[c]);
    if constexpr (std::is_same_v<Out, ScoreAdapters>) {
      out.emplace_back(DocIterator::ptr{std::move(qit)});
    } else {
      out.emplace_back(std::move(qit));
    }
  }
  return true;
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
    src = MergeWithInner(std::move(src), std::move(inner_it), docs_count,
                         ScoreMergeType::Noop);
    if (!src) {
      return nullptr;
    }
  }

  const auto d = static_cast<uint32_t>(state.vector_column->ArraySize());
  return memory::make_managed<RawVectorIterator>(
    std::move(src), *state.vector_column, *col_reader, d, query, metric, boost,
    state.estimation);
}

class DisjointClusterUnion : public DocIterator {
 public:
  DisjointClusterUnion(QVectorIterators&& itrs, doc_id_t docs_count)
    : _itrs{std::move(itrs)}, _attrs{docs_count} {}

  doc_id_t advance() final { SDB_UNREACHABLE(); }
  doc_id_t seek(doc_id_t) final { SDB_UNREACHABLE(); }
  uint32_t count() final { SDB_UNREACHABLE(); }
  uint32_t EmitDocs(doc_id_t*, doc_id_t, doc_id_t) final { SDB_UNREACHABLE(); }
  uint32_t EmitScoredDocs(doc_id_t*, score_t*, doc_id_t, const ScoreFunction&,
                          ColumnArgsFetcher*, doc_id_t) final {
    SDB_UNREACHABLE();
  }
  std::pair<doc_id_t, bool> FillBlock(doc_id_t, doc_id_t, uint64_t*,
                                      FillBlockScoreContext,
                                      FillBlockMatchContext) final {
    SDB_UNREACHABLE();
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == Type<CostAttr>::id()) {
      return &_attrs;
    }
    return nullptr;
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    _scorers.clear();
    _scorers.reserve(_itrs.size());
    for (auto& it : _itrs) {
      _scorers.emplace_back(it->PrepareScore(ctx));
    }
    return ScoreFunction::Default();
  }

  void Collect(const ScoreFunction& /*scorer*/, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    SDB_ASSERT(_scorers.size() == _itrs.size());
    for (size_t i = 0, n = _itrs.size(); i < n; ++i) {
      _itrs[i]->Collect(_scorers[i], fetcher, collector);
    }
  }

 private:
  QVectorIterators _itrs;
  std::vector<ScoreFunction> _scorers;
  CostAttr _attrs;
};

}  // namespace

void RerankExactDistances(const SubReader& segment,
                          const ColumnReader& vector_column, uint32_t d,
                          std::span<const float> query, VectorMetric metric,
                          std::span<ScoreDoc> hits) {
  const auto* col_reader = segment.GetColReader();
  if (!col_reader) {
    return;
  }
  RawVectorReader reader{vector_column, *col_reader, d};
  reader.SetQuery(query, metric);
  std::vector<score_t> scratch;
  size_t i = 0;
  while (i < hits.size()) {
    size_t run = 1;
    while (i + run < hits.size() &&
           hits[i + run].doc == hits[i + run - 1].doc + 1) {
      ++run;
    }
    scratch.resize(run);
    reader.ComputeDistanceRun(hits[i].doc, run, kNoBoost, scratch);
    for (size_t k = 0; k < run; ++k) {
      hits[i + k].score = scratch[k];
    }
    i += run;
  }
}

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
    SDB_ASSERT(_state.codebook);

    if (ctx.top_k_collect && !_inner && _segment.docs_mask() == nullptr) {
      QVectorIterators children;
      if (BuildClusterIterators(_state, _boost, children) &&
          !children.empty()) {
        return memory::make_managed<DisjointClusterUnion>(std::move(children),
                                                          docs_count);
      }
    } else {
      ScoreAdapters children;
      if (BuildClusterIterators(_state, _boost, children) &&
          !children.empty()) {
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
        return MergeWithInner(
          std::move(v),
          memory::make_managed<FilterIterator>(std::move(inner_it)), docs_count,
          ScoreMergeType::Sum);
      }
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
  DocIterator::ptr res;
  // RawVectorReader now yields "larger = nearer" scores, so map the radius into
  // that scoring space: distance metrics (nearest = smallest) get negated.
  const float threshold =
    VectorMetricNearestIsLargest(_metric) ? _radius : -_radius;
  irs::ResolveBool(_inclusive, [&]<bool Inclusive>() {
    auto v_it = memory::make_managed<VectorRangeIterator<Inclusive>>(
      std::move(it), threshold);
    res = std::move(v_it);
  });
  return res;
}

}  // namespace irs
