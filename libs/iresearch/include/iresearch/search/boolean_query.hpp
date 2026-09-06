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

#pragma once

#include <array>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>

#include "basics/resource_manager.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/states/term_state.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

struct TermReader;

using search::AllDocsClause;
using search::PostingClause;

class BooleanQuery : public QueryBuilderImpl<BooleanQuery> {
 public:
  using Filters = ManagedVector<QueryBuilder::ptr>;

  struct PreparedBucket {
    ManagedVector<PostingClause> postings;
    ManagedVector<AllDocsClause> all_docs;
    Filters filters;
    search::Terms uniformity = search::Terms::Mixed;

    explicit PreparedBucket(IResourceManager& memory)
      : postings{{memory}}, all_docs{{memory}}, filters{{memory}} {}

    size_t size() const noexcept {
      return postings.size() + all_docs.size() + filters.size();
    }
    bool empty() const noexcept { return size() == 0; }
  };

  using Clauses = std::array<PreparedBucket, kNumOccur>;

  BooleanQuery(const SubReader& segment, Clauses&& clauses,
               uint32_t declared_msm, uint32_t min_should_match, score_t boost,
               uint32_t estimate, ScoreMergeType merge_type)
    : QueryBuilderImpl{segment, estimate, QueryKind::Boolean},
      _clauses{std::move(clauses)},
      _declared_msm{declared_msm},
      _min_should_match{min_should_match},
      _boost{boost},
      _merge_type{merge_type} {}

  void Visit(PreparedStateVisitor& visitor, score_t boost) const final;

  score_t Boost() const noexcept final { return _boost; }

  void SetBoost(score_t value) noexcept final { _boost = value; }

  const PreparedBucket& Bucket(Occur occur) const noexcept {
    return _clauses[OccurIndex(occur)];
  }

  std::span<const PostingClause> Terms(Occur occur) const noexcept {
    return _clauses[OccurIndex(occur)].postings;
  }

  std::span<const QueryBuilder::ptr> Queries(Occur occur) const noexcept {
    return _clauses[OccurIndex(occur)].filters;
  }

  search::Terms Uniformity(Occur occur) const noexcept {
    return _clauses[OccurIndex(occur)].uniformity;
  }

  uint32_t MinShouldMatch() const noexcept { return _min_should_match; }

  uint32_t DeclaredMinShouldMatch() const noexcept { return _declared_msm; }

  score_t Absorbed() const noexcept;

  ScoreMergeType MergeType() const noexcept { return _merge_type; }

  template<typename TermCb, typename QueryCb>
  bool VisitHead(Occur occur, TermCb&& term_cb, QueryCb&& query_cb) const {
    const auto& bucket = _clauses[OccurIndex(occur)];
    return search::VisitOrderedOf(
      std::span<const search::PostingClause>{bucket.postings},
      std::span<const QueryBuilder::ptr>{bucket.filters}, occur == Occur::Must,
      0, 1, std::forward<TermCb>(term_cb), std::forward<QueryCb>(query_cb));
  }

  static PreparedBucket& Steal(const BooleanQuery& query,
                               Occur occur) noexcept {
    return const_cast<BooleanQuery&>(query)._clauses[OccurIndex(occur)];
  }

 private:
  Clauses _clauses;
  uint32_t _declared_msm;
  uint32_t _min_should_match;
  score_t _boost;
  ScoreMergeType _merge_type;
};

class BooleanBuilder {
 public:
  BooleanBuilder(const SubReader& segment, IResourceManager& memory,
                 uint32_t min_should_match, score_t boost,
                 ScoreMergeType merge_type,
                 PrepareCollector* collector = nullptr,
                 bool needs_terms = false)
    : _clauses{BooleanQuery::PreparedBucket{memory},
               BooleanQuery::PreparedBucket{memory},
               BooleanQuery::PreparedBucket{memory}},
      _segment{segment},
      _memory{memory},
      _collector{collector},
      _msm{min_should_match},
      _boost{boost},
      _merge_type{merge_type},
      _collects{collector != nullptr},
      _needs_terms{needs_terms} {}

  void Add(QueryBuilder::ptr query, Occur occur);

  void AddTerm(const TermReader* reader, const PostingMeta& meta, score_t boost,
               Occur occur, search::StatsRecord stats);

  QueryBuilder::ptr Finish();

 private:
  void Push(BooleanQuery::PreparedBucket& bucket, const TermReader* reader,
            const PostingMeta& meta, score_t boost, search::StatsRecord stats);

  bool Absorb(QueryKind kind, Occur occur);

  bool KeepsTerms() const noexcept { return _collects || _needs_terms; }

  bool SplicesTerms(Occur occur) const noexcept {
    return occur == Occur::MustNot || (occur == Occur::Should && _msm == 1);
  }

  bool Scores(const QueryBuilder& child, Occur occur) const noexcept {
    return _collects && occur != Occur::MustNot && child.Scores();
  }

  const BooleanQuery* Dissolves(const QueryBuilder& child,
                                Occur occur) const noexcept;

  void FlattenAll();

  void Merge(const BooleanQuery& nested, Occur to);

  void MergeBucket(BooleanQuery::PreparedBucket& from, Occur to, score_t boost);

  void Order(BooleanQuery::PreparedBucket& bucket, bool ascending);

  bool Dedup(uint32_t msm);

  uint32_t MinEstimate(const BooleanQuery::PreparedBucket& bucket,
                       uint32_t docs) const noexcept;
  uint32_t MaxEstimate(
    const BooleanQuery::PreparedBucket& bucket) const noexcept;
  uint64_t SumEstimate(
    const BooleanQuery::PreparedBucket& bucket) const noexcept;

  void Drop(QueryBuilder::ptr query);

  QueryBuilder::ptr DropAll();

  BooleanQuery::Clauses _clauses;
  const SubReader& _segment;
  IResourceManager& _memory;
  PrepareCollector* _collector;
  uint32_t _msm;
  score_t _boost;
  ScoreMergeType _merge_type;
  bool _collects = false;
  bool _needs_terms = false;
  bool _empty = false;
};

}  // namespace irs
