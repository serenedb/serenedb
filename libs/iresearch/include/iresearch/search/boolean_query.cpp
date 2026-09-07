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

#include "boolean_query.hpp"

#include <algorithm>
#include <array>
#include <iterator>
#include <utility>

#include "basics/down_cast.h"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/estimate.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs {
namespace {

bool SamePosting(const PostingMeta& l, const PostingMeta& r) noexcept {
  return l.doc_start == r.doc_start && l.docs_count > 1 && r.docs_count > 1;
}

bool SameStats(const search::StatsRecord& lhs,
               const search::StatsRecord& rhs) noexcept {
  return lhs.stats == rhs.stats && lhs.scorer == rhs.scorer;
}

template<typename Clause>
void Fold(Clause& lhs, const Clause& rhs, ScoreMergeType merge) noexcept {
  MergeBoost(lhs.boost, rhs.boost, merge);
}

template<typename Clauses, typename Same>
bool Unique(Clauses& clauses, Same same, ScoreMergeType merge) {
  if (clauses.size() < 2) {
    return false;
  }
  size_t kept = 0;
  for (size_t i = 1; i != clauses.size(); ++i) {
    if (same(clauses[kept], clauses[i])) {
      Fold(clauses[kept], clauses[i], merge);
      continue;
    }
    clauses[++kept] = std::move(clauses[i]);
  }
  const auto removed = clauses.size() != kept + 1;
  clauses.erase(clauses.begin() + static_cast<ptrdiff_t>(kept + 1),
                clauses.end());
  return removed;
}

score_t AbsorbedOf(const BooleanQuery::Clauses& clauses,
                   const SubReader& segment, ScoreMergeType merge) {
  score_t out = 0;
  bool any = false;
  for (const auto occur : {Occur::Must, Occur::Should}) {
    for (const auto& clause : clauses[OccurIndex(occur)].all_docs) {
      const auto& record = clause.stats;
      if (record.stats == nullptr) {
        continue;
      }
      const auto one = search::AllDocsScore(segment, search::ScoreArgs{
                                                       .scorer = record.scorer,
                                                       .stats = record.stats,
                                                       .boost = clause.boost,
                                                     });
      if (!any) {
        any = true;
        out = one;
        continue;
      }
      MergeBoost(out, one, merge);
    }
  }
  return out;
}

void Classify(BooleanQuery::PreparedBucket& bucket) {
  bool all_per_doc = true;
  bool any_per_doc = false;
  bool bounded = true;
  for (const auto& clause : bucket.postings) {
    const auto* const field = clause.state.reader;
    const auto& stats = clause.stats;
    const bool per_doc =
      field != nullptr && search::FreqOf(*field) && ScoresPerDoc(stats.scorer);
    if (stats.stats == nullptr) {
      bounded = false;
      continue;
    }
    all_per_doc = all_per_doc && per_doc;
    any_per_doc = any_per_doc || per_doc;
    bounded = bounded && per_doc && search::BoundsOf(*field) &&
              HasScoreBounds(stats.scorer);
  }
  bucket.uniformity = bounded        ? search::Terms::Bounded
                      : all_per_doc  ? search::Terms::Scored
                      : !any_per_doc ? search::Terms::Constant
                                     : search::Terms::Mixed;
}

template<typename To, typename From>
void Append(To& to, From& from) {
  to.insert(to.end(), std::make_move_iterator(from.begin()),
            std::make_move_iterator(from.end()));
  from.clear();
}

}  // namespace

void BooleanBuilder::Push(BooleanQuery::PreparedBucket& bucket,
                          const TermReader* reader, const PostingMeta& meta,
                          score_t boost, search::StatsRecord stats) {
  SDB_ASSERT(meta.docs_count != 0);
  bucket.postings.emplace_back(TermState{reader, meta}, boost, stats);
}

bool BooleanBuilder::Absorb(QueryKind kind, Occur occur) {
  switch (kind) {
    case QueryKind::Empty:
      _empty = occur == Occur::Must;
      return true;
    case QueryKind::All:
      _empty = occur == Occur::MustNot;
      return true;
    default:
      return false;
  }
}

void BooleanBuilder::AddTerm(const TermReader* reader, const PostingMeta& meta,
                             score_t boost, Occur occur,
                             search::StatsRecord stats) {
  if (_empty) {
    return;
  }
  auto& bucket = _clauses[OccurIndex(occur)];
  const auto docs = _segment.docs_count();
  const auto keeps = KeepsTerms();
  const auto kind = meta.docs_count == 0                ? QueryKind::Empty
                    : meta.docs_count == docs && !keeps ? QueryKind::All
                                                        : QueryKind::Term;
  if (Absorb(kind, occur)) {
    if (kind == QueryKind::All && !_empty) {
      bucket.all_docs.emplace_back(kNoBoost, search::StatsRecord{});
    }
    return;
  }
  Push(bucket, reader, meta, boost, stats);
}

void BooleanBuilder::Drop(QueryBuilder::ptr query) {
  if (_collector != nullptr && query != nullptr) {
    _collector->Retain(std::move(query));
  }
}

QueryBuilder::ptr BooleanBuilder::DropAll() {
  if (_collector != nullptr) {
    for (auto& bucket : _clauses) {
      for (auto& filter : bucket.filters) {
        _collector->Retain(std::move(filter));
      }
    }
  }
  return QueryBuilder::Empty();
}

void BooleanBuilder::Add(QueryBuilder::ptr query, Occur occur) {
  if (_empty) {
    Drop(std::move(query));
    return;
  }
  auto& bucket = _clauses[OccurIndex(occur)];
  const auto kind = query ? query->Kind() : QueryKind::Empty;
  if (Absorb(kind, occur)) {
    if (kind == QueryKind::All && !_empty) {
      bucket.all_docs.emplace_back(query->Boost(), query->Stats());
    }
    return;
  }
  if (kind == QueryKind::Term) {
    const auto& term = sdb::basics::downCast<TermQuery>(*query);
    const auto& state = term.State();
    Push(bucket, state.reader, state.cookie, query->Boost(), term.Stats());
    return;
  }
  if (kind == QueryKind::Terms && SplicesTerms(occur)) {
    const auto& multi = sdb::basics::downCast<MultiTermQuery>(*query);
    const auto& state = multi.State();
    const auto boost = multi.Boost();
    if (occur == Occur::MustNot) {
      for (const auto& entry : state.Terms()) {
        Push(bucket, state.Reader(), entry.cookie, kNoBoost, {});
      }
      return;
    }
    if (multi.Pinned()) {
      bucket.filters.emplace_back(std::move(query));
      return;
    }
    const auto* const scorer = multi.Stats().scorer;
    for (const auto& entry : state.Terms()) {
      Push(bucket, state.Reader(), entry.cookie, boost * entry.boost,
           search::StatsRecord{entry.stats, entry.stats ? scorer : nullptr});
    }
    return;
  }
  bucket.filters.emplace_back(std::move(query));
}

const BooleanQuery* BooleanBuilder::Dissolves(const QueryBuilder& child,
                                              Occur occur) const noexcept {
  if (child.Kind() != QueryKind::Boolean) {
    return nullptr;
  }
  const auto& nested = sdb::basics::downCast<BooleanQuery>(child);
  if (occur != Occur::MustNot && nested.MergeType() != _merge_type) {
    return nullptr;
  }
  const auto nested_msm = nested.DeclaredMinShouldMatch();
  const bool only_should =
    nested.Bucket(Occur::Must).empty() && nested.Bucket(Occur::MustNot).empty();
  switch (occur) {
    case Occur::Must:
      return nested_msm == 0 && nested.Bucket(Occur::Should).empty() ? &nested
                                                                     : nullptr;
    case Occur::Should:
      return _msm == 1 && nested_msm == 1 && only_should ? &nested : nullptr;
    case Occur::MustNot:
      return nested_msm == 1 && only_should ? &nested : nullptr;
  }
  SDB_UNREACHABLE();
}

void BooleanBuilder::FlattenAll() {
  for (bool changed = true; changed && !_empty;) {
    changed = false;
    for (const auto occur : kAllOccur) {
      auto& filters = _clauses[OccurIndex(occur)].filters;
      for (size_t i = 0; i != filters.size(); ++i) {
        if (!filters[i] || !Dissolves(*filters[i], occur)) {
          continue;
        }
        const auto child = std::move(filters[i]);
        Merge(sdb::basics::downCast<BooleanQuery>(*child), occur);
        changed = true;
      }
      std::erase_if(filters,
                    [](const QueryBuilder::ptr& child) { return !child; });
    }
  }
}

void BooleanBuilder::MergeBucket(BooleanQuery::PreparedBucket& from, Occur to,
                                 score_t boost) {
  auto& bucket = _clauses[OccurIndex(to)];
  const auto fold = [boost](auto& clauses) {
    for (auto& clause : clauses) {
      clause.boost *= boost;
    }
  };
  fold(from.postings);
  fold(from.all_docs);
  Append(bucket.postings, from.postings);
  Append(bucket.all_docs, from.all_docs);
  for (auto& clause : from.filters) {
    if (clause && boost != kNoBoost && !QueryBuilder::IsEmpty(*clause)) {
      auto& child = const_cast<QueryBuilder&>(*clause);
      child.SetBoost(child.Boost() * boost);
    }
    Add(std::move(clause), to);
  }
  from.filters.clear();
}

void BooleanBuilder::Merge(const BooleanQuery& nested, Occur to) {
  const auto boost = nested.Boost();
  switch (to) {
    case Occur::Must:
      MergeBucket(BooleanQuery::Steal(nested, Occur::Must), Occur::Must, boost);
      MergeBucket(BooleanQuery::Steal(nested, Occur::MustNot), Occur::MustNot,
                  kNoBoost);
      return;
    case Occur::Should:
      MergeBucket(BooleanQuery::Steal(nested, Occur::Should), to, boost);
      return;
    case Occur::MustNot:
      MergeBucket(BooleanQuery::Steal(nested, Occur::Should), to, kNoBoost);
      return;
  }
  SDB_UNREACHABLE();
}

void BooleanBuilder::Order(BooleanQuery::PreparedBucket& bucket,
                           bool ascending) {
  absl::c_sort(bucket.postings, [=](const auto& l, const auto& r) {
    auto& lhs = l.state.cookie;
    auto& rhs = r.state.cookie;
    if (lhs.docs_count != rhs.docs_count) {
      return ascending ? lhs.docs_count < rhs.docs_count
                       : lhs.docs_count > rhs.docs_count;
    }
    return ascending ? lhs.doc_start < rhs.doc_start
                     : lhs.doc_start > rhs.doc_start;
  });
  absl::c_sort(bucket.filters, [=](const auto& l, const auto& r) {
    return ascending ? l->EstimateMax() < r->EstimateMax()
                     : l->EstimateMax() > r->EstimateMax();
  });
}

bool BooleanBuilder::Dedup(uint32_t msm) {
  bool changed = false;
  for (const auto occur : kAllOccur) {
    if (occur == Occur::Should && msm > 1) {
      continue;
    }
    auto& bucket = _clauses[OccurIndex(occur)];
    const auto merge =
      occur == Occur::MustNot ? ScoreMergeType::Noop : _merge_type;
    changed |= Unique(
      bucket.postings,
      [](const auto& l, const auto& r) {
        return SamePosting(l.state.cookie, r.state.cookie) &&
               SameStats(l.stats, r.stats);
      },
      merge);
  }
  return changed;
}

uint32_t BooleanBuilder::MinEstimate(const BooleanQuery::PreparedBucket& bucket,
                                     uint32_t docs) const noexcept {
  auto least = bucket.postings.empty()
                 ? docs
                 : bucket.postings.front().state.cookie.docs_count;
  if (!bucket.filters.empty()) {
    least = std::min(least, bucket.filters.front()->EstimateMax());
  }
  return least;
}

uint32_t BooleanBuilder::MaxEstimate(
  const BooleanQuery::PreparedBucket& bucket) const noexcept {
  uint32_t widest = bucket.postings.empty()
                      ? 0
                      : bucket.postings.front().state.cookie.docs_count;
  if (!bucket.filters.empty()) {
    widest = std::max(widest, bucket.filters.front()->EstimateMax());
  }
  return widest;
}

uint64_t BooleanBuilder::SumEstimate(
  const BooleanQuery::PreparedBucket& bucket) const noexcept {
  uint64_t sum = 0;
  for (const auto& clause : bucket.postings) {
    sum += clause.state.cookie.docs_count;
  }
  for (const auto& child : bucket.filters) {
    sum += child->EstimateMax();
  }
  return sum;
}

QueryBuilder::ptr BooleanBuilder::Finish() {
  FlattenAll();
  auto& must = _clauses[OccurIndex(Occur::Must)];
  auto& should = _clauses[OccurIndex(Occur::Should)];
  auto& must_not = _clauses[OccurIndex(Occur::MustNot)];
  if (_empty || should.size() < _msm) {
    return DropAll();
  }
  auto msm = _msm;
  if (msm != 0 && msm == should.size()) {
    Append(must.postings, should.postings);
    Append(must.all_docs, should.all_docs);
    Append(must.filters, should.filters);
    _msm = msm = 0;
    FlattenAll();
    if (_empty) {
      return DropAll();
    }
  }
  Order(must, true);
  Order(should, false);
  Order(must_not, false);

  if (Dedup(msm) && _empty) {
    return DropAll();
  }

  if (must_not.empty() && must.all_docs.empty() && should.all_docs.empty()) {
    const auto lone =
      [&](BooleanQuery::PreparedBucket& bucket) -> QueryBuilder::ptr {
      if (bucket.postings.empty() && bucket.filters.size() == 1) {
        return std::move(bucket.filters.front());
      }
      if (bucket.filters.empty() && bucket.postings.size() == 1) {
        const auto& clause = bucket.postings.front();
        return MakeTermQuery(_memory, _segment, clause.state.reader,
                             clause.state.cookie, clause.boost, clause.stats);
      }
      return nullptr;
    };
    if (msm == 0 && should.empty()) {
      if (must.empty()) {
        auto all = memory::make_tracked<AllQuery>(_memory, _segment, _boost);
        all->SetStats(_collector != nullptr ? _collector->Record()
                                            : search::StatsRecord{});
        return all;
      }
      if (auto only = lone(must)) {
        return only;
      }
    } else if (msm == 1 && must.empty()) {
      if (auto only = lone(should)) {
        return only;
      }
    }
  }

  for (const auto occur : kAllOccur) {
    Classify(_clauses[OccurIndex(occur)]);
  }

  const auto matching_must =
    static_cast<uint32_t>(must.postings.size() + must.filters.size());
  const auto matching_should =
    static_cast<uint32_t>(should.postings.size() + should.filters.size());
  const auto matching_msm =
    msm - std::min<uint32_t>(
            msm, static_cast<uint32_t>(should.size()) - matching_should);

  const auto docs = static_cast<uint32_t>(_segment.docs_count());

  const auto threshold_estimate = [&] {
    return ClampEstimate(SumEstimate(should) / matching_msm, _segment);
  };

  uint32_t estimate = docs;
  if (matching_must != 0) {
    estimate = MinEstimate(must, docs);
    if (matching_msm != 0) {
      estimate = std::min(estimate, threshold_estimate());
    }
  } else if (matching_msm != 0) {
    estimate = threshold_estimate();
  } else if (!must_not.empty()) {
    if (must.all_docs.empty() && should.all_docs.empty()) {
      return DropAll();
    }
    estimate = docs - std::min(docs, MaxEstimate(must_not));
  }
  auto query = memory::make_tracked<BooleanQuery>(
    _memory, _segment, std::move(_clauses), msm, matching_msm, _boost, estimate,
    _merge_type);
  query->SetStats(_collector != nullptr ? _collector->Record()
                                        : search::StatsRecord{});
  return query;
}

score_t BooleanQuery::Absorbed() const noexcept {
  return AbsorbedOf(_clauses, _segment, _merge_type);
}

void BooleanQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  if (!visitor.Visit(*this, boost)) {
    return;
  }
  for (const auto occur : {Occur::Must, Occur::Should}) {
    const auto& bucket = _clauses[OccurIndex(occur)];
    for (const auto& clause : bucket.postings) {
      if (!visitor.Visit(clause.state, boost * clause.boost)) {
        return;
      }
    }
    for (const auto& child : bucket.filters) {
      child->Visit(visitor, boost);
    }
  }
}

}  // namespace irs
