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

#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/bitset_build.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_groups.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/exclusion_of.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/utils/down_cast.hpp"

namespace irs::detail {

inline bool ClauseTerms(const QueryBuilder& child,
                        std::vector<PostingClause>& terms) {
  if (child.Kind() != QueryKind::Terms) {
    return false;
  }
  const auto& state = irs::utils::downCast<MultiTermQuery>(child).State();
  SDB_ASSERT(!state.Empty());
  const auto* const field = state.Reader();
  if (field == nullptr || DocOf(*field) == nullptr) {
    return false;
  }
  terms.reserve(state.TermsSize());
  for (const auto& entry : state.Terms()) {
    SDB_ASSERT(entry.cookie.docs_count != 0);
    terms.emplace_back(TermState{field, entry.cookie});
  }
  return true;
}

template<typename Term>
bool CollectConjunctionBuckets(std::span<const Term> terms,
                               std::span<const QueryBuilder::ptr> filters,
                               const TermReader* field, BitsetBuckets& out) {
  out.must.reserve(terms.size() + filters.size());
  return VisitOrderedOf(
    terms, filters, true, 0, std::numeric_limits<size_t>::max(),
    [&](const Term& term) {
      out.must.emplace_back().emplace_back(ClauseOf(term, field));
      return true;
    },
    [&](const QueryBuilder& child) {
      return ClauseTerms(child, out.must.emplace_back());
    });
}

template<typename Result, typename Term>
Result MakeConjunctionBitset(std::span<const Term> terms,
                             std::span<const QueryBuilder::ptr> filters,
                             const TermReader* field, const SubReader& segment,
                             TableFilter* table) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const auto* const doc = SegmentDoc(segment);
  if (doc == nullptr) {
    return {};
  }
  BitsetBuckets buckets;
  if (!CollectConjunctionBuckets(terms, filters, field, buckets)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (!TakeConjunctionFold(buckets, *doc, docs_count,
                           HeadEstimate(terms, filters))) {
    return {};
  }
  return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
}

struct ExcludeFills {
  std::vector<const QueryBuilder*> children;
  double cost = 0;
};

template<typename Term>
void CollectExcludeBuckets(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const TermReader* field, doc_id_t docs_count,
                           BitsetBuckets& out, ExcludeFills& fills) {
  out.must_not.reserve(out.must_not.size() + terms.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    out.must_not.emplace_back(ClauseOf(terms[i], field));
  }
  for (const auto& child : filters) {
    SDB_ASSERT(child);
    const auto clause = ChildClauseCost(*child, docs_count);
    fills.cost +=
      clause.fill +
      static_cast<double>(SegmentWindows(docs_count)) *
        (clause.nested ? kEagerNestedWindowCost : kEagerChildWindowCost);
    fills.children.emplace_back(child.get());
  }
}

inline bool PlanExcludeFills(const ExcludeFills& fills, BitsetBuckets& out) {
  out.exclude_fills.reserve(fills.children.size());
  for (const auto* child : fills.children) {
    auto node = child->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return false;
    }
    out.exclude_fills.emplace_back(std::move(node));
  }
  return true;
}

inline bool TakeExclusionFold(const BitsetBuckets& buckets,
                              const ExcludeFills& fills, const FoldEmit& emit,
                              const ExcludeCosts<PostingClause>& exclusion,
                              const IndexInput& doc, doc_id_t docs_count,
                              uint64_t candidates) noexcept {
  if (fills.children.empty() && !buckets.NeedsSet() &&
      !buckets.DenseLead(docs_count)) {
    return false;
  }
  const auto words = static_cast<double>(SegmentWords(docs_count));
  const auto docs = static_cast<double>(candidates);
  const bool single =
    buckets.must.size() == 1 && buckets.must.front().size() == 1;
  double cost = words * kFoldWordCost;
  if (single) {
    cost +=
      PostingBuildCost(buckets.must.front().front().state.cookie.docs_count,
                       docs_count, kSeedBuildCost);
  } else {
    cost += static_cast<double>(FoldConjunctionCost(
      buckets.must, 0, buckets.Seed(docs_count), docs_count));
  }
  for (const auto& clause : buckets.must_not) {
    cost +=
      PostingBuildCost(clause.state.cookie.docs_count, docs_count, kClearCost);
  }
  cost += fills.cost + emit.word * words +
          (emit.docs ? DocsEmitCost(docs, words) : 0.0);
  double walk =
    static_cast<double>(WalkConjunctionCost(buckets.must, 0, candidates)) +
    exclusion.Sparse(emit.sparse_lead, true);
  if (single) {
    walk = std::min(walk, exclusion.WindowLead(docs, emit.docs, true, true));
  }
  size_t terms = buckets.must_not.size();
  for (const auto& clause : buckets.must) {
    terms += clause.size();
  }
  return TakeFold(cost < walk, terms, doc, docs_count);
}

template<typename Result>
Result MakeBooleanBitset(const BooleanGroups& groups, const SubReader& segment,
                         TableFilter* table) {
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (groups.must.empty() && groups.must_filters.empty()) {
    if (groups.should.empty() || !groups.must_not.empty() ||
        !groups.must_not_filters.empty() ||
        (!groups.should_filters.empty() && groups.should_fills == nullptr)) {
      return {};
    }
    const auto* const doc = DocOf(FieldOf(groups.should.front(), nullptr));
    if (doc == nullptr ||
        !TakeBitset<Result>(groups.should, *doc, docs_count)) {
      return {};
    }
    auto buckets = DisjunctionBuckets(groups.should, nullptr);
    if (groups.should_fills != nullptr) {
      buckets.fills = std::move(*groups.should_fills);
    }
    return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
  }
  if (!groups.should.empty() || !groups.should_filters.empty()) {
    return {};
  }
  SDB_ASSERT(groups.must.size() + groups.must_filters.size() > 1 ||
             !groups.must_not.empty() || !groups.must_not_filters.empty());
  const auto* const doc = SegmentDoc(segment);
  if (doc == nullptr) {
    return {};
  }
  BitsetBuckets buckets;
  if (!CollectConjunctionBuckets(groups.must, groups.must_filters, nullptr,
                                 buckets)) {
    return {};
  }
  if (groups.must_not.empty() && groups.must_not_filters.empty()) {
    if (!TakeConjunctionFold(buckets, *doc, docs_count,
                             HeadEstimate(groups.must, groups.must_filters))) {
      return {};
    }
    return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
  }
  const auto candidates =
    IncludeCandidates(groups.must, groups.must_filters, segment);
  ExcludeFills fills;
  CollectExcludeBuckets(groups.must_not, groups.must_not_filters, nullptr,
                        docs_count, buckets, fills);
  const ExcludeCosts<PostingClause> exclusion{
    groups.must_not, groups.must_not_filters, candidates, candidates,
    docs_count,      ExcludeUse::PerDoc};
  if (!TakeExclusionFold(buckets, fills, kFoldEmit<Result>, exclusion, *doc,
                         docs_count, candidates) ||
      !PlanExcludeFills(fills, buckets)) {
    return {};
  }
  return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
}

}  // namespace irs::detail
