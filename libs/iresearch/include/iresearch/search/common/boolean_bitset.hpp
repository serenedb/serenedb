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

#include "basics/down_cast.h"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/multiterm_query.hpp"

namespace irs::search {

inline bool ClauseTerms(const QueryBuilder& child,
                        std::vector<PostingClause>& terms) {
  if (child.Kind() != QueryKind::Terms) {
    return false;
  }
  const auto& state = sdb::basics::downCast<MultiTermQuery>(child).State();
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

template<typename Term>
bool CollectExcludeBuckets(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const TermReader* field, BitsetBuckets& out,
                           uint64_t& fill_docs) {
  out.must_not.reserve(out.must_not.size() + terms.size());
  for (size_t i = 0; i != terms.size(); ++i) {
    out.must_not.emplace_back(ClauseOf(terms[i], field));
  }
  for (const auto& child : filters) {
    SDB_ASSERT(child);
    if (ClauseTerms(*child, out.must_not)) {
      continue;
    }
    auto node = child->PlanFill({}, ScoreMergeType::Noop);
    if (!node) {
      return false;
    }
    fill_docs += child->EstimateMax();
    out.exclude_fills.emplace_back(std::move(node));
  }
  return true;
}

inline bool TakeExclusionFold(const BitsetBuckets& buckets, uint64_t fill_docs,
                              const IndexInput& doc, doc_id_t docs_count,
                              uint64_t candidates) noexcept {
  if (!buckets.NeedsSet()) {
    return false;
  }
  const auto words = SegmentWords(docs_count);
  auto cost =
    FoldConjunctionCost(buckets.must, 0, buckets.Seed(docs_count), docs_count) +
    FoldReadClause(std::span<const PostingClause>{buckets.must_not}, docs_count,
                   words);
  if (!buckets.exclude_fills.empty()) {
    cost += fill_docs + 2 * words;
  }
  const auto walk =
    WalkConjunctionCost(buckets.must, 0, candidates) +
    candidates * (buckets.must_not.size() + buckets.exclude_fills.size());
  size_t terms = buckets.must_not.size();
  for (const auto& clause : buckets.must) {
    terms += clause.size();
  }
  return TakeFold(walk >= cost, terms, doc, docs_count);
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
  uint64_t fill_docs = 0;
  if (!CollectExcludeBuckets(groups.must_not, groups.must_not_filters, nullptr,
                             buckets, fill_docs)) {
    return {};
  }
  const auto candidates =
    IncludeCandidates(groups.must, groups.must_filters, segment);
  if (!TakeExclusionFold(buckets, fill_docs, *doc, docs_count, candidates)) {
    return {};
  }
  return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
}

}  // namespace irs::search
