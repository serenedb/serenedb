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
#include "iresearch/search/common/bitset_build.hpp"
#include "iresearch/search/common/bitset_of.hpp"
#include "iresearch/search/common/clause_terms.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/conjunction_bitset.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"

namespace irs::search {

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

template<typename Result, typename Term, typename Exclude>
Result MakeExclusionBitset(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const TermReader* field,
                           std::span<const Exclude> exclude_terms,
                           std::span<const QueryBuilder::ptr> exclude_filters,
                           const TermReader* exclude_field,
                           const SubReader& segment, uint64_t candidates,
                           TableFilter* table) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto* const doc = SegmentDoc(segment);
  if (doc == nullptr) {
    return {};
  }
  BitsetBuckets buckets;
  if (!CollectConjunctionBuckets(terms, filters, field, buckets)) {
    return {};
  }
  uint64_t fill_docs = 0;
  if (!CollectExcludeBuckets(exclude_terms, exclude_filters, exclude_field,
                             buckets, fill_docs)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (!TakeExclusionFold(buckets, fill_docs, *doc, docs_count, candidates)) {
    return {};
  }
  return MakeBitsetNode<Result>(std::move(buckets), *doc, docs_count, table);
}

}  // namespace irs::search
