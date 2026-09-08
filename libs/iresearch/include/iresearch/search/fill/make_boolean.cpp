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

#include "iresearch/search/fill/make_boolean.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::fill {

Node::ptr MakeConjunctionDocs(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  if (terms.empty() && filters.empty()) {
    return MakeAllDocs(segment);
  }
  if (terms.size() + filters.size() == 1) {
    if (search::HeadIsTerm(terms, filters)) {
      return FillOf(terms.front(), nullptr, segment);
    }
    return filters.front()->PlanFill({}, ScoreMergeType::Noop);
  }
  if (auto folded =
        MakeBitsetDocs({.must = terms, .must_filters = filters}, segment)) {
    return folded;
  }
  if (auto windowed = MakeWindowConjunctionDocs(terms, filters, segment)) {
    return windowed;
  }
  return MakeSparseConjunctionDocs(terms, filters, segment);
}

Node::ptr MakeDisjunctionDocs(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<Node::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (auto folded =
        MakeBitsetDocs({.should = terms, .should_filters = filters}, segment)) {
    return folded;
  }
  return MakeWindowDisjunctionDocs(terms, doc, rest);
}

Node::ptr MakeThresholdDocs(std::span<const search::PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader&, uint32_t min_match) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  const IndexInput* doc = nullptr;
  std::vector<Node::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (min_match > search::kBitplaneMaxMatch) {
    if (auto counted = MakeCountThresholdDocs(terms, doc, rest, min_match)) {
      return counted;
    }
  }
  return MakeBitsThresholdDocs(terms, doc, rest, min_match);
}

Node::ptr MakeRequiredDocs(std::span<const search::PostingClause> must_terms,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const search::PostingClause> should_terms,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match,
                           const SubReader& segment) {
  if (min_should_match == 0) {
    return MakeConjunctionDocs(must_terms, must_filters, segment);
  }
  if (must_terms.empty() && must_filters.empty()) {
    return min_should_match == 1
             ? MakeDisjunctionDocs(should_terms, should_filters, segment)
             : MakeThresholdDocs(should_terms, should_filters, segment,
                                 min_should_match);
  }
  auto probe = probe::BuildOptionalProbe(
    should_terms, should_filters, min_should_match, segment,
    search::IncludeCandidates(must_terms, must_filters, segment));
  if (!probe) {
    return {};
  }
  return MakeSparseConjunctionWithDocs(must_terms, must_filters, segment,
                                       std::move(probe));
}

Node::ptr MakeExclusionDocs(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters,
  const SubReader& segment) {
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  if (min_should_match != 0) {
    auto include =
      lead::MakeRequiredDocs(must_terms, must_filters, should_terms,
                             should_filters, min_should_match, segment);
    if (!include) {
      return {};
    }
    return MakeSparseExclusionOfDocs(std::move(include), exclude_terms,
                                     exclude_filters, segment, candidates);
  }
  if (must_terms.empty() && must_filters.empty()) {
    return MakeWindowExclusionDocs(must_terms, must_filters, exclude_terms,
                                   exclude_filters, segment, candidates);
  }
  if (auto folded = MakeBitsetDocs({.must = must_terms,
                                    .must_filters = must_filters,
                                    .must_not = exclude_terms,
                                    .must_not_filters = exclude_filters},
                                   segment)) {
    return folded;
  }
  if (auto windowed =
        MakeWindowExclusionDocs(must_terms, must_filters, exclude_terms,
                                exclude_filters, segment, candidates)) {
    return windowed;
  }
  return MakeSparseExclusionDocs(must_terms, must_filters, exclude_terms,
                                 exclude_filters, segment, candidates);
}

Node::ptr Make(const BooleanQuery& query) {
  const auto& segment = query.Segment();
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (exclude_terms.empty() && exclude_filters.empty()) {
    return MakeRequiredDocs(must_terms, must_filters, should_terms,
                            should_filters, min_should_match, segment);
  }
  return MakeExclusionDocs(must_terms, must_filters, should_terms,
                           should_filters, min_should_match, exclude_terms,
                           exclude_filters, segment);
}

}  // namespace irs::fill
