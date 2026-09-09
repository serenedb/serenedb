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

#include "iresearch/search/lead/make_boolean.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::lead {

Node::ptr MakeConjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  if (terms.empty() && filters.empty()) {
    return MakeAllDocs(segment);
  }
  if (terms.size() + filters.size() == 1) {
    return search::HeadIsTerm(terms, filters)
             ? LeadOf(terms.front(), nullptr, segment)
             : filters.front()->PlanLead({});
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

Node::ptr MakeDisjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (auto folded =
        MakeBitsetDocs({.should = terms, .should_filters = filters}, segment)) {
    return folded;
  }
  return MakeWindowDisjunctionDocs(terms, doc, rest);
}

Node::ptr MakeThresholdDocs(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader&, uint32_t min_match) {
  SDB_ASSERT(min_match > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest) ||
      terms.size() + rest.size() < min_match) {
    return {};
  }
  return MakeWindowThresholdDocs(terms, doc, rest, min_match);
}

Node::ptr MakeRequiredDocs(std::span<const PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match,
                           const SubReader& segment) {
  if (min_should_match == 0) {
    return MakeConjunctionDocs(must, must_filters, segment);
  }
  if (must.empty() && must_filters.empty()) {
    return min_should_match == 1
             ? MakeDisjunctionDocs(should, should_filters, segment)
             : MakeThresholdDocs(should, should_filters, segment,
                                 min_should_match);
  }
  auto other = probe::BuildOptionalProbe(
    should, should_filters, min_should_match, segment,
    search::IncludeCandidates(must, must_filters, segment));
  if (!other) {
    return {};
  }
  return MakeSparseConjunctionWithDocs(must, must_filters, segment,
                                       std::move(other));
}

Node::ptr MakeExclusionDocs(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            uint32_t min_should_match,
                            std::span<const PostingClause> excludes,
                            std::span<const QueryBuilder::ptr> exclude_filters,
                            const SubReader& segment) {
  SDB_ASSERT(!excludes.empty() || !exclude_filters.empty());
  const auto candidates = IncludeCandidates(must, must_filters, segment);
  if (min_should_match != 0 || (must.empty() && must_filters.empty())) {
    auto include = MakeRequiredDocs(must, must_filters, should, should_filters,
                                    min_should_match, segment);
    if (!include) {
      return {};
    }
    return MakeSparseExclusionOfDocs(std::move(include), excludes,
                                     exclude_filters, segment, candidates);
  }
  if (auto folded = MakeBitsetDocs({.must = must,
                                    .must_filters = must_filters,
                                    .must_not = excludes,
                                    .must_not_filters = exclude_filters},
                                   segment)) {
    return folded;
  }
  if (auto windowed = MakeWindowExclusionDocs(
        must, must_filters, excludes, exclude_filters, segment, candidates)) {
    return windowed;
  }
  return MakeSparseExclusionDocs(must, must_filters, excludes, exclude_filters,
                                 segment, candidates);
}

Node::ptr Make(const BooleanQuery& query) {
  const auto& segment = query.Segment();
  const auto excludes = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (excludes.empty() && exclude_filters.empty()) {
    return MakeRequiredDocs(must, must_filters, should, should_filters,
                            min_should_match, segment);
  }
  return MakeExclusionDocs(must, must_filters, should, should_filters,
                           min_should_match, excludes, exclude_filters,
                           segment);
}

}  // namespace irs::lead
