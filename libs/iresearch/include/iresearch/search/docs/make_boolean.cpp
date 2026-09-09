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

#include "iresearch/search/docs/make_boolean.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::docs {

Root::ptr MakeDisjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (auto folded = MakeBitset(
        {.should = terms, .should_filters = filters, .should_fills = &rest},
        segment, ctx)) {
    return folded;
  }
  return MakeWindowDisjunction(terms, doc, rest, ctx);
}

Root::ptr MakeConjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() == 1) {
    return search::HeadIsTerm(terms, filters)
             ? MakePosting(terms.front(), segment, ctx)
             : filters.front()->PlanDocs(ctx);
  }
  if (auto windowed = MakeWindowConjunction(terms, filters, segment, ctx)) {
    return windowed;
  }
  if (!filters.empty()) {
    if (auto folded =
          MakeBitset({.must = terms, .must_filters = filters}, segment, ctx)) {
      return folded;
    }
  }
  return MakeSparseConjunction(terms, filters, segment, ctx);
}

Root::ptr MakeThreshold(std::span<const search::PostingClause> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const SubReader& segment, uint32_t min_match,
                        const Context& ctx) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  SDB_ASSERT(min_match != terms.size() + filters.size());
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  return MakeWindowThreshold(terms, doc, rest, min_match, ctx);
}

Root::ptr MakeRequired(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto min_match = query.MinShouldMatch();
  const bool no_must = must_terms.empty() && must_filters.empty();
  if (min_match == 0) {
    if (no_must) {
      return MakeAll(static_cast<doc_id_t>(segment.docs_count()), ctx);
    }
    return MakeConjunction(must_terms, must_filters, segment, ctx);
  }
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  if (no_must) {
    return min_match == 1
             ? MakeDisjunction(should_terms, should_filters, segment, ctx)
             : MakeThreshold(should_terms, should_filters, segment, min_match,
                             ctx);
  }
  auto probe = probe::BuildOptionalProbe(
    should_terms, should_filters, min_match, segment,
    search::IncludeCandidates(must_terms, must_filters, segment));
  if (!probe) {
    return {};
  }
  return MakeSparseConjunctionWith(must_terms, must_filters, segment,
                                   std::move(probe), ctx);
}

Root::ptr MakeExclusion(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  const auto min_match = query.MinShouldMatch();
  if (min_match != 0) {
    auto driven = lead::MakeRequiredDocs(
      must_terms, must_filters, query.Terms(Occur::Should),
      query.Queries(Occur::Should), min_match, segment);
    if (!driven) {
      return {};
    }
    return MakeSparseExclusionOf(std::move(driven), exclude_terms,
                                 exclude_filters, segment, candidates, ctx);
  }
  if (must_terms.empty() && must_filters.empty()) {
    return MakeWindowExclusion(must_terms, must_filters, exclude_terms,
                               exclude_filters, segment, candidates, ctx);
  }
  if (auto folded = MakeBitset({.must = must_terms,
                                .must_filters = must_filters,
                                .must_not = exclude_terms,
                                .must_not_filters = exclude_filters},
                               segment, ctx)) {
    return folded;
  }
  if (auto windowed =
        MakeWindowExclusion(must_terms, must_filters, exclude_terms,
                            exclude_filters, segment, candidates, ctx)) {
    return windowed;
  }
  return MakeSparseExclusion(must_terms, must_filters, exclude_terms,
                             exclude_filters, segment, candidates, ctx);
}

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  if (query.Terms(Occur::MustNot).empty() &&
      query.Queries(Occur::MustNot).empty()) {
    return MakeRequired(query, ctx);
  }
  return MakeExclusion(query, ctx);
}

}  // namespace irs::docs
