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

#include "iresearch/search/count/make_boolean.hpp"

#include <span>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/count/boolean_sparse.hpp"
#include "iresearch/search/count/subtract.hpp"

namespace irs::count {

Root::ptr Api::MakeNegation(
  std::span<const detail::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  uint64_t candidates, const Context& ctx) {
  if (ctx.table != nullptr) {
    return detail::builder::MakeSparseNegation<Api>(
      exclude_terms, exclude_filters, segment, candidates, ctx);
  }
  Root::ptr excluded;
  if (exclude_terms.size() + exclude_filters.size() == 1) {
    excluded = exclude_terms.empty()
                 ? exclude_filters.front()->PlanCount(ctx)
                 : count::MakeTerm(exclude_terms.front(), segment, ctx);
  } else {
    if (exclude_filters.empty() && SubtractsPair(exclude_terms)) {
      excluded = MakeSubtractDisjunction(exclude_terms.front(),
                                         exclude_terms.back(), segment, ctx);
    }
    if (!excluded) {
      excluded = detail::builder::MakeDisjunction<Api>(
        exclude_terms, exclude_filters, segment, ctx);
    }
  }
  if (!excluded) {
    return {};
  }
  return memory::make_managed<Subtract>(segment.docs_count(),
                                        std::move(excluded));
}

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const bool no_must = must_terms.empty() && must_filters.empty();
  if (query.Terms(Occur::MustNot).empty() &&
      query.Queries(Occur::MustNot).empty()) {
    if (ctx.table == nullptr && should_terms.empty() &&
        should_filters.empty() && must_filters.empty() &&
        must_terms.size() == 2 &&
        detail::SubtractsConjunction(
          RarestOf(must_terms), static_cast<doc_id_t>(segment.docs_count()))) {
      if (auto subtracted =
            MakeSubtractConjunction(must_terms, must_filters, segment, ctx)) {
        return subtracted;
      }
    } else if (ctx.table == nullptr && no_must && query.MinShouldMatch() == 1 &&
               should_filters.empty() && SubtractsPair(should_terms)) {
      if (auto subtracted = MakeSubtractDisjunction(
            should_terms.front(), should_terms.back(), segment, ctx)) {
        return subtracted;
      }
    }
  }
  return detail::builder::Make<Api>(query, ctx);
}

}  // namespace irs::count
