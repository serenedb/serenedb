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

#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/live_count.hpp"
#include "iresearch/search/count/make.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/with_mask.hpp"
#include "iresearch/search/queries/boolean_query.hpp"

namespace irs::count {
namespace {

bool FewRuns(const detail::PostingClause& term,
             const SubReader& segment) noexcept {
  const auto* mask = segment.docs_mask();
  uint64_t runs = mask != nullptr ? mask->RunsBound() : 0;
  if (segment.Meta().visible_end < LiveEnd(segment)) {
    ++runs;
  }
  return runs * doc_limits::kBlockSize <
         uint64_t{kDenseRuns} * detail::CookieOf(term).docs_count;
}

}  // namespace

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const bool no_must = must_terms.empty() && must_filters.empty();
  if (ctx.table == nullptr && must_terms.size() == 1 && must_filters.empty() &&
      should_terms.empty() && should_filters.empty() &&
      detail::OnlyMask(query.Terms(Occur::MustNot),
                       query.Queries(Occur::MustNot))) {
    const auto& term = must_terms.front();
    if (FewRuns(term, segment)) {
      if (auto live = MakeLiveTerm(term, segment)) {
        return live;
      }
    }
    if (auto folded = detail::builder::MakeBitset<Api>(
          {.must = must_terms,
           .must_not_filters = query.Queries(Occur::MustNot)},
          segment, ctx)) {
      return folded;
    }
    if (auto live = MakeLiveTerm(term, segment)) {
      return live;
    }
  }
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
