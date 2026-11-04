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

#include "iresearch/search/count/make.hpp"

#include <span>

#include "iresearch/formats/term_reader.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/boolean_bitset.hpp"
#include "iresearch/search/count/disjoint.hpp"
#include "iresearch/search/count/family.hpp"
#include "iresearch/search/count/subtract.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs::count {
namespace {

Root::ptr PlanTerms(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (ctx.table == nullptr &&
      SubtractsPair(terms,
                    static_cast<doc_id_t>(query.Segment().docs_count()))) {
    if (auto subtracted = MakeSubtractDisjunction(
          detail::ClauseOf(terms.front(), field),
          detail::ClauseOf(terms.back(), field), query.Segment(), ctx)) {
      return subtracted;
    }
  }
  return detail::MakeQuery<Family>(query, ctx);
}

}  // namespace

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeConstant(0);
  }
  if (ctx.fold != nullptr) {
    return memory::make_managed<FoldWindow>(*ctx.fold);
  }
  const detail::FoldReachScope reach{ctx.span};
  return query.PlanCount(ctx);
}

detail::BitsetStorage FoldTerms(const QueryBuilder& query) {
  if (query.Kind() != QueryKind::Terms) {
    return {};
  }
  const auto& terms_query = irs::utils::downCast<MultiTermQuery>(query);
  const auto& state = terms_query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  const auto* const doc = field != nullptr ? detail::DocOf(*field) : nullptr;
  if (doc == nullptr || terms.size() < 2) {
    return {};
  }
  const auto docs_count =
    static_cast<doc_id_t>(terms_query.Segment().docs_count());
  const detail::FoldReachScope whole{0};
  if (!detail::TakeBitset<Root::ptr>(terms, *doc, docs_count)) {
    return {};
  }
  auto buckets = detail::DisjunctionBuckets(terms, field);
  return detail::BuildBitset(buckets, *doc, docs_count);
}

Root::ptr Family::Make(const MultiTermQuery& query, const Context& ctx) {
  const auto* const field = query.State().Reader();
  const auto& segment = query.Segment();
  if (ctx.table == nullptr && field != nullptr && field->single_valued &&
      segment.live_docs_count() == segment.docs_count()) {
    return MakeWholeCount(query.Postings(), ctx,
                          [&query, ctx] { return PlanTerms(query, ctx); });
  }
  return PlanTerms(query, ctx);
}

}  // namespace irs::count
