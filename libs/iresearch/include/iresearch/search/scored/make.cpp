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

#include "iresearch/search/scored/make.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/detail/boolean_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/ngram_of.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/scored/detail/walk.hpp"
#include "iresearch/search/scored/empty.hpp"
#include "iresearch/search/scored/make_boolean.hpp"
#include "iresearch/search/scored/masked.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"

namespace irs::scored {
namespace {

Root::ptr MakeUnscored(const FixedPhraseQuery& query, const Context& ctx) {
  if (ctx.table != nullptr) {
    return irs::detail::MakeFixedPhrase<FilteredConstantWalk, Root::ptr>(
      query, ctx.table, score_t{0});
  }
  return irs::detail::MakeFixedPhrase<PlainConstantWalk, Root::ptr>(
    query, utils::Empty{}, score_t{0});
}

Root::ptr MakeUnscored(const VariadicPhraseQuery& query, const Context& ctx) {
  if (ctx.table != nullptr) {
    return irs::detail::MakeVariadicPhrase<FilteredConstantWalk, Root::ptr>(
      query, ctx.table, score_t{0});
  }
  return irs::detail::MakeVariadicPhrase<PlainConstantWalk, Root::ptr>(
    query, utils::Empty{}, score_t{0});
}

Root::ptr MakeUnscored(const NGramSimilarityQuery& query, const Context& ctx) {
  return irs::detail::Build(query, [&]<typename Slots>(auto&&... args) -> Root::ptr {
    using Node = lead::TwoPhaseDocs<Slots>;
    return MakeShape<detail::ConstantWalk, Node>(
      ctx, score_t{0}, std::forward<decltype(args)>(args)...);
  });
}

}  // namespace

Root::ptr MakeEmpty() { return memory::make_managed<Empty>(); }

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  const irs::detail::PostingClause posting{.state = query.State(),
                              .boost = query.Boost(),
                              .stats = query.Stats(ScoredOf(ctx))};
  return posting.state.cookie.docs_count == 1
           ? MakeSinglePosting(posting, query.Segment(), ctx)
           : MakePosting(posting, query.Segment(), ctx);
}

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto merge = query.MergeType();
  const auto* const field = state.Reader();
  const auto* const scorer = query.Stats(ScoredOf(ctx)).scorer;
  const auto boost = query.Boost();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    const auto posting = irs::detail::ClauseOf(terms.front(), field, scorer, boost);
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePosting(posting, query.Segment(), ctx)
             : MakePosting(posting, query.Segment(), ctx);
  }
  return MakeWindowDisjunction(
    terms, {}, irs::detail::UniformityOf(*state.Reader(), scorer), field, scorer,
    boost, query.Segment(), ctx, merge, {});
}

Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return MakeUnscored(query, ctx);
  }
  return irs::detail::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlop(query, ctx); },
    [&] { return MakeFixedPhraseIntervals(query, ctx); },
    [&] { return MakeFixedPhrase(query, ctx); });
}

Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return MakeUnscored(query, ctx);
  }
  return irs::detail::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlop(query, ctx); },
    [&] { return MakeVariadicPhraseIntervals(query, ctx); },
    [&] { return MakeVariadicPhrase(query, ctx); });
}

Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return MakeUnscored(query, ctx);
  }
  return query.Every() ? MakeNGramAll(query, ctx) : MakeNGram(query, ctx);
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return MakeAll(query.Segment(), ctx, query.Stats(ScoredOf(ctx)),
                 query.Boost());
}

Root::ptr Make(const WildcardNGramQuery& query, const Context& ctx) {
  return MakeWildcardNGram(query, ctx);
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeEmpty();
  }
  auto plan = query.PlanScored(ctx);
  const auto* const docs_mask = query.Segment().docs_mask();
  if (docs_mask == nullptr || !plan) [[likely]] {
    return plan;
  }
  return memory::make_managed<Masked>(std::move(plan), *docs_mask);
}

}  // namespace irs::scored
