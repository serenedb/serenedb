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

#include "iresearch/search/top/make.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/phrase_match.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/top/empty.hpp"
#include "iresearch/search/top/family.hpp"

namespace irs::top {
namespace {

template<typename Query>
Root::ptr MakeAnyPhrase(const Query& query, const Context& ctx) {
  return irs::detail::ResolveMatch(
    query, [&]<irs::detail::PhraseMatch M> -> Root::ptr {
      if constexpr (M != irs::detail::PhraseMatch::Slop) {
        if (ctx.prune) {
          if (auto pruned = MakePhrasePruned<M>(query, ctx)) {
            return pruned;
          }
        }
      }
      return irs::detail::MakeScored<Family, irs::detail::PhraseKind<M>>(query,
                                                                         ctx);
    });
}

}  // namespace

Root::ptr MakeEmpty() { return memory::make_managed<Empty>(); }

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeEmpty();
  }
  const detail::FoldReachScope reach{ctx.span};
  return query.PlanTop(ctx);
}

Root::ptr Family::Make(const FixedPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Family::Make(const VariadicPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Family::Make(const TokenPhraseQuery& query, const Context& ctx) {
  if (ctx.prune) {
    if (auto pruned = MakeTokenPhrasePruned(query, ctx)) {
      return pruned;
    }
  }
  return irs::detail::MakeScored<Family, irs::detail::TokenPhraseKind>(query,
                                                                       ctx);
}

Root::ptr Family::Make(const TermQuery& query, const Context& ctx) {
  if (ctx.prune && query.State().cookie.docs_count != 1) {
    const irs::detail::PostingClause posting{
      .state = query.State(),
      .boost = query.Boost(),
      .stats = query.Stats(ScoredOf(ctx)),
    };
    if (auto pruned = MakePrunedPosting(posting, query.Segment(), ctx)) {
      return pruned;
    }
  }
  return irs::detail::MakeQuery<Family>(query, ctx);
}

Root::ptr Family::Make(const MultiTermQuery& query, const Context& ctx) {
  if (ctx.prune) {
    const auto& state = query.State();
    const auto* const field = state.Reader();
    const auto* const scorer = query.Stats(ScoredOf(ctx)).scorer;
    const auto boost = query.Boost();
    const std::span<const MultiTermState::Entry> terms{state.Terms()};
    if (terms.size() == 1) {
      const auto posting =
        irs::detail::ClauseOf(terms.front(), field, scorer, boost);
      if (posting.state.cookie.docs_count != 1) {
        if (auto pruned = MakePrunedPosting(posting, query.Segment(), ctx)) {
          return pruned;
        }
      }
    } else if (query.MergeType() == ScoreMergeType::Sum) {
      if (auto pruned = MakePrunedDisjunction<false>(
            terms, {}, irs::detail::UniformityOf(*field, scorer), field, scorer,
            boost, {}, {}, query.Segment(), ctx, ScoreMergeType::Sum)) {
        return pruned;
      }
    }
  }
  return irs::detail::MakeQuery<Family>(query, ctx);
}

}  // namespace irs::top
