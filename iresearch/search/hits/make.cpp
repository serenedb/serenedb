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

#include "iresearch/search/hits/make.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/approx_of.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_boolean.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/hits/empty.hpp"
#include "iresearch/search/hits/family.hpp"
#include "iresearch/search/hits/walk.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::hits {
namespace {

template<typename Query>
Root::ptr MakeAnyPhrase(const Query& query, const Context& ctx) {
  return irs::detail::ResolveMatch(query, [&]<irs::detail::PhraseMatch M> {
    return irs::detail::MakeScored<Family, irs::detail::PhraseKind<M>>(query,
                                                                       ctx);
  });
}

}  // namespace

Root::ptr MakeEmpty() { return memory::make_managed<Empty>(); }

Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx) {
  return irs::detail::ResolveNGram(query, [&]<irs::detail::NGramMatch M> {
    return irs::detail::MakeScored<Family, irs::detail::NGramKind<M>>(query,
                                                                      ctx);
  });
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return MakeAll(query.Segment(), ctx, query.Stats(ScoredOf(ctx)),
                 query.Boost());
}

Root::ptr Make(const WildcardNGramQuery& query, const Context& ctx) {
  return irs::detail::MakeScored<Family, irs::detail::WildcardNGramKind>(query,
                                                                         ctx);
}

Root::ptr Make(const TokenPhraseQuery& query, const Context& ctx) {
  return irs::detail::MakeScored<Family, irs::detail::TokenPhraseKind>(query,
                                                                       ctx);
}

Root::ptr Make(const ApproxQuery& query, const Context& ctx) {
  return irs::detail::MakeApprox<Family>(query, ctx);
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeEmpty();
  }
  const detail::FoldReachScope reach{ctx.span};
  return query.PlanScored(ctx);
}

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  return irs::detail::MakeBoolean<Family>(query, ctx);
}

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  return irs::detail::MakeTerm<Family>(query, ctx);
}

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  return irs::detail::MakeMultiTerm<Family>(query, ctx);
}

}  // namespace irs::hits
