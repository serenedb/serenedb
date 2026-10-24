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

#include "iresearch/search/docs/make.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_all.hpp"
#include "iresearch/search/detail/make_boolean.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/docs/empty.hpp"
#include "iresearch/search/docs/family.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/queries/approx_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"

namespace irs::docs {
namespace {

template<typename Query>
Root::ptr MakeAnyPhrase(const Query& query, const Context& ctx) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeDocs<Family, detail::PhraseKind<M>>(query, ctx);
  });
}

}  // namespace

Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeDocs<Family, detail::NGramKind<M>>(query, ctx);
  });
}

Root::ptr Make(const TokenPhraseQuery& query, const Context& ctx) {
  return detail::MakeDocs<Family, detail::TokenPhraseKind>(query, ctx);
}

Root::ptr Make(const WildcardNGramQuery& query, const Context& ctx) {
  return detail::MakeDocs<Family, detail::WildcardNGramKind>(query, ctx);
}

Root::ptr Make(const ApproxQuery& query, const Context& ctx) {
  return query.Approx().PlanDocs(ctx);
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return memory::make_managed<Empty>();
  }
  const detail::FoldReachScope reach{ctx.span};
  return query.PlanDocs(ctx);
}

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  return detail::MakeBoolean<Family>(query, ctx);
}

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  return detail::MakeTerm<Family>(query, ctx);
}

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  return detail::MakeMultiTerm<Family>(query, ctx);
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return detail::MakeAllQuery<Family>(query, ctx);
}

}  // namespace irs::docs
