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

#include "iresearch/search/lead/make.hpp"

#include <cstdint>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/approx_of.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_all.hpp"
#include "iresearch/search/detail/make_boolean.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/lead/constant_scored.hpp"
#include "iresearch/search/lead/family.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::lead {
namespace {

template<typename Query>
Node::ptr MakeAnyPhrase(const Query& query) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeDocs<Family, detail::PhraseKind<M>>(query, {});
  });
}

template<typename Query>
Node::ptr MakeAnyPhrase(const Query& query, const detail::ScoredCtx& ctx) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeNode<Family, detail::PhraseKind<M>>(query, ctx);
  });
}

}  // namespace

Node::ptr Make(const FixedPhraseQuery& query) { return MakeAnyPhrase(query); }

Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Node::ptr Make(const VariadicPhraseQuery& query) {
  return MakeAnyPhrase(query);
}

Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx) {
  return MakeAnyPhrase(query, ctx);
}

Node::ptr Make(const NGramSimilarityQuery& query) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeDocs<Family, detail::NGramKind<M>>(query, {});
  });
}

Node::ptr Make(const NGramSimilarityQuery& query,
               const detail::ScoredCtx& ctx) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeNode<Family, detail::NGramKind<M>>(query, ctx);
  });
}

Node::ptr Make(const TokenPhraseQuery& query) {
  return detail::MakeDocs<Family, detail::TokenPhraseKind>(query, {});
}

Node::ptr Make(const TokenPhraseQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeNode<Family, detail::TokenPhraseKind>(query, ctx);
}

Node::ptr Make(const WildcardNGramQuery& query) {
  return detail::MakeDocs<Family, detail::WildcardNGramKind>(query, {});
}

Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeNode<Family, detail::WildcardNGramKind>(query, ctx);
}

Node::ptr Make(const ApproxQuery& query) { return query.Approx().PlanLead({}); }

Node::ptr Make(const ApproxQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeApprox<Family>(query, ctx);
}

Node::ptr Make(const BooleanQuery& query) {
  return detail::MakeBoolean<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeBoolean<Family>(query, ctx);
}

Node::ptr Make(const TermQuery& query) {
  return detail::MakeTerm<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeTerm<Family>(query, ctx);
}

Node::ptr Make(const MultiTermQuery& query) {
  return detail::MakeMultiTerm<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeMultiTerm<Family>(query, ctx);
}

Node::ptr Make(const AllQuery& query) {
  return detail::MakeAllQuery<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx) {
  return detail::MakeAllQuery<Family>(query, ctx);
}

}  // namespace irs::lead
