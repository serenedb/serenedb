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

#include "iresearch/search/probe/make.hpp"

#include <algorithm>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/approx_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_all.hpp"
#include "iresearch/search/detail/make_boolean.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/probe/constant_scored.hpp"
#include "iresearch/search/probe/family.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::probe {
namespace {

template<typename Query>
Node::ptr MakeAnyPhrase(const Query& query, uint64_t interrogations) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeDocs<Family, detail::PhraseKind<M>>(
      query, {.interrogations = interrogations});
  });
}

template<typename Query>
Node::ptr MakeAnyPhrase(const Query& query, const detail::ScoredCtx& ctx,
                        uint64_t interrogations) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeNode<Family, detail::PhraseKind<M>>(
      query, {.scored = ctx, .interrogations = interrogations});
  });
}

}  // namespace

Node::ptr Make(const FixedPhraseQuery& query, uint64_t interrogations) {
  return MakeAnyPhrase(query, interrogations);
}

Node::ptr Make(const VariadicPhraseQuery& query, uint64_t interrogations) {
  return MakeAnyPhrase(query, interrogations);
}

Node::ptr Make(const NGramSimilarityQuery& query, uint64_t interrogations) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeDocs<Family, detail::NGramKind<M>>(
      query, {.interrogations = interrogations});
  });
}

Node::ptr Make(const WildcardNGramQuery& query, uint64_t interrogations) {
  return detail::MakeDocs<Family, detail::WildcardNGramKind>(
    query, {.interrogations = interrogations});
}

Node::ptr Make(const TokenPhraseQuery& query, uint64_t interrogations) {
  return detail::MakeDocs<Family, detail::TokenPhraseKind>(
    query, {.interrogations = interrogations});
}

Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return MakeAnyPhrase(query, ctx, interrogations);
}

Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return MakeAnyPhrase(query, ctx, interrogations);
}

Node::ptr Make(const NGramSimilarityQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeNode<Family, detail::NGramKind<M>>(
      query, {.scored = ctx, .interrogations = interrogations});
  });
}

Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeNode<Family, detail::WildcardNGramKind>(
    query, {.scored = ctx, .interrogations = interrogations});
}

Node::ptr Make(const TokenPhraseQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeNode<Family, detail::TokenPhraseKind>(
    query, {.scored = ctx, .interrogations = interrogations});
}

Node::ptr Make(const ApproxQuery& query, uint64_t interrogations) {
  return query.Approx().PlanProbe({}, interrogations);
}

Node::ptr Make(const ApproxQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeApprox<Family>(
    query, {.scored = ctx, .interrogations = interrogations});
}

Node::ptr Make(const BooleanQuery& query, uint64_t interrogations) {
  return detail::MakeBoolean<Family>(query,
                                     Family::DocsContext{interrogations});
}

Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeBoolean<Family>(
    query, Family::ScoredContext{ctx, interrogations});
}

Node::ptr Make(const TermQuery& query, uint64_t interrogations) {
  return detail::MakeTerm<Family>(query, Family::DocsContext{interrogations});
}

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeTerm<Family>(query,
                                  Family::ScoredContext{ctx, interrogations});
}

Node::ptr Make(const MultiTermQuery& query, uint64_t interrogations) {
  return detail::MakeMultiTerm<Family>(query,
                                       Family::DocsContext{interrogations});
}

Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeMultiTerm<Family>(
    query, Family::ScoredContext{ctx, interrogations});
}

Node::ptr Make(const AllQuery& query, uint64_t interrogations) {
  return detail::MakeAllQuery<Family>(query,
                                      Family::DocsContext{interrogations});
}

Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx,
               uint64_t interrogations) {
  return detail::MakeAllQuery<Family>(
    query, Family::ScoredContext{ctx, interrogations});
}

}  // namespace irs::probe
