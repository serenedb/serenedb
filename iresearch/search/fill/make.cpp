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

#include "iresearch/search/fill/make.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/approx_of.hpp"
#include "iresearch/search/detail/bitset_of.hpp"
#include "iresearch/search/detail/boolean_builder.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/fill_posting_scored.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/make_all.hpp"
#include "iresearch/search/detail/make_boolean.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/detail/window_set.hpp"
#include "iresearch/search/fill/family.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/set_leaves.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/fill/window_scored.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::fill {
namespace {

template<typename Query>
Node::ptr MakeAnyPhrase(const Query& query) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeDocs<Family, detail::PhraseKind<M>>(query, {});
  });
}

template<typename Query>
Node::ptr MakeAnyPhrase(const Query& query, const detail::ScoredCtx& ctx,
                        ScoreMergeType merge) {
  return detail::ResolveMatch(query, [&]<detail::PhraseMatch M> {
    return detail::MakeNode<Family, detail::PhraseKind<M>>(
      query, {.scored = ctx, .merge = merge});
  });
}

}  // namespace

Node::ptr Make(const FixedPhraseQuery& query) { return MakeAnyPhrase(query); }

Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return MakeAnyPhrase(query, ctx, merge);
}

Node::ptr Make(const VariadicPhraseQuery& query) {
  return MakeAnyPhrase(query);
}

Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return MakeAnyPhrase(query, ctx, merge);
}

Node::ptr Make(const NGramSimilarityQuery& query) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeDocs<Family, detail::NGramKind<M>>(query, {});
  });
}

Node::ptr Make(const NGramSimilarityQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::ResolveNGram(query, [&]<detail::NGramMatch M> {
    return detail::MakeNode<Family, detail::NGramKind<M>>(
      query, {.scored = ctx, .merge = merge});
  });
}

Node::ptr Make(const WildcardNGramQuery& query) {
  return detail::MakeDocs<Family, detail::WildcardNGramKind>(query, {});
}

Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeNode<Family, detail::WildcardNGramKind>(
    query, {.scored = ctx, .merge = merge});
}

Node::ptr Make(const TokenPhraseQuery& query) {
  return detail::MakeDocs<Family, detail::TokenPhraseKind>(query, {});
}

Node::ptr Make(const TokenPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeNode<Family, detail::TokenPhraseKind>(
    query, {.scored = ctx, .merge = merge});
}

Node::ptr Make(const ApproxQuery& query) {
  return query.Approx().PlanFill({}, ScoreMergeType::Noop);
}

Node::ptr Make(const ApproxQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeApprox<Family>(query, {.scored = ctx, .merge = merge});
}

Node::ptr Make(const BooleanQuery& query) {
  return detail::MakeBoolean<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const BooleanQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeBoolean<Family>(query, Family::ScoredContext{ctx, merge});
}

Node::ptr Make(const TermQuery& query) {
  return detail::MakeTerm<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeTerm<Family>(query, Family::ScoredContext{ctx, merge});
}

Node::ptr Make(const MultiTermQuery& query) {
  return detail::MakeMultiTerm<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeMultiTerm<Family>(query,
                                       Family::ScoredContext{ctx, merge});
}

Node::ptr Make(const AllQuery& query) {
  return detail::MakeAllQuery<Family>(query, Family::DocsContext{});
}

Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return detail::MakeAllQuery<Family>(query, Family::ScoredContext{ctx, merge});
}

}  // namespace irs::fill
