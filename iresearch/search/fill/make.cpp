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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/boolean_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collect_scored.hpp"
#include "iresearch/search/detail/ngram_of.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/fill/make_boolean.hpp"
#include "iresearch/search/fill/plan.hpp"
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
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::fill {

Node::ptr Make(const TermQuery& query, DocRange range) {
  const detail::PostingClause posting{query.State()};
  return posting.state.cookie.docs_count == 1
           ? MakeSinglePostingDocs(posting, range)
           : MakePostingDocs(posting, query.Segment(), range);
}

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  const detail::PostingClause posting{query.State(), query.Boost(),
                                      query.Stats(ctx)};
  return posting.state.cookie.docs_count == 1
           ? MakeSinglePostingScored(posting, query.Segment(), ctx, merge)
           : MakePostingScored(posting, query.Segment(), ctx, merge);
}

Node::ptr Make(const MultiTermQuery& query, DocRange range) {
  const auto& state = query.State();
  const auto& segment = query.Segment();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    const auto posting = detail::ClauseOf(terms.front(), field);
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePostingDocs(posting, range)
             : MakePostingDocs(posting, segment, range);
  }
  return MakeDisjunctionOfTermsDocs(terms, field, *detail::DocOf(*field),
                                    static_cast<doc_id_t>(segment.docs_count()),
                                    range);
}

Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  const auto& state = query.State();
  const auto& segment = query.Segment();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  const auto* const scorer = query.Stats(ctx).scorer;
  const auto boost = query.Boost();
  if (terms.size() == 1) {
    const auto posting = detail::ClauseOf(terms.front(), field, scorer, boost);
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePostingScored(posting, segment, ctx, merge)
             : MakePostingScored(posting, segment, ctx, merge);
  }
  const detail::ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  std::vector<Node::ptr> rest;
  return MakeWindowDisjunctionScored(
    terms, field, scorer, boost, detail::DocOf(*field), ctx.range, rest,
    detail::UniformityOf(*field, scorer), recipe, merge);
}

Node::ptr Make(const FixedPhraseQuery& query, DocRange range) {
  return detail::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopDocs(query, range); },
    [&] { return MakeFixedPhraseIntervalsDocs(query, range); },
    [&] { return MakeFixedPhraseDocs(query, range); });
}

Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.Stats().stats == nullptr) {
    return Make(query, ctx.range);
  }
  return detail::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopScored(query, ctx, merge); },
    [&] { return MakeFixedPhraseIntervalsScored(query, ctx, merge); },
    [&] { return MakeFixedPhraseScored(query, ctx, merge); });
}

Node::ptr Make(const VariadicPhraseQuery& query, DocRange range) {
  return detail::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopDocs(query, range); },
    [&] { return MakeVariadicPhraseIntervalsDocs(query, range); },
    [&] { return MakeVariadicPhraseDocs(query, range); });
}

Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.Stats().stats == nullptr) {
    return Make(query, ctx.range);
  }
  return detail::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopScored(query, ctx, merge); },
    [&] { return MakeVariadicPhraseIntervalsScored(query, ctx, merge); },
    [&] { return MakeVariadicPhraseScored(query, ctx, merge); });
}

Node::ptr Make(const NGramSimilarityQuery& query, DocRange range) {
  return query.Every() ? MakeNGramAllDocs(query, range)
                       : MakeNGramDocs(query, range);
}

Node::ptr Make(const NGramSimilarityQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.Stats().stats == nullptr) {
    return Make(query, ctx.range);
  }
  return query.Every() ? MakeNGramAllScored(query, ctx, merge)
                       : MakeNGramScored(query, ctx, merge);
}

Node::ptr Make(const AllQuery& query, DocRange range) {
  return MakeAllDocs(query.Segment(), range);
}

Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return MakeAllScored(query.Segment(), ctx, query.Stats(ctx), merge,
                       query.Boost());
}

Node::ptr Make(const WildcardNGramQuery& query, DocRange range) {
  return MakeWildcardNGramDocs(query, range);
}

Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx,
               ScoreMergeType merge) {
  return MakeWildcardNGramScored(query, ctx, merge);
}

}  // namespace irs::fill
