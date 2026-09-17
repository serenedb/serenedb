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
#include "iresearch/search/detail/boolean_of.hpp"
#include "iresearch/search/detail/collect.hpp"
#include "iresearch/search/detail/collectors.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/filters/all_filter.hpp"
#include "iresearch/search/filters/boolean_filter.hpp"
#include "iresearch/search/filters/wildcard_ngram_filter.hpp"
#include "iresearch/search/lead/make_boolean.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/queries/boolean_query.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/queries/term_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::lead {

Node::ptr Make(const TermQuery& query, DocRange range) {
  return MakePostingDocs(detail::PostingClause{query.State()}, query.Segment(),
                         range);
}

Node::ptr Make(const TermQuery& query, const detail::ScoredCtx& ctx) {
  const detail::PostingClause posting{query.State(), query.Boost(),
                                      query.Stats(ctx)};
  const detail::ScoreRecipe recipe{.segment = &query.Segment(),
                                   .fetcher = ctx.fetcher};
  return MakePostingScored(posting, query.Segment(), recipe, ctx.range);
}

Node::ptr Make(const MultiTermQuery& query, DocRange range) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakePostingDocs(detail::ClauseOf(terms.front(), field),
                           query.Segment(), range);
  }
  return MakeDisjunctionOfTermsDocs<MultiTermState::Entry>(
    terms, field, *detail::DocOf(*field),
    static_cast<doc_id_t>(query.Segment().docs_count()), range);
}

Node::ptr Make(const MultiTermQuery& query, const detail::ScoredCtx& ctx) {
  const auto& state = query.State();
  const auto merge = query.MergeType();
  const auto* const field = state.Reader();
  const auto* const scorer = query.Stats(ctx).scorer;
  const auto boost = query.Boost();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    const detail::ScoreRecipe recipe{.segment = &query.Segment(),
                                     .fetcher = ctx.fetcher};
    return MakePostingScored(
      detail::ClauseOf(terms.front(), field, scorer, boost), query.Segment(),
      recipe, ctx.range);
  }
  return MakeWindowDisjunctionOfTermsScored<MultiTermState::Entry>(
    terms, field, scorer, boost, *detail::DocOf(*field),
    detail::UniformityOf(*field, scorer), query.Segment(), ctx, merge, 0);
}

Node::ptr Make(const FixedPhraseQuery& query, DocRange range) {
  return detail::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopDocs(query, range); },
    [&] { return MakeFixedPhraseIntervalsDocs(query, range); },
    [&] { return MakeFixedPhraseDocs(query, range); });
}

Node::ptr Make(const FixedPhraseQuery& query, const detail::ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  const detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = ctx.fetcher,
                               .boost = query.Boost()};
  if (args.stats == nullptr) {
    return Make(query, ctx.range);
  }
  return detail::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopScored(query, args, ctx.range); },
    [&] { return MakeFixedPhraseIntervalsScored(query, args, ctx.range); },
    [&] { return MakeFixedPhraseScored(query, args, ctx.range); });
}

Node::ptr Make(const VariadicPhraseQuery& query, DocRange range) {
  return detail::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopDocs(query, range); },
    [&] { return MakeVariadicPhraseIntervalsDocs(query, range); },
    [&] { return MakeVariadicPhraseDocs(query, range); });
}

Node::ptr Make(const VariadicPhraseQuery& query, const detail::ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  const detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = ctx.fetcher,
                               .boost = query.Boost()};
  if (args.stats == nullptr) {
    return Make(query, ctx.range);
  }
  return detail::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopScored(query, args, ctx.range); },
    [&] { return MakeVariadicPhraseIntervalsScored(query, args, ctx.range); },
    [&] { return MakeVariadicPhraseScored(query, args, ctx.range); });
}

Node::ptr Make(const NGramSimilarityQuery& query, DocRange range) {
  return query.Every() ? MakeNGramAllDocs(query, range)
                       : MakeNGramDocs(query, range);
}

Node::ptr Make(const NGramSimilarityQuery& query,
               const detail::ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  if (record.stats == nullptr) {
    return Make(query, ctx.range);
  }
  const detail::ScoreArgs args{.scorer = record.scorer,
                               .stats = record.stats,
                               .fetcher = ctx.fetcher,
                               .boost = query.Boost()};
  return query.Every() ? MakeNGramAllScored(query, args, ctx.range)
                       : MakeNGramScored(query, args, ctx.range);
}

Node::ptr Make(const AllQuery& query, DocRange range) {
  return MakeAllDocs(query.Segment(), range);
}

Node::ptr Make(const AllQuery& query, const detail::ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  return MakeAllScored(query.Segment(),
                       detail::ScoreArgs{.scorer = record.scorer,
                                         .stats = record.stats,
                                         .fetcher = ctx.fetcher,
                                         .boost = query.Boost()},
                       ctx.range);
}

Node::ptr Make(const WildcardNGramQuery& query, DocRange range) {
  return MakeWildcardNGramDocs(query, range);
}

Node::ptr Make(const WildcardNGramQuery& query, const detail::ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  return MakeWildcardNGramScored(
    query,
    detail::AllDocsScore(query.Segment(),
                         detail::ScoreArgs{.scorer = record.scorer,
                                           .stats = record.stats,
                                           .fetcher = ctx.fetcher,
                                           .boost = query.Boost()}),
    ctx.range);
}

}  // namespace irs::lead
