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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::probe {
namespace {

search::ScoreRecipe RecipeOf(const SubReader& segment, const ScoredCtx& ctx) {
  return {.segment = &segment, .fetcher = ctx.fetcher};
}

}  // namespace

Node::ptr MakeRequiredDocs(std::span<const search::PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const search::PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match, const SubReader& segment,
                           uint64_t interrogations) {
  if (min_should_match == 0) {
    return MakeSparseConjunctionDocs(must, must_filters, segment,
                                     interrogations);
  }
  if (must.empty() && must_filters.empty()) {
    return probe::BuildOptionalProbe(should, should_filters, min_should_match,
                                     segment, interrogations);
  }
  auto other = probe::BuildOptionalProbe(
    should, should_filters, min_should_match, segment,
    std::min(interrogations,
             search::IncludeCandidates(must, must_filters, segment)));
  if (!other) {
    return {};
  }
  return MakeSparseConjunctionWithDocs(must, must_filters, segment,
                                       interrogations, std::move(other));
}

Node::ptr Make(const TermQuery& query, uint64_t) {
  return MakePostingDocs(search::PostingClause{query.State()}, query.Segment());
}

Node::ptr Make(const MultiTermQuery& query, uint64_t interrogations) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakePostingDocs(search::ClauseOf(terms.front(), field),
                           query.Segment());
  }
  return MakeDisjunctionDocs(terms, {}, field, query.Segment(), interrogations);
}

Node::ptr Make(const FixedPhraseQuery& query, uint64_t) {
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopDocs(query); },
    [&] { return MakeFixedPhraseIntervalsDocs(query); },
    [&] { return MakeFixedPhraseDocs(query); });
}

Node::ptr Make(const VariadicPhraseQuery& query, uint64_t) {
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopDocs(query); },
    [&] { return MakeVariadicPhraseIntervalsDocs(query); },
    [&] { return MakeVariadicPhraseDocs(query); });
}

Node::ptr Make(const NGramSimilarityQuery& query, uint64_t) {
  return query.Every() ? MakeNGramAllDocs(query) : MakeNGramDocs(query);
}

Node::ptr Make(const AllQuery& query, uint64_t) {
  return MakeAllDocs(query.Segment());
}

Node::ptr Make(const WildcardNgramQuery& query, uint64_t interrogations) {
  return MakeWildcardNgramDocs(query, interrogations);
}

Node::ptr Make(const BooleanQuery& query, uint64_t interrogations) {
  const auto& segment = query.Segment();
  const auto exclude = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (exclude.empty() && exclude_filters.empty()) {
    return MakeRequiredDocs(must, must_filters, should, should_filters,
                            min_should_match, segment, interrogations);
  }
  return MakeSparseExclusionDocs(must, must_filters, should, should_filters,
                                 min_should_match, exclude, exclude_filters,
                                 segment, interrogations);
}

Node::ptr Make(const TermQuery& query, const ScoredCtx& ctx, uint64_t) {
  const search::PostingClause posting{query.State(), query.Boost(),
                                      query.Stats(ctx)};
  return MakePostingScored(posting, query.Segment(),
                           RecipeOf(query.Segment(), ctx));
}

Node::ptr Make(const MultiTermQuery& query, const ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto merge = query.MergeType();
  const auto& segment = query.Segment();
  const auto recipe = RecipeOf(segment, ctx);
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const auto* const scorer = query.Stats(ctx).scorer;
  const auto boost = query.Boost();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakePostingScored(
      search::ClauseOf(terms.front(), field, scorer, boost), segment, recipe);
  }
  const auto clause = ScoredClauseOf(segment, ctx, recipe);
  return MakeSparseDisjunctionScored(
    terms, {}, search::UniformityOf(*field, scorer), field, scorer, boost,
    segment, recipe, merge, interrogations, clause);
}

Node::ptr Make(const FixedPhraseQuery& query, const ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto record = query.Stats(ctx);
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = record.stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  if (args.stats == nullptr) {
    return Make(query, interrogations);
  }
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopScored(query, args); },
    [&] { return MakeFixedPhraseIntervalsScored(query, args); },
    [&] { return MakeFixedPhraseScored(query, args); });
}

Node::ptr Make(const VariadicPhraseQuery& query, const ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto record = query.Stats(ctx);
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = record.stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  if (args.stats == nullptr) {
    return Make(query, interrogations);
  }
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopScored(query, args); },
    [&] { return MakeVariadicPhraseIntervalsScored(query, args); },
    [&] { return MakeVariadicPhraseScored(query, args); });
}

Node::ptr Make(const NGramSimilarityQuery& query, const ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto record = query.Stats(ctx);
  if (record.stats == nullptr) {
    return Make(query, interrogations);
  }
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = record.stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  return query.Every() ? MakeNGramAllScored(query, args)
                       : MakeNGramScored(query, args);
}

Node::ptr Make(const AllQuery& query, const ScoredCtx& ctx, uint64_t) {
  const auto record = query.Stats(ctx);
  return MakeAllScored(
    query.Segment(),
    search::AllDocsScore(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                    .stats = record.stats,
                                                    .fetcher = ctx.fetcher,
                                                    .boost = query.Boost()}));
}

Node::ptr Make(const WildcardNgramQuery& query, const ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto record = query.Stats(ctx);
  return MakeWildcardNgramScored(
    query,
    search::AllDocsScore(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                    .stats = record.stats,
                                                    .fetcher = ctx.fetcher,
                                                    .boost = query.Boost()}),
    interrogations);
}

Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx,
               uint64_t interrogations) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto recipe = RecipeOf(segment, ctx);
  const auto absorbed = query.Absorbed();
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto must_uniformity = query.Uniformity(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  const auto exclude = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);

  if (!exclude.empty() || !exclude_filters.empty()) {
    return MakeSparseExclusionScored(
      must, must_filters, must_uniformity, should, should_filters,
      should_uniformity, min_should_match, exclude, exclude_filters, segment,
      recipe, merge, interrogations, ctx, absorbed);
  }
  if ((!should.empty() || !should_filters.empty()) && min_should_match == 0) {
    return MakeSparseBoostScored(must, must_filters, must_uniformity, should,
                                 should_filters, should_uniformity, segment,
                                 recipe, merge, interrogations, ctx, absorbed);
  }
  return MakeRequiredScored(must, must_filters, must_uniformity, should,
                            should_filters, should_uniformity, min_should_match,
                            segment, recipe, merge, interrogations, ctx,
                            absorbed);
}

}  // namespace irs::probe
