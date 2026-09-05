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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/plan.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::lead {

Node::ptr MakeConjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  if (terms.empty() && filters.empty()) {
    return MakeAllDocs(segment);
  }
  if (auto folded = MakeBitsetConjunctionDocs(terms, filters, segment)) {
    return folded;
  }
  if (auto windowed = MakeWindowConjunctionDocs(terms, filters, segment)) {
    return windowed;
  }
  return MakeSparseConjunctionDocs(terms, filters, segment);
}

Node::ptr MakeDisjunctionDocs(std::span<const PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  if (auto folded = MakeBitsetDisjunctionDocs(terms, filters, segment)) {
    return folded;
  }
  return MakeWindowDisjunctionDocs(terms, filters, segment);
}

Node::ptr MakeThresholdDocs(std::span<const PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader& segment, uint32_t min_match) {
  SDB_ASSERT(min_match > 1);
  if (min_match > search::kBitplaneMaxMatch) {
    if (auto counted =
          MakeCountThresholdDocs(terms, filters, segment, min_match)) {
      return counted;
    }
  }
  return MakeBitsThresholdDocs(terms, filters, segment, min_match);
}

Node::ptr MakeRequiredDocs(std::span<const PostingClause> must,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const PostingClause> should,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match,
                           const SubReader& segment) {
  if (min_should_match == 0) {
    return MakeConjunctionDocs(must, must_filters, segment);
  }
  if (must.empty() && must_filters.empty()) {
    return min_should_match == 1
             ? MakeDisjunctionDocs(should, should_filters, segment)
             : MakeThresholdDocs(should, should_filters, segment,
                                 min_should_match);
  }
  auto other = probe::BuildOptionalProbe(
    should, should_filters, min_should_match, segment,
    search::IncludeCandidates(must, must_filters, segment));
  if (!other) {
    return {};
  }
  return MakeSparseConjunctionWithDocs(must, must_filters, segment,
                                       std::move(other));
}

Node::ptr MakeExclusionDocs(std::span<const PostingClause> must,
                            std::span<const QueryBuilder::ptr> must_filters,
                            std::span<const PostingClause> should,
                            std::span<const QueryBuilder::ptr> should_filters,
                            uint32_t min_should_match,
                            std::span<const PostingClause> excludes,
                            std::span<const QueryBuilder::ptr> exclude_filters,
                            const SubReader& segment) {
  if (auto folded = MakeBitsetExclusionDocs(
        must, must_filters, should, should_filters, min_should_match, excludes,
        exclude_filters, segment)) {
    return folded;
  }
  if (auto windowed = MakeWindowExclusionDocs(
        must, must_filters, should, should_filters, min_should_match, excludes,
        exclude_filters, segment)) {
    return windowed;
  }
  return MakeSparseExclusionDocs(must, must_filters, should, should_filters,
                                 min_should_match, excludes, exclude_filters,
                                 segment);
}

Node::ptr Make(const TermQuery& query) {
  return MakePostingDocs(PostingClause{query.State()}, query.Segment());
}

Node::ptr Make(const TermQuery& query, const ScoredCtx& ctx) {
  const PostingClause posting{query.State(), query.Boost(), query.Stats(ctx)};
  const ScoreRecipe recipe{.segment = &query.Segment(), .fetcher = ctx.fetcher};
  return MakePostingScored(posting, query.Segment(), recipe);
}

Node::ptr Make(const MultiTermQuery& query) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakePostingDocs(search::ClauseOf(terms.front(), field),
                           query.Segment());
  }
  return MakeDisjunctionOfTermsDocs<MultiTermState::Entry>(
    terms, field, *search::DocOf(*field),
    static_cast<doc_id_t>(query.Segment().docs_count()));
}

Node::ptr Make(const MultiTermQuery& query, const ScoredCtx& ctx) {
  const auto& state = query.State();
  const auto merge = query.MergeType();
  const auto* const field = state.Reader();
  const auto* const scorer = query.Stats(ctx).scorer;
  const auto boost = query.Boost();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    const ScoreRecipe recipe{.segment = &query.Segment(),
                             .fetcher = ctx.fetcher};
    return MakePostingScored(
      search::ClauseOf(terms.front(), field, scorer, boost), query.Segment(),
      recipe);
  }
  return MakeWindowDisjunctionOfTermsScored<MultiTermState::Entry>(
    terms, field, scorer, boost, *search::DocOf(*field),
    search::UniformityOf(*field, scorer), query.Segment(), ctx, merge, 0);
}

Node::ptr Make(const FixedPhraseQuery& query) {
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopDocs(query); },
    [&] { return MakeFixedPhraseIntervalsDocs(query); },
    [&] { return MakeFixedPhraseDocs(query); });
}

Node::ptr Make(const FixedPhraseQuery& query, const ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = record.stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  if (args.stats == nullptr) {
    return Make(query);
  }
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopScored(query, args); },
    [&] { return MakeFixedPhraseIntervalsScored(query, args); },
    [&] { return MakeFixedPhraseScored(query, args); });
}

Node::ptr Make(const VariadicPhraseQuery& query) {
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopDocs(query); },
    [&] { return MakeVariadicPhraseIntervalsDocs(query); },
    [&] { return MakeVariadicPhraseDocs(query); });
}

Node::ptr Make(const VariadicPhraseQuery& query, const ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = record.stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  if (args.stats == nullptr) {
    return Make(query);
  }
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopScored(query, args); },
    [&] { return MakeVariadicPhraseIntervalsScored(query, args); },
    [&] { return MakeVariadicPhraseScored(query, args); });
}

Node::ptr Make(const NGramSimilarityQuery& query) {
  return query.Every() ? MakeNGramAllDocs(query) : MakeNGramDocs(query);
}

Node::ptr Make(const NGramSimilarityQuery& query, const ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  if (record.stats == nullptr) {
    return Make(query);
  }
  const ScoreArgs args{.scorer = record.scorer,
                       .stats = record.stats,
                       .fetcher = ctx.fetcher,
                       .boost = query.Boost()};
  return query.Every() ? MakeNGramAllScored(query, args)
                       : MakeNGramScored(query, args);
}

Node::ptr Make(const AllQuery& query) { return MakeAllDocs(query.Segment()); }

Node::ptr Make(const AllQuery& query, const ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  return MakeAllScored(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                  .stats = record.stats,
                                                  .fetcher = ctx.fetcher,
                                                  .boost = query.Boost()});
}

Node::ptr Make(const WildcardNgramQuery& query) {
  return MakeWildcardNgramDocs(query);
}

Node::ptr Make(const WildcardNgramQuery& query, const ScoredCtx& ctx) {
  const auto record = query.Stats(ctx);
  return MakeWildcardNgramScored(
    query,
    search::AllDocsScore(query.Segment(), ScoreArgs{.scorer = record.scorer,
                                                    .stats = record.stats,
                                                    .fetcher = ctx.fetcher,
                                                    .boost = query.Boost()}));
}

Node::ptr Make(const BooleanQuery& query) {
  const auto& segment = query.Segment();
  const auto excludes = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (excludes.empty() && exclude_filters.empty()) {
    return MakeRequiredDocs(must, must_filters, should, should_filters,
                            min_should_match, segment);
  }
  return MakeExclusionDocs(must, must_filters, should, should_filters,
                           min_should_match, excludes, exclude_filters,
                           segment);
}

Node::ptr MakeRequiredScored(std::span<const PostingClause> must,
                             std::span<const QueryBuilder::ptr> must_filters,
                             std::span<const PostingClause> should,
                             std::span<const QueryBuilder::ptr> should_filters,
                             search::Terms should_uniformity,
                             uint32_t min_should_match,
                             const SubReader& segment, const ScoredCtx& ctx,
                             ScoreMergeType merge, score_t absorbed) {
  if (!(should.empty() && should_filters.empty()) && min_should_match == 0) {
    return MakeSparseBoostScored(must, must_filters, should, should_filters,
                                 should_uniformity, segment, ctx, merge,
                                 absorbed);
  }
  if (must.empty() && must_filters.empty()) {
    if (min_should_match == 0) {
      return MakeAllScored(segment, absorbed);
    }
    if (min_should_match == 1) {
      return MakeWindowDisjunctionScored(should, should_filters,
                                         should_uniformity, segment, ctx, merge,
                                         absorbed);
    }
    if (min_should_match > search::kBitplaneMaxMatch) {
      if (auto counted = MakeCountThresholdScored(
            should, should_filters, should_uniformity, segment, ctx, merge,
            min_should_match, absorbed)) {
        return counted;
      }
    }
    return MakeBitsThresholdScored(should, should_filters, should_uniformity,
                                   segment, ctx, merge, min_should_match,
                                   absorbed);
  }
  if (min_should_match != 0) {
    return MakeSparseConjunctionWithScored(
      must, must_filters, should, should_filters, should_uniformity,
      min_should_match, segment, ctx, merge, absorbed);
  }
  return MakeSparseConjunctionScored(must, must_filters, segment, ctx, merge,
                                     absorbed);
}

Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto absorbed = query.Absorbed();
  const auto excludes = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto should_uniformity = query.Uniformity(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();

  if (!excludes.empty() || !exclude_filters.empty()) {
    return MakeSparseExclusionScored(must, must_filters, should, should_filters,
                                     should_uniformity, min_should_match,
                                     excludes, exclude_filters, segment, ctx,
                                     merge, absorbed);
  }
  return MakeRequiredScored(must, must_filters, should, should_filters,
                            should_uniformity, min_should_match, segment, ctx,
                            merge, absorbed);
}

}  // namespace irs::lead
