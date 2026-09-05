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
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/collect_scored.hpp"
#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/fill/plan.hpp"
#include "iresearch/search/fill/walk.hpp"
#include "iresearch/search/fill/window_scored.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::fill {

Node::ptr Make(const TermQuery& query) {
  const search::PostingClause posting{query.State()};
  return posting.state.cookie.docs_count == 1
           ? MakeSinglePostingDocs(posting)
           : MakePostingDocs(posting, query.Segment());
}

Node::ptr Make(const TermQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  const search::PostingClause posting{query.State(), query.Boost(),
                                      query.Stats(ctx)};
  return posting.state.cookie.docs_count == 1
           ? MakeSinglePostingScored(posting, query.Segment(), ctx, merge)
           : MakePostingScored(posting, query.Segment(), ctx, merge);
}

Node::ptr Make(const MultiTermQuery& query) {
  const auto& state = query.State();
  const auto& segment = query.Segment();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    const auto posting = search::ClauseOf(terms.front(), field);
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePostingDocs(posting)
             : MakePostingDocs(posting, segment);
  }
  return MakeDisjunctionOfTermsDocs(
    terms, field, *search::DocOf(*field),
    static_cast<doc_id_t>(segment.docs_count()));
}

Node::ptr Make(const MultiTermQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  const auto& state = query.State();
  const auto& segment = query.Segment();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  const auto* const scorer = query.Stats(ctx).scorer;
  const auto boost = query.Boost();
  if (terms.size() == 1) {
    const auto posting = search::ClauseOf(terms.front(), field, scorer, boost);
    return posting.state.cookie.docs_count == 1
             ? MakeSinglePostingScored(posting, segment, ctx, merge)
             : MakePostingScored(posting, segment, ctx, merge);
  }
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  std::vector<Node::ptr> rest;
  return MakeWindowDisjunctionScored(
    terms, field, scorer, boost, search::DocOf(*field), rest,
    search::UniformityOf(*field, scorer), recipe, merge);
}

Node::ptr Make(const FixedPhraseQuery& query) {
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopDocs(query); },
    [&] { return MakeFixedPhraseIntervalsDocs(query); },
    [&] { return MakeFixedPhraseDocs(query); });
}

Node::ptr Make(const FixedPhraseQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.Stats().stats == nullptr) {
    return Make(query);
  }
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlopScored(query, ctx, merge); },
    [&] { return MakeFixedPhraseIntervalsScored(query, ctx, merge); },
    [&] { return MakeFixedPhraseScored(query, ctx, merge); });
}

Node::ptr Make(const VariadicPhraseQuery& query) {
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopDocs(query); },
    [&] { return MakeVariadicPhraseIntervalsDocs(query); },
    [&] { return MakeVariadicPhraseDocs(query); });
}

Node::ptr Make(const VariadicPhraseQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.Stats().stats == nullptr) {
    return Make(query);
  }
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlopScored(query, ctx, merge); },
    [&] { return MakeVariadicPhraseIntervalsScored(query, ctx, merge); },
    [&] { return MakeVariadicPhraseScored(query, ctx, merge); });
}

Node::ptr Make(const NGramSimilarityQuery& query) {
  return query.Every() ? MakeNGramAllDocs(query) : MakeNGramDocs(query);
}

Node::ptr Make(const NGramSimilarityQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  if (query.Stats().stats == nullptr) {
    return Make(query);
  }
  return query.Every() ? MakeNGramAllScored(query, ctx, merge)
                       : MakeNGramScored(query, ctx, merge);
}

Node::ptr Make(const AllQuery& query) { return MakeAllDocs(query.Segment()); }

Node::ptr Make(const AllQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  return MakeAllScored(query.Segment(), ctx, query.Stats(ctx), merge,
                       query.Boost());
}

Node::ptr Make(const WildcardNgramQuery& query) {
  return MakeWildcardNgramDocs(query);
}

Node::ptr Make(const WildcardNgramQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  return MakeWildcardNgramScored(query, ctx, merge);
}

Node::ptr MakeConjunctionDocs(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  if (terms.empty() && filters.empty()) {
    return MakeAllDocs(segment);
  }
  if (terms.size() + filters.size() == 1) {
    if (search::HeadIsTerm(terms, filters)) {
      return FillOf(terms.front(), nullptr, segment);
    }
    return filters.front()->PlanFill({}, ScoreMergeType::Noop);
  }
  if (auto folded = MakeBitsetConjunctionDocs(terms, filters, segment)) {
    return folded;
  }
  if (auto windowed = MakeWindowConjunctionDocs(terms, filters, segment)) {
    return windowed;
  }
  return MakeSparseConjunctionDocs(terms, filters, segment);
}

Node::ptr MakeDisjunctionDocs(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              const SubReader& segment) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<Node::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (auto folded = MakeBitsetDisjunctionDocs(
        terms, doc, rest, static_cast<doc_id_t>(segment.docs_count()))) {
    return folded;
  }
  return MakeWindowDisjunctionDocs(terms, doc, rest);
}

Node::ptr MakeDisjunctionScored(std::span<const search::PostingClause> terms,
                                std::span<const QueryBuilder::ptr> filters,
                                search::Terms uniformity,
                                const SubReader& segment, const ScoredCtx& ctx,
                                ScoreMergeType merge, score_t absorbed) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<Node::ptr> rest;
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest,
                          [&](const QueryBuilder& child) {
                            return child.PlanFill(ctx, merge);
                          })) {
    return {};
  }
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  return MakeWindowDisjunctionScored(terms, nullptr, nullptr, kNoBoost, doc,
                                     rest, uniformity, recipe, merge, absorbed);
}

Node::ptr MakeThresholdDocs(std::span<const search::PostingClause> terms,
                            std::span<const QueryBuilder::ptr> filters,
                            const SubReader&, uint32_t min_match) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  const IndexInput* doc = nullptr;
  std::vector<Node::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (min_match > search::kBitplaneMaxMatch) {
    if (auto counted = MakeCountThresholdDocs(terms, doc, rest, min_match)) {
      return counted;
    }
  }
  return MakeBitsThresholdDocs(terms, doc, rest, min_match);
}

Node::ptr MakeThresholdScored(std::span<const search::PostingClause> terms,
                              std::span<const QueryBuilder::ptr> filters,
                              search::Terms uniformity,
                              const SubReader& segment, const ScoredCtx& ctx,
                              ScoreMergeType merge, uint32_t min_match,
                              score_t absorbed) {
  SDB_ASSERT(min_match > 1);
  const IndexInput* doc = nullptr;
  std::vector<Node::ptr> rest;
  if (!CollectDenseScored(terms, filters, nullptr, doc, rest,
                          [&](const QueryBuilder& child) {
                            return child.PlanFill(ctx, merge);
                          })) {
    return {};
  }
  const ScoreRecipe recipe{.segment = &segment, .fetcher = ctx.fetcher};
  if (min_match > search::kBitplaneMaxMatch) {
    if (auto counted = MakeCountThresholdScored(
          terms, doc, rest, uniformity, recipe, merge, min_match, absorbed)) {
      return counted;
    }
  }
  return MakeBitsThresholdScored(terms, doc, rest, uniformity, recipe, merge,
                                 min_match, absorbed);
}

Node::ptr MakeRequiredDocs(std::span<const search::PostingClause> must_terms,
                           std::span<const QueryBuilder::ptr> must_filters,
                           std::span<const search::PostingClause> should_terms,
                           std::span<const QueryBuilder::ptr> should_filters,
                           uint32_t min_should_match,
                           const SubReader& segment) {
  if (min_should_match == 0) {
    return MakeConjunctionDocs(must_terms, must_filters, segment);
  }
  if (must_terms.empty() && must_filters.empty()) {
    return min_should_match == 1
             ? MakeDisjunctionDocs(should_terms, should_filters, segment)
             : MakeThresholdDocs(should_terms, should_filters, segment,
                                 min_should_match);
  }
  auto probe = probe::BuildOptionalProbe(
    should_terms, should_filters, min_should_match, segment,
    search::IncludeCandidates(must_terms, must_filters, segment));
  if (!probe) {
    return {};
  }
  return MakeSparseConjunctionWithDocs(must_terms, must_filters, segment,
                                       std::move(probe));
}

Node::ptr MakeExclusionDocs(
  std::span<const search::PostingClause> must_terms,
  std::span<const QueryBuilder::ptr> must_filters,
  std::span<const search::PostingClause> should_terms,
  std::span<const QueryBuilder::ptr> should_filters, uint32_t min_should_match,
  std::span<const search::PostingClause> exclude_terms,
  std::span<const QueryBuilder::ptr> exclude_filters,
  const SubReader& segment) {
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  if (min_should_match == 0 && (!must_terms.empty() || !must_filters.empty())) {
    if (auto folded =
          MakeBitsetExclusionDocs(must_terms, must_filters, exclude_terms,
                                  exclude_filters, segment, candidates)) {
      return folded;
    }
    if (auto windowed =
          MakeWindowExclusionDocs(must_terms, must_filters, exclude_terms,
                                  exclude_filters, segment, candidates)) {
      return windowed;
    }
    return MakeSparseExclusionDocs(must_terms, must_filters, exclude_terms,
                                   exclude_filters, segment, candidates);
  }
  auto include =
    lead::MakeRequiredDocs(must_terms, must_filters, should_terms,
                           should_filters, min_should_match, segment);
  if (!include) {
    return {};
  }
  return MakeSparseExclusionOfDocs(std::move(include), exclude_terms,
                                   exclude_filters, segment, candidates);
}

Node::ptr Make(const BooleanQuery& query) {
  const auto& segment = query.Segment();
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto min_should_match = query.MinShouldMatch();
  if (exclude_terms.empty() && exclude_filters.empty()) {
    return MakeRequiredDocs(must_terms, must_filters, should_terms,
                            should_filters, min_should_match, segment);
  }
  return MakeExclusionDocs(must_terms, must_filters, should_terms,
                           should_filters, min_should_match, exclude_terms,
                           exclude_filters, segment);
}

Node::ptr Make(const BooleanQuery& query, const ScoredCtx& ctx,
               ScoreMergeType merge) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  const auto min_should_match = query.MinShouldMatch();
  const auto absorbed = query.Absorbed();
  if (!exclude_terms.empty() || !exclude_filters.empty()) {
    return MakeSparseExclusionScored(
      must_terms, must_filters, should_terms, should_filters,
      query.Uniformity(Occur::Should), min_should_match, exclude_terms,
      exclude_filters, segment, ctx, merge, query.MergeType(), absorbed);
  }
  if (const auto own = query.MergeType(); own != merge) {
    auto child = Make(query, ctx, own);
    if (!child) {
      return {};
    }
    return memory::make_managed<ByWindowScored<Erased>>(
      merge, Erased{std::move(child)});
  }
  const bool has_should = !should_terms.empty() || !should_filters.empty();
  if (has_should && min_should_match == 0) {
    return MakeSparseBoostScored(
      must_terms, must_filters, should_terms, should_filters,
      query.Uniformity(Occur::Should), segment, ctx, merge, absorbed);
  }
  if (must_terms.empty() && must_filters.empty()) {
    if (!has_should) {
      return MakeAllScored(segment, merge, absorbed);
    }
    return min_should_match == 1
             ? MakeDisjunctionScored(should_terms, should_filters,
                                     query.Uniformity(Occur::Should), segment,
                                     ctx, merge, absorbed)
             : MakeThresholdScored(should_terms, should_filters,
                                   query.Uniformity(Occur::Should), segment,
                                   ctx, merge, min_should_match, absorbed);
  }
  if (min_should_match != 0) {
    auto node =
      lead::MakeRequiredScored(must_terms, must_filters, should_terms,
                               should_filters, query.Uniformity(Occur::Should),
                               min_should_match, segment, ctx, merge, absorbed);
    if (!node) {
      return {};
    }
    return memory::make_managed<ByWalkScored<lead::Erased>>(
      merge, lead::Erased{std::move(node)});
  }
  return MakeSparseConjunctionScored(must_terms, must_filters, segment, ctx,
                                     merge, absorbed);
}

}  // namespace irs::fill
