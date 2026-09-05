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

#include "iresearch/search/top/make.hpp"

#include <span>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/query_builder_impl.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/top/detail/walk.hpp"
#include "iresearch/search/top/empty.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"

namespace irs::top {
namespace {

template<typename Query>
Root::ptr MakeUnscored(const Query& query, const Context& ctx) {
  auto node = lead::Make(query);
  if (!node) {
    return {};
  }
  return MakeShape<detail::ConstantWalk, lead::Erased>(
    ctx, score_t{0}, lead::Erased{std::move(node)});
}

}  // namespace

Root::ptr MakeEmpty() { return memory::make_managed<Empty>(); }

Root::ptr Make(const TermQuery& query, const Context& ctx) {
  const PostingClause posting{.state = query.State(),
                              .boost = query.Boost(),
                              .stats = query.Stats(ScoredOf(ctx))};
  if (posting.state.cookie.docs_count == 1) {
    return MakeSinglePosting(posting, query.Segment(), ctx);
  }
  if (ctx.prune) {
    if (auto pruned = MakePrunedPosting(posting, query.Segment(), ctx)) {
      return pruned;
    }
  }
  return MakePosting(posting, query.Segment(), ctx);
}

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto merge = query.MergeType();
  const auto* const field = state.Reader();
  const auto* const scorer = query.Stats(ScoredOf(ctx)).scorer;
  const auto boost = query.Boost();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    const auto posting = search::ClauseOf(terms.front(), field, scorer, boost);
    if (posting.state.cookie.docs_count == 1) {
      return MakeSinglePosting(posting, query.Segment(), ctx);
    }
    if (ctx.prune) {
      if (auto pruned = MakePrunedPosting(posting, query.Segment(), ctx)) {
        return pruned;
      }
    }
    return MakePosting(posting, query.Segment(), ctx);
  }
  const auto uniformity = search::UniformityOf(*state.Reader(), scorer);
  if (ctx.prune) {
    if (merge == ScoreMergeType::Sum) {
      if (auto pruned =
            MakeMaxScoreDisjunction(terms, {}, uniformity, field, scorer, boost,
                                    query.Segment(), ctx, merge)) {
        return pruned;
      }
    }
  }
  return MakeWindowDisjunction(terms, {}, uniformity, field, scorer, boost,
                               query.Segment(), ctx, merge, 0);
}

Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return MakeUnscored(query, ctx);
  }
  const auto match = search::MatchOf(query);
  if (match == search::PhraseMatch::Slop) {
    return MakeFixedPhraseSlop(query, ctx);
  }
  const auto intervals = match == search::PhraseMatch::Intervals;
  if (ctx.prune) {
    if (auto pruned = intervals ? MakeFixedPhraseIntervalsPruned(query, ctx)
                                : MakeFixedPhrasePruned(query, ctx)) {
      return pruned;
    }
  }
  return intervals ? MakeFixedPhraseIntervals(query, ctx)
                   : MakeFixedPhrase(query, ctx);
}

Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return MakeUnscored(query, ctx);
  }
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlop(query, ctx); },
    [&] { return MakeVariadicPhraseIntervals(query, ctx); },
    [&] { return MakeVariadicPhrase(query, ctx); });
}

Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx) {
  if (query.Stats().stats == nullptr) {
    return MakeUnscored(query, ctx);
  }
  return query.Every() ? MakeNGramAll(query, ctx) : MakeNGram(query, ctx);
}

Root::ptr Make(const AllQuery& query, const Context& ctx) {
  return MakeAll(query.Segment(), ctx, query.Stats(ScoredOf(ctx)),
                 query.Boost());
}

Root::ptr Make(const WildcardNgramQuery& query, const Context& ctx) {
  return MakeWildcardNgram(query, ctx);
}

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto merge = query.MergeType();
  const auto absorbed = query.Absorbed();
  const std::span must = query.Terms(Occur::Must);
  const std::span must_filters = query.Queries(Occur::Must);
  const std::span should = query.Terms(Occur::Should);
  const std::span should_filters = query.Queries(Occur::Should);
  const auto min_match = query.MinShouldMatch();

  const bool optional = !should.empty() || !should_filters.empty();
  const bool only_scores = optional && min_match == 0;

  if (!query.Terms(Occur::MustNot).empty() ||
      !query.Queries(Occur::MustNot).empty()) {
    return MakeSparseExclusion(query, segment, ctx, merge, absorbed);
  }
  if (must.empty() && must_filters.empty() && !only_scores) {
    if (!optional) {
      return MakeAll(segment, ctx, absorbed);
    }
    const auto uniformity = query.Uniformity(Occur::Should);
    if (min_match == 1) {
      if (ctx.prune && merge == ScoreMergeType::Sum && absorbed == 0) {
        if (auto pruned = MakeMaxScoreDisjunction(
              should, should_filters, uniformity, nullptr, nullptr, kNoBoost,
              segment, ctx, merge)) {
          return pruned;
        }
      }
      return MakeWindowDisjunction(should, should_filters, uniformity, nullptr,
                                   nullptr, kNoBoost, segment, ctx, merge,
                                   absorbed);
    }
    if (min_match > search::kBitplaneMaxMatch) {
      if (auto counted =
            MakeCountThreshold(should, should_filters, uniformity, segment, ctx,
                               merge, min_match, absorbed)) {
        return counted;
      }
    }
    return MakeBitsThreshold(should, should_filters, uniformity, segment, ctx,
                             merge, min_match, absorbed);
  }
  if (ctx.prune && !optional && merge == ScoreMergeType::Sum && absorbed == 0) {
    if (auto pruned =
          MakeWandConjunction(must, must_filters, query.Uniformity(Occur::Must),
                              segment, ctx, merge)) {
      return pruned;
    }
  }
  return MakeSparseConjunction(query, segment, ctx, merge, absorbed);
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeEmpty();
  }
  const auto* const mask = query.Segment().docs_mask();
  if (mask != nullptr) [[unlikely]] {
    return MakeMasked(query, ctx, *mask);
  }
  return query.PlanTop(ctx);
}

}  // namespace irs::top
