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

#include "iresearch/search/count/make.hpp"

#include <array>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/subtract.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/query_builder_impl.hpp"

namespace irs::count {

Root::ptr MakeDisjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(terms.size() + filters.size() > 1);
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  const auto docs_count = static_cast<doc_id_t>(segment.docs_count());
  if (auto folded = MakeBitsetDisjunction(terms, doc, rest, docs_count, ctx)) {
    return folded;
  }
  return MakeWindowDisjunction(terms, doc, rest, ctx);
}

Root::ptr MakeConjunction(std::span<const search::PostingClause> terms,
                          std::span<const QueryBuilder::ptr> filters,
                          const SubReader& segment, const Context& ctx) {
  SDB_ASSERT(terms.size() + filters.size() != 0);
  if (terms.size() + filters.size() == 1) {
    return search::HeadIsTerm(terms, filters)
             ? MakeTerm(terms.front(), segment, ctx)
             : filters.front()->PlanCount(ctx);
  }
  if (auto windowed = MakeWindowConjunction(terms, filters, segment, ctx)) {
    return windowed;
  }
  if (!filters.empty()) {
    if (auto folded = MakeBitsetConjunction(terms, filters, segment, ctx)) {
      return folded;
    }
  }
  return MakeSparseConjunction(terms, filters, segment, ctx);
}

Root::ptr MakeThreshold(std::span<const search::PostingClause> terms,
                        std::span<const QueryBuilder::ptr> filters,
                        const SubReader& segment, uint32_t min_match,
                        const Context& ctx) {
  SDB_ASSERT(min_match > 1);
  SDB_ASSERT(terms.size() + filters.size() >= min_match);
  SDB_ASSERT(min_match != terms.size() + filters.size());
  const IndexInput* doc = nullptr;
  std::vector<FillNode::ptr> rest;
  if (!CollectDense(terms, filters, nullptr, doc, rest)) {
    return {};
  }
  if (min_match > search::kBitplaneMaxMatch) {
    if (auto counted = MakeCountThreshold(terms, doc, rest, min_match, ctx)) {
      return counted;
    }
  }
  return MakeBitsThreshold(terms, doc, rest, min_match, ctx);
}

Root::ptr MakeRequired(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto min_match = query.MinShouldMatch();
  const bool no_must = must_terms.empty() && must_filters.empty();
  if (min_match == 0) {
    if (no_must) {
      return MakeAll(segment, ctx);
    }
    return MakeConjunction(must_terms, must_filters, segment, ctx);
  }
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  if (no_must) {
    return min_match == 1
             ? MakeDisjunction(should_terms, should_filters, segment, ctx)
             : MakeThreshold(should_terms, should_filters, segment, min_match,
                             ctx);
  }
  auto probe = probe::BuildOptionalProbe(
    should_terms, should_filters, min_match, segment,
    search::IncludeCandidates(must_terms, must_filters, segment));
  if (!probe) {
    return {};
  }
  return MakeSparseConjunctionWith(must_terms, must_filters, segment,
                                   std::move(probe), ctx);
}

namespace {

template<typename Term>
doc_id_t RarestOf(std::span<const Term> terms) noexcept {
  SDB_ASSERT(terms.size() == 2);
  return std::min(search::CookieOf(terms.front()).docs_count,
                  search::CookieOf(terms.back()).docs_count);
}

template<typename Term>
bool SubtractsPair(std::span<const Term> terms) noexcept {
  if (terms.size() != 2) {
    return false;
  }
  const auto densest = std::max(search::CookieOf(terms.front()).docs_count,
                                search::CookieOf(terms.back()).docs_count);
  return search::SubtractsDisjunction(RarestOf(terms), densest);
}

}  // namespace

Root::ptr MakeExclusion(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  const auto min_match = query.MinShouldMatch();
  if (min_match != 0) {
    auto driven = lead::MakeRequiredDocs(
      must_terms, must_filters, query.Terms(Occur::Should),
      query.Queries(Occur::Should), min_match, segment);
    if (!driven) {
      return {};
    }
    return MakeSparseExclusionOf(std::move(driven), exclude_terms,
                                 exclude_filters, segment, candidates, ctx);
  }
  if (must_terms.empty() && must_filters.empty()) {
    if (ctx.table != nullptr) {
      auto driven = lead::MakeAllDocs(segment);
      if (!driven) {
        return {};
      }
      return MakeSparseExclusionOf(std::move(driven), exclude_terms,
                                   exclude_filters, segment, candidates, ctx);
    }
    Root::ptr excluded;
    if (exclude_terms.size() + exclude_filters.size() == 1) {
      excluded = exclude_terms.empty()
                   ? exclude_filters.front()->PlanCount(ctx)
                   : MakeTerm(exclude_terms.front(), segment, ctx);
    } else {
      if (exclude_filters.empty() && SubtractsPair(exclude_terms)) {
        excluded = MakeSubtractDisjunction(exclude_terms.front(),
                                           exclude_terms.back(), segment, ctx);
      }
      if (!excluded) {
        excluded =
          MakeDisjunction(exclude_terms, exclude_filters, segment, ctx);
      }
    }
    if (!excluded) {
      return {};
    }
    return memory::make_managed<Subtract>(segment.docs_count(),
                                          std::move(excluded));
  }
  if (auto folded =
        MakeBitsetExclusion(must_terms, must_filters, exclude_terms,
                            exclude_filters, segment, candidates, ctx)) {
    return folded;
  }
  if (auto windowed =
        MakeWindowExclusion(must_terms, must_filters, exclude_terms,
                            exclude_filters, segment, candidates, ctx)) {
    return windowed;
  }
  return MakeSparseExclusion(must_terms, must_filters, exclude_terms,
                             exclude_filters, segment, candidates, ctx);
}

Root::ptr Make(const BooleanQuery& query, const Context& ctx) {
  const auto& segment = query.Segment();
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto should_terms = query.Terms(Occur::Should);
  const auto should_filters = query.Queries(Occur::Should);
  const bool no_must = must_terms.empty() && must_filters.empty();
  if (query.Terms(Occur::MustNot).empty() &&
      query.Queries(Occur::MustNot).empty()) {
    if (ctx.table == nullptr && should_terms.empty() &&
        should_filters.empty() && must_filters.empty() &&
        must_terms.size() == 2 &&
        search::SubtractsConjunction(
          RarestOf(must_terms), static_cast<doc_id_t>(segment.docs_count()))) {
      if (auto subtracted =
            MakeSubtractConjunction(must_terms, must_filters, segment, ctx)) {
        return subtracted;
      }
    } else if (ctx.table == nullptr && no_must && query.MinShouldMatch() == 1 &&
               should_filters.empty() && SubtractsPair(should_terms)) {
      if (auto subtracted = MakeSubtractDisjunction(
            should_terms.front(), should_terms.back(), segment, ctx)) {
        return subtracted;
      }
    }
    return MakeRequired(query, ctx);
  }
  return MakeExclusion(query, ctx);
}

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakeTerm(search::ClauseOf(terms.front(), field), query.Segment(),
                    ctx);
  }
  if (ctx.table == nullptr && SubtractsPair(terms)) {
    if (auto subtracted = MakeSubtractDisjunction(
          search::ClauseOf(terms.front(), field),
          search::ClauseOf(terms.back(), field), query.Segment(), ctx)) {
      return subtracted;
    }
  }
  return MakeDisjunctionOfTerms(
    terms, field, *search::DocOf(*field),
    static_cast<doc_id_t>(query.Segment().docs_count()), ctx);
}

Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx) {
  return search::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlop(query, ctx); },
    [&] { return MakeFixedPhraseIntervals(query, ctx); },
    [&] { return MakeFixedPhrase(query, ctx); });
}

Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx) {
  return search::ResolveMatch(
    query, [&] { return MakeVariadicPhraseSlop(query, ctx); },
    [&] { return MakeVariadicPhraseIntervals(query, ctx); },
    [&] { return MakeVariadicPhrase(query, ctx); });
}

Root::ptr Make(const NGramSimilarityQuery& query, const Context& ctx) {
  return query.Every() ? MakeNGramAll(query, ctx) : MakeNGram(query, ctx);
}

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  const auto& segment = query.Segment();
  if (query.Kind() == QueryKind::Empty) {
    return MakeConstant(0);
  }
  if (segment.docs_mask() == nullptr) [[likely]] {
    return query.PlanCount(ctx);
  }
  return MakeMasked(query, ctx);
}

}  // namespace irs::count
