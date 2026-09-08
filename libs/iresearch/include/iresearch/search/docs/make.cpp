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

#include "iresearch/search/docs/make.hpp"

#include <span>
#include <utility>
#include <vector>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/boolean_of.hpp"
#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/docs/empty.hpp"
#include "iresearch/search/docs/masked.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/probe/plan.hpp"
#include "iresearch/search/query_builder_impl.hpp"

namespace irs::docs {

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
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() == 1) {
    return search::HeadIsTerm(terms, filters)
             ? MakePosting(terms.front(), segment, ctx)
             : filters.front()->PlanDocs(ctx);
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

Root::ptr MakeRequired(const BooleanQuery& query, const SubReader& segment,
                       const Context& ctx) {
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto min_match = query.MinShouldMatch();
  const auto no_must = must_terms.empty() && must_filters.empty();
  if (min_match == 0) {
    if (no_must) {
      return MakeAll(static_cast<doc_id_t>(segment.docs_count()), ctx);
    }
    return MakeConjunction(must_terms, must_filters, segment, ctx);
  }
  if (no_must) {
    const auto should_terms = query.Terms(Occur::Should);
    const auto should_filters = query.Queries(Occur::Should);
    return min_match == 1
             ? MakeDisjunction(should_terms, should_filters, segment, ctx)
             : MakeThreshold(should_terms, should_filters, segment, min_match,
                             ctx);
  }
  auto probe = probe::BuildOptionalProbe(
    query.Terms(Occur::Should), query.Queries(Occur::Should), min_match,
    segment, search::IncludeCandidates(must_terms, must_filters, segment));
  if (!probe) {
    return {};
  }
  return MakeSparseConjunctionWith(must_terms, must_filters, segment,
                                   std::move(probe), ctx);
}

Root::ptr MakeExclusion(const BooleanQuery& query, const SubReader& segment,
                        const Context& ctx) {
  const auto exclude_terms = query.Terms(Occur::MustNot);
  const auto exclude_filters = query.Queries(Occur::MustNot);
  SDB_ASSERT(!exclude_terms.empty() || !exclude_filters.empty());
  const auto must_terms = query.Terms(Occur::Must);
  const auto must_filters = query.Queries(Occur::Must);
  const auto candidates =
    search::IncludeCandidates(must_terms, must_filters, segment);
  if (query.MinShouldMatch() != 0) {
    auto driven = lead::MakeRequiredDocs(
      must_terms, must_filters, query.Terms(Occur::Should),
      query.Queries(Occur::Should), query.MinShouldMatch(), segment);
    if (!driven) {
      return {};
    }
    return MakeSparseExclusionOf(std::move(driven), exclude_terms,
                                 exclude_filters, segment, candidates, ctx);
  }
  if (must_terms.empty() && must_filters.empty()) {
    return MakeComplement(exclude_terms, exclude_filters, segment, ctx);
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
  if (query.Terms(Occur::MustNot).empty() &&
      query.Queries(Occur::MustNot).empty()) {
    return MakeRequired(query, segment, ctx);
  }
  return MakeExclusion(query, segment, ctx);
}

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakePosting(search::ClauseOf(terms.front(), field), query.Segment(),
                       ctx);
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
  if (query.Kind() == QueryKind::Empty) {
    return memory::make_managed<Empty>();
  }
  auto plan = query.PlanDocs(ctx);
  const auto* const docs_mask = query.Segment().docs_mask();
  if (docs_mask == nullptr || !plan) [[likely]] {
    return plan;
  }
  return memory::make_managed<Masked>(std::move(plan), *docs_mask);
}

}  // namespace irs::docs
