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

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/ngram_of.hpp"
#include "iresearch/search/common/phrase_of.hpp"
#include "iresearch/search/count/make_boolean.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/phrase_query.hpp"
#include "iresearch/search/query_builder_impl.hpp"

namespace irs::count {

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
