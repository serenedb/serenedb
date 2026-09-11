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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/ngram_of.hpp"
#include "iresearch/search/detail/phrase_of.hpp"
#include "iresearch/search/docs/empty.hpp"
#include "iresearch/search/docs/make_boolean.hpp"
#include "iresearch/search/docs/masked.hpp"
#include "iresearch/search/docs/plan.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"
#include "iresearch/search/queries/ngram_similarity_query.hpp"
#include "iresearch/search/queries/phrase_query.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"

namespace irs::docs {

Root::ptr Make(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (terms.size() == 1) {
    return MakePosting(detail::ClauseOf(terms.front(), field), query.Segment(),
                       ctx);
  }
  return MakeDisjunctionOfTerms(
    terms, field, *detail::DocOf(*field),
    static_cast<doc_id_t>(query.Segment().docs_count()), ctx);
}

Root::ptr Make(const FixedPhraseQuery& query, const Context& ctx) {
  return detail::ResolveMatch(
    query, [&] { return MakeFixedPhraseSlop(query, ctx); },
    [&] { return MakeFixedPhraseIntervals(query, ctx); },
    [&] { return MakeFixedPhrase(query, ctx); });
}

Root::ptr Make(const VariadicPhraseQuery& query, const Context& ctx) {
  return detail::ResolveMatch(
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
