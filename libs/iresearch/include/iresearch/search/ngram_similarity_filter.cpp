////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2020 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrei Lobov
////////////////////////////////////////////////////////////////////////////////

#include "ngram_similarity_filter.hpp"

#include "basics/shared.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/search/term_query.hpp"
#include "iresearch/search/term_set.hpp"

namespace irs {

size_t MinMatchCount(size_t terms_count, float_t threshold) noexcept {
  threshold = std::clamp(threshold, 0.f, 1.f);
  return std::clamp(static_cast<size_t>(
                      std::ceil(static_cast<float_t>(terms_count) * threshold)),
                    size_t{1}, terms_count);
}

QueryBuilder::ptr ByNGramSimilarity::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field_name,
  const std::vector<irs::bstring>& ngrams, float_t threshold, score_t boost) {
  SDB_ASSERT(irs::field_limits::valid(field_name));
  SDB_ASSERT(!ngrams.empty());
  const auto terms_count = ngrams.size();
  const auto min_match_count = MinMatchCount(terms_count, threshold);

  if (terms_count == 1) {
    return ByTerm::PrepareSegment(segment, ctx, field_name, ngrams.front());
  }

  auto* ngram_collector =
    ctx.collector ? &sdb::basics::downCast<PhraseCollector>(*ctx.collector)
                  : nullptr;

  const TermReader* field = segment.field(field_name);

  if (!field) {
    return QueryBuilder::Empty();
  }

  NGramState state{ctx.memory};
  state.reader = field;
  if (!search::ResolvePhrase(field, state.handles)) {
    return QueryBuilder::Empty();
  }

  if (ngram_collector) {
    ngram_collector->Field(ctx.thread).Collect(*field);
  }

  state.total_terms = terms_count;
  state.terms.reserve(terms_count);

  size_t term_idx = 0;
  auto term = field->iterator();
  for (const auto& ngram : ngrams) {
    std::vector<TermCollector>* part = nullptr;
    if (ngram_collector) {
      part = &ngram_collector->Part(ctx.thread, term_idx);
      if (part->empty()) {
        part->emplace_back();
      }
    }
    if (term->seek(ngram)) {
      const auto& term_state = state.terms.emplace_back(term->cookie());
      if (part) {
        part->front().Collect(term_state);
      }
    }
    ++term_idx;
    if (!ngram_collector &&
        state.terms.size() + (terms_count - term_idx) < min_match_count) {
      return QueryBuilder::Empty();
    }
  }

  if (state.terms.size() < min_match_count) {
    return QueryBuilder::Empty();
  }

  if (state.terms.size() == 1) {
    return MakeTermQuery(ctx.memory, segment, field, state.terms.front(),
                         ctx.boost * boost, ctx.Record());
  }

  auto query = memory::make_tracked<NGramSimilarityQuery>(
    ctx.memory, segment, min_match_count, std::move(state), ctx.boost * boost);
  query->SetStats(ctx.Record());
  return query;
}

PrepareCollector::ptr ByNGramSimilarity::MakeCollectorImpl(
  const Scorer* scorer, StatsArena& stats, uint32_t threads) const {
  const auto& ngrams = options().ngrams;
  const auto terms_count = ngrams.size();
  SDB_ASSERT(irs::field_limits::valid(field_id()));
  if (terms_count == 1) {
    return std::make_unique<ByTermsCollector>(scorer, 1, stats, threads);
  }
  return std::make_unique<PhraseCollector>(scorer, terms_count, stats, threads);
}

}  // namespace irs
