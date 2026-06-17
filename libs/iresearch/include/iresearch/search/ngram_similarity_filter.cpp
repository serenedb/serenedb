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
#include "iresearch/search/terms_filter.hpp"

namespace irs {
namespace {

size_t MinMatchCount(size_t terms_count, float_t threshold) noexcept {
  threshold = std::clamp(threshold, 0.f, 1.f);
  return std::clamp(static_cast<size_t>(
                      std::ceil(static_cast<float_t>(terms_count) * threshold)),
                    size_t{1}, terms_count);
}

}  // namespace

QueryBuilder::ptr ByNGramSimilarity::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx, irs::field_id field_name,
  const std::vector<irs::bstring>& ngrams, float_t threshold) {
  if (ngrams.empty() || !irs::field_limits::valid(field_name)) {
    return QueryBuilder::Empty();
  }
  const auto terms_count = ngrams.size();
  const auto min_match_count = MinMatchCount(terms_count, threshold);

  auto* ngram_collector = dynamic_cast<NGramCollector*>(ctx.collector);
  if (ngram_collector == nullptr) {
    irs::ByTermsOptions options;
    for (const auto& term : ngrams) {
      options.terms.emplace(term, irs::kNoBoost);
    }
    return ByTerms::PrepareSegment(segment, ctx, field_name, options);
  }

  const TermReader* field = segment.field(field_name);

  if (!field) {
    return nullptr;
  }

  if (NGramSimilarityQuery::kRequiredFeatures !=
      (field->meta().index_features &
       NGramSimilarityQuery::kRequiredFeatures)) {
    return nullptr;
  }

  ngram_collector->Field().Collect(*field);

  NGramState state{ctx.memory};
  state.terms.reserve(terms_count);

  size_t term_idx = 0;
  size_t count_terms = 0;
  auto term = field->iterator(SeekMode::NORMAL);
  for (const auto& ngram : ngrams) {
    auto& term_state = state.terms.emplace_back();
    if (term->seek(ngram)) {
      term->read();
      ngram_collector->Terms().Collect(term_idx, *term);
      term_state = term->cookie();
      ++count_terms;
    }

    ++term_idx;
  }

  if (count_terms < min_match_count) {
    return nullptr;
  }

  state.reader = field;

  return memory::make_tracked<NGramSimilarityQuery>(
    ctx.memory, segment, min_match_count, std::move(state), ctx.boost);
}

PrepareCollector::ptr ByNGramSimilarity::MakeCollector(
  const Scorer* scorer) const {
  const auto& ngrams = options().ngrams;
  const auto terms_count = ngrams.size();
  if (ngrams.empty() || !irs::field_limits::valid(field_id())) {
    return std::make_unique<NoopCollector>();
  }
  const auto min_match_count = MinMatchCount(terms_count, options().threshold);
  if (!scorer && 1 == min_match_count) {
    irs::ByTermsOptions opts;
    for (const auto& term : ngrams) {
      opts.terms.emplace(term, irs::kNoBoost);
    }
    return std::make_unique<TermsCollector>(scorer, opts.terms.size());
  }
  return std::make_unique<NGramCollector>(scorer, terms_count);
}

}  // namespace irs
