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

#include "basics/down_cast.h"
#include "basics/shared.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/terms_filter.hpp"

namespace irs {
namespace {

size_t ComputeMinMatch(size_t terms_count, float_t threshold) {
  threshold = std::clamp(threshold, 0.f, 1.f);
  return std::clamp(static_cast<size_t>(
                      std::ceil(static_cast<float_t>(terms_count) * threshold)),
                    size_t{1}, terms_count);
}

}  // namespace

void ByNGramSimilarity::Buffer::PrepareSegment(const SubReader& segment) {
  const TermReader* field = segment.field(_field);
  if (!field) {
    return;
  }

  if (NGramSimilarityQuery::kRequiredFeatures !=
      (field->meta().index_features &
       NGramSimilarityQuery::kRequiredFeatures)) {
    return;
  }

  _field_stats.collect(segment, *field);

  ManagedVector<SeekCookie::ptr> term_states{{*_memory}};
  term_states.reserve(_ngrams->size());

  size_t term_idx = 0;
  size_t count_terms = 0;
  auto term = field->iterator(SeekMode::NORMAL);
  for (const auto& ngram : *_ngrams) {
    auto& state = term_states.emplace_back();
    if (term->seek(ngram)) {
      term->read();
      _term_stats.collect(segment, *field, term_idx, *term);
      state = term->cookie();
      ++count_terms;
    }
    ++term_idx;
  }

  if (count_terms < _min_match_count) {
    return;
  }

  auto& state = _states.insert(segment);
  state.terms = std::move(term_states);
  state.reader = field;
}

void ByNGramSimilarity::Buffer::Merge(PrepareBuffer&& other) {
  auto& rhs = sdb::basics::downCast<Buffer>(other);
  _field_stats.collect(std::move(rhs._field_stats));
  _term_stats.collect(std::move(rhs._term_stats));
  _states.Merge(std::move(rhs._states));
}

Filter::Query::ptr ByNGramSimilarity::Buffer::Compile(
  const PrepareContext& ctx) && {
  bstring stats(GetStatsSize(ctx.scorer), 0);
  auto* stats_buf = stats.data();
  const auto terms_count = _ngrams->size();
  for (size_t term_idx = 0; term_idx < terms_count; ++term_idx) {
    _term_stats.finish(stats_buf, term_idx, _field_stats, ctx.index);
  }

  return memory::make_tracked<NGramSimilarityQuery>(
    ctx.memory, _min_match_count, std::move(_states), std::move(stats),
    ctx.boost);
}

std::unique_ptr<Filter::PrepareBuffer> ByNGramSimilarity::CreateBuffer(
  const PrepareContext& ctx) const {
  if (options().ngrams.empty() || field().empty()) {
    return std::make_unique<EmptyBuffer>();
  }

  const auto terms_count = options().ngrams.size();
  const auto min_match_count =
    ComputeMinMatch(terms_count, options().threshold);

  if (!ctx.scorer && 1 == min_match_count) {
    return std::make_unique<LazyQueryBuffer>(
      [ctx, field_name = std::string{field()},
       ngrams = options().ngrams](const PrepareContext&) {
        irs::ByTermsOptions opts;
        for (const auto& term : ngrams) {
          opts.terms.emplace(term, irs::kNoBoost);
        }
        return ByTerms::Prepare(ctx, field_name, opts);
      });
  }

  if (options().allow_phrase && min_match_count == terms_count) {
    return std::make_unique<LazyQueryBuffer>(
      [ctx, field_name = std::string{field()},
       ngrams = options().ngrams](const PrepareContext&) {
        irs::ByPhraseOptions opts;
        for (const auto& ngram : ngrams) {
          opts.push_back(ByTermOptions{ngram});
        }
        return ByPhrase::Prepare(ctx, field_name, opts);
      });
  }

  return std::make_unique<Buffer>(ctx, field(), options().ngrams,
                                  min_match_count);
}

Filter::Query::ptr ByNGramSimilarity::Prepare(
  const PrepareContext& ctx, std::string_view field_name,
  const std::vector<irs::bstring>& ngrams, float_t threshold,
  bool allow_phrase) {
  if (ngrams.empty() || field_name.empty()) {
    return Query::empty();
  }

  const auto terms_count = ngrams.size();
  const auto min_match_count = ComputeMinMatch(terms_count, threshold);

  if (!ctx.scorer && 1 == min_match_count) {
    irs::ByTermsOptions options;
    for (const auto& term : ngrams) {
      options.terms.emplace(term, irs::kNoBoost);
    }
    return ByTerms::Prepare(ctx, field_name, options);
  }

  if (allow_phrase && min_match_count == terms_count) {
    irs::ByPhraseOptions options;
    for (const auto& ngram : ngrams) {
      options.push_back(ByTermOptions{ngram});
    }
    return ByPhrase::Prepare(ctx, field_name, options);
  }

  Buffer buf{ctx, field_name, ngrams, min_match_count};
  for (const auto& segment : ctx.index) {
    buf.PrepareSegment(segment);
  }
  if (buf.Empty()) {
    return Query::empty();
  }
  return std::move(buf).Compile(ctx);
}

}  // namespace irs
