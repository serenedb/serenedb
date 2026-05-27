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

#pragma once

#include "iresearch/search/collectors.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/ngram_similarity_query.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByNGramSimilarity;

// Options for ngram similarity filter
struct ByNGramSimilarityOptions {
  using FilterType = ByNGramSimilarity;

  std::vector<bstring> ngrams;
  float_t threshold{1.F};
#ifdef SDB_GTEST
  bool allow_phrase{true};
#else
  static constexpr bool allow_phrase{true};
#endif

  bool operator==(const ByNGramSimilarityOptions& rhs) const noexcept {
    return ngrams == rhs.ngrams && threshold == rhs.threshold;
  }
};

class ByNGramSimilarity : public FilterWithField<ByNGramSimilarityOptions> {
 public:
  class Buffer final : public ScoredBuffer {
   public:
    Buffer(const PrepareContext& ctx, std::string_view field,
           const std::vector<bstring>& ngrams, size_t min_match_count,
           score_t boost = kNoBoost)
      : ScoredBuffer{ctx, boost},
        _field{field},
        _ngrams{&ngrams},
        _min_match_count{min_match_count},
        _field_stats{ctx.scorer},
        _term_stats{ctx.scorer, ngrams.size()},
        _states{ctx.memory, ctx.index.size()},
        _term_states{{ctx.memory}} {}

    void PrepareSegment(const SubReader& segment) final;
    void Merge(PrepareBuffer&& other) final;
    bool Empty() const noexcept final { return _states.empty(); }
    Query::ptr Compile(const PrepareContext& ctx) && final;

   private:
    std::string_view _field;
    const std::vector<bstring>* _ngrams;
    size_t _min_match_count;
    FieldCollectors _field_stats;
    TermCollectors _term_stats;
    NGramStates _states;
    ManagedVector<SeekCookie::ptr> _term_states;
  };

  static Query::ptr Prepare(const PrepareContext& ctx,
                            std::string_view field_name,
                            const std::vector<irs::bstring>& ngrams,
                            float_t threshold, bool allow_phrase = true);

  std::unique_ptr<PrepareBuffer> CreateBuffer(
    const PrepareContext& ctx) const final;

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return Prepare(ctx.Boost(Boost()), field(), options().ngrams,
                   options().threshold, options().allow_phrase);
  }
};

}  // namespace irs
