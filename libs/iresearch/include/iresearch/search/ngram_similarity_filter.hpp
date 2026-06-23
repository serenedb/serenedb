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

#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByNGramSimilarity;

size_t MinMatchCount(size_t terms_count, float_t threshold) noexcept;

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
  static QueryBuilder::ptr PrepareSegment(
    const SubReader& segment, const PrepareContext& ctx,
    irs::field_id field_name, const std::vector<irs::bstring>& ngrams,
    float_t threshold, score_t boost);

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final {
    return PrepareSegment(segment, ctx, field_id(), options().ngrams,
                          options().threshold, Boost());
  }

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;
};

}  // namespace irs
