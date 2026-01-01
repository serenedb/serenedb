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
  static Query::ptr Prepare(const PrepareContext& ctx,
                            std::string_view field_name,
                            const std::vector<irs::bstring>& ngrams,
                            float_t threshold, bool allow_phrase = true);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return Prepare(ctx.Boost(Boost()), field(), options().ngrams,
                   options().threshold, options().allow_phrase);
  }
};

}  // namespace irs
