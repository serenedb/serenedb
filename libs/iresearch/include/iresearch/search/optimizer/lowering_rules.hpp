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

#pragma once

#include <array>
#include <string_view>

#include "iresearch/search/filter_optimizer.hpp"
#include "iresearch/search/levenshtein_filter.hpp"
#include "iresearch/search/ngram_similarity_filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"

namespace irs::optimizer {

struct WildcardLowerRule {
  static constexpr std::string_view kName = "wildcard_lower";
  static constexpr std::array kTargets{Type<ByWildcard>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct RegexpLowerRule {
  static constexpr std::string_view kName = "regexp_lower";
  static constexpr std::array kTargets{Type<ByRegexp>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct EditDistanceLowerRule {
  static constexpr std::string_view kName = "edit_distance_lower";
  static constexpr std::array kTargets{Type<ByEditDistance>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct PhraseLowerRule {
  static constexpr std::string_view kName = "phrase_lower";
  static constexpr std::array kTargets{Type<ByPhrase>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

struct NGramSimilarityLowerRule {
  static constexpr std::string_view kName = "ngram_similarity_lower";
  static constexpr std::array kTargets{Type<ByNGramSimilarity>::id()};
  static constexpr bool kEnable = true;

  static bool Apply(Filter::ptr& slot, const OptimizeContext& ctx);
};

}  // namespace irs::optimizer
