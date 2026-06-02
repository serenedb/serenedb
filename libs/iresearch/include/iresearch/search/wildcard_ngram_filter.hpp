////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2014-2024 ArangoDB GmbH, Cologne, Germany
/// Copyright 2004-2014 triAGENS GmbH, Cologne, Germany
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
/// @author Valery Mironov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <re2/re2.h>

#include <cstddef>
#include <string>
#include <vector>

#include "iresearch/search/filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {
namespace analysis {

class WildcardAnalyzer;

}  // namespace analysis

class ByWildcardNgram;

struct ByWildcardNgramOptions {
  using FilterType = ByWildcardNgram;

  std::vector<ByPhraseOptions> parts;
  bstring token;
  bool has_pos{true};
  std::shared_ptr<RE2> matcher;
  field_id store_field_id{0};

  bool operator==(const ByWildcardNgramOptions& other) const noexcept {
    if (parts != other.parts || token != other.token ||
        has_pos != other.has_pos || store_field_id != other.store_field_id) {
      return false;
    }
    if (!matcher && !other.matcher) {
      return true;
    }
    if (!matcher || !other.matcher) {
      return false;
    }
    return matcher->pattern() == other.matcher->pattern();
  }

  ByWildcardNgramOptions() noexcept = default;
  ByWildcardNgramOptions(ByWildcardNgramOptions&&) noexcept = default;
  ByWildcardNgramOptions& operator=(ByWildcardNgramOptions&&) noexcept =
    default;

  // Build options from a LIKE wildcard pattern using the given analyzer.
  // has_positions indicates whether the index has position features enabled.
  ByWildcardNgramOptions(std::string_view pattern,
                         analysis::WildcardAnalyzer& analyzer,
                         bool has_positions);
};

class ByWildcardNgram final : public FilterWithField<ByWildcardNgramOptions> {
 public:
  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;

  PrepareCollector::ptr MakeCollector(const Scorer* scorer) const final;
};

}  // namespace irs
