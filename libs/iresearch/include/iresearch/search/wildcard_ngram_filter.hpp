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

#include <unicode/regex.h>

#include <cstddef>
#include <string>
#include <vector>

#include "iresearch/search/filter.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

namespace analysis {
class WildcardAnalyzer;
}

class WildcardFilter;

struct WildcardFilterOptions {
  using FilterType = WildcardFilter;

  std::vector<ByPhraseOptions> parts;
  bstring token;
  bool has_pos{true};
  std::shared_ptr<icu::RegexMatcher> matcher;

  bool operator==(const WildcardFilterOptions&) const noexcept { return false; }

  WildcardFilterOptions() noexcept = default;
  WildcardFilterOptions(WildcardFilterOptions&&) noexcept = default;
  WildcardFilterOptions& operator=(WildcardFilterOptions&&) noexcept = default;

  // Build options from a LIKE wildcard pattern using the given analyzer.
  // has_positions indicates whether the index has position features enabled.
  WildcardFilterOptions(std::string_view pattern,
                        analysis::WildcardAnalyzer& analyzer,
                        bool has_positions);
};

class WildcardFilter final : public FilterWithField<WildcardFilterOptions> {
 public:
  static Query::ptr Prepare(const PrepareContext& ctx, std::string_view field,
                            const WildcardFilterOptions& options);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return Prepare(ctx.Boost(Boost()), field(), options());
  }
};

}  // namespace irs
