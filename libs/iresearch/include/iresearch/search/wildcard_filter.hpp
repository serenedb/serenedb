////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByWildcard;
struct FilterVisitor;

struct ByWildcardFilterOptions {
  bstring term;

  bool operator==(const ByWildcardFilterOptions& rhs) const noexcept {
    return term == rhs.term;
  }
};

// Options for wildcard filter
struct ByWildcardOptions : ByWildcardFilterOptions {
  using FilterType = ByWildcard;
  using filter_options = ByWildcardFilterOptions;

  // The maximum number of most frequent terms to consider for scoring
  size_t scored_terms_limit{1024};

  bool operator==(const ByWildcardOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

// User-side wildcard filter
class ByWildcard final : public FilterWithField<ByWildcardOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            bytes_view term, size_t scored_terms_limit);

  static field_visitor visitor(bytes_view term);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return prepare(ctx.Boost(Boost()), field(), options().term,
                   options().scored_terms_limit);
  }
};

}  // namespace irs
