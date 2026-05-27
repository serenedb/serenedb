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

// How a wildcard pattern dispatches once resolved.
enum class WildcardKind { Term, Prefix, Wildcard };

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

  // Sets the raw wildcard `term` and resolves it once into a dispatch kind and
  // the bytes the prepared buffer borrows. Must be used instead of writing
  // `term` directly so the resolved form stays in sync.
  void set_term(bytes_view term);

  WildcardKind kind() const noexcept { return _kind; }
  bytes_view resolved() const noexcept { return _resolved; }

  bool operator==(const ByWildcardOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }

 private:
  WildcardKind _kind{WildcardKind::Term};
  bstring _resolved;
};

// User-side wildcard filter
class ByWildcard final : public FilterWithField<ByWildcardOptions> {
 public:
  static Query::ptr Prepare(const PrepareContext& ctx, std::string_view field,
                            bytes_view term, size_t scored_terms_limit);

  static field_visitor visitor(bytes_view term);

  std::unique_ptr<PrepareBuffer> CreateBuffer(
    const PrepareContext& ctx) const final;

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return PrepareWithBuffer(*CreateBuffer(ctx), ctx);
  }
};

}  // namespace irs
