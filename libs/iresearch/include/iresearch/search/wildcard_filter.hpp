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

#include <memory>

#include "basics/shared.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/automaton_decl.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {

class ByWildcard;
struct FilterVisitor;

inline bytes_view Unescape(bytes_view in, bstring& out) {
  out.reserve(in.size());

  bool copy = true;
  for (byte_type c : in) {
    copy = c == WildcardMatch::kEscape ? !copy : true;
    if (copy) {
      out.push_back(c);
    }
  }

  return out;
}

template<typename Term, typename Prefix, typename WildCard>
auto ExecuteWildcard(bstring& buf, bytes_view term, Term&& t, Prefix&& p,
                     WildCard&& w) {
  switch (ComputeWildcardType(term)) {
    case WildcardType::TermEscaped:
      term = Unescape(term, buf);
      [[fallthrough]];
    case WildcardType::Term:
      return t(term);
    case WildcardType::PrefixEscaped:
      term = Unescape(term, buf);
      [[fallthrough]];
    case WildcardType::Prefix: {
      SDB_ASSERT(!term.empty());
      const auto idx = term.find_first_of(WildcardMatch::kAnyStr);
      SDB_ASSERT(idx != bytes_view::npos);
      term = bytes_view{term.data(), idx};  // remove trailing '%'
      return p(term);
    }
    case WildcardType::Wildcard:
      return w(term);
  }
}

struct ByWildcardFilterOptions {
  bstring term;
  automaton acceptor;

  ByWildcardFilterOptions() = default;
  // Stores the pattern and compiles its automaton, keeping the two in sync.
  explicit ByWildcardFilterOptions(bytes_view pattern);

  bool operator==(const ByWildcardFilterOptions& rhs) const noexcept {
    return term == rhs.term;
  }
};

// Options for wildcard filter
struct ByWildcardOptions : ByWildcardFilterOptions {
  using FilterType = ByWildcard;
  using filter_options = ByWildcardFilterOptions;
  using ByWildcardFilterOptions::ByWildcardFilterOptions;

  // The maximum number of most frequent terms to consider for scoring
  size_t scored_terms_limit{1024};

  bool operator==(const ByWildcardOptions& rhs) const noexcept = default;
};

// Resolves a wildcard pattern into a concrete filter at construction time:
// a ByTerm (exact term), a ByPrefix (prefix scan), or a ByWildcard (genuine
// wildcard, with its automaton compiled eagerly).
Filter::ptr CreateByWildcard(irs::field_id id, bytes_view term,
                             size_t scored_terms_limit = 1024,
                             score_t boost = kNoBoost);

// User-side wildcard filter. Handles only genuine wildcard patterns; the
// automaton is compiled at construction and stored in the options.
class ByWildcard final : public FilterWithField<ByWildcardOptions> {
 public:
  static field_visitor visitor(const automaton& acceptor);

  Query::ptr prepare(const PrepareContext& ctx) const final;
};

}  // namespace irs
