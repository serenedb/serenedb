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

#include "regexp_filter.hpp"

#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs {

Filter::Query::ptr ByRegexp::prepare(const PrepareContext&) const {
  SDB_VERIFY(false, "ByRegexp must be lowered by the optimizer before prepare");
}

Filter::ptr LowerRegexp(irs::field_id id, bytes_view pattern,
                        RegexpSyntax syntax, size_t scored_terms_limit,
                        score_t boost) {
  bstring buf;
  return ExecuteRegexp(
    buf, pattern,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = term;
      filter->boost(boost);
      return filter;
    },
    [&](bytes_view prefix) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = prefix;
      filter->mutable_options()->scored_terms_limit = scored_terms_limit;
      filter->boost(boost);
      return filter;
    },
    [&](bytes_view pattern) -> Filter::ptr {
      auto filter = std::make_unique<AutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() =
        AutomatonOptions{FromRegexp(pattern, kDefaultMaxDfaStates, syntax),
                         pattern, scored_terms_limit};
      filter->boost(boost);
      return filter;
    });
}

Filter::ptr CreateByRegexp(irs::field_id id, bytes_view pattern,
                           RegexpSyntax syntax, size_t scored_terms_limit,
                           score_t boost) {
  auto filter = std::make_unique<ByRegexp>();
  *filter->mutable_field_id() = id;
  filter->mutable_options()->pattern = pattern;
  filter->mutable_options()->syntax = syntax;
  filter->mutable_options()->scored_terms_limit = scored_terms_limit;
  filter->boost(boost);
  return filter;
}

}  // namespace irs
