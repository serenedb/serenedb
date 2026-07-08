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

#include "wildcard_filter.hpp"

#include "basics/exceptions.h"
#include "iresearch/search/automaton_filter.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {

QueryBuilder::ptr ByWildcard::PrepareSegment(const SubReader&,
                                             const PrepareContext&) const {
  SDB_THROW(sdb::ERROR_INTERNAL,
            "ByWildcard must be lowered by the optimizer before prepare");
}

Filter::ptr LowerWildcard(irs::field_id id, bytes_view term,
                          size_t scored_terms_limit, score_t boost) {
  bstring buf;
  return ExecuteWildcard(
    buf, term,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = term;
      filter->boost(boost);
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = term;
      filter->mutable_options()->scored_terms_limit = scored_terms_limit;
      filter->boost(boost);
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<AutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() =
        AutomatonOptions{FromWildcard(term), term, scored_terms_limit};
      filter->boost(boost);
      return filter;
    });
}

Filter::ptr CreateByWildcard(irs::field_id id, bytes_view term,
                             size_t scored_terms_limit, score_t boost) {
  auto filter = std::make_unique<ByWildcard>();
  *filter->mutable_field_id() = id;
  filter->mutable_options()->term = term;
  filter->mutable_options()->scored_terms_limit = scored_terms_limit;
  filter->boost(boost);
  return filter;
}

TermPredicate::ptr ByWildcard::CompileTermPredicate() const {
  auto acceptor = FromWildcard(options().term);
  if (!Validate(acceptor)) {
    return nullptr;
  }
  return MakeAutomatonTermPredicate(
    std::make_shared<const CompiledAcceptor>(std::move(acceptor)));
}

}  // namespace irs
