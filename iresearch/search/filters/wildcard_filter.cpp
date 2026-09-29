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

#include "iresearch/search/detail/term_acceptor.hpp"
#include "iresearch/search/filters/automaton_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {

QueryBuilder::ptr ByWildcard::PrepareSegment(const SubReader&,
                                             const PrepareContext&) const {
  THROW_SQL_ERROR(
    ERR_MSG("ByWildcard must be lowered by the optimizer before prepare"));
}

Filter::ptr LowerWildcard(irs::field_id id, bytes_view term, score_t boost) {
  bstring buf;
  return ExecuteWildcard(
    buf, term,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = term;
      filter->SetBoost(boost);
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = term;
      filter->SetBoost(boost);
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<AutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() =
        AutomatonOptions{term, PatternKind::Wildcard};
      filter->SetBoost(boost);
      return filter;
    });
}

Filter::ptr CreateByWildcard(irs::field_id id, bytes_view term, score_t boost) {
  auto filter = std::make_unique<ByWildcard>();
  *filter->mutable_field_id() = id;
  filter->mutable_options()->term = term;
  filter->SetBoost(boost);
  return filter;
}

TermPredicate::ptr ByWildcard::CompileTermPredicate() const {
  const auto source = MakePatternSource(options().term, PatternKind::Wildcard);
  if (!source->ok()) {
    return nullptr;
  }
  return source->Predicate();
}

}  // namespace irs
