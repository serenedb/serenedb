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

#include "iresearch/search/detail/term_acceptor.hpp"
#include "iresearch/search/filters/automaton_filter.hpp"
#include "iresearch/search/filters/prefix_filter.hpp"
#include "iresearch/search/filters/term_filter.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs {

QueryBuilder::ptr ByRegexp::PrepareSegment(const SubReader&,
                                           const PrepareContext&) const {
  THROW_SQL_ERROR(
    ERR_MSG("ByRegexp must be lowered by the optimizer before prepare"));
}

Filter::ptr LowerRegexp(irs::field_id id, bytes_view pattern,
                        RegexpSyntax syntax, score_t boost) {
  bstring buf;
  return ExecuteRegexp(
    buf, pattern,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = term;
      filter->SetBoost(boost);
      return filter;
    },
    [&](bytes_view prefix) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field_id() = id;
      filter->mutable_options()->term = prefix;
      filter->SetBoost(boost);
      return filter;
    },
    [&](bytes_view pattern) -> Filter::ptr {
      auto filter = std::make_unique<AutomatonFilter>();
      *filter->mutable_field_id() = id;
      *filter->mutable_options() =
        AutomatonOptions{pattern, RegexpPattern(syntax)};
      filter->SetBoost(boost);
      return filter;
    });
}

Filter::ptr CreateByRegexp(irs::field_id id, bytes_view pattern,
                           RegexpSyntax syntax, score_t boost) {
  auto filter = std::make_unique<ByRegexp>();
  *filter->mutable_field_id() = id;
  filter->mutable_options()->pattern = pattern;
  filter->mutable_options()->syntax = syntax;
  filter->SetBoost(boost);
  return filter;
}

TermPredicate::ptr ByRegexp::CompileTermPredicate() const {
  bstring buf;
  return ExecuteRegexp(
    buf, options().pattern,
    [](bytes_view term) -> TermPredicate::ptr {
      return MakeTermPredicate(
        [term = bstring{term}](bytes_view key) { return key == term; });
    },
    [](bytes_view prefix) -> TermPredicate::ptr {
      return MakeTermPredicate([prefix = bstring{prefix}](bytes_view key) {
        return key.starts_with(prefix);
      });
    },
    [&](bytes_view pattern) -> TermPredicate::ptr {
      const auto source =
        MakePatternSource(pattern, RegexpPattern(options().syntax));
      return source->ok() ? source->Predicate() : nullptr;
    });
}

}  // namespace irs
