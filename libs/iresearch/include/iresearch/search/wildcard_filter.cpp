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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {

ByWildcardFilterOptions::ByWildcardFilterOptions(bytes_view pattern)
  : term{pattern},
    acceptor{std::make_shared<const automaton>(FromWildcard(pattern))} {}

field_visitor ByWildcard::visitor(std::shared_ptr<const automaton> acceptor) {
  if (!acceptor || !Validate(*acceptor)) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  struct AutomatonContext : util::Noncopyable {
    explicit AutomatonContext(std::shared_ptr<const automaton> a)
      : acceptor{std::move(a)}, matcher{MakeAutomatonMatcher(*acceptor)} {}

    std::shared_ptr<const automaton> acceptor;
    automaton_table_matcher matcher;
  };

  auto ctx = std::make_shared<AutomatonContext>(std::move(acceptor));

  return
    [ctx = std::move(ctx)](const SubReader& segment, const TermReader& field,
                           FilterVisitor& visitor) mutable {
      return irs::Visit(segment, field, ctx->matcher, visitor);
    };
}

Filter::Query::ptr ByWildcard::prepare(const PrepareContext& ctx) const {
  SDB_ASSERT(options().acceptor);
  return PrepareAutomatonFilter(ctx.Boost(Boost()), field(),
                                *options().acceptor,
                                options().scored_terms_limit);
}

Filter::ptr CreateByWildcard(std::string_view field, bytes_view term,
                             size_t scored_terms_limit, score_t boost) {
  bstring buf;
  return ExecuteWildcard(
    buf, term,
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByTerm>();
      *filter->mutable_field() = field;
      filter->mutable_options()->term = term;
      filter->boost(boost);
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByPrefix>();
      *filter->mutable_field() = field;
      filter->mutable_options()->term = term;
      filter->mutable_options()->scored_terms_limit = scored_terms_limit;
      filter->boost(boost);
      return filter;
    },
    [&](bytes_view term) -> Filter::ptr {
      auto filter = std::make_unique<ByWildcard>();
      *filter->mutable_field() = field;
      auto& options = *filter->mutable_options();
      options = ByWildcardOptions{term};
      options.scored_terms_limit = scored_terms_limit;
      filter->boost(boost);
      return filter;
    });
}

}  // namespace irs
