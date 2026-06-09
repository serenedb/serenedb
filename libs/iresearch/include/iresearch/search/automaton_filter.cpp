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

#include "automaton_filter.hpp"

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/utils/automaton_utils.hpp"

namespace irs {

field_visitor AutomatonFilter::visitor(const automaton& acceptor) {
  if (!Validate(acceptor)) {
    return [](const SubReader&, const TermReader&, FilterVisitor&) {};
  }

  struct AutomatonContext : util::Noncopyable {
    explicit AutomatonContext(const automaton& a)
      : matcher{MakeAutomatonMatcher(a)} {}

    automaton_table_matcher matcher;
  };

  auto ctx = AutomatonContext{acceptor};

  return [context = std::move(ctx)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) mutable {
    return irs::Visit(segment, field, context.matcher, visitor);
  };
}

Filter::Query::ptr AutomatonFilter::prepare(const PrepareContext& ctx) const {
  return PrepareAutomatonFilter(ctx.Boost(Boost()), field_id(),
                                options().acceptor, options().scored_terms_limit);
}

}  // namespace irs
