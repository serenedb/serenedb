////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs {

field_visitor ByRegexp::visitor(bytes_view pattern) {
  const auto type = ComputeRegexpType(pattern);

  switch (type) {
    case RegexpType::Literal: {
      return [term = bstring(pattern)](const SubReader& segment,
                                       const TermReader& field,
                                       FilterVisitor& visitor) {
        ByTerm::visit(segment, field, term, visitor);
      };
    }

    case RegexpType::Prefix: {
      auto prefix = ExtractRegexpPrefix(pattern);
      return [prefix = bstring(prefix)](const SubReader& segment,
                                        const TermReader& field,
                                        FilterVisitor& visitor) {
        ByPrefix::visit(segment, field, prefix, visitor);
      };
    }

    case RegexpType::Complex: {
      struct AutomatonContext : util::Noncopyable {
        explicit AutomatonContext(bytes_view pattern)
          : acceptor{FromRegexp(pattern)},              //  dfa
            matcher{MakeAutomatonMatcher(acceptor)} {}  //  matcher

        automaton acceptor;               // dfa from regexp
        automaton_table_matcher matcher;  // matcher for fst intersection
      };

      auto ctx = std::make_shared<AutomatonContext>(pattern);

      if (!Validate(ctx->acceptor)) {
        SDB_UNREACHABLE();
      }

      return [ctx = std::move(ctx)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) mutable {
        return irs::Visit(segment, field, ctx->matcher, visitor);
      };
    }
  }

  return [](const SubReader&, const TermReader&, FilterVisitor&) {};
}

Filter::Query::ptr ByRegexp::prepare(const PrepareContext& ctx,
                                     std::string_view field, bytes_view pattern,
                                     size_t scored_terms_limit) {
  const auto type = ComputeRegexpType(pattern);

  switch (type) {
    case RegexpType::Literal:
      return ByTerm::prepare(ctx, field, pattern);

    case RegexpType::Prefix: {
      auto prefix = ExtractRegexpPrefix(pattern);
      return ByPrefix::prepare(ctx, field, prefix, scored_terms_limit);
    }

    case RegexpType::Complex:
      break;
  }

  auto acceptor = FromRegexp(pattern);

  if (!Validate(acceptor)) {
    return Query::empty();
  }

  return PrepareAutomatonFilter(ctx, field, acceptor, scored_terms_limit);
}

}  // namespace irs
