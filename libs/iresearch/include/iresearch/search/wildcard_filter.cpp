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

#include "basics/shared.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/hash_utils.hpp"
#include "iresearch/utils/wildcard_utils.hpp"

namespace irs {
namespace {

bytes_view Unescape(bytes_view in, bstring& out) {
  out.reserve(in.size());

  bool copy = true;
  absl::c_copy_if(in, std::back_inserter(out), [&copy](byte_type c) {
    if (c == WildcardMatch::kEscape) {
      copy = !copy;
    } else {
      copy = true;
    }
    return copy;
  });

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

}  // namespace

field_visitor ByWildcard::visitor(bytes_view term) {
  bstring buf;
  return ExecuteWildcard(
    buf, term,
    [](bytes_view term) -> field_visitor {
      // must copy term as it may point to temporary string
      return [term = bstring(term)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) {
        ByTerm::visit(segment, field, term, visitor);
      };
    },
    [](bytes_view term) -> field_visitor {
      // must copy term as it may point to temporary string
      return [term = bstring(term)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) {
        ByPrefix::visit(segment, field, term, visitor);
      };
    },
    [](bytes_view term) -> field_visitor {
      struct AutomatonContext : util::Noncopyable {
        explicit AutomatonContext(bytes_view term)
          : acceptor{FromWildcard(term)},
            matcher{MakeAutomatonMatcher(acceptor)} {}

        automaton acceptor;
        automaton_table_matcher matcher;
      };

      auto ctx = std::make_shared<AutomatonContext>(term);

      if (!Validate(ctx->acceptor)) {
        return [](const SubReader&, const TermReader&, FilterVisitor&) {};
      }

      return [ctx = std::move(ctx)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) mutable {
        return irs::Visit(segment, field, ctx->matcher, visitor);
      };
    });
}

Filter::Query::ptr ByWildcard::prepare(const PrepareContext& ctx,
                                       std::string_view field, bytes_view term,
                                       size_t scored_terms_limit) {
  bstring buf;
  return ExecuteWildcard(
    buf, term,
    [&](bytes_view term) -> Query::ptr {
      return ByTerm::prepare(ctx, field, term);
    },
    [&, scored_terms_limit](bytes_view term) -> Query::ptr {
      return ByPrefix::prepare(ctx, field, term, scored_terms_limit);
    },
    [&, scored_terms_limit](bytes_view term) -> Query::ptr {
      return PrepareAutomatonFilter(ctx, field, FromWildcard(term),
                                    scored_terms_limit);
    });
}

}  // namespace irs
