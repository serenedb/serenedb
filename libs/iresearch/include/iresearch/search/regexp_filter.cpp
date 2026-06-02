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

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/filter_visitor.hpp"
#include "iresearch/search/limited_sample_selector.hpp"
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs {

field_visitor ByRegexp::visitor(bytes_view pattern, RegexpSyntax syntax) {
  const auto type = ComputeRegexpType(pattern);

  switch (type) {
    case RegexpType::LiteralEscaped: {
      bstring unescaped;
      UnescapeRegexp(pattern, unescaped);
      return [term = std::move(unescaped)](const SubReader& segment,
                                           const TermReader& field,
                                           FilterVisitor& visitor) {
        ByTerm::Visit(segment, field, term, visitor);
      };
    }

    case RegexpType::Literal: {
      return [term = bstring(pattern)](const SubReader& segment,
                                       const TermReader& field,
                                       FilterVisitor& visitor) {
        ByTerm::Visit(segment, field, term, visitor);
      };
    }

    case RegexpType::PrefixEscaped: {
      auto raw_prefix = ExtractRegexpPrefix(pattern);
      bstring unescaped;
      UnescapeRegexp(raw_prefix, unescaped);
      return [prefix = std::move(unescaped)](const SubReader& segment,
                                             const TermReader& field,
                                             FilterVisitor& visitor) {
        ByPrefix::visit(segment, field, prefix, visitor);
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
      auto acceptor = FromRegexp(pattern, kDefaultMaxDfaStates, syntax);

      if (!Validate(acceptor)) {
        // Invalid pattern or too complex - return visitor that matches nothing
        return [](const SubReader&, const TermReader&, FilterVisitor&) {};
      }

      struct AutomatonContext : util::Noncopyable {
        explicit AutomatonContext(automaton&& a)
          : acceptor{std::move(a)}, matcher{MakeAutomatonMatcher(acceptor)} {}

        automaton acceptor;
        automaton_table_matcher matcher;
      };

      auto ctx = std::make_shared<AutomatonContext>(std::move(acceptor));

      return [ctx = std::move(ctx)](const SubReader& segment,
                                    const TermReader& field,
                                    FilterVisitor& visitor) mutable {
        return irs::Visit(segment, field, ctx->matcher, visitor);
      };
    }
    default:
      SDB_UNREACHABLE();
  }
}

QueryBuilder::ptr ByRegexp::PrepareSegment(
  const SubReader& segment, const PrepareContext& ctx, std::string_view field,
  bytes_view pattern, size_t scored_terms_limit, RegexpSyntax syntax) {
  const auto type = ComputeRegexpType(pattern);

  switch (type) {
    case RegexpType::LiteralEscaped: {
      bstring buf;
      UnescapeRegexp(pattern, buf);
      return ByTerm::PrepareSegment(segment, ctx, field, buf);
    }

    case RegexpType::Literal:
      return ByTerm::PrepareSegment(segment, ctx, field, pattern);

    case RegexpType::PrefixEscaped: {
      auto raw_prefix = ExtractRegexpPrefix(pattern);
      bstring unescaped;
      UnescapeRegexp(raw_prefix, unescaped);
      return ByPrefix::PrepareSegment(segment, ctx, field, unescaped,
                                      scored_terms_limit);
    }

    case RegexpType::Prefix: {
      auto prefix = ExtractRegexpPrefix(pattern);
      return ByPrefix::PrepareSegment(segment, ctx, field, prefix,
                                      scored_terms_limit);
    }

    case RegexpType::Complex: {
      auto acceptor = FromRegexp(pattern, kDefaultMaxDfaStates, syntax);
      if (!Validate(acceptor)) {
        return QueryBuilder::Empty();
      }
      return PrepareAutomatonSegment(segment, ctx, field, acceptor);
    }

    default:
      SDB_UNREACHABLE();
  }
}

QueryBuilder::ptr ByRegexp::PrepareSegment(const SubReader& segment,
                                           const PrepareContext& ctx) const {
  auto sub_ctx = ctx;
  sub_ctx.boost *= Boost();
  return PrepareSegment(segment, sub_ctx, field(), options().pattern,
                        options().scored_terms_limit, options().syntax);
}

PrepareCollector::ptr ByRegexp::MakeCollector(const Scorer* scorer) const {
  const auto limit = options().scored_terms_limit;
  switch (ComputeRegexpType(options().pattern)) {
    case RegexpType::LiteralEscaped:
    case RegexpType::Literal:
      return std::make_unique<TermsCollector>(scorer, 1);
    case RegexpType::PrefixEscaped:
    case RegexpType::Prefix:
    case RegexpType::Complex:
      return std::make_unique<LimitedTermsCollector>(scorer, limit);
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace irs
