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
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs {
namespace {

template<typename Term, typename Prefix, typename Complex>
auto ExecuteRegexp(bstring& buf, bytes_view pattern, Term&& t, Prefix&& p,
                   Complex&& c) {
  switch (ComputeRegexpType(pattern)) {
    case RegexpType::LiteralEscaped:
      return t(UnescapeRegexp(pattern, buf));
    case RegexpType::Literal:
      return t(pattern);
    case RegexpType::PrefixEscaped:
      return p(UnescapeRegexp(ExtractRegexpPrefix(pattern), buf));
    case RegexpType::Prefix:
      return p(ExtractRegexpPrefix(pattern));
    case RegexpType::Complex:
      return c(pattern);
    default:
      SDB_UNREACHABLE();
  }
}

}  // namespace

ByRegexpFilterOptions::ByRegexpFilterOptions(bytes_view pattern,
                                             RegexpSyntax syntax)
  : pattern{pattern}, syntax{syntax} {
  if (!pattern.empty()) {
    acceptor = FromRegexp(pattern, kDefaultMaxDfaStates, syntax);
  }
}

field_visitor ByRegexp::visitor(const automaton& acceptor) {
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

Filter::Query::ptr ByRegexp::prepare(const PrepareContext& ctx) const {
  if (options().pattern.empty()) {
    return Query::empty();
  }
  return PrepareAutomatonFilter(ctx.Boost(Boost()), field(), options().acceptor,
                                options().scored_terms_limit);
}

Filter::ptr CreateByRegexp(std::string_view field, bytes_view pattern,
                           RegexpSyntax syntax, size_t scored_terms_limit,
                           score_t boost) {
  bstring buf;
  return ExecuteRegexp(
    buf, pattern,
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
    [&](bytes_view pattern) -> Filter::ptr {
      auto filter = std::make_unique<ByRegexp>();
      *filter->mutable_field() = field;
      auto& options = *filter->mutable_options();
      options = ByRegexpOptions{pattern, syntax};
      options.scored_terms_limit = scored_terms_limit;
      filter->boost(boost);
      return filter;
    });
}

}  // namespace irs
