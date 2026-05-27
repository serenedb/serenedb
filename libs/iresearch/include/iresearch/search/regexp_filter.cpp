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
#include "iresearch/search/multiterm_query.hpp"
#include "iresearch/search/prefix_filter.hpp"
#include "iresearch/search/term_filter.hpp"
#include "iresearch/utils/automaton_utils.hpp"
#include "iresearch/utils/regexp_utils.hpp"

namespace irs {
namespace {

// Classifies `pattern` and materializes the bytes the prepared buffer borrows:
// the unescaped term/prefix for the escaped forms, the trimmed prefix for
// Prefix, and the raw pattern for the automaton form.
void ResolveRegexp(bytes_view pattern, RegexpKind& kind, bstring& resolved) {
  resolved.clear();
  switch (ComputeRegexpType(pattern)) {
    case RegexpType::LiteralEscaped:
      kind = RegexpKind::Literal;
      UnescapeRegexp(pattern, resolved);
      return;
    case RegexpType::Literal:
      kind = RegexpKind::Literal;
      resolved.assign(pattern);
      return;
    case RegexpType::PrefixEscaped:
      kind = RegexpKind::Prefix;
      UnescapeRegexp(ExtractRegexpPrefix(pattern), resolved);
      return;
    case RegexpType::Prefix:
      kind = RegexpKind::Prefix;
      resolved.assign(ExtractRegexpPrefix(pattern));
      return;
    case RegexpType::Complex:
      kind = RegexpKind::Automaton;
      resolved.assign(pattern);
      return;
    default:
      SDB_UNREACHABLE();
  }
}

std::unique_ptr<Filter::PrepareBuffer> BuildBuffer(
  RegexpKind kind, bytes_view resolved, const PrepareContext& ctx,
  std::string_view field, size_t scored_terms_limit, RegexpSyntax syntax,
  score_t boost) {
  switch (kind) {
    case RegexpKind::Literal:
      return std::make_unique<ByTerm::Buffer>(ctx, field, resolved, boost);
    case RegexpKind::Prefix:
      return std::make_unique<ByPrefix::Buffer>(ctx, field, resolved,
                                                scored_terms_limit, boost);
    case RegexpKind::Automaton: {
      auto acceptor = FromRegexp(resolved, kDefaultMaxDfaStates, syntax);
      if (!Validate(acceptor)) {
        return std::make_unique<Filter::EmptyBuffer>();
      }
      if (auto buf = MakeAutomatonBuffer(ctx, field, std::move(acceptor),
                                         scored_terms_limit, boost)) {
        return buf;
      }
      return std::make_unique<Filter::EmptyBuffer>();
    }
  }
  SDB_UNREACHABLE();
}

}  // namespace

void ByRegexpOptions::set_pattern(bytes_view pattern) {
  this->pattern = pattern;
  ResolveRegexp(this->pattern, _kind, _resolved);
}

field_visitor ByRegexp::visitor(bytes_view pattern, RegexpSyntax syntax) {
  const auto type = ComputeRegexpType(pattern);

  switch (type) {
    case RegexpType::LiteralEscaped: {
      bstring unescaped;
      UnescapeRegexp(pattern, unescaped);
      return [term = std::move(unescaped)](const SubReader& segment,
                                           const TermReader& field,
                                           FilterVisitor& visitor) {
        ByTerm::visit(segment, field, term, visitor);
      };
    }

    case RegexpType::Literal: {
      return [term = bstring(pattern)](const SubReader& segment,
                                       const TermReader& field,
                                       FilterVisitor& visitor) {
        ByTerm::visit(segment, field, term, visitor);
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

Filter::Query::ptr ByRegexp::Prepare(const PrepareContext& ctx,
                                     std::string_view field, bytes_view pattern,
                                     size_t scored_terms_limit,
                                     RegexpSyntax syntax) {
  RegexpKind kind;
  bstring resolved;
  ResolveRegexp(pattern, kind, resolved);
  auto buf = BuildBuffer(kind, resolved, ctx, field, scored_terms_limit, syntax,
                         kNoBoost);
  return PrepareWithBuffer(*buf, ctx);
}

std::unique_ptr<Filter::PrepareBuffer> ByRegexp::CreateBuffer(
  const PrepareContext& ctx) const {
  const auto& opts = options();
  return BuildBuffer(opts.kind(), opts.resolved(), ctx, field(),
                     opts.scored_terms_limit, opts.syntax, Boost());
}

}  // namespace irs
