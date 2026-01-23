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
      // General case - use automaton
      struct AutomatonContext : util::Noncopyable {
        explicit AutomatonContext(bytes_view pattern)
          : acceptor{FromRegexp(pattern)},
            matcher{MakeAutomatonMatcher(acceptor)} {}

        automaton acceptor;
        automaton_table_matcher matcher;
      };

      auto ctx = std::make_shared<AutomatonContext>(pattern);

      if (!Validate(ctx->acceptor)) {
        return [](const SubReader&, const TermReader&, FilterVisitor&) {};
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

}  
