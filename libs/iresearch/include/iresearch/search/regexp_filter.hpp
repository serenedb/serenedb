#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByRegexp;
struct FilterVisitor;

struct ByRegexpFilterOptions {
  bstring pattern;

  bool operator==(const ByRegexpFilterOptions& rhs) const noexcept {
    return pattern == rhs.pattern;
  }
};

struct ByRegexpOptions : ByRegexpFilterOptions {
  using FilterType = ByRegexp;
  using filter_options = ByRegexpFilterOptions;

  size_t scored_terms_limit{1024};

  bool operator==(const ByRegexpOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};


class ByRegexp final : public FilterWithField<ByRegexpOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            bytes_view pattern, size_t scored_terms_limit);

  static field_visitor visitor(bytes_view pattern);

  Query::ptr prepare(const PrepareContext& ctx) const final {

    return prepare(ctx.Boost(Boost()), field(), options().pattern,
                   options().scored_terms_limit);
  }
};

}  // namespace irs
