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

#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByRegexp;
struct FilterVisitor;

// How a regexp pattern dispatches once resolved.
enum class RegexpKind { Literal, Prefix, Automaton };

struct ByRegexpFilterOptions {
  bstring pattern;

  bool operator==(const ByRegexpFilterOptions&) const noexcept = default;
};

struct ByRegexpOptions : ByRegexpFilterOptions {
  using FilterType = ByRegexp;
  using filter_options = ByRegexpFilterOptions;

  size_t scored_terms_limit{1024};
  RegexpSyntax syntax{RegexpSyntax::Perl};

  // Sets the raw `pattern` and resolves it once into a dispatch kind and the
  // bytes the prepared buffer borrows. Must be used instead of writing
  // `pattern` directly so the resolved form stays in sync.
  void set_pattern(bytes_view pattern);

  RegexpKind kind() const noexcept { return _kind; }
  bytes_view resolved() const noexcept { return _resolved; }

  bool operator==(const ByRegexpOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit && syntax == rhs.syntax;
  }

 private:
  RegexpKind _kind{RegexpKind::Literal};
  bstring _resolved;
};

class ByRegexp final : public FilterWithField<ByRegexpOptions> {
 public:
  static Query::ptr Prepare(const PrepareContext& ctx, std::string_view field,
                            bytes_view pattern, size_t scored_terms_limit,
                            RegexpSyntax syntax = RegexpSyntax::Perl);

  static field_visitor visitor(bytes_view pattern,
                               RegexpSyntax syntax = RegexpSyntax::Perl);

  std::unique_ptr<PrepareBuffer> CreateBuffer(
    const PrepareContext& ctx) const final;

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return PrepareWithBuffer(*CreateBuffer(ctx), ctx);
  }
};

}  // namespace irs
