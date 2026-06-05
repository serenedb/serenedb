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

#include <memory>

#include "basics/shared.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/utils/automaton_decl.hpp"
#include "iresearch/utils/regexp_utils.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByRegexp;
struct FilterVisitor;

struct ByRegexpFilterOptions {
  bstring pattern;
  RegexpSyntax syntax{RegexpSyntax::Perl};
  std::shared_ptr<const automaton> acceptor;

  ByRegexpFilterOptions() = default;
  explicit ByRegexpFilterOptions(bytes_view pattern,
                                 RegexpSyntax syntax = RegexpSyntax::Perl);

  bool operator==(const ByRegexpFilterOptions& rhs) const noexcept {
    return pattern == rhs.pattern && syntax == rhs.syntax;
  }
};

struct ByRegexpOptions : ByRegexpFilterOptions {
  using FilterType = ByRegexp;
  using filter_options = ByRegexpFilterOptions;
  using ByRegexpFilterOptions::ByRegexpFilterOptions;

  size_t scored_terms_limit{1024};

  bool operator==(const ByRegexpOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

Filter::ptr CreateByRegexp(std::string_view field, bytes_view pattern,
                           RegexpSyntax syntax = RegexpSyntax::Perl,
                           size_t scored_terms_limit = 1024,
                           score_t boost = kNoBoost);

class ByRegexp final : public FilterWithField<ByRegexpOptions> {
 public:
  static field_visitor visitor(std::shared_ptr<const automaton> acceptor);

  Query::ptr prepare(const PrepareContext& ctx) const final;
};

}  // namespace irs
