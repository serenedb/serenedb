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

struct ByRegexpFilterOptions {
  bstring pattern;

  bool operator==(const ByRegexpFilterOptions&) const noexcept = default;
};

struct ByRegexpOptions : ByRegexpFilterOptions {
  using FilterType = ByRegexp;
  using filter_options = ByRegexpFilterOptions;

  size_t scored_terms_limit{1024};
  RegexpSyntax syntax{RegexpSyntax::Perl};

  bool operator==(const ByRegexpOptions&) const noexcept = default;
};

class ByRegexp final : public FilterWithField<ByRegexpOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, irs::field_id id,
                            bytes_view pattern, size_t scored_terms_limit,
                            RegexpSyntax syntax = RegexpSyntax::Perl);

  static field_visitor visitor(bytes_view pattern,
                               RegexpSyntax syntax = RegexpSyntax::Perl);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return prepare(ctx.Boost(Boost()), field_id(), options().pattern,
                   options().scored_terms_limit, options().syntax);
  }
};

}  // namespace irs
