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

#include <cstdint>

#include "iresearch/search/queries/boolean_query.hpp"

namespace irs::probe {

struct BooleanGroups {
  std::span<const detail::PostingClause> must;
  std::span<const QueryBuilder::ptr> must_filters;
  std::span<const detail::PostingClause> should;
  std::span<const QueryBuilder::ptr> should_filters;
  std::span<const detail::PostingClause> exclude;
  std::span<const QueryBuilder::ptr> exclude_filters;
  uint32_t min_should_match = 0;

  bool Excludes() const noexcept {
    return !exclude.empty() || !exclude_filters.empty();
  }

  bool Optional() const noexcept {
    return !should.empty() || !should_filters.empty();
  }
};

inline BooleanGroups GroupsOf(const BooleanQuery& query) {
  return {
    .must = query.Terms(Occur::Must),
    .must_filters = query.Queries(Occur::Must),
    .should = query.Terms(Occur::Should),
    .should_filters = query.Queries(Occur::Should),
    .exclude = query.Terms(Occur::MustNot),
    .exclude_filters = query.Queries(Occur::MustNot),
    .min_should_match = query.MinShouldMatch(),
  };
}

template<typename Api>
typename Api::Result MakeBoolean(const BooleanQuery& query,
                                 const typename Api::Context& ctx,
                                 uint64_t interrogations) {
  const auto groups = GroupsOf(query);
  if (groups.Excludes()) {
    return Api::MakeExclusion(query, ctx, interrogations, groups);
  }
  if constexpr (Api::kScored) {
    if (groups.Optional() && groups.min_should_match == 0) {
      return Api::MakeBoost(query, ctx, interrogations, groups);
    }
  }
  return Api::MakeRequired(query, ctx, interrogations, groups);
}

}  // namespace irs::probe
