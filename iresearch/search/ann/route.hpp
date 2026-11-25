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

#include <algorithm>

#include "iresearch/search/ann/root.hpp"
#include "iresearch/search/filters/filter.hpp"

namespace irs::ann {

struct Route {
  const QueryBuilder* prefilter;
  Context ctx;
};

inline double ShareOf(const QueryBuilder* query, double docs) noexcept {
  return !query || docs == 0
           ? 1.0
           : std::min(1.0,
                      static_cast<double>(query->EstimateMatches()) / docs);
}

template<typename Query, typename ReadsColumns>
Route RouteOf(const Query& query, const Context& ctx,
              ReadsColumns&& reads_columns) {
  const auto* inner = query.Inner();
  const auto* index_part = query.IndexPart();
  if (!inner || !query.HasColumnPart() || !ctx.columns ||
      !reads_columns(*inner, index_part)) {
    return {inner, ctx};
  }
  return {index_part,
          {.scorer = ctx.scorer,
           .fetcher = ctx.fetcher,
           .table = ctx.columns,
           .k = ctx.k}};
}

}  // namespace irs::ann
