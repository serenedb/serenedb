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

#include <cstdint>
#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/scored_context.hpp"
#include "iresearch/search/top/make.hpp"
#include "iresearch/search/top/posting_pruned_disj.hpp"
#include "iresearch/search/top/pruned_disjunction.hpp"

namespace irs::top {

Root::ptr MakePrunedDisjunction(
  std::span<const irs::detail::PostingClause> terms,
  std::span<const QueryBuilder::ptr> filters, irs::detail::Terms uniformity,
  std::span<const irs::detail::PostingClause> excludes,
  std::span<const QueryBuilder::ptr> exclude_filters, const SubReader& segment,
  const Context& ctx, ScoreMergeType merge, uint32_t min_match) {
  return MakePrunedDisjunction<true>(
    terms, filters, uniformity, nullptr, nullptr, kNoBoost, excludes,
    exclude_filters, segment, ctx, merge, min_match);
}

}  // namespace irs::top
