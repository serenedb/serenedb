////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByGranularRange;
class NumericTokenizer;
struct FilterVisitor;

struct ByGranularRangeOptions {
  using FilterType = ByGranularRange;

  using terms = std::vector<bstring>;
  using range_type = SearchRange<terms>;

  range_type range;

  bool is_granular{true};

  bool operator==(const ByGranularRangeOptions& rhs) const noexcept = default;
};

template<typename T>
void SetGranularTerm(ByGranularRangeOptions::terms& boundary, T&& value) {
  boundary.clear();
  boundary.emplace_back(std::forward<T>(value));
}

void SetGranularTerm(ByGranularRangeOptions::terms& boundary,
                     NumericTokenizer& term);

class ByGranularRange : public FilterWithField<ByGranularRangeOptions> {
 public:
  ByGranularRange() noexcept { SetScorer(&DefaultConstScore()); }

  QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                   const PrepareContext& ctx) const final;
  static QueryBuilder::ptr PrepareSegment(const SubReader& segment,
                                          const PrepareContext& ctx,
                                          const irs::field_id field,
                                          const options_type& options);

  PrepareCollector::ptr MakeCollectorImpl(const Scorer* scorer,
                                          StatsArena& stats,
                                          uint32_t threads) const final;
};

}  // namespace irs
