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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/search/filter.hpp"
#include "iresearch/search/search_range.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ByRange;
struct FilterVisitor;

struct ByRangeFilterOptions {
  using range_type = SearchRange<bstring>;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief search range
  //////////////////////////////////////////////////////////////////////////////
  range_type range;

  bool operator==(const ByRangeFilterOptions& rhs) const noexcept {
    return range == rhs.range;
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @struct ByPrefixOptions
/// @brief options for prefix filter
////////////////////////////////////////////////////////////////////////////////
struct ByRangeOptions : ByRangeFilterOptions {
  using FilterType = ByRange;
  using filter_options = ByRangeFilterOptions;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t scored_terms_limit{1024};

  bool operator==(const ByRangeOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

//////////////////////////////////////////////////////////////////////////////
/// @class by_range
/// @brief user-side term range filter
//////////////////////////////////////////////////////////////////////////////
class ByRange : public FilterWithField<ByRangeOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            const options_type::range_type& rng,
                            size_t scored_terms_limit);

  static void visit(const SubReader& segment, const TermReader& reader,
                    const options_type::range_type& rng,
                    FilterVisitor& visitor);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return prepare(ctx.Boost(Boost()), field(), options().range,
                   options().scored_terms_limit);
  }
};

}  // namespace irs
