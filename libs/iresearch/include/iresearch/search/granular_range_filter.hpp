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

////////////////////////////////////////////////////////////////////////////////
/// @struct by_granular_range_options
/// @brief options for granular range filter
////////////////////////////////////////////////////////////////////////////////
struct ByGranularRangeOptions {
  using FilterType = ByGranularRange;

  using terms = std::vector<bstring>;
  using range_type = SearchRange<terms>;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief search range
  /// @note terms are expected to be placed by granularity levels from the most
  ///       precise term to the less precise one, i.e. lower indexes denote more
  ///       precise term
  /// @note consider using "SetGranularTerm" function for convenience
  //////////////////////////////////////////////////////////////////////////////
  range_type range;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t scored_terms_limit{1024};

  bool is_granular{true};

  bool operator==(const ByGranularRangeOptions& rhs) const noexcept = default;
};

//////////////////////////////////////////////////////////////////////////////
/// @brief convenient helper for setting granular term at a specified range
///        boundary
/// @note use the most precise value of 'granularity_level'
//////////////////////////////////////////////////////////////////////////////
template<typename T>
void SetGranularTerm(ByGranularRangeOptions::terms& boundary, T&& value) {
  boundary.clear();
  boundary.emplace_back(std::forward<T>(value));
}

//////////////////////////////////////////////////////////////////////////////
/// @brief convenient helper for setting granular term at a specified range
///        boundary
//////////////////////////////////////////////////////////////////////////////
void SetGranularTerm(ByGranularRangeOptions::terms& boundary,
                     NumericTokenizer& term);

//////////////////////////////////////////////////////////////////////////////
/// @class by_granular_range
/// @brief user-side term range filter for granularity-enabled terms
///        when indexing, the lower the value for attributes().get<position>()
///        the higher the granularity of the term value
///        the lower granularity terms are <= higher granularity terms
///        NOTE: it is assumed that granularity level gaps are identical for
///              all terms, i.e. the behavour for the following is undefined:
///              termA@0 + termA@2 + termA@5 + termA@10
///              termB@0 + termB@2 + termB@6 + termB@10
//////////////////////////////////////////////////////////////////////////////
class ByGranularRange : public FilterWithField<ByGranularRangeOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            const options_type& options);

  static void visit(const SubReader& segment, const TermReader& reader,
                    const options_type& options, FilterVisitor& visitor);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    return prepare(ctx.Boost(Boost()), field(), options());
  }
};

}  // namespace irs
