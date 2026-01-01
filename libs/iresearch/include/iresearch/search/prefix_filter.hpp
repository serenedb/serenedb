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
#include "iresearch/utils/string.hpp"

namespace irs {

class ByPrefix;
struct FilterVisitor;

struct ByPrefixFilterOptions {
  //////////////////////////////////////////////////////////////////////////////
  /// @brief search prefix
  //////////////////////////////////////////////////////////////////////////////
  bstring term;

  bool operator==(const ByPrefixFilterOptions& rhs) const noexcept {
    return term == rhs.term;
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @struct ByPrefixOptions
/// @brief options for prefix filter
////////////////////////////////////////////////////////////////////////////////
struct ByPrefixOptions : ByPrefixFilterOptions {
  using FilterType = ByPrefix;
  using filter_options = ByPrefixFilterOptions;

  //////////////////////////////////////////////////////////////////////////////
  /// @brief the maximum number of most frequent terms to consider for scoring
  //////////////////////////////////////////////////////////////////////////////
  size_t scored_terms_limit{1024};

  bool operator==(const ByPrefixOptions& rhs) const noexcept {
    return filter_options::operator==(rhs) &&
           scored_terms_limit == rhs.scored_terms_limit;
  }
};

////////////////////////////////////////////////////////////////////////////////
/// @class by_prefix
/// @brief user-side prefix filter
////////////////////////////////////////////////////////////////////////////////
class ByPrefix : public FilterWithField<ByPrefixOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            bytes_view prefix, size_t scored_terms_limit);

  static void visit(const SubReader& segment, const TermReader& reader,
                    bytes_view prefix, FilterVisitor& visitor);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    auto sub_ctx = ctx;
    sub_ctx.boost *= Boost();
    return prepare(sub_ctx, field(), options().term,
                   options().scored_terms_limit);
  }
};

}  // namespace irs
