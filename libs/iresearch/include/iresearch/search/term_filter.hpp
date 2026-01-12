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

class ByTerm;
struct FilterVisitor;

// Options for term filter
struct ByTermOptions {
  using FilterType = ByTerm;

  bstring term;

  bool operator==(const ByTermOptions& rhs) const noexcept {
    return term == rhs.term;
  }
};

// User-side term filter
class ByTerm : public FilterWithField<ByTermOptions> {
 public:
  static Query::ptr prepare(const PrepareContext& ctx, std::string_view field,
                            bytes_view term);

  static void visit(const SubReader& segment, const TermReader& field,
                    bytes_view term, FilterVisitor& visitor);

  Query::ptr prepare(const PrepareContext& ctx) const final {
    auto sub_ctx = ctx;
    sub_ctx.boost *= Boost();
    return prepare(sub_ctx, field(), options().term);
  }
};

}  // namespace irs
