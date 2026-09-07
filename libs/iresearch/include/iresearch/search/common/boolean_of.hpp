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
#include <limits>
#include <span>
#include <tuple>
#include <type_traits>
#include <utility>
#include <vector>

#include "iresearch/search/boolean_query.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/collect.hpp"
#include "iresearch/search/common/conjunction_of.hpp"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/scored_context.hpp"
#include "iresearch/search/probe/make.hpp"

namespace irs::search {

template<typename Result, typename Term, typename Make>
Result BuildRequiredLeadOf(std::span<const Term> terms,
                           std::span<const QueryBuilder::ptr> filters,
                           const TermReader* field, const SubReader& segment,
                           Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() > 1) {
    auto node =
      BuildConjunctionOf<LeadNode::ptr>(terms, filters, field, segment, 0);
    if (!node) {
      return {};
    }
    return make.template operator()<lead::Erased>(
      std::forward_as_tuple(std::move(node)));
  }
  if (!terms.empty()) {
    const auto& own = FieldOf(terms.front(), field);
    return ResolveInput(*DocOf(own), [&]<typename Input> -> Result {
      using Lead = PostingLead<Input>;
      return make.template operator()<Lead>(std::forward_as_tuple(
        CookieOf(terms.front()), *DocOf(own), LayoutOf(own), BoundsOf(own)));
    });
  }
  auto node = filters.front()->PlanLead({});
  if (!node) {
    return {};
  }
  return make.template operator()<lead::Erased>(
    std::forward_as_tuple(std::move(node)));
}

}  // namespace irs::search
