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

#include <span>
#include <vector>

#include "basics/down_cast.h"
#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/common/resolve.hpp"
#include "iresearch/search/multiterm_query.hpp"

namespace irs::search {

inline bool ClauseTerms(const QueryBuilder& child,
                        std::vector<PostingClause>& terms) {
  if (child.Kind() != QueryKind::Terms) {
    return false;
  }
  const auto& state = sdb::basics::downCast<MultiTermQuery>(child).State();
  SDB_ASSERT(!state.Empty());
  const auto* const field = state.Reader();
  if (field == nullptr || DocOf(*field) == nullptr) {
    return false;
  }
  terms.reserve(state.TermsSize());
  for (const auto& entry : state.Terms()) {
    SDB_ASSERT(entry.cookie.docs_count != 0);
    terms.emplace_back(TermState{field, entry.cookie});
  }
  return true;
}

}  // namespace irs::search
