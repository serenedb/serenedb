////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2019 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include "multiterm_query.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <limits>

#include "basics/shared.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/all_filter.hpp"
#include "iresearch/search/prepared_state_visitor.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/term_query.hpp"

namespace irs {

QueryBuilder::ptr MultiTermQuery::Finish(
  memory::managed_ptr<MultiTermQuery> query, const PrepareContext& ctx) {
  auto& terms = query->_state.Terms();
  if (!query->Pinned()) {
    absl::c_sort(terms, [](const auto& l, const auto& r) {
      if (l.cookie.docs_count != r.cookie.docs_count) {
        return l.cookie.docs_count > r.cookie.docs_count;
      }
      return l.cookie.doc_start > r.cookie.doc_start;
    });
  }
  uint64_t sum = 0;
  for (const auto& entry : terms) {
    sum += entry.cookie.docs_count;
  }
  if (sum == 0) {
    return QueryBuilder::Empty();
  }
  query->_estimate_max = ClampEstimate(sum, query->_segment);

  if (terms.size() == 1 && !query->Pinned()) {
    const auto& entry = terms.front();
    if (!ctx.KeepsTerms() &&
        entry.cookie.docs_count == query->_segment.docs_count()) {
      return memory::make_tracked<AllQuery>(ctx.memory, query->_segment,
                                            entry.boost * query->_boost);
    }
    return MakeTermQuery(ctx.memory, query->_segment, query->_state.Reader(),
                         entry.cookie, entry.boost * query->_boost,
                         search::StatsRecord{entry.stats, ctx.Record().scorer});
  }
  query->SetStats(ctx.Record());
  return query;
}

void MultiTermQuery::Visit(PreparedStateVisitor& visitor, score_t boost) const {
  visitor.Visit(*this, _state, boost * _boost);
}

}  // namespace irs
