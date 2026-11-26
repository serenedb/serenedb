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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/search/count/root.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/table_filter.hpp"
#include "iresearch/search/queries/term_state.hpp"
#include "iresearch/utils/query_memory.hpp"

namespace irs::count {

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx);

Root::ptr MakeConstant(PlanMemory& memory, uint64_t count);

Root::ptr MakeAllCount(PlanMemory& memory, doc_id_t count);

Root::ptr MakeTermCount(PlanMemory& memory,
                        const detail::PostingClause& posting, bool partial);

Root::ptr MakeSum(PlanMemory& memory, Root::ptr lhs, Root::ptr rhs);

Root::ptr MakeSum(PlanMemory& memory, memory::ArenaVector<Root::ptr>&& parts);

template<typename Term>
Root::ptr MakeTermsSum(std::span<const Term> terms, const TermReader* field,
                       const Context& ctx) {
  memory::ArenaVector<Root::ptr> counts{ctx.memory.Arena()};
  counts.reserve(terms.size());
  for (const auto& term : terms) {
    counts.push_back(
      MakeTermCount(ctx.memory, detail::ClauseOf(term, field), ctx.partial));
  }
  return MakeSum(ctx.memory, std::move(counts));
}

Root::ptr MakeLiveTerm(PlanMemory& memory, const detail::PostingClause& term,
                       const SubReader& segment);

}  // namespace irs::count
