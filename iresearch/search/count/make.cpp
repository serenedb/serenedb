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

#include "iresearch/search/count/make.hpp"

#include <span>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/family.hpp"
#include "iresearch/search/count/subtract.hpp"
#include "iresearch/search/detail/fold_reach.hpp"
#include "iresearch/search/detail/make_term.hpp"
#include "iresearch/search/queries/multiterm_query.hpp"

namespace irs::count {

Root::ptr MakeRoot(const QueryBuilder& query, const Context& ctx) {
  if (query.Kind() == QueryKind::Empty) {
    return MakeConstant(0);
  }
  const detail::FoldReachScope reach{ctx.span};
  return query.PlanCount(ctx);
}

Root::ptr Family::Make(const MultiTermQuery& query, const Context& ctx) {
  const auto& state = query.State();
  const auto* const field = state.Reader();
  const std::span<const MultiTermState::Entry> terms{state.Terms()};
  if (ctx.table == nullptr &&
      SubtractsPair(terms,
                    static_cast<doc_id_t>(query.Segment().docs_count()))) {
    if (auto subtracted = MakeSubtractDisjunction(
          detail::ClauseOf(terms.front(), field),
          detail::ClauseOf(terms.back(), field), query.Segment(), ctx)) {
      return subtracted;
    }
  }
  return detail::MakeQuery<Family>(query, ctx);
}

}  // namespace irs::count
