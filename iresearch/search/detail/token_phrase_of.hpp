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

#include <optional>
#include <tuple>
#include <utility>

#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/token_phrase_slots.hpp"
#include "iresearch/search/queries/token_phrase_query.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"

namespace irs::detail {

inline bool IsSloppy(const TokenPhraseQuery& query) noexcept {
  return query.Compiled().slop.max != 0;
}

template<typename Result, bool Scored, typename F>
Result Build(TokenPhraseKind, const TokenPhraseQuery& query,
             uint64_t interrogations, F&& f) {
  auto node = PlanApprox<Result>(query.Approx(), interrogations);
  if (!node) {
    return {};
  }
  const auto make = [&]<bool Sloppy> -> Result {
    using Slots = TokenPhraseSlots<ApproxNode<Result>, Sloppy>;
    return f.template operator()<Slots>(std::piecewise_construct,
                                        std::forward_as_tuple(std::move(node)),
                                        query, Scored);
  };
  if constexpr (Scored) {
    return ResolveBool(IsSloppy(query), make);
  } else {
    return make.template operator()<false>();
  }
}

inline std::optional<score_t> ConstantOf(TokenPhraseKind,
                                         const TokenPhraseQuery& query,
                                         const ScoreArgs& args) {
  return ConstantOf(query.Segment(), query.Reader(), args, IsSloppy(query));
}

inline const TermReader& FieldOf(TokenPhraseKind,
                                 const TokenPhraseQuery& query) {
  return query.Reader();
}

}  // namespace irs::detail
