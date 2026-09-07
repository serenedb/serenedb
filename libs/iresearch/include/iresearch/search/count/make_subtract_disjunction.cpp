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

#include <array>
#include <span>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/count/plan.hpp"
#include "iresearch/search/count/subtract.hpp"

namespace irs::count {

Root::ptr MakeSubtractDisjunction(const search::PostingClause& first,
                                  const search::PostingClause& second,
                                  const SubReader& segment,
                                  const Context& ctx) {
  const auto* rarest = &first;
  const auto* densest = &second;
  if (rarest->state.cookie.docs_count > densest->state.cookie.docs_count) {
    std::swap(rarest, densest);
  }
  const std::array<search::PostingClause, 2> terms{*rarest, *densest};
  auto conjunction = MakeSparseConjunction(terms, {}, segment, ctx);
  if (!conjunction) {
    return {};
  }
  const uint64_t total = uint64_t{rarest->state.cookie.docs_count} +
                         uint64_t{densest->state.cookie.docs_count};
  return memory::make_managed<Subtract>(total, std::move(conjunction));
}

}  // namespace irs::count
