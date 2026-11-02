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

#include <tuple>
#include <type_traits>
#include <utility>

#include "iresearch/search/detail/geo_slots.hpp"
#include "iresearch/search/detail/make.hpp"
#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/queries/geo_query.hpp"

namespace irs::detail {

template<typename Result, bool Scored, typename Parser, typename Acceptor,
         typename F>
Result Build(GeoKind, const GeoQuery<Parser, Acceptor>& query,
             uint64_t interrogations, F&& f) {
  static_assert(!Scored);
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto& cells = query.Cells();
  SDB_ASSERT(cells.Kind() != QueryKind::Empty);
  auto node = [&] {
    if constexpr (kProbed) {
      return cells.PlanProbe({}, interrogations);
    } else {
      return cells.PlanLead({});
    }
  }();
  if (!node) {
    return {};
  }
  using Cells = std::conditional_t<kProbed, probe::Erased, lead::Erased>;
  return f.template operator()<GeoSlots<Parser, Acceptor, Cells>>(
    std::piecewise_construct, std::forward_as_tuple(std::move(node)),
    query.MakeRecipe());
}

}  // namespace irs::detail
