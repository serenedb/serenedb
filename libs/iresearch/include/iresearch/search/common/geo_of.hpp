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

#include <type_traits>
#include <utility>

#include "iresearch/search/common/plan.hpp"
#include "iresearch/search/lead/geo_slots_docs.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/two_phase_docs.hpp"
#include "iresearch/search/probe/geo_slots_docs.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/two_phase_docs.hpp"

namespace irs::search {

template<template<typename> class Impl, typename Result, typename Parser,
         typename Acceptor, typename... Prefix>
Result MakeGeo(const GeoQuery<Parser, Acceptor>& query, uint64_t interrogations,
               Prefix&&... prefix) {
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  const auto check = query.MakeCheck();
  SDB_ASSERT(check.recipe.has_value());
  const auto& recipe = *check.recipe;
  const auto& cells = query.Cells();
  SDB_ASSERT(cells.Kind() != QueryKind::Empty);

  if constexpr (kProbed) {
    auto approx = cells.PlanProbe({}, interrogations);
    if (!approx) {
      return {};
    }
    using Slots = probe::GeoSlotsDocs<Parser, Acceptor, probe::Erased>;
    return memory::make_managed<Impl<probe::TwoPhaseDocs<Slots>>>(
      std::forward<Prefix>(prefix)..., std::piecewise_construct,
      std::forward_as_tuple(std::move(approx)), recipe);
  } else {
    auto approx = cells.PlanLead({});
    if (!approx) {
      return {};
    }
    using Slots = lead::GeoSlotsDocs<Parser, Acceptor, lead::Erased>;
    return memory::make_managed<Impl<lead::TwoPhaseDocs<Slots>>>(
      std::forward<Prefix>(prefix)..., std::piecewise_construct,
      std::forward_as_tuple(std::move(approx)), recipe);
  }
}

}  // namespace irs::search
