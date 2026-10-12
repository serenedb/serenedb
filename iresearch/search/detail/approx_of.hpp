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

#include "iresearch/search/detail/plan.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/queries/approx_query.hpp"

namespace irs::detail {

template<template<typename> class Impl, typename Result, typename... Prefix>
Result MakeApprox(const ApproxQuery& query, uint64_t interrogations,
                  Prefix&&... prefix) {
  constexpr bool kProbed = std::is_same_v<Result, ProbeNode::ptr>;
  SDB_ASSERT(query.Kind() != QueryKind::Empty);
  auto node = [&] {
    if constexpr (kProbed) {
      return query.Approx().PlanProbe({}, interrogations);
    } else {
      return query.Approx().PlanLead({});
    }
  }();
  if (!node) {
    return {};
  }
  using Approx = std::conditional_t<kProbed, probe::Erased, lead::Erased>;
  return memory::make_managed<Impl<Approx>>(std::forward<Prefix>(prefix)...,
                                            std::move(node));
}

}  // namespace irs::detail
