////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "iresearch/search/filters/filter.hpp"

namespace irs {

template<typename Derived, typename Base = QueryBuilder>
class QueryBuilderImpl : public Base {
 public:
  using Base::Base;

  count::Root::ptr PlanCount(const count::Context& ctx) const final;

  docs::Root::ptr PlanDocs(const docs::Context& ctx) const final;

  hits::Root::ptr PlanScored(const hits::Context& ctx) const final;

  top::Root::ptr PlanTop(const top::Context& ctx) const final;

  lead::Node::ptr PlanLead(const detail::ScoredCtx& ctx) const final;

  probe::Node::ptr PlanProbe(const detail::ScoredCtx& ctx,
                             uint64_t interrogations) const final;

  fill::Node::ptr PlanFill(const detail::ScoredCtx& ctx,
                           ScoreMergeType merge) const final;

 private:
  const Derived& Self() const noexcept {
    return static_cast<const Derived&>(*this);
  }
};

}  // namespace irs
