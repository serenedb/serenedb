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

#include "iresearch/search/count/make.hpp"
#include "iresearch/search/docs/make.hpp"
#include "iresearch/search/fill/impl.hpp"
#include "iresearch/search/fill/make.hpp"
#include "iresearch/search/filter.hpp"
#include "iresearch/search/lead/impl.hpp"
#include "iresearch/search/lead/make.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/scored/make.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs {

template<typename Derived, typename Base = QueryBuilder>
class QueryBuilderImpl : public Base {
 public:
  using Base::Base;

  count::Root::ptr PlanCount(const count::Context& ctx) const final {
    return count::Make(Self(), ctx);
  }

  docs::Root::ptr PlanDocs(const docs::Context& ctx) const final {
    return docs::Make(Self(), ctx);
  }

  scored::Root::ptr PlanScored(const scored::Context& ctx) const final {
    return scored::Make(Self(), ctx);
  }

  top::Root::ptr PlanTop(const top::Context& ctx) const final {
    return top::Make(Self(), ctx);
  }

  lead::Node::ptr PlanLead(const search::ScoredCtx& ctx) const final {
    if (!Base::Scores()) {
      if (auto node = lead::Make(Self())) {
        return node;
      }
    }
    return lead::Make(Self(), ctx);
  }

  probe::Node::ptr PlanProbe(const search::ScoredCtx& ctx,
                             uint64_t interrogations) const final {
    if (!Base::Scores()) {
      if (auto node = probe::Make(Self(), interrogations)) {
        return node;
      }
    }
    return probe::Make(Self(), ctx, interrogations);
  }

  fill::Node::ptr PlanFill(const search::ScoredCtx& ctx,
                           ScoreMergeType merge) const final {
    if (!Base::Scores()) {
      if (auto node = fill::Make(Self())) {
        return node;
      }
    }
    return fill::Make(Self(), ctx, merge);
  }

 private:
  const Derived& Self() const noexcept {
    return static_cast<const Derived&>(*this);
  }
};

}  // namespace irs
