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

#include "iresearch/search/count/family.hpp"
#include "iresearch/search/detail/make_query.hpp"
#include "iresearch/search/docs/family.hpp"
#include "iresearch/search/fill/family.hpp"
#include "iresearch/search/hits/family.hpp"
#include "iresearch/search/lead/family.hpp"
#include "iresearch/search/probe/family.hpp"
#include "iresearch/search/queries/query_builder_impl.hpp"
#include "iresearch/search/top/family.hpp"

namespace irs {

template<typename Derived, typename Base>
count::Root::ptr QueryBuilderImpl<Derived, Base>::PlanCount(
  const count::Context& ctx) const {
  return detail::Plan<count::Family>(Self(), ctx);
}

template<typename Derived, typename Base>
docs::Root::ptr QueryBuilderImpl<Derived, Base>::PlanDocs(
  const docs::Context& ctx) const {
  return detail::Plan<docs::Family>(Self(), ctx);
}

template<typename Derived, typename Base>
hits::Root::ptr QueryBuilderImpl<Derived, Base>::PlanScored(
  const hits::Context& ctx) const {
  return detail::Plan<hits::Family>(Self(), ctx);
}

template<typename Derived, typename Base>
top::Root::ptr QueryBuilderImpl<Derived, Base>::PlanTop(
  const top::Context& ctx) const {
  return detail::Plan<top::Family>(Self(), ctx);
}

template<typename Derived, typename Base>
lead::Node::ptr QueryBuilderImpl<Derived, Base>::PlanLead(
  const detail::ScoredCtx& ctx) const {
  if (!ctx.fetcher) {
    return detail::Plan<lead::Family>(Self(), lead::Family::DocsContext{});
  }
  if (!Base::Scores()) {
    if (auto node =
          detail::Plan<lead::Family>(Self(), lead::Family::DocsContext{})) {
      return node;
    }
  }
  return detail::Plan<lead::Family>(Self(), ctx);
}

template<typename Derived, typename Base>
probe::Node::ptr QueryBuilderImpl<Derived, Base>::PlanProbe(
  const detail::ScoredCtx& ctx, uint64_t interrogations) const {
  if (!ctx.fetcher) {
    return detail::Plan<probe::Family>(
      Self(), probe::Family::DocsContext{interrogations});
  }
  if (!Base::Scores()) {
    if (auto node = detail::Plan<probe::Family>(
          Self(), probe::Family::DocsContext{interrogations})) {
      return node;
    }
  }
  return detail::Plan<probe::Family>(
    Self(), probe::Family::ScoredContext{ctx, interrogations});
}

template<typename Derived, typename Base>
fill::Node::ptr QueryBuilderImpl<Derived, Base>::PlanFill(
  const detail::ScoredCtx& ctx, ScoreMergeType merge) const {
  if (!ctx.fetcher) {
    return detail::Plan<fill::Family>(Self(), fill::Family::DocsContext{});
  }
  if (!Base::Scores()) {
    if (auto node =
          detail::Plan<fill::Family>(Self(), fill::Family::DocsContext{})) {
      return node;
    }
  }
  return detail::Plan<fill::Family>(Self(),
                                    fill::Family::ScoredContext{ctx, merge});
}

}  // namespace irs
