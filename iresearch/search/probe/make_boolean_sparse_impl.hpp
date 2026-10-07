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
#include <tuple>
#include <utility>

#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/probe_leaves.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/probe/boolean_sparse.hpp"
#include "iresearch/search/probe/impl.hpp"
#include "iresearch/search/probe/leaves.hpp"
#include "iresearch/search/probe/make.hpp"
#include "iresearch/search/probe/plan.hpp"

namespace irs::probe {

template<typename Musts, typename Optional, typename Excludes, typename... Args>
Node::ptr MakeSparse(Args&&... args) {
  using Node = BooleanSparse<Musts, Optional, Excludes>;
  return memory::make_managed<Impl<Node>>(std::piecewise_construct,
                                          std::forward<Args>(args)...);
}

template<typename Make>
Node::ptr BuildMusts(std::span<const detail::PostingClause> terms,
                     std::span<const QueryBuilder::ptr> filters,
                     const SubReader& segment, uint64_t interrogations,
                     Make&& make) {
  SDB_ASSERT(!terms.empty() || !filters.empty());
  if (terms.size() + filters.size() == 1) {
    if (filters.empty()) {
      return ResolvePostingDocs<Node::ptr>(
        terms.front(), [&]<typename Leaf>(auto&&... args) -> Node::ptr {
          return make.template operator()<Leaf>(
            std::forward_as_tuple(std::forward<decltype(args)>(args)...));
        });
    }
    auto node = filters.front()->PlanProbe({}, interrogations);
    if (!node) {
      return {};
    }
    return make.template operator()<Erased>(
      std::forward_as_tuple(std::move(node)));
  }
  return detail::BuildProbeLeaves<Node::ptr>(
    terms, filters, nullptr, segment, interrogations,
    detail::ProbeOrder::Narrowest,
    [&]<typename Leaf>(size_t size, auto&& init) -> Node::ptr {
      return detail::ResolveArity<detail::kRunArity, detail::kRunFloor>(
        size, [&]<size_t N> -> Node::ptr {
          return make.template operator()<AndLeaves<Leaf, N>>(
            std::forward_as_tuple(size, std::forward<decltype(init)>(init)));
        });
    });
}

}  // namespace irs::probe
