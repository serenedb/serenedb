////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
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
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <absl/algorithm/container.h>

#include "iresearch/search/block_disjunction.hpp"
#include "iresearch/search/conjunction.hpp"
#include "iresearch/search/disjunction.hpp"

// Disjunction is template for Adapter instead of direct use of ScoreAdapter
// only because of variadic phrase
namespace irs {

// Returns disjunction iterator created from the specified sub iterators
template<typename Disjunction, typename... Args>
DocIterator::ptr MakeDisjunction(WandContext ctx,
                                 typename Disjunction::Adapters&& itrs,
                                 Args&&... args) {
  const auto size = itrs.size();

  if (0 == size) {
    // Empty or unreachable search criteria
    return DocIterator::empty();
  }

  using UnaryDisjunction = typename RebindIterator<Disjunction>::Unary;
  if (1 == size) {
    if constexpr (std::is_void_v<UnaryDisjunction>) {
      return std::move(itrs.front());
    } else {
      SDB_ASSERT(!ctx.Enabled());
      return memory::make_managed<UnaryDisjunction>(std::move(itrs.front()));
    }
  }

  using BasicDisjunction = typename RebindIterator<Disjunction>::Basic;
  if constexpr (!std::is_void_v<BasicDisjunction>) {
    if (2 == size) {
      // 2-way disjunction
      return memory::make_managed<BasicDisjunction>(
        std::move(itrs.front()), std::move(itrs.back()),
        std::forward<Args>(args)...);
    }
  }

  using SmallDisjunction = typename RebindIterator<Disjunction>::Small;
  if constexpr (!std::is_void_v<SmallDisjunction>) {
    if (size <= Disjunction::kSmallDisjunctionUpperBound) {
      return memory::make_managed<SmallDisjunction>(
        std::move(itrs), std::forward<Args>(args)...);
    }
  }

  return memory::make_managed<Disjunction>(std::move(itrs),
                                           std::forward<Args>(args)...);
}

// Returns weak conjunction iterator created from the specified sub iterators
template<typename WeakConjunction, typename... Args>
DocIterator::ptr MakeWeakDisjunction(WandContext ctx,
                                     typename WeakConjunction::Adapters&& itrs,
                                     size_t min_match, Args&&... args) {
  // This case must be handled by a caller, we're unable to process it here
  SDB_ASSERT(min_match > 0);

  const auto size = itrs.size();

  if (0 == size || min_match > size) {
    // Empty or unreachable search criteria
    return DocIterator::empty();
  }

  if (1 == min_match) {
    // Pure disjunction
    using Disjunction = typename RebindIterator<WeakConjunction>::Disjunction;
    return MakeDisjunction<Disjunction>(ctx, std::move(itrs),
                                        std::forward<Args>(args)...);
  }

  if (min_match == size) {
    // Pure conjunction
    return MakeConjunction(WeakConjunction::kMergeType, ctx, std::move(itrs));
  }

  return memory::make_managed<WeakConjunction>(std::move(itrs), min_match,
                                               std::forward<Args>(args)...);
}

}  // namespace irs
