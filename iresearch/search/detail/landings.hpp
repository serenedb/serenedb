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

#include "iresearch/formats/posting/block_index.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

struct LandingPlan {
  uint32_t block;
  uint32_t next;
  doc_id_t upper;
  uint32_t cap;
};

template<typename Prefetch>
IRS_FORCE_INLINE uint32_t PlanLandings(const BlockIndex& index,
                                       LandingPlan plan, const doc_id_t* in,
                                       uint32_t n, uint32_t* blocks,
                                       Prefetch&& prefetch) {
  const auto size = index.Size();
  auto block = plan.block;
  auto next = plan.next;
  auto upper = plan.upper;
  uint32_t landings = 0;
  for (uint32_t i = 0; i != n && landings != plan.cap; ++i) {
    if (in[i] <= upper) {
      continue;
    }
    if (block == size || index.Last(block) < in[i]) {
      block = index.Find(block, in[i]);
    }
    if (block == size) {
      break;
    }
    blocks[landings++] = block;
    if (block != next) {
      prefetch(block);
    }
    upper = index.Last(block);
    next = ++block;
  }
  return landings;
}

}  // namespace irs::detail
