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

#include "basics/memory.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/common/score/root_window.hpp"
#include "iresearch/search/common/scored_context.hpp"

namespace irs::top {

using search::RootWindowScore;

struct Context {
  const Scorer& scorer;
  ColumnArgsFetcher& fetcher;
  search::TableFilter* table = nullptr;
  bool prune = false;
  uint32_t k = 0;
};

inline search::ScoredCtx ScoredOf(const Context& ctx) noexcept {
  return {
    .scorer = &ctx.scorer,
    .fetcher = &ctx.fetcher,
  };
}

struct Root : memory::Managed {
  using ptr = memory::managed_ptr<Root>;

  virtual void Run(LoserScoreCollector& collector) = 0;
};

}  // namespace irs::top
