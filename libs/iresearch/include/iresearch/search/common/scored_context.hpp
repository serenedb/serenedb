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

#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/table_filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

class ColumnArgsFetcher;

namespace search {

struct ScoredCtx {
  const Scorer* scorer = nullptr;
  ColumnArgsFetcher* fetcher = nullptr;
};

}  // namespace search
namespace scored {

using search::ScoreArgs;

struct Context {
  const Scorer& scorer;
  ColumnArgsFetcher& fetcher;
  search::DeadRuns* table = nullptr;
};

inline search::ScoredCtx ScoredOf(const Context& ctx) noexcept {
  return {
    .scorer = &ctx.scorer,
    .fetcher = &ctx.fetcher,
  };
}

}  // namespace scored
}  // namespace irs
