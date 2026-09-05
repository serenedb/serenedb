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

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Leaves, typename Excludes, typename Table>
class WindowDisjunction : public Root {
 public:
  static constexpr size_t kNumWords = search::kWindowWords;
  static constexpr doc_id_t kWindow = search::kWindowDocs;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename LeavesArgs, typename ExcludesArgs>
  WindowDisjunction(Table table, std::piecewise_construct_t,
                    LeavesArgs&& leaves, ExcludesArgs&& excludes,
                    ScoreMergeType merge, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _score{merge, absorbed},
      _admit{table} {}

  void Run(LoserScoreCollector& collector) final {
    doc_id_t next = 0;
    while (!_leaves.Empty()) {
      const auto min = next;
      const auto max = min + kWindow;
      next = _leaves.Visit(max, [min, max, this](auto& leaf) IRS_FORCE_INLINE {
        return leaf.Fill(min, max, _mask, _window);
      });
      if constexpr (kExcludes) {
        _excludes.Remove(min, max, _mask, _window, score_t{0});
      }
      _score.Apply(_window, _mask, kNumWords);
      _admit.Window(collector, _window, _mask, min, kNumWords);
    }
    _admit.Flush(collector);
  }

 private:
  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED score_t _window[kWindow]{};
  Leaves _leaves;
  [[no_unique_address]] Excludes _excludes;
  RootWindowScore _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
