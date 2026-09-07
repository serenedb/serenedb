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

#include <algorithm>
#include <bit>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Leaves, typename Table>
class BitsThreshold : public Root {
 public:
  template<typename LeavesArgs>
  BitsThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                uint32_t min_match, ScoreMergeType merge, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _planes(size_t{min_match} * search::kWindowWords, 0),
      _min_match{min_match},
      _score{merge, absorbed},
      _admit{table} {
    SDB_ASSERT(_min_match > 1);
  }

  void Run(LoserScoreCollector& collector) final {
    auto* const planes = _planes.data();
    const auto top = size_t{_min_match} - 1;
    auto* const answers = planes + top * search::kWindowWords;
    doc_id_t next = 0;

    while (_leaves.Live() >= _min_match) {
      const auto min = next;
      std::fill(_planes.begin(), _planes.end(), uint64_t{0});
      next = _leaves.Visit(min + search::kWindowDocs, [&](auto& leaf) {
        search::Clear(_scratch.data(), search::kWindowWords);
        const auto doc =
          leaf.Fill(min, min + search::kWindowDocs, _scratch.data(), _window);
        search::FoldCarry(planes, _scratch.data(), search::kWindowWords, top);
        return doc;
      });

      _score.Apply(_window, answers, search::kWindowWords);
      _admit.Window(collector, _window, answers, min, search::kWindowWords);
      for (size_t w = 0; w != search::kWindowWords; ++w) {
        auto touched = planes[w];
        const size_t base = w * search::kWindowBits;
        while (touched != 0) {
          _window[base + static_cast<uint32_t>(std::countr_zero(touched))] = 0;
          touched = PopBit(touched);
        }
      }
    }
    _admit.Flush(collector);
  }

 private:
  ABSL_CACHELINE_ALIGNED score_t _window[search::kWindowDocs]{};
  search::Scratch _scratch{};
  Leaves _leaves;
  std::vector<uint64_t> _planes;
  uint32_t _min_match;
  RootWindowScore _score;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
