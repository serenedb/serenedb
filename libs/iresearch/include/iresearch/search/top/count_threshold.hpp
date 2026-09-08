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

#include "basics/bit_utils.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Leaves, typename Table>
class CountThreshold : public Root {
 public:
  static constexpr size_t kNumWords = search::kWindowWords;
  static constexpr doc_id_t kWindow = search::kWindowDocs;
  static constexpr uint32_t kBatch = kScoreBlock;

  template<typename LeavesArgs>
  CountThreshold(Table table, std::piecewise_construct_t, LeavesArgs&& leaves,
                 uint32_t min_match, score_t absorbed = 0)
    : _leaves{std::make_from_tuple<Leaves>(std::forward<LeavesArgs>(leaves))},
      _min_match{min_match},
      _constant{absorbed},
      _admit{table} {
    SDB_ASSERT(_min_match > 1);
    std::fill_n(_window, kWindow, _constant);
  }

  void Run(LoserScoreCollector& collector) final {
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kBatch];
    ABSL_CACHELINE_ALIGNED score_t scores[kBatch];
    uint32_t batch = 0;
    doc_id_t next = doc_limits::min();

    while (_leaves.Live() >= _min_match) {
      const doc_id_t min = next;
      next = _leaves.Visit(min + kWindow, [min, max = min + kWindow,
                                           this](auto& leaf) IRS_FORCE_INLINE {
        return leaf.Count(min, max, _counts, _mask, _window);
      });

      for (size_t w = 0; w != kNumWords; ++w) {
        const auto word = _mask[w];
        _mask[w] = 0;
        batch = DrainWord(word, min, w * BitsRequired<uint64_t>(), docs, scores,
                          batch, collector);
      }
    }

    if (batch != 0) {
      _admit.AddDocs(collector, docs, batch, scores);
    }
    _admit.Flush(collector);
  }

 private:
  uint32_t DrainWord(uint64_t word, doc_id_t min, size_t base,
                     doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
                     uint32_t batch, LoserScoreCollector& collector) {
    while (word != 0) {
      const size_t offset =
        base + static_cast<uint32_t>(std::countr_zero(word));
      word = PopBit(word);
      if (_counts[offset] >= _min_match) {
        docs[batch] = min + static_cast<doc_id_t>(offset);
        scores[batch] = _window[offset];
        if (++batch == kBatch) {
          _admit.AddDocs(collector, docs, kBatch, scores);
          batch = 0;
        }
      }
      _counts[offset] = 0;
      _window[offset] = _constant;
    }
    return batch;
  }

  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED uint32_t _counts[kWindow]{};
  ABSL_CACHELINE_ALIGNED score_t _window[kWindow]{};
  Leaves _leaves;
  uint32_t _min_match;
  score_t _constant;
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
