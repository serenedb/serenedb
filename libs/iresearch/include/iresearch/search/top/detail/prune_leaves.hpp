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
#include <cstddef>
#include <limits>
#include <span>
#include <utility>
#include <vector>

#include "basics/shared.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/score_filter.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top::detail {

template<typename Leaf, size_t N = 0>
class PruneLeaves {
 public:
  template<typename Init>
  PruneLeaves(ColumnArgsFetcher& fetcher, size_t size, Init&& init)
    : _fetcher{fetcher},
      _leaves{size, std::forward<Init>(init)},
      _scorers{size, [this](ScoreFunction& scorer,
                            size_t i) { scorer = _leaves[i].PrepareScore(); }},
      _remaining{size},
      _suffix{size},
      _order{size} {}

  PruneLeaves(PruneLeaves&&) = delete;
  PruneLeaves& operator=(PruneLeaves&&) = delete;

  size_t size() const noexcept { return _leaves.size(); }

  doc_id_t AdvanceTo(doc_id_t min) {
    doc_id_t end = doc_limits::eof();
    for (auto& leaf : _leaves) {
      const auto e = leaf.AdvanceBlock(std::max(leaf.Value(), min));
      if (!doc_limits::eof(e)) {
        end = std::min(end, e);
      }
    }
    return end;
  }

  score_t OpenWindow(doc_id_t min, doc_id_t last) {
    const auto count = _leaves.size();
    for (size_t i = 0; i != count; ++i) {
      auto& leaf = _leaves[i];
      _order[i] = static_cast<uint32_t>(i);
      _remaining[i] = leaf.MaxScore(last);
    }
    absl::c_sort(_order, [&](uint32_t a, uint32_t b) noexcept {
      return _remaining[a] > _remaining[b];
    });
    score_t total = 0;
    for (size_t i = count; i-- != 0;) {
      total += _remaining[_order[i]];
      _suffix[i] = total;
    }
    return total;
  }

  uint32_t Apply(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
                 uint32_t len, score_t threshold) {
    const auto count = _leaves.size();
    uint32_t fetched = 0;
    for (size_t i = 0; i != count && len != 0; ++i) {
      if (const auto required = threshold - _suffix[i]; required > 0) {
        len = search::FilterScores(docs, scores, len, required);
        if (len == 0) {
          break;
        }
      }
      auto& leaf = _leaves[_order[i]];
      uint32_t out = 0;
      for (uint32_t j = 0; j != len; ++j) {
        const auto doc = docs[j];
        const auto hit = static_cast<uint32_t>(leaf.Probe(doc) == doc);
        docs[out] = doc;
        scores[out] = scores[j];
        leaf.FetchScoreArgs(out);
        out += hit;
      }
      len = out;
      if (len == 0) {
        break;
      }
      if (fetched != len) {
        _fetcher.Fetch(std::span<const doc_id_t>{docs, len});
        fetched = len;
      }
      _scorers[_order[i]].template Score<ScoreMergeType::Sum>(
        scores, static_cast<scores_size_t>(len));
    }
    return len;
  }

 private:
  ColumnArgsFetcher& _fetcher;
  search::RunOf<Leaf, N> _leaves;
  search::RunOf<ScoreFunction, N> _scorers;
  search::RunOf<score_t, N> _remaining;
  search::RunOf<score_t, N> _suffix;
  search::RunOf<uint32_t, N> _order;
};

}  // namespace irs::top::detail
