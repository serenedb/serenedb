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

#include "iresearch/search/detail/column_collector.hpp"
#include "iresearch/search/detail/score_filter.hpp"
#include "iresearch/search/scorers/score_args.hpp"
#include "iresearch/search/scorers/score_function.hpp"
#include "iresearch/utils/containers/fixed.hpp"
#include "iresearch/utils/shared.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Leaf, size_t N = 0>
class PruneLeaves {
 public:
  template<typename Init>
  PruneLeaves(ColumnArgsFetcher& fetcher, size_t size, Init&& init)
    : _fetcher{fetcher},
      _leaves{size, std::forward<Init>(init)},
      _scorers{size, [this](ScoreFunction& scorer,
                            size_t i) { scorer = Scorer(_leaves[i]); }},
      _remaining{size},
      _suffix{size},
      _order{size} {}

  template<typename Args>
  PruneLeaves(ColumnArgsFetcher& fetcher, size_t size,
              std::piecewise_construct_t, Args&& args)
    : _fetcher{fetcher},
      _leaves{size, std::piecewise_construct, std::forward<Args>(args)},
      _scorers{size, [this](ScoreFunction& scorer,
                            size_t i) { scorer = Scorer(_leaves[i]); }},
      _remaining{size},
      _suffix{size},
      _order{size} {}

  PruneLeaves(PruneLeaves&&) = delete;
  PruneLeaves& operator=(PruneLeaves&&) = delete;

  size_t size() const noexcept { return _leaves.size(); }

  uint32_t TakeDropped() noexcept { return std::exchange(_dropped, 0); }

  uint32_t TakeReads() noexcept {
    uint32_t reads = 0;
    if constexpr (requires(Leaf& leaf) { leaf.TakeReads(); }) {
      for (auto& leaf : _leaves) {
        reads += leaf.TakeReads();
      }
    }
    return reads;
  }

  doc_id_t AdvanceTo(doc_id_t min) {
    doc_id_t end = doc_limits::eof();
    for (auto& leaf : _leaves) {
      end = std::min(end, leaf.AdvanceBlock(min));
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

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    auto* leaf = _leaves.begin();
    const auto* const end = _leaves.end();
    do {
      if (const auto probe = leaf->Probe(target); probe != target) {
        return probe;
      }
    } while (++leaf != end);
    return target;
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) {
    for (auto& leaf : _leaves) {
      leaf.FetchScoreArgs(slot);
    }
  }

  void Fetch(const doc_id_t* docs, uint32_t len) {
    if (len == kScoreBlock) {
      _fetcher.FetchScoreBlock(
        std::span<const doc_id_t, kScoreBlock>{docs, kScoreBlock});
    } else {
      _fetcher.Fetch(std::span<const doc_id_t>{docs, len});
    }
  }

  void Score(score_t* scores, uint32_t len) {
    if (len == kScoreBlock) {
      for (auto& scorer : _scorers) {
        scorer.template ScoreBlock<ScoreMergeType::Sum>(scores);
      }
    } else {
      for (auto& scorer : _scorers) {
        scorer.template Score<ScoreMergeType::Sum>(
          scores, static_cast<scores_size_t>(len));
      }
    }
  }

  uint32_t Apply(doc_id_t* IRS_RESTRICT docs, score_t* IRS_RESTRICT scores,
                 uint32_t len, score_t threshold) {
    const auto count = _leaves.size();
    uint32_t fetched = 0;
    for (size_t i = 0; i != count && len != 0; ++i) {
      if (const auto required = threshold - _suffix[i]; required > 0) {
        const auto kept =
          irs::detail::FilterScores(docs, scores, len, required);
        _dropped += len - kept;
        len = kept;
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
  static ScoreFunction Scorer(Leaf& leaf) {
    if constexpr (requires { leaf.PrepareScore(); }) {
      return leaf.PrepareScore();
    } else {
      return leaf.PrepareScore(ScoreMergeType::Sum, score_t{0});
    }
  }

  ColumnArgsFetcher& _fetcher;
  irs::containers::Fixed<Leaf, N> _leaves;
  irs::containers::Fixed<ScoreFunction, N> _scorers;
  irs::containers::Fixed<score_t, N> _remaining;
  irs::containers::Fixed<score_t, N> _suffix;
  irs::containers::Fixed<uint32_t, N> _order;
  uint32_t _dropped = 0;
};

}  // namespace irs::top
