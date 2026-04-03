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
#include <vector>

#include "iresearch/index/iterators.hpp"
#include "iresearch/types.hpp"

namespace sdb::connector {

struct SegmentScoreDoc {
  irs::score_t score;
  uint32_t segment_index;
  irs::doc_id_t doc_id;
};
static_assert(sizeof(SegmentScoreDoc) == 12);

// Index-level top-k collector that maintains global top-k across all segments.
// Uses nth_element partitioning (same algorithm as NthPartitionScoreCollector)
// but stores SegmentScoreDoc (12 bytes) instead of ScoreDoc (8 bytes).
class TopKScoreCollector {
 public:
  explicit TopKScoreCollector(size_t k) noexcept
    : _k{k},
      _hits(2 * k),
      _hits_it{_hits.data()},
      _hits_pivot{_hits.data() + k},
      _hits_end{_hits.data() + 2 * k},
      _score_threshold{std::numeric_limits<irs::score_t>::min()} {}

  void Add(irs::score_t score, uint32_t segment_index,
           irs::doc_id_t doc_id) noexcept {
    ++_count;
    if (score > _score_threshold) {
      Push(score, segment_index, doc_id);
    }
  }

  // Bulk-add results from a segment-level collector after its Finalize().
  void AddFromSegment(std::span<const irs::ScoreDoc> results,
                      uint32_t segment_index) noexcept {
    for (const auto& [score, doc_id] : results) {
      Add(score, segment_index, doc_id);
    }
  }

  // Sort results by score descending and return total doc count.
  uint64_t Finalize() noexcept {
    auto* end = std::min(_hits_it, _hits_end);
    std::sort(_hits.data(), end, [](const auto& l, const auto& r) {
      return l.score > r.score;
    });
    return _count;
  }

  irs::score_t threshold() const noexcept { return _score_threshold; }

  std::span<const SegmentScoreDoc> results() const noexcept {
    auto count = std::min<size_t>(_count, _k);
    return {_hits.data(), count};
  }

 private:
  void Push(irs::score_t score, uint32_t segment_index,
            irs::doc_id_t doc_id) noexcept {
    *_hits_it = {score, segment_index, doc_id};
    ++_hits_it;
    if (_hits_it != _hits_end) {
      return;
    }
    _hits_it = _hits_pivot;
    std::nth_element(_hits.data(), _hits_pivot, _hits_end,
                     [](const auto& l, const auto& r) {
                       return l.score > r.score;
                     });
    _score_threshold = _hits_pivot->score;
  }

  size_t _k;
  uint64_t _count{0};
  std::vector<SegmentScoreDoc> _hits;
  SegmentScoreDoc* _hits_it;
  SegmentScoreDoc* const _hits_pivot;
  SegmentScoreDoc* const _hits_end;
  irs::score_t _score_threshold;
};

}  // namespace sdb::connector
