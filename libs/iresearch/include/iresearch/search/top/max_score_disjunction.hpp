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
#include <limits>
#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "basics/bit_utils.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

template<typename Leaf, typename Table>
class MaxScoreDisjunction : public Root {
 public:
  static constexpr doc_id_t kWordBits = search::kWindowBits;
  static constexpr size_t kNumWords = search::kWindowWords;
  static constexpr doc_id_t kWindow = search::kWindowDocs;

  template<typename Init>
  MaxScoreDisjunction(Table table, size_t size, Init&& init)
    : _own{size, std::forward<Init>(init)},
      _sorted{size,
              [this](Entry& entry, size_t i) noexcept {
                entry = Entry{.leaf = &_own[i],
                              .cost = std::max<uint32_t>(1, _own[i].Cost())};
              }},
      _admit{table} {}

  void Run(LoserScoreCollector& collector) final {
    _collector = &collector;
    const doc_id_t max = doc_limits::eof();
    doc_id_t window_min = doc_limits::min();

    _num_candidates = 0;
    _num_outer_windows = 0;
    _min_window_size = 1;

  outer:
    while (window_min < max) {
      doc_id_t window_max = ComputeOuterWindow(window_min);

      for (;;) {
        UpdateWindowScores(window_min, window_max);
        if (!Split(collector.ScoreThreshold())) {
          window_min = window_max;
          goto outer;
        }
        const doc_id_t next = ComputeOuterWindow(window_min);
        if (next >= window_max) {
          break;
        }
        window_max = next;
      }

      ProcessEssential([&](Entry& entry) {
        if (entry.leaf->Value() < window_min) {
          entry.leaf->Seek(window_min);
        }
      });

      for (auto min = Top(); min < window_max; min = Top()) {
        ScoreAndCollectWindow(collector, min, window_max);
        if (collector.ScoreThreshold() >= _next_threshold) {
          break;
        }
      }

      window_min = std::min(Top(), window_max);
      ++_num_outer_windows;
    }
    _admit.Flush(collector);
  }

 private:
  struct Entry {
    Leaf* leaf = nullptr;
    uint32_t cost = 1;
    score_t max_score = 0;
    double prefix_score_sum = 0;
  };

  template<typename T>
  struct View {
    T* data;
    size_t count;

    size_t size() const noexcept { return count; }
    bool empty() const noexcept { return count == 0; }
    void resize(size_t n) noexcept { count = n; }
    T& operator[](size_t i) const noexcept { return data[i]; }
  };

  IRS_FORCE_INLINE doc_id_t Top() const noexcept {
    return _sorted[_first_essential].leaf->Value();
  }

  IRS_FORCE_INLINE doc_id_t SecondEssentialDoc() const noexcept {
    SDB_ASSERT(_num_essential >= 2);
    const auto* const first = _sorted.data() + _first_essential;
    if (_num_essential == 2) {
      return first[1].leaf->Value();
    }
    return std::min(first[1].leaf->Value(), first[2].leaf->Value());
  }

  static bool Ahead(const Entry& lhs, const Entry& rhs) noexcept {
    return lhs.leaf->Value() > rhs.leaf->Value();
  }

  void UpdateHeapTop() {
    auto* const begin = _sorted.data() + _first_essential;
    auto* const end = _sorted.data() + _sorted.size();
    std::pop_heap(begin, end, Ahead);
    std::push_heap(begin, end, Ahead);
  }

  template<typename Visitor>
  void ProcessEssential(Visitor&& visit) {
    auto* const begin = _sorted.data() + _first_essential;
    auto* const end = _sorted.data() + _sorted.size();
    std::for_each(begin, end, visit);
    std::make_heap(begin, end, Ahead);
  }

  void UpdateWindowScores(doc_id_t min, doc_id_t max) {
    for (auto& entry : _sorted) {
      if (entry.leaf->Value() >= max) {
        entry.max_score = 0;
        continue;
      }
      if (entry.leaf->Value() < min) {
        entry.leaf->SeekToBlock(min);
      }
      entry.max_score = entry.leaf->MaxScore(max - 1);
    }
  }

  bool Split(score_t score_threshold) {
    absl::c_sort(_sorted, [](const Entry& lhs, const Entry& rhs) noexcept {
      return static_cast<double>(lhs.max_score) / lhs.cost <
             static_cast<double>(rhs.max_score) / rhs.cost;
    });

    const auto threshold = static_cast<double>(score_threshold);
    const auto size = _sorted.size();
    double max_score_sum = 0;
    _first_essential = 0;
    _next_threshold = std::numeric_limits<score_t>::max();

    for (size_t i = 0; i != size; ++i) {
      const double sum = max_score_sum + _sorted[i].max_score;
      if (sum < threshold) {
        max_score_sum = sum;
        if (i != _first_essential) {
          std::swap(_sorted[i], _sorted[_first_essential]);
        }
        _sorted[_first_essential].prefix_score_sum = max_score_sum;
        ++_first_essential;
      } else {
        _next_threshold = std::min(_next_threshold, static_cast<score_t>(sum));
      }
    }

    if (_first_essential == size) {
      return false;
    }

    _num_essential = size - _first_essential;
    _has_non_essential = _first_essential != 0;

    _first_required = _first_essential;
    if (_num_essential == 1) {
      double required = _sorted[_first_essential].max_score;
      while (_first_required != 0) {
        double without = required;
        if (_first_required > 1) {
          without += _sorted[_first_required - 2].prefix_score_sum;
        }
        if (without >= threshold) {
          break;
        }
        --_first_required;
        required += _sorted[_first_required].max_score;
      }
    }
    return true;
  }

  doc_id_t ComputeOuterWindow(doc_id_t min) {
    doc_id_t max = doc_limits::eof();
    for (size_t i = _first_essential; i != _sorted.size(); ++i) {
      auto* const leaf = _sorted[i].leaf;
      const doc_id_t block_max =
        leaf->SeekToBlock(std::max(leaf->Value(), min));
      if (!doc_limits::eof(block_max)) {
        max = std::min(block_max + 1, max);
      }
    }

    if (_num_essential > 1) {
      const auto sparse =
        32U * _num_outer_windows * static_cast<uint32_t>(_sorted.size());
      if (_num_candidates < sparse) {
        _min_window_size = std::min<doc_id_t>(2 * _min_window_size, kWindow);
      } else {
        _min_window_size = 1;
      }
      if (!doc_limits::eof(max)) {
        max = std::max(max, min + _min_window_size);
      }
    }
    return max;
  }

  void ProcessSingleEssential(LoserScoreCollector& collector, doc_id_t max) {
    auto* const leaf = _sorted[_first_essential].leaf;
    leaf->ForEachScoredBlock(
      max, [&](doc_id_t* IRS_RESTRICT docs, uint32_t len,
               score_t* IRS_RESTRICT scores) IRS_FORCE_INLINE {
        if (_has_non_essential) {
          View<doc_id_t> cand_docs{docs, len};
          View<score_t> cand_scores{scores, len};
          ProcessNonEssential(cand_docs, cand_scores, max);
          len = static_cast<uint32_t>(cand_docs.count);
        }
        if (len != 0) {
          _admit.AddDocs(collector, docs, len, scores);
        }
      });
  }

  void ScoreAndCollectWindow(LoserScoreCollector& collector, doc_id_t min,
                             doc_id_t max) {
    if (_num_essential == 1) {
      ProcessSingleEssential(collector, max);
      return;
    }
    if (const auto second = SecondEssentialDoc(); second >= min + kWindow / 2) {
      ProcessSingleEssential(collector, std::min(max, second));
      UpdateHeapTop();
      return;
    }

    max = std::min(min + kWindow, max);
    ProcessEssential([&](Entry& entry) IRS_FORCE_INLINE {
      entry.leaf->Fill(min, max, _mask, _scores);
    });

    if (!_has_non_essential) {
      _admit.Window(collector, _scores, _mask, min, kNumWords);
      return;
    }
    const auto count = DrainCandidates(min);
    View<doc_id_t> cand_docs{_cand_docs, count};
    View<score_t> cand_scores{_cand_scores, count};
    ProcessNonEssential(cand_docs, cand_scores, max);
    if (cand_docs.count != 0) {
      _admit.AddDocs(collector, _cand_docs, cand_docs.count, _cand_scores);
    }
  }

  size_t DrainCandidates(doc_id_t min) {
    size_t count = 0;
    for (size_t i = 0; i != kNumWords; ++i) {
      auto word = _mask[i];
      if (word == 0) {
        continue;
      }
      _mask[i] = 0;
      const size_t base = i * kWordBits;
      do {
        const size_t offset =
          base + static_cast<size_t>(std::countr_zero(word));
        word = PopBit(word);
        _cand_docs[count] = min + static_cast<doc_id_t>(offset);
        _cand_scores[count] = std::exchange(_scores[offset], 0.f);
        ++count;
      } while (word != 0);
    }
    return count;
  }

  template<typename Docs, typename Scores>
  static void FilterCompetitive(Docs& docs, Scores& scores,
                                score_t score_threshold) {
    SDB_ASSERT(score_threshold > 0);
    const auto out =
      search::FilterScores(docs.data, scores.data,
                           static_cast<uint32_t>(docs.size()), score_threshold);
    docs.resize(out);
    scores.resize(out);
  }

  template<typename Docs, typename Scores>
  void ProcessNonEssential(Docs& cand_docs, Scores& cand_scores, doc_id_t max) {
    _num_candidates += static_cast<uint32_t>(cand_docs.size());
    const score_t threshold = _collector->ScoreThreshold();

    for (size_t i = _first_essential; i-- != 0;) {
      auto& entry = _sorted[i];
      const auto score_threshold =
        threshold - static_cast<score_t>(entry.prefix_score_sum);
      if (score_threshold > 0) {
        FilterCompetitive(cand_docs, cand_scores, score_threshold);
        if (cand_docs.empty()) {
          return;
        }
      }
      entry.leaf->ScoreCandidates(cand_docs, cand_scores, i >= _first_required,
                                  max);
    }
  }

  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED score_t _scores[kWindow]{};
  ABSL_CACHELINE_ALIGNED doc_id_t _cand_docs[kWindow];
  ABSL_CACHELINE_ALIGNED score_t _cand_scores[kWindow];

  LoserScoreCollector* _collector = nullptr;
  search::FixedArray<Leaf> _own;
  search::FixedArray<Entry> _sorted;
  size_t _first_essential = 0;
  size_t _first_required = 0;
  size_t _num_essential = 0;
  bool _has_non_essential = false;
  uint32_t _num_candidates = 0;
  uint32_t _num_outer_windows = 0;
  doc_id_t _min_window_size = 1;
  score_t _next_threshold = std::numeric_limits<score_t>::lowest();
  [[no_unique_address]] Admit<Table> _admit;
};

}  // namespace irs::top
