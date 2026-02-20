////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025-2026 SereneDB GmbH, Berlin, Germany
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

#include <immintrin.h>

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<typename T, size_t Capacity, size_t Align = 64>
struct FixedBuffer {
  auto* data(this auto& self) noexcept { return self._data; }
  size_t size() const noexcept { return _size; }
  bool empty() const noexcept { return _size == 0; }
  constexpr size_t capacity() const noexcept { return Capacity; }
  auto& operator[](this auto& self, size_t i) noexcept { return self._data[i]; }
  auto* begin(this auto& self) noexcept { return self._data; }
  auto* end(this auto& self) noexcept { return self.data() + self.size(); }
  void clear() noexcept { _size = 0; }
  void push_back(T v) noexcept {
    SDB_ASSERT(_size < Capacity);
    _data[_size++] = v;
  }
  void resize(size_t n) noexcept {
    SDB_ASSERT(n <= Capacity);
    _size = n;
  }

 private:
  alignas(Align) T _data[Capacity];
  size_t _size = 0;
};

template<typename Adapter>
class MaxScoreIterator : public DocIterator {
 public:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();
  static constexpr auto kNumBlocks = 64;
  static constexpr doc_id_t kWindow = kBlockSize * kNumBlocks;
  static constexpr size_t kSubBlock = kPostingBlock;
  static constexpr size_t kSubWindows = kWindow / kSubBlock;

  struct AdapterWrapper : Adapter {
    ScoreFunction scorer;
    CostAttr::Type cost{};
    score_t max_score{};
    double prefix_score_sum{};
  };

  explicit MaxScoreIterator(std::vector<DocIterator::ptr> itrs) {
    _itrs.reserve(itrs.size());
    _itrs_sorted.resize(itrs.size());
    for (size_t i = 0; i < itrs.size(); ++i) {
      auto& it = _itrs.emplace_back(std::move(itrs[i]));
      it.cost = std::max<CostAttr::Type>(1, CostAttr::extract(it, 0));
      _itrs_sorted[i] = &it;
    }
    _first_essential = _itrs_sorted.begin();
    _first_required = _itrs_sorted.begin();

    std::get<CostAttr>(_attrs).reset([&] noexcept {
      return absl::c_accumulate(
        _itrs, CostAttr::Type{0},
        [](CostAttr::Type lhs, const AdapterWrapper& rhs) noexcept {
          return lhs + rhs.cost;
        });
    });
  }

  doc_id_t advance() final {
    SDB_ASSERT(false);
    return _doc;
  }
  doc_id_t seek(doc_id_t target) final { return advance(); }
  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    return irs::GetMutable(_attrs, type);
  }

  ScoreFunction PrepareScore(const PrepareScoreContext& ctx) final {
    const PrepareScoreContext sub{
      .scorer = ctx.scorer,
      .segment = ctx.segment,
      .fetcher = &_fetcher,
    };

    for (auto& it : _itrs) {
      it.scorer = it.PrepareScore(sub);
    }

    return ScoreFunction::Default();
  }

  void Collect(const ScoreFunction& scorer, ColumnArgsFetcher& fetcher,
               ScoreCollector& collector) final {
    ResolveScoreCollector(collector, [&](auto& collector) {
      const doc_id_t max = doc_limits::eof();
      doc_id_t window_min = doc_limits::min();

      _num_candidates = 0;
      _num_outer_windows = 0;
      _min_window_size = 1;
    outer:
      while (window_min < max) {
        doc_id_t window_max = ComputeOuterWindow(window_min);

        while (true) {
          UpdateWindowScores(window_min, window_max);
          if (!Split()) {
            window_min = window_max;
            goto outer;
          }

          const doc_id_t new_window_max = ComputeOuterWindow(window_min);
          if (new_window_max >= window_max) {
            break;
          }

          window_max = new_window_max;
        }

        ProcessEssential([&](auto* it) {
          if (it->value() < window_min) {
            it->seek(window_min);
          }
        });

        for (auto min = Top(); min < window_max; min = Top()) {
          ScoreAndCollectWindow(collector, min, window_max);

          if (std::get<ScoreThresholdAttr>(_attrs).value >= _next_threshold) {
            break;
          }
        }

        window_min = std::min(Top(), window_max);
        ++_num_outer_windows;
      }
    });

    _doc = doc_limits::eof();
  }

 private:
  IRS_FORCE_INLINE doc_id_t Top() const noexcept {
    return (*_first_essential)->value();
  }

  // Second smallest doc among essentials (children of heap root).
  IRS_FORCE_INLINE doc_id_t SecondEssentialDoc() noexcept {
    const auto n = std::distance(_first_essential, _itrs_sorted.end());
    SDB_ASSERT(n >= 2);
    if (n == 2) {
      return _first_essential[1]->value();
    }
    // Min-heap children of root are at indices 1 and 2.
    return std::min(_first_essential[1]->value(), _first_essential[2]->value());
  }

  void UpdateHeapTop() {
    const auto cmp = [](auto* lhs, auto* rhs) {
      return lhs->value() > rhs->value();
    };
    std::pop_heap(_first_essential, _itrs_sorted.end(), cmp);
    std::push_heap(_first_essential, _itrs_sorted.end(), cmp);
  }

  void ProcessSingleEssential(auto& collector, doc_id_t min, doc_id_t max) {
    auto* it = *_first_essential;
    _cand_docs.clear();
    _cand_scores.clear();
    it->CollectRange(_cand_docs, _cand_scores, it->scorer, &_fetcher, min, max);

    if (!_cand_docs.empty()) {
      if (_has_non_essential) {
        ProcessNonEssentialFromCandidates(min, max);
      }
      if (!_cand_docs.empty()) {
        collector.AddDocs(_cand_docs.data(), _cand_docs.size(),
                          _cand_scores.data());
      }
    }
  }

  void ScoreAndCollectWindow(auto& collector, doc_id_t min, doc_id_t max) {
    max = std::min(min + kWindow, max);

    if (_num_essential == 1) {
      ProcessSingleEssential(collector, min, max);
    } else if (auto top2 = SecondEssentialDoc(); top2 >= min + kWindow / 2) {
      // 2+ essentials but 2nd is far ahead: treat lead as single essential.
      max = std::min(max, top2);
      ProcessSingleEssential(collector, min, max);
      UpdateHeapTop();
    } else {
      // Multiple essentials
      FillBlockScoreContext score_ctx{
        .fetcher = &_fetcher,
        .score_window = _scores,
        .merge_type = ScoreMergeType::Sum,
      };

      ProcessEssential([&](auto* it) {
        score_ctx.score = &it->scorer;
        it->FillBlock(min, max, _mask, score_ctx, {});
      });

      if (_has_non_essential) {
        DrainCandidates(min);
        ProcessNonEssentialFromCandidates(min, max);
        if (!_cand_docs.empty()) {
          collector.AddDocs(_cand_docs.data(), _cand_docs.size(),
                            _cand_scores.data());
        }
      } else {
        collector.AddWindow(_scores, _mask, min, kNumBlocks, true);
        std::memset(_mask, 0, sizeof _mask);
      }
    }
  }

  void DrainCandidates(doc_id_t min) {
    _cand_docs.clear();
    _cand_scores.clear();
    auto* IRS_RESTRICT mask = _mask;
    for (size_t i = 0; i < kNumBlocks; ++i) {
      for (auto bits = std::exchange(mask[i], 0); bits; bits = PopBit(bits)) {
        const size_t bit = std::countr_zero(bits);
        const size_t offset = i * kBlockSize + bit;
        _cand_docs.push_back(min + static_cast<doc_id_t>(offset));
        _cand_scores.push_back(std::exchange(_scores[offset], 0));
      }
    }
  }

  // Remove candidates whose score cannot reach score_threshold.
  static void FilterCompetitiveHits(auto& docs, auto& scores,
                                    score_t score_threshold) {
    SDB_ASSERT(score_threshold > 0);

    const size_t n = docs.size();
    SDB_ASSERT(scores.size() == n);
    size_t out = 0;
    size_t i = 0;

    const auto threshold = _mm256_set1_ps(score_threshold);
    for (; i + 8 <= n; i += 8) {
      auto vscores = _mm256_loadu_ps(&scores[i]);
      auto cmp = _mm256_cmp_ps(vscores, threshold, _CMP_GT_OQ);
      auto mask = static_cast<unsigned>(_mm256_movemask_ps(cmp));
      while (mask) {
        const int bit = std::countr_zero(mask);
        mask = PopBit(mask);
        docs[out] = docs[i + bit];
        scores[out] = scores[i + bit];
        ++out;
      }
    }

    for (; i < n; ++i) {
      if (scores[i] > score_threshold) {
        docs[out] = docs[i];
        scores[out] = scores[i];
        ++out;
      }
    }

    docs.resize(out);
    scores.resize(out);
  }

  void ProcessNonEssentialFromCandidates(doc_id_t min, doc_id_t max) {
    _num_candidates += _cand_docs.size();
    const score_t threshold = std::get<ScoreThresholdAttr>(_attrs).value;

    const auto first_required = _first_required;
    for (auto it = _first_essential; it != _itrs_sorted.begin();) {
      --it;
      auto* IRS_RESTRICT itr = *it;
      const bool is_required = (it >= first_required);
      const score_t budget = static_cast<score_t>(itr->prefix_score_sum);
      const score_t score_threshold = threshold - budget;

      if (score_threshold > 0) {
        FilterCompetitiveHits(_cand_docs, _cand_scores, score_threshold);
        if (_cand_docs.empty()) {
          break;
        }
      }

      itr->ScoreCandidates(_cand_docs, _cand_scores, itr->scorer, &_fetcher,
                           is_required, max);
    }
  }

  void ProcessEssential(auto&& visitor) {
    std::for_each(_first_essential, _itrs_sorted.end(), visitor);
    std::make_heap(
      _first_essential, _itrs_sorted.end(),
      [](auto* lhs, auto* rhs) { return lhs->value() > rhs->value(); });
  }

  void UpdateWindowScores(doc_id_t min, doc_id_t max) {
    for (auto& it : _itrs) {
      if (it.value() >= max) {
        it.max_score = 0;
        continue;
      }
      if (it.value() < min) {
        it.SeekToBlock(min);
      }
      it.max_score = it.GetMaxScore(max - 1);
    }
  }

  bool Split() {
    absl::c_sort(_itrs_sorted, [](auto* lhs, auto* rhs) {
      SDB_ASSERT(lhs->cost);
      SDB_ASSERT(rhs->cost);
      return static_cast<double>(lhs->max_score) / lhs->cost <
             static_cast<double>(rhs->max_score) / rhs->cost;
    });

    const double threshold = std::get<ScoreThresholdAttr>(_attrs).value;
    double max_score_sum = 0;
    _first_essential = _itrs_sorted.begin();
    _next_threshold = std::numeric_limits<score_t>::max();

    for (auto it = _itrs_sorted.begin(); it != _itrs_sorted.end(); ++it) {
      const double new_sum = max_score_sum + (*it)->max_score;
      if (new_sum < threshold) {
        max_score_sum = new_sum;
        if (it != _first_essential) {
          std::swap(*it, *_first_essential);
        }
        (*_first_essential)->prefix_score_sum = max_score_sum;
        ++_first_essential;
      } else {
        // Essential term
        _next_threshold =
          std::min(_next_threshold, static_cast<score_t>(new_sum));
      }
    }

    if (_first_essential == _itrs_sorted.end()) {
      return false;
    }

    _num_essential = std::distance(_first_essential, _itrs_sorted.end());
    _has_non_essential = (_first_essential != _itrs_sorted.begin());

    // Compute _first_required_idx only when there's exactly 1 essential scorer.
    // Walk backward from essential: a non-essential is "required" if
    // total score without it can't beat threshold.
    _first_required = _first_essential;

    if (_num_essential == 1) {
      double max_required_score = (*_first_essential)->max_score;

      while (_first_required != _itrs_sorted.begin()) {
        double max_possible_without = max_required_score;
        if (_first_required - _itrs_sorted.begin() > 1) {
          max_possible_without += (*(_first_required - 2))->prefix_score_sum;
        }
        if (max_possible_without >= threshold) {
          break;
        }
        --_first_required;
        max_required_score += (*_first_required)->max_score;
      }
    }

    return true;
  }

  doc_id_t ComputeOuterWindow(doc_id_t min) {
    doc_id_t max = doc_limits::eof();
    for (auto begin = _first_essential; begin != _itrs_sorted.end(); ++begin) {
      const doc_id_t seek_target = std::max((*begin)->value(), min);
      const doc_id_t block_max = (*begin)->SeekToBlock(seek_target);
      if (!doc_limits::eof(block_max)) {
        max = std::min(block_max + 1, max);
      }
    }

    // Adaptive window sizing: grow minimum window when candidates are
    // sparse, only with >1 essential.
    if (_num_essential > 1) {
      const uint32_t threshold =
        32U * _num_outer_windows * static_cast<uint32_t>(_itrs_sorted.size());
      if (_num_candidates < threshold) {
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

  using Attributes = std::tuple<ScoreThresholdAttr, CostAttr>;

  alignas(4096) uint64_t _mask[kNumBlocks]{};
  alignas(4096) score_t _scores[kWindow]{};
  FixedBuffer<doc_id_t, kWindow> _cand_docs;
  FixedBuffer<score_t, kWindow> _cand_scores;  // TODO(gnusi): single buffer
  std::vector<AdapterWrapper> _itrs;
  std::vector<AdapterWrapper*> _itrs_sorted;
  std::vector<AdapterWrapper*>::iterator _first_essential;
  std::vector<AdapterWrapper*>::iterator _first_required;
  size_t _num_essential = 0;
  bool _has_non_essential = false;
  uint32_t _num_candidates = 0;
  uint32_t _num_outer_windows = 0;
  doc_id_t _min_window_size = 1;
  ColumnArgsFetcher _fetcher;
  score_t _next_threshold = std::numeric_limits<score_t>::min();
  Attributes _attrs;
};

}  // namespace irs
