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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

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

    std::get<CostAttr>(_attrs).reset([&] noexcept {
      return absl::c_accumulate(
        _itrs, CostAttr::Type{0},
        [](CostAttr::Type lhs, const AdapterWrapper& rhs) noexcept {
          return lhs + rhs.cost;
        });
    });
  }

  doc_id_t value() const noexcept final {
    SDB_ASSERT(false);
    return doc_limits::eof();
  }
  doc_id_t advance() final { return value(); }
  doc_id_t seek(doc_id_t target) final { return value(); }
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
          ScoreWindow(min, window_max);
          CollectWindow(collector, min);

          if (std::get<ScoreThresholdAttr>(_attrs).value >= _next_threshold) {
            break;
          }
        }

        window_min = std::min(Top(), window_max);
      }
    });
  }

 private:
  IRS_FORCE_INLINE doc_id_t Top() const noexcept {
    return (*_first_essential)->value();
  }

  IRS_FORCE_INLINE static uint64_t GetScoreMask(
    const score_t* IRS_RESTRICT scores, const __m256 threshold) noexcept {
    uint64_t mask = 0;
    for (int j = 0; j < 8; ++j) {
      const uint32_t bits = _mm256_movemask_ps(
        _mm256_cmp_ps(_mm256_loadu_ps(scores + j * 8), threshold, _CMP_GT_OQ));
      mask |= uint64_t{bits} << (j * 8);
    }
    return mask;
  }

  void CollectWindow(auto& collector, doc_id_t min) {
    static_assert(BitsRequired<uint64_t>() == kWindow / kNumBlocks);

    uint64_t block_mask = 0;
    for (size_t i = 0; i < kNumBlocks; ++i) {
      if (_mask[i]) {
        SetBit(block_mask, i);
      }
    }

    if (!block_mask) {
      return;
    }

    const bool only_essential = (_first_essential == _itrs_sorted.begin());

    const __m256 threshold =
      _mm256_set1_ps(std::get<ScoreThresholdAttr>(_attrs).value);
    for (; block_mask; block_mask = PopBit(block_mask)) {
      const doc_id_t i = std::countr_zero(block_mask);
      float* IRS_RESTRICT const block = _scores + i * kBlockSize;
      const uint64_t score_mask = GetScoreMask(block, threshold);
      const doc_id_t base = min + i * kBlockSize;
      for (auto word = _mask[i] & score_mask; word; word = PopBit(word)) {
        const doc_id_t bit = std::countr_zero(word);
        collector.Add(block[bit], base + bit);
      }
      if (only_essential) {
        std::memset(block, 0, kBlockSize * sizeof(score_t));
      }
    }
    std::memset(_mask, 0, sizeof _mask);
    if (!only_essential) {
      std::memset(_scores, 0, sizeof _scores);
    }
  }

  void ScoreWindow(doc_id_t min, doc_id_t max) {
    max = std::min(min + kWindow, max);

    SDB_ASSERT(absl::c_all_of(_mask, [](auto v) { return v == 0; }));
    SDB_ASSERT(absl::c_all_of(_scores, [](auto v) { return v == 0; }));

    FillBlockScoreContext score_ctx{
      .fetcher = &_fetcher,
      .score_window = _scores,
      .merge_type = ScoreMergeType::Sum,
    };

    ProcessEssential([&](auto* it) {
      score_ctx.score = &it->scorer;
      it->FillBlock(min, max, _mask, score_ctx, {});
    });
    ProcessNonEssential(min, max);
  }

  void ProcessNonEssential(doc_id_t min, doc_id_t max) {
    const score_t threshold = std::get<ScoreThresholdAttr>(_attrs).value;

    FillBlockScoreContext score_ctx{
      .fetcher = &_fetcher,
      .merge_type = ScoreMergeType::Sum,
    };

    for (auto it = _first_essential; it != _itrs_sorted.begin();) {
      --it;
      const score_t budget = static_cast<score_t>((*it)->prefix_score_sum);
      SDB_ASSERT(threshold >= budget);
      const score_t score_threshold = threshold - budget;
      score_ctx.score = &(*it)->scorer;

      for (size_t i = 0; i < kSubWindows; ++i) {
        const doc_id_t min = min + static_cast<doc_id_t>(i * kSubBlock);
        if (min >= max) {
          break;
        }

        const score_t* IRS_RESTRICT sub_scores = _scores + i * kSubBlock;
        const bool skip =
          std::all_of(sub_scores, sub_scores + kSubBlock,
                      [&](auto v) { return v <= score_threshold; });
        if (skip) {
          continue;
        }

        // TODO(gnusi): need?
        if ((*it)->value() < min) {
          (*it)->seek(min);
        }

        score_ctx.score_window = _scores + i * kSubBlock;
        (*it)->FillBlock(min,
                         std::min(min + static_cast<doc_id_t>(kSubBlock), max),
                         nullptr, score_ctx, {});
      }
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
    auto end = _itrs_sorted.end();

    absl::c_sort(_itrs_sorted, [](auto* lhs, auto* rhs) {
      auto get_value = [](auto& it) {
        SDB_ASSERT(it->cost);
        return static_cast<double>(it->max_score) / it->cost;
      };
      return get_value(lhs) < get_value(rhs);
    });

    double max_score = 0;
    auto begin = _itrs_sorted.begin();
    for (; begin != end; ++begin) {
      const double new_max_score = max_score + (*begin)->max_score;
      if (new_max_score >= std::get<ScoreThresholdAttr>(_attrs).value) {
        break;
      }

      max_score = new_max_score;
      (*begin)->prefix_score_sum = max_score;
    }

    _first_essential = begin;

    if (_first_essential == _itrs_sorted.end()) {
      return false;
    }

    _next_threshold = static_cast<score_t>(
      max_score +
      std::accumulate(
        begin, end, std::numeric_limits<score_t>::max(),
        [](score_t acc, auto* it) { return std::min(acc, it->max_score); }));

    return true;
  }

  doc_id_t ComputeOuterWindow(doc_id_t min) {
    doc_id_t max = doc_limits::eof();
    for (auto begin = _first_essential; begin != _itrs_sorted.end(); ++begin) {
      const doc_id_t block_max =
        (*begin)->SeekToBlock(std::max((*begin)->value(), min));
      if (!doc_limits::eof(block_max)) {
        max = std::min(block_max + 1, max);
      }
    }
    if (!doc_limits::eof(max)) {
      max = std::max(max, min + kWindow);
    }
    return max;
  }

  using Attributes = std::tuple<ScoreThresholdAttr, CostAttr>;

  alignas(4096) uint64_t _mask[kNumBlocks]{};
  alignas(4096) score_t _scores[kWindow]{};
  std::vector<AdapterWrapper> _itrs;
  std::vector<AdapterWrapper*> _itrs_sorted;
  std::vector<AdapterWrapper*>::iterator _first_essential;
  ColumnArgsFetcher _fetcher;
  score_t _next_threshold = std::numeric_limits<score_t>::min();
  Attributes _attrs;
};

}  // namespace irs
