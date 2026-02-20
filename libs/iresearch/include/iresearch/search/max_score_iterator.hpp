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

#include "basics/shared.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<typename Adapter>
class MaxScoreIterator : public DocIterator {
 public:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();
  static constexpr auto kNumBlocks = 32;
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

    _doc = doc_limits::eof();
  }

 private:
  IRS_FORCE_INLINE doc_id_t Top() const noexcept {
    return (*_first_essential)->value();
  }

  IRS_FORCE_INLINE void CollectWindow(auto& collector, doc_id_t min) {
    const bool only_essential = (_first_essential == _itrs_sorted.begin());
    collector.AddWindow(_scores, _mask, min, kNumBlocks, only_essential);
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
        const doc_id_t sub_min = min + static_cast<doc_id_t>(i * kSubBlock);
        if (sub_min >= max) {
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
        if ((*it)->value() < sub_min) {
          (*it)->seek(sub_min);
        }

        score_ctx.score_window = _scores + i * kSubBlock;

        (*it)->FillBlock(
          sub_min, std::min(sub_min + static_cast<doc_id_t>(kSubBlock), max),
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
    absl::c_sort(_itrs_sorted, [](auto* lhs, auto* rhs) {
      return static_cast<double>(lhs->max_score) /
               std::max<CostAttr::Type>(1, lhs->cost) <
             static_cast<double>(rhs->max_score) /
               std::max<CostAttr::Type>(1, rhs->cost);
    });

    const score_t threshold = std::get<ScoreThresholdAttr>(_attrs).value;

    double max_score_sum = 0;
    _first_essential = _itrs_sorted.begin();
    _next_threshold = std::numeric_limits<score_t>::max();

    for (size_t i = 0; i < _itrs_sorted.size(); ++i) {
      const double new_sum = max_score_sum + _itrs_sorted[i]->max_score;
      if (static_cast<score_t>(new_sum) < threshold) {
        // Non-essential.
        max_score_sum = new_sum;
        if (i != static_cast<size_t>(
                   std::distance(_itrs_sorted.begin(), _first_essential))) {
          std::swap(_itrs_sorted[i], *_first_essential);
        }
        ++_first_essential;
      } else {
        // Essential.
        _next_threshold =
          std::min(_next_threshold, static_cast<score_t>(new_sum));
      }
    }

    if (_first_essential == _itrs_sorted.end()) {
      return false;
    }

    std::sort(_itrs_sorted.begin(), _first_essential, [](auto* lhs, auto* rhs) {
      return lhs->max_score < rhs->max_score;
    });
    double prefix = 0;
    for (auto it = _itrs_sorted.begin(); it != _first_essential; ++it) {
      prefix += (*it)->max_score;
      (*it)->prefix_score_sum = prefix;
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
