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

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "basics/bit_utils.hpp"
#include "basics/shared.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/cost.hpp"
#include "iresearch/utils/attribute_helper.hpp"
#include "iresearch/utils/fixed_buffer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

template<typename Adapter>
class MaxScoreIterator : public DocIterator {
 public:
  static constexpr doc_id_t kBlockSize = BitsRequired<uint64_t>();
  static constexpr auto kNumBlocks = 64;
  static constexpr doc_id_t kWindow = kBlockSize * kNumBlocks;

  struct AdapterWrapper : Adapter {
    ScoreFunction scorer;
    CostAttr::Type cost = 0;
    score_t max_score = 0;
    double prefix_score_sum = 0;
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

  // WAND/topk iterator: entered through Collect (top-k) and EmitScoredDocs (a
  // windowed scored drain, e.g. TableFilterDocIterator::Collect over a filtered
  // top-k). count()/EmitDocs()/FillBlock() are unscored paths never reached.
  uint32_t count() final {
    SDB_ASSERT(false);
    return 0;
  }
  uint32_t EmitDocs(doc_id_t* /*out*/, doc_id_t /*min*/,
                    doc_id_t /*max*/) final {
    SDB_ASSERT(false);
    return 0;
  }
  uint32_t EmitScoredDocs(doc_id_t* out, score_t* scores, doc_id_t max,
                          const ScoreFunction& /*scorer*/,
                          ColumnArgsFetcher* /*fetcher*/, doc_id_t min) final {
    // Collect's body bounded to [min, max), emitting (doc, score) ascending
    // into out/scores instead of pushing a ScoreCollector. scorer/fetcher are
    // unused as in Collect: children were prepared against _fetcher and scored
    // inside the reused window machinery. Self-positions via seek (advance()
    // asserts here), so no external prime is needed.
    EmitSink sink{out, scores};

    doc_id_t window_min = min;
    _num_candidates = 0;
    _num_outer_windows = 0;
    _min_window_size = 1;
  outer:
    while (window_min < max) {
      doc_id_t window_max = std::min(ComputeOuterWindow(window_min), max);

      while (true) {
        UpdateWindowScores(window_min, window_max);
        if (!Split()) {
          window_min = window_max;
          goto outer;
        }
        const doc_id_t new_window_max =
          std::min(ComputeOuterWindow(window_min), max);
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

      for (auto sub_min = Top(); sub_min < window_max; sub_min = Top()) {
        ScoreAndCollectWindow(sink, sub_min, window_max);
        if (std::get<ScoreThresholdAttr>(_attrs).value >= _next_threshold) {
          break;
        }
      }

      window_min = std::min(Top(), window_max);
      ++_num_outer_windows;
    }

    if (_first_essential != _itrs_sorted.end()) {
      // Essentials were advanced to >= max; the heap root is the next match.
      _doc = Top();
    } else {
      // Split failed for the whole tail [window_min, max): nothing competitive
      // there. Resume at the first real doc >= max (eof once exhausted) so a
      // caller looping to eof terminates.
      _doc = doc_limits::eof();
      for (auto& it : _itrs) {
        _doc = std::min(_doc, it.seek(max));
      }
    }
    return sink.count;
  }
  std::pair<doc_id_t, bool> FillBlock(doc_id_t /*min*/, doc_id_t /*max*/,
                                      uint64_t* /*mask*/,
                                      FillBlockScoreContext /*score*/,
                                      FillBlockMatchContext /*match*/) final {
    SDB_ASSERT(false);
    return {doc_limits::eof(), true};
  }
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
  // Appends (doc, score) pairs produced by the shared window-scoring path into
  // the caller's parallel arrays -- the AddDocs/AddWindow surface a
  // ScoreCollector exposes, minus the top-k threshold (EmitScoredDocs emits
  // every WAND survivor; the downstream collector applies its own threshold).
  struct EmitSink {
    doc_id_t* IRS_RESTRICT docs;
    score_t* IRS_RESTRICT scores;
    uint32_t count = 0;

    IRS_FORCE_INLINE void AddDocs(
      const doc_id_t* IRS_RESTRICT in_docs, size_t n,
      const score_t* IRS_RESTRICT in_scores) noexcept {
      std::memcpy(docs + count, in_docs, n * sizeof(doc_id_t));
      std::memcpy(scores + count, in_scores, n * sizeof(score_t));
      count += static_cast<uint32_t>(n);
    }

    IRS_FORCE_INLINE void AddWindow(const score_t* IRS_RESTRICT score_window,
                                    const uint64_t* IRS_RESTRICT mask,
                                    doc_id_t min, size_t num_blocks,
                                    bool clear_score) noexcept {
      for (size_t i = 0; i < num_blocks; ++i) {
        auto word = mask[i];
        if (word == 0) {
          continue;
        }
        const score_t* IRS_RESTRICT const base = score_window + i * kBlockSize;
        const doc_id_t doc_base = min + static_cast<doc_id_t>(i) * kBlockSize;
        do {
          const doc_id_t bit = std::countr_zero(word);
          word = PopBit(word);
          docs[count] = doc_base + bit;
          scores[count] = base[bit];
          ++count;
        } while (word != 0);
        if (clear_score) {
          std::memset(const_cast<score_t*>(base), 0,
                      kBlockSize * sizeof(score_t));
        }
      }
    }
  };

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
      auto bits = mask[i];
      if (!bits) {
        continue;
      }
      mask[i] = 0;
      do {
        const size_t bit = std::countr_zero(bits);
        const size_t offset = i * kBlockSize + bit;
        _cand_docs.push_back(min + static_cast<doc_id_t>(offset));
        _cand_scores.push_back(std::exchange(_scores[offset], 0));
        bits = PopBit(bits);
      } while (bits);
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

#ifdef __AVX2__
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
#endif

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

  // TODO(mbkkt) This is strange alignment, probably allocation is better way
  alignas(4096) uint64_t _mask[kNumBlocks]{};
  alignas(4096) score_t _scores[kWindow]{};

  // TODO(gnusi): single buffer?
  utils::FixedBuffer<doc_id_t, kWindow, ABSL_CACHELINE_SIZE> _cand_docs;
  utils::FixedBuffer<score_t, kWindow, ABSL_CACHELINE_SIZE> _cand_scores;

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
