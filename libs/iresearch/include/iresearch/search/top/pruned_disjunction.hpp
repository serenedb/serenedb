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
#include <tuple>
#include <type_traits>
#include <utility>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "basics/bit_utils.hpp"
#include "basics/empty.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/search/common/boolean_groups.hpp"
#include "iresearch/search/common/exclude_block.hpp"
#include "iresearch/search/common/fixed_array.hpp"
#include "iresearch/search/common/score_filter.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/top/admit.hpp"
#include "iresearch/search/top/root.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::top {

struct MinMatch {
  uint32_t value;
};

template<typename Leaf, typename Match, typename Excludes, typename Table>
class PrunedDisjunction : public Root {
 public:
  static constexpr doc_id_t kWordBits = search::kWindowBits;
  static constexpr size_t kNumWords = search::kWindowWords;
  static constexpr doc_id_t kWindow = search::kWindowDocs;
  static constexpr doc_id_t kExhaustiveWindowsMin = 2;
  static constexpr doc_id_t kExhaustiveWindowsMax = 16;
  static constexpr bool kMinMatch = !std::is_same_v<Match, utils::Empty>;
  static constexpr bool kExcludes = !std::is_same_v<Excludes, utils::Empty>;

  template<typename Init, typename ExcludesArgs>
  PrunedDisjunction(Table table, size_t size, doc_id_t docs, Match match,
                    Init&& init, ExcludesArgs&& excludes)
    : _entries{size,
               [&](Entry& entry, size_t i) {
                 entry.cost = std::max<uint32_t>(1, init(entry.leaf, i));
               }},
      _sorted{size, [this](Entry*& entry,
                           size_t i) noexcept { entry = &_entries[i]; }},
      _excludes{
        std::make_from_tuple<Excludes>(std::forward<ExcludesArgs>(excludes))},
      _admit{table},
      _docs_count{static_cast<double>(std::max<doc_id_t>(1, docs))},
      _docs_end{std::max<doc_id_t>(1, docs)},
      _match{match} {
    if constexpr (kMinMatch) {
      SDB_ASSERT(_match.value > 1);
      SDB_ASSERT(_match.value < size);
    }
  }

  PrunedDisjunction(PrunedDisjunction&&) = delete;
  PrunedDisjunction& operator=(PrunedDisjunction&&) = delete;

  void Run(LoserScoreCollector& collector) final {
    _collector = &collector;
    const doc_id_t max = doc_limits::eof();
    doc_id_t window_min = doc_limits::min();

    _num_candidates = 0;
    _num_outer_windows = 0;
    _min_window_size = 1;
    _promote_ticks = 0;
    _exhaustive_windows = kExhaustiveWindowsMin;

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
      if (_first_essential != 0 && ++_promote_ticks % kSampleEvery != 0 &&
          Promote(window_max - window_min)) {
        Finish(collector.ScoreThreshold());
        if (_first_essential == 0 && !doc_limits::eof(window_max)) {
          window_max =
            std::max(window_max, window_min + kWindow * _exhaustive_windows);
        }
      }
      if (_first_essential == 0) {
        if (window_max <= _docs_end - kWindow && Saturating()) {
          window_max = std::max(window_max, window_min + kWindow);
        }
        _exhaustive_windows =
          std::min(2 * _exhaustive_windows, kExhaustiveWindowsMax);
      } else {
        _exhaustive_windows = kExhaustiveWindowsMin;
      }

      ProcessEssential([&](Entry* entry) {
        if (entry->leaf.Value() < window_min) {
          entry->leaf.Seek(window_min);
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
    Leaf leaf;
    uint32_t cost = 1;
    score_t max_score = 0;
    double prefix_score_sum = 0;
    double ratio = 0;
    uint64_t candidates = 0;
    uint64_t probes = 0;
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
    return _sorted[_first_essential]->leaf.Value();
  }

  IRS_FORCE_INLINE doc_id_t SecondEssentialDoc() const noexcept {
    SDB_ASSERT(_num_essential >= 2);
    const auto* const first = _sorted.data() + _first_essential;
    if (_num_essential == 2) {
      return first[1]->leaf.Value();
    }
    return std::min(first[1]->leaf.Value(), first[2]->leaf.Value());
  }

  static bool Ahead(const Entry* lhs, const Entry* rhs) noexcept {
    return lhs->leaf.Value() > rhs->leaf.Value();
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
    for (auto& entry : _entries) {
      if (entry.leaf.Value() >= max) {
        entry.max_score = 0;
        entry.ratio = 0;
        continue;
      }
      if (entry.leaf.Value() < min) {
        entry.leaf.SeekToBlock(min);
      }
      entry.max_score = entry.leaf.MaxScore(max - 1);
      entry.ratio = static_cast<double>(entry.max_score) / entry.cost;
    }
  }

  bool Split(score_t score_threshold) {
    absl::c_sort(_sorted, [](const Entry* lhs, const Entry* rhs) noexcept {
      return lhs->ratio < rhs->ratio;
    });

    const auto threshold = static_cast<double>(score_threshold);
    const auto size = _sorted.size();
    double max_score_sum = 0;
    _first_essential = 0;
    _next_threshold = std::numeric_limits<score_t>::max();

    for (size_t i = 0; i != size; ++i) {
      const double sum = max_score_sum + _sorted[i]->max_score;
      if (sum < threshold) {
        max_score_sum = sum;
        if (i != _first_essential) {
          std::swap(_sorted[i], _sorted[_first_essential]);
        }
        _sorted[_first_essential]->prefix_score_sum = max_score_sum;
        ++_first_essential;
      } else {
        _next_threshold = std::min(_next_threshold, static_cast<score_t>(sum));
      }
    }

    if (_first_essential == size) {
      return false;
    }
    Finish(threshold);
    return true;
  }

  void Finish(double threshold) noexcept {
    _num_essential = _sorted.size() - _first_essential;
    _has_non_essential = _first_essential != 0;

    _first_required = _first_essential;
    if (_num_essential == 1) {
      double required = _sorted[_first_essential]->max_score;
      while (_first_required != 0) {
        double without = required;
        if (_first_required > 1) {
          without += _sorted[_first_required - 2]->prefix_score_sum;
        }
        if (without >= threshold) {
          break;
        }
        --_first_required;
        required += _sorted[_first_required]->max_score;
      }
    }
  }

  static constexpr double kDrainCost = 1.5;
  static constexpr double kAdmitCost = 0.3;
  static constexpr double kProbeCost = 6;
  static constexpr double kDecodeCost = 0.2;
  static constexpr double kCallCost = 15;
  static constexpr double kDenseSecond = 2;
  static constexpr double kPromoteGain = 0.75;
  static constexpr uint32_t kSampleEvery = 32;
  static constexpr uint64_t kMinCandidates = doc_limits::kBlockSize;
  static constexpr uint64_t kSurvivalWindow = 16 * doc_limits::kBlockSize;

  static double Survival(const Entry& entry) noexcept {
    if (entry.candidates < kMinCandidates) {
      return 0.0;
    }
    return static_cast<double>(entry.probes) /
           static_cast<double>(entry.candidates);
  }

  static void Observe(Entry& entry, uint32_t candidates,
                      size_t probed) noexcept {
    entry.candidates += candidates;
    entry.probes += probed;
    if (entry.candidates > kSurvivalWindow) {
      entry.candidates /= 2;
      entry.probes /= 2;
    }
  }

  bool WindowPath(size_t first) const noexcept {
    if (_sorted.size() - first < 2) {
      return false;
    }
    uint32_t largest = 0;
    uint32_t second = 0;
    for (size_t i = first; i != _sorted.size(); ++i) {
      const auto cost = _sorted[i]->cost;
      if (cost > largest) {
        second = largest;
        largest = cost;
      } else if (cost > second) {
        second = cost;
      }
    }
    return second * (kWindow / 2.0) >= kDenseSecond * _docs_count;
  }

  double Cost(double fill, size_t first, double scale,
              double windows) const noexcept {
    if (first == 0) {
      return fill * (1 + kAdmitCost);
    }
    double cost = fill * (1 + kDrainCost);
    for (size_t i = 0; i != first; ++i) {
      const auto& entry = *_sorted[i];
      const double probes = fill * Survival(entry);
      const double docs = entry.cost * scale;
      cost += probes * kProbeCost + windows * kCallCost +
              std::min(docs, probes * doc_limits::kBlockSize) * kDecodeCost;
    }
    return cost;
  }

  bool Promote(doc_id_t span) noexcept {
    const size_t scored = _first_essential;
    if (!WindowPath(scored)) {
      return false;
    }
    const double scale = static_cast<double>(span) / _docs_count;
    const double windows = static_cast<double>(span) / kWindow;
    double fill = 0;
    for (size_t i = scored; i != _sorted.size(); ++i) {
      fill += _sorted[i]->cost * scale;
    }
    double best = Cost(fill, scored, scale, windows) * kPromoteGain;
    for (size_t first = scored; first != 0;) {
      --first;
      fill += _sorted[first]->cost * scale;
      const double cost = Cost(fill, first, scale, windows);
      if (cost < best) {
        best = cost;
        _first_essential = first;
      }
    }
    return _first_essential != scored;
  }

  bool Saturating() const noexcept {
    uint64_t postings = 0;
    for (size_t i = _first_essential; i != _sorted.size(); ++i) {
      postings += _sorted[i]->cost;
    }
    return postings >= _docs_end;
  }

  doc_id_t ComputeOuterWindow(doc_id_t min) {
    doc_id_t max = doc_limits::eof();
    for (size_t i = _first_essential; i != _sorted.size(); ++i) {
      auto& leaf = _sorted[i]->leaf;
      const doc_id_t block_max = leaf.SeekToBlock(std::max(leaf.Value(), min));
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
    SDB_ASSERT(_has_non_essential || !kMinMatch);
    auto& leaf = _sorted[_first_essential]->leaf;
    leaf.ForEachScoredBlock(
      max, [&](doc_id_t* IRS_RESTRICT docs, uint32_t len,
               score_t* IRS_RESTRICT scores) IRS_FORCE_INLINE {
        if constexpr (kExcludes) {
          len = search::ExcludeBlock(_excludes, docs, scores, len);
        }
        if (_has_non_essential) {
          View<doc_id_t> cand_docs{docs, len};
          View<score_t> cand_scores{scores, len};
          if constexpr (kMinMatch) {
            std::fill_n(_tally.cand_matches, len, uint32_t{1});
            View<uint32_t> cand_matches{_tally.cand_matches, len};
            ProcessNonEssential<true>(cand_docs, cand_scores, cand_matches,
                                      max);
          } else {
            utils::Empty none;
            ProcessNonEssential<false>(cand_docs, cand_scores, none, max);
          }
          len = static_cast<uint32_t>(cand_docs.count);
        } else {
          _num_candidates += len;
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
      const auto stop = std::min(max, second);
      if (kMinMatch && !_has_non_essential) {
        _sorted[_first_essential]->leaf.Seek(stop);
      } else {
        ProcessSingleEssential(collector, stop);
      }
      UpdateHeapTop();
      return;
    }

    max = std::min(min + kWindow, max);
    if constexpr (kMinMatch) {
      ProcessEssential([&](Entry* entry) IRS_FORCE_INLINE {
        entry->leaf.FillCounted(min, max, _mask, _scores, _tally.counts);
      });
    } else {
      ProcessEssential([&](Entry* entry) IRS_FORCE_INLINE {
        entry->leaf.Fill(min, max, _mask, _scores);
      });
    }
    const uint64_t* touched = _mask;
    if constexpr (kExcludes) {
      if constexpr (kMinMatch) {
        std::copy_n(_mask, kNumWords, _touched.words);
        touched = _touched.words;
      }
      _excludes.Remove(min, max, _mask, _scores, score_t{0});
    }

    if (!_has_non_essential) {
      if constexpr (kMinMatch) {
        search::TallyMask(touched, _mask, _tally.counts, _scores, _match.value,
                          kNumWords);
      }
      const auto before = collector.TotalMatches();
      _admit.Window(collector, _scores, _mask, min, kNumWords);
      _num_candidates +=
        static_cast<uint32_t>(collector.TotalMatches() - before);
      return;
    }
    if constexpr (kMinMatch) {
      const auto count = DrainCounted(min, touched);
      View<doc_id_t> cand_docs{_cand_docs, count};
      View<score_t> cand_scores{_cand_scores, count};
      View<uint32_t> cand_matches{_tally.cand_matches, count};
      ProcessNonEssential<true>(cand_docs, cand_scores, cand_matches, max);
      if (cand_docs.count != 0) {
        _admit.AddDocs(collector, _cand_docs, cand_docs.count, _cand_scores);
      }
    } else {
      const auto count = DrainCandidates(min);
      View<doc_id_t> cand_docs{_cand_docs, count};
      View<score_t> cand_scores{_cand_scores, count};
      utils::Empty none;
      ProcessNonEssential<false>(cand_docs, cand_scores, none, max);
      if (cand_docs.count != 0) {
        _admit.AddDocs(collector, _cand_docs, cand_docs.count, _cand_scores);
      }
    }
  }

  size_t DrainCounted(doc_id_t min, const uint64_t* touched) {
    size_t count = 0;
    for (size_t i = 0; i != kNumWords; ++i) {
      auto word = touched[i];
      if (word == 0) {
        continue;
      }
      const auto kept = _mask[i];
      _mask[i] = 0;
      const size_t base = i * kWordBits;
      do {
        const auto bit = static_cast<uint32_t>(std::countr_zero(word));
        const size_t offset = base + bit;
        const auto hits = std::exchange(_tally.counts[offset], uint32_t{0});
        const auto score = std::exchange(_scores[offset], 0.f);
        if (((kept >> bit) & 1) != 0) {
          _cand_docs[count] = min + static_cast<doc_id_t>(offset);
          _cand_scores[count] = score;
          _tally.cand_matches[count] = hits;
          ++count;
        }
        word = PopBit(word);
      } while (word != 0);
    }
    return count;
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

  template<bool Counted, typename Docs, typename Scores, typename Matches>
  static void FilterCompetitive(Docs& docs, Scores& scores, Matches& matches,
                                score_t score_threshold) {
    SDB_ASSERT(score_threshold > 0);
    uint32_t out;
    if constexpr (Counted) {
      out = search::FilterScores(docs.data, scores.data, matches.data,
                                 static_cast<uint32_t>(docs.size()),
                                 score_threshold);
      matches.resize(out);
    } else {
      out = search::FilterScores(docs.data, scores.data,
                                 static_cast<uint32_t>(docs.size()),
                                 score_threshold);
    }
    docs.resize(out);
    scores.resize(out);
  }

  template<typename Docs, typename Scores, typename Matches>
  static void FilterMatches(Docs& docs, Scores& scores, Matches& matches,
                            uint32_t need) {
    const auto out =
      search::FilterMatches(docs.data, scores.data, matches.data,
                            static_cast<uint32_t>(docs.size()), need);
    docs.resize(out);
    scores.resize(out);
    matches.resize(out);
  }

  template<bool Counted, typename Docs, typename Scores, typename Matches>
  void ProcessNonEssential(Docs& cand_docs, Scores& cand_scores,
                           Matches& cand_matches, doc_id_t max) {
    const auto candidates = static_cast<uint32_t>(cand_docs.size());
    if (candidates == 0) {
      return;
    }
    _num_candidates += candidates;
    const score_t threshold = _collector->ScoreThreshold();
    const auto give_up = [&](size_t i) {
      Observe(*_sorted[i], candidates, 0);
      while (i-- != 0) {
        Observe(*_sorted[i], candidates, 0);
      }
    };

    for (size_t i = _first_essential; i-- != 0;) {
      auto& entry = *_sorted[i];
      const auto score_threshold =
        threshold - static_cast<score_t>(entry.prefix_score_sum);
      if (score_threshold > 0) {
        FilterCompetitive<Counted>(cand_docs, cand_scores, cand_matches,
                                   score_threshold);
        if (cand_docs.empty()) {
          give_up(i);
          return;
        }
      }
      if constexpr (Counted) {
        if (const auto reach = static_cast<uint32_t>(i + 1);
            _match.value > reach) {
          FilterMatches(cand_docs, cand_scores, cand_matches,
                        _match.value - reach);
          if (cand_docs.empty()) {
            give_up(i);
            return;
          }
        }
      }
      Observe(entry, candidates, cand_docs.size());
      if constexpr (Counted) {
        entry.leaf.ScoreCandidates(cand_docs, cand_scores, cand_matches,
                                   i >= _first_required, max);
      } else {
        entry.leaf.ScoreCandidates(cand_docs, cand_scores, i >= _first_required,
                                   max);
      }
    }
    if constexpr (Counted) {
      if (!cand_docs.empty()) {
        FilterMatches(cand_docs, cand_scores, cand_matches, _match.value);
      }
    }
  }

  struct Tally {
    ABSL_CACHELINE_ALIGNED uint32_t counts[kWindow]{};
    ABSL_CACHELINE_ALIGNED uint32_t cand_matches[kWindow];
  };

  struct Touched {
    uint64_t words[kNumWords];
  };

  ABSL_CACHELINE_ALIGNED uint64_t _mask[kNumWords]{};
  ABSL_CACHELINE_ALIGNED score_t _scores[kWindow]{};
  ABSL_CACHELINE_ALIGNED doc_id_t _cand_docs[kWindow];
  ABSL_CACHELINE_ALIGNED score_t _cand_scores[kWindow];
  [[no_unique_address]] utils::Need<kMinMatch, Tally> _tally{};
  [[no_unique_address]] utils::Need<kMinMatch && kExcludes, Touched> _touched;

  LoserScoreCollector* _collector = nullptr;
  search::FixedArray<Entry> _entries;
  search::FixedArray<Entry*> _sorted;
  [[no_unique_address]] Excludes _excludes;
  size_t _first_essential = 0;
  size_t _first_required = 0;
  size_t _num_essential = 0;
  bool _has_non_essential = false;
  uint32_t _num_candidates = 0;
  uint32_t _num_outer_windows = 0;
  doc_id_t _min_window_size = 1;
  score_t _next_threshold = std::numeric_limits<score_t>::lowest();
  [[no_unique_address]] Admit<Table> _admit;
  const double _docs_count;
  const doc_id_t _docs_end;
  uint32_t _promote_ticks = 0;
  doc_id_t _exhaustive_windows = kExhaustiveWindowsMin;
  [[no_unique_address]] const Match _match;
};

}  // namespace irs::top
