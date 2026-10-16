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

#include <algorithm>
#include <functional>
#include <limits>
#include <vector>

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/search/detail/exact_scan.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {
namespace {

class ExactChain : public Root {
 public:
  ExactChain(const ExactVectorQuery& query, const Context& ctx,
             const irs::detail::ScoreArgs& score)
    : _scanner{query},
      _codes{query.Codes()},
      _fetcher{ctx.fetcher},
      _table{ctx.table},
      _boost{score.boost},
      _k{ctx.k},
      _ranked{_codes.bound && !ctx.table && score.boost > 0.f && ctx.k != 0 &&
              ctx.k < query.Segment().live_docs_count()} {
    _score = irs::detail::PrepareBlockScorer(
      score, query.Segment(), irs::detail::NoField(), _provider, _block);
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    _scanner.Reset(min, max);
    for (;;) {
      auto n = _scanner.NextDocs(_docs);
      if (n == 0) {
        return;
      }
      if (_codes.bound) {
        n = Prune(n, collector);
        if (n == 0) {
          continue;
        }
      }
      _scanner.Distances({_docs, n}, _dists);
      const auto bar = Bar(collector);
      uint32_t kept = 0;
      for (uint32_t i = 0; i < n; ++i) {
        if (_dists[i] > bar && _scanner.Inside(_dists[i])) {
          _docs[kept] = _docs[i];
          _block[kept] = _dists[i];
          ++kept;
        }
      }
      if (kept == 0) {
        continue;
      }
      n = kept;
      _fetcher.Fetch(std::span<const doc_id_t>{_docs, n});
      _score.Score(_scores, static_cast<scores_size_t>(n));
      if (_table) {
        n = _table->Narrow(_docs, _scores, n);
      }
      collector.AddDocs(_docs, n, _scores);
    }
  }

 private:
  static constexpr uint32_t kRun = irs::detail::ExactScanner::kRun;

  score_t Bar(const LoserScoreCollector& collector) const noexcept {
    if (collector.AcceptedCount() < _k || _boost <= 0.f) {
      return std::numeric_limits<score_t>::lowest();
    }
    return collector.ScoreThreshold() / _boost;
  }

  uint32_t Prune(uint32_t n, const LoserScoreCollector& collector) {
    uint32_t coded = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const auto node = static_cast<uint32_t>(_docs[i] - doc_limits::min());
      if (node < _codes.levels.size() && _codes.levels[node] != 0) {
        _nodes[coded] = node;
        _slots[coded] = i;
        ++coded;
      }
    }
    if (coded == 0) {
      return n;
    }
    _codes.bound->Score(_codes.codes, _codes.record_size, {_nodes, coded},
                        _approx);
    _codes.bound->Bounds({_approx, coded}, _lower, _upper);
    if (_ranked) {
      for (uint32_t j = 0; j < coded; ++j) {
        if (_scanner.Inside(_lower[j])) {
          Offer(_lower[j]);
        }
      }
    }
    const auto bar = Bar(collector);
    const auto floor = _floor.size() == _k
                         ? _floor.front()
                         : std::numeric_limits<score_t>::lowest();
    uint32_t kept = 0;
    for (uint32_t i = 0, j = 0; i < n; ++i) {
      if (j < coded && _slots[j] == i) {
        const auto upper = _upper[j++];
        if (upper <= bar || upper < floor || !_scanner.Inside(upper)) {
          continue;
        }
      }
      _docs[kept++] = _docs[i];
    }
    return kept;
  }

  void Offer(score_t lower) {
    if (_floor.size() < _k) {
      _floor.push_back(lower);
      std::push_heap(_floor.begin(), _floor.end(), std::greater<>{});
    } else if (lower > _floor.front()) {
      std::pop_heap(_floor.begin(), _floor.end(), std::greater<>{});
      _floor.back() = lower;
      std::push_heap(_floor.begin(), _floor.end(), std::greater<>{});
    }
  }

  ABSL_CACHELINE_ALIGNED score_t _block[kRun];
  ABSL_CACHELINE_ALIGNED score_t _scores[kRun];
  ABSL_CACHELINE_ALIGNED score_t _dists[kRun];
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kRun];
  ABSL_CACHELINE_ALIGNED score_t _approx[kRun];
  ABSL_CACHELINE_ALIGNED score_t _lower[kRun];
  ABSL_CACHELINE_ALIGNED score_t _upper[kRun];
  uint32_t _nodes[kRun];
  uint32_t _slots[kRun];
  irs::detail::ExactScanner _scanner;
  AnnExactCodes _codes;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  std::vector<score_t> _floor;
  score_t _boost;
  uint32_t _k;
  bool _ranked;
};

}  // namespace

Root::ptr Make(const ExactVectorQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs score{.scorer = record.scorer,
                                     .stats = record.stats,
                                     .fetcher = &ctx.fetcher,
                                     .boost = query.Boost()};
  return memory::make_managed<ExactChain>(query, ctx, score);
}

}  // namespace irs::top
