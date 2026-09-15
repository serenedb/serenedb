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
#include <limits>

#include "iresearch/search/detail/exact_scan.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {
namespace {

// Every row of the part scored exactly and offered to the collector. Rows
// whose distance cannot enter the collector's top-k are dropped before the
// column fetch and the scorer run.
class ExactChain : public Root {
 public:
  ExactChain(const ExactVectorQuery& query, const Context& ctx,
             const irs::detail::ScoreArgs& score)
    : _scanner{query, ctx.part, ctx.parts},
      _fetcher{ctx.fetcher},
      _table{ctx.table},
      _boost{score.boost},
      _k{ctx.k} {
    SDB_ASSERT(score.scorer != nullptr);
    _provider.attr.value = _block;
    _score = score.scorer->PrepareScorer({
      .segment = query.Segment(),
      .field = irs::detail::NoField(),
      .doc_attrs = _provider,
      .fetcher = ctx.fetcher,
      .stats = score.stats,
      .boost = score.boost,
    });
  }

  void Run(LoserScoreCollector& collector) final {
    for (;;) {
      auto n = _scanner.Next(_docs, _dists);
      if (n == 0) {
        return;
      }
      const auto bar = Bar(collector);
      uint32_t kept = 0;
      for (uint32_t i = 0; i < n; ++i) {
        if (_dists[i] > bar) {
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
      if (_table != nullptr) {
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

  ABSL_CACHELINE_ALIGNED score_t _block[kRun];
  ABSL_CACHELINE_ALIGNED score_t _scores[kRun];
  ABSL_CACHELINE_ALIGNED score_t _dists[kRun];
  ABSL_CACHELINE_ALIGNED doc_id_t _docs[kRun];
  irs::detail::ExactScanner _scanner;
  irs::detail::BoostProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  score_t _boost;
  uint32_t _k;
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
