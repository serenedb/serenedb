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
#include <cmath>
#include <duckdb/common/fault_points.hpp>
#include <functional>
#include <limits>
#include <vector>

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/search/ann/family.hpp"
#include "iresearch/search/detail/exact_scan.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::ann {
namespace {

inline constexpr double kVectorDecodeFloats = 1024;
inline constexpr double kExactSkipCost = 0.5;

double ScanCost(const QueryBuilder* prefilter, double docs,
                double rows_per_decode) noexcept {
  if (prefilter == nullptr || docs == 0) {
    return docs;
  }
  const auto share =
    std::min(1.0, static_cast<double>(prefilter->EstimateMatches()) / docs);
  return docs * (1.0 - std::pow(1.0 - share, rows_per_decode) +
                 kExactSkipCost * share * (1.0 - share));
}

struct ExactRoute {
  const QueryBuilder* prefilter;
  irs::detail::TableFilter* table;
};

ExactRoute RouteOf(const ExactVectorQuery& query, const Context& ctx) {
  const auto* inner = query.Inner();
  if (inner == nullptr || !query.Dual() || ctx.columns == nullptr) {
    return {inner, ctx.table};
  }
  const auto docs = static_cast<double>(query.Segment().docs_count());
  const auto rows_per_decode = std::max(
    1.0, kVectorDecodeFloats / static_cast<double>(query.Column().ArraySize()));
  if (ScanCost(inner, docs, rows_per_decode) <=
      ScanCost(query.IndexOnly(), docs, rows_per_decode)) {
    return {inner, ctx.table};
  }
  SDB_IF_FAILURE("irs::ExactReadsColumns") {
    THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
  }
  return {query.IndexOnly(), ctx.columns};
}

class ExactChain : public Root {
 public:
  ExactChain(const ExactVectorQuery& query, const Context& ctx,
             const irs::detail::ScoreArgs& score, const ExactRoute& route)
    : _scanner{query, route.prefilter},
      _codes{query.Codes()},
      _fetcher{ctx.fetcher},
      _table{route.table},
      _boost{score.boost},
      _k{ctx.k},
      _ranked{_codes.bound && !route.table && score.boost > 0.f && ctx.k != 0 &&
              ctx.k < query.Segment().live_docs_count()} {
    _score = irs::detail::PrepareBlockScorer(
      score, query.Segment(), irs::detail::NoField(), _provider, _block);
  }

  void Run(Collector& collector) final {
    _scanner.Reset(doc_limits::min(), doc_limits::eof());
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
      std::fill_n(_spreads, n, score_t{0});
      collector.Append(_docs, _scores, _spreads, n);
    }
  }

 private:
  static constexpr uint32_t kRun = irs::detail::ExactScanner::kRun;

  score_t Bar(const Collector& collector) const noexcept {
    if (_boost <= 0.f) {
      return std::numeric_limits<score_t>::lowest();
    }
    return collector.Floor() / _boost;
  }

  uint32_t Prune(uint32_t n, const Collector& collector) {
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
  ABSL_CACHELINE_ALIGNED score_t _spreads[kRun];
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
}  // namespace irs::ann
namespace irs {

ann::Root::ptr ExactVectorQuery::PlanAnn(const ann::Context& ctx) const {
  const auto record = Stats(ann::ScoredOf(ctx));
  const detail::ScoreArgs score{.scorer = record.scorer,
                                .stats = record.stats,
                                .fetcher = &ctx.fetcher,
                                .boost = Boost()};
  return memory::make_managed<ann::ExactChain>(*this, ctx, score,
                                               ann::RouteOf(*this, ctx));
}

}  // namespace irs
