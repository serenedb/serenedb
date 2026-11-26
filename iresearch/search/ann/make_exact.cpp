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
#include "iresearch/search/ann/route.hpp"
#include "iresearch/search/detail/exact_scan.hpp"
#include "iresearch/search/scorers/all_docs_score.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs::ann {
namespace {

constexpr double kVectorDecodeFloats = 1024;
constexpr double kExactSkipCost = 0.5;

double ScanCost(const QueryBuilder* prefilter, double docs,
                double rows_per_decode) noexcept {
  const auto share = ShareOf(prefilter, docs);
  return docs * (1.0 - std::pow(1.0 - share, rows_per_decode) +
                 kExactSkipCost * share * (1.0 - share));
}

Route ExactRouteOf(const ExactVectorQuery& query, const Context& ctx) {
  return RouteOf(
    query, ctx, [&](const QueryBuilder& inner, const QueryBuilder* index_part) {
      const auto docs = static_cast<double>(query.Segment().docs_count());
      const auto rows_per_decode =
        std::max(1.0, kVectorDecodeFloats /
                        static_cast<double>(query.Column().ArraySize()));
      if (ScanCost(&inner, docs, rows_per_decode) <=
          ScanCost(index_part, docs, rows_per_decode)) {
        return false;
      }
      SDB_IF_FAILURE("irs::ExactReadsColumns") {
        THROW_SQL_ERROR(ERR_MSG("intentional debug error"));
      }
      return true;
    });
}

class ExactChain : public Root {
 public:
  ExactChain(const ExactVectorQuery& query, const irs::detail::ScoreArgs& score,
             const Route& route)
    : _scanner{route.ctx.memory, query, route.prefilter, false},
      _codes{query.Codes()},
      _fetcher{route.ctx.fetcher},
      _table{route.ctx.table},
      _floor{route.ctx.memory.Scratch()},
      _boost{score.boost},
      _k{route.ctx.k},
      _ranked{_codes.bound && !_table && score.boost > 0.f && _k != 0 &&
              _k < query.Segment().live_docs_count()} {
    _score = irs::detail::PrepareBlockScorer(
      score, query.Segment(), irs::detail::NoField(), _provider, _block);
    if (_ranked) {
      _floor.reserve(_k);
    }
  }

  void Run(Collector& collector) final {
    Batch batch;
    _scanner.Reset(doc_limits::min(), doc_limits::eof());
    for (;;) {
      auto n = _scanner.NextDocs(batch.docs);
      if (n == 0) {
        return;
      }
      if (_codes.bound) {
        n = Prune(batch, n, collector);
        if (n == 0) {
          continue;
        }
      }
      _scanner.Distances({batch.docs, n}, batch.dists);
      const auto bar = Bar(collector);
      uint32_t kept = 0;
      for (uint32_t i = 0; i < n; ++i) {
        if (batch.dists[i] > bar && _scanner.Inside(batch.dists[i])) {
          batch.docs[kept] = batch.docs[i];
          _block[kept] = batch.dists[i];
          ++kept;
        }
      }
      if (kept == 0) {
        continue;
      }
      n = kept;
      FetchAndScore(_fetcher, _score, batch.docs, batch.scores, n);
      if (_table) {
        n = _table->Narrow(batch.docs, batch.scores, n);
      }
      std::fill_n(batch.spreads, n, score_t{0});
      collector.Append(batch.docs, batch.scores, batch.spreads, n);
    }
  }

 private:
  static constexpr uint32_t kRun = irs::detail::ExactScanner::kRun;

  struct Batch {
    ABSL_CACHELINE_ALIGNED score_t scores[kRun];
    ABSL_CACHELINE_ALIGNED score_t spreads[kRun];
    ABSL_CACHELINE_ALIGNED score_t dists[kRun];
    ABSL_CACHELINE_ALIGNED doc_id_t docs[kRun];
    ABSL_CACHELINE_ALIGNED score_t approx[kRun];
    ABSL_CACHELINE_ALIGNED score_t lower[kRun];
    ABSL_CACHELINE_ALIGNED score_t upper[kRun];
    uint32_t nodes[kRun];
    uint32_t slots[kRun];
  };

  score_t Bar(const Collector& collector) const noexcept {
    if (_boost <= 0.f) {
      return std::numeric_limits<score_t>::lowest();
    }
    return collector.Floor() / _boost;
  }

  uint32_t Prune(Batch& batch, uint32_t n, const Collector& collector) {
    uint32_t coded = 0;
    for (uint32_t i = 0; i < n; ++i) {
      const auto node =
        static_cast<uint32_t>(batch.docs[i] - doc_limits::min());
      if (node < _codes.levels.size() && _codes.levels[node] != 0) {
        batch.nodes[coded] = node;
        batch.slots[coded] = i;
        ++coded;
      }
    }
    if (coded == 0) {
      return n;
    }
    _codes.bound->Score(_codes.codes, _codes.record_size, {batch.nodes, coded},
                        batch.approx);
    _codes.bound->Bounds({batch.approx, coded}, batch.lower, batch.upper);
    if (_ranked) {
      for (uint32_t j = 0; j < coded; ++j) {
        if (_scanner.Inside(batch.lower[j])) {
          Offer(batch.lower[j]);
        }
      }
    }
    const auto bar = Bar(collector);
    const auto floor = _floor.size() == _k
                         ? _floor.front()
                         : std::numeric_limits<score_t>::lowest();
    uint32_t kept = 0;
    for (uint32_t i = 0, j = 0; i < n; ++i) {
      if (j < coded && batch.slots[j] == i) {
        const auto upper = batch.upper[j++];
        if (upper <= bar || upper < floor || !_scanner.Inside(upper)) {
          continue;
        }
      }
      batch.docs[kept++] = batch.docs[i];
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
  irs::detail::ExactScanner _scanner;
  AnnExactCodes _codes;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  memory::ArenaVector<score_t> _floor;
  score_t _boost;
  uint32_t _k;
  bool _ranked;
};

}  // namespace
}  // namespace irs::ann
namespace irs {

ann::Root::ptr ExactVectorQuery::PlanAnn(const ann::Context& ctx) const {
  const auto record = Stats(ann::ScoredOf(ctx));
  const detail::ScoreArgs score{.memory = &ctx.memory,
                                .scorer = record.scorer,
                                .stats = record.stats,
                                .fetcher = &ctx.fetcher,
                                .boost = Boost()};
  return ctx.memory.Make<ann::ExactChain>(*this, score,
                                          ann::ExactRouteOf(*this, ctx));
}

}  // namespace irs
