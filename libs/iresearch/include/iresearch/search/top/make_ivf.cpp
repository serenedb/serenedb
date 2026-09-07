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

#include "iresearch/search/common/score_provider.hpp"
#include "iresearch/search/common/vector_of.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {

namespace {

template<typename Cluster>
class VectorChain : public Root {
 public:
  template<typename Args>
  VectorChain(ColumnArgsFetcher& fetcher, const SubReader& segment,
              const TermReader& field, const search::ScoreArgs& score,
              uint32_t k, search::TableFilter* table, size_t count, Args&& args)
    : _clusters{count, std::forward<Args>(args)},
      _fetcher{fetcher},
      _table{table},
      _boost{score.boost},
      _k{k} {
    SDB_ASSERT(score.scorer != nullptr);
    _provider.attr.value = _block;
    _score = score.scorer->PrepareScorer({
      .segment = segment,
      .field = field.meta(),
      .doc_attrs = _provider,
      .fetcher = &fetcher,
      .stats = score.stats,
      .boost = score.boost,
    });
  }

  void Run(LoserScoreCollector& collector) final {
    for (size_t i = 0, n = _clusters.size(); i != n; ++i) {
      auto& cluster = _clusters[i];
      for (;;) {
        cluster.SetThreshold(Bar(collector));
        if (!cluster.NextRun()) {
          break;
        }
        const auto docs = cluster.RunDocs();
        auto n = static_cast<uint32_t>(docs.size());
        std::copy_n(cluster.RunScores().data(), n, _block);
        _fetcher.Fetch(docs);
        _score.Score(_scores, static_cast<scores_size_t>(n));
        if (_table == nullptr) {
          collector.AddDocs(docs.data(), n, _scores);
          continue;
        }
        std::copy_n(docs.data(), n, _own);
        n = _table->Narrow(_own, _scores, n);
        collector.AddDocs(_own, n, _scores);
      }
    }
  }

 private:
  static constexpr uint32_t kRun = Cluster::kRun;

  score_t Bar(const LoserScoreCollector& collector) const noexcept {
    if (collector.AcceptedCount() < _k || _boost <= 0.f) {
      return std::numeric_limits<score_t>::lowest();
    }
    return collector.ScoreThreshold() / _boost;
  }

  ABSL_CACHELINE_ALIGNED score_t _block[kRun];
  ABSL_CACHELINE_ALIGNED score_t _scores[kRun];
  ABSL_CACHELINE_ALIGNED doc_id_t _own[kRun];
  search::VectorClusters<Cluster> _clusters;
  search::BoostProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  search::TableFilter* _table;
  score_t _boost;
  uint32_t _k;
};

}  // namespace

Root::ptr Make(const RangeVectorQuery& query, const Context& ctx) {
  auto inner = search::InnerProbe(query);
  if (query.Inner() != nullptr && !inner) {
    return {};
  }
  const auto record = query.Stats(ScoredOf(ctx));
  const search::ScoreArgs score{.scorer = record.scorer,
                                .stats = record.stats,
                                .fetcher = &ctx.fetcher,
                                .boost = query.Boost()};
  return ResolveBool(query.Inclusive(), [&]<bool Inclusive>() -> Root::ptr {
    return ResolveBool(query.Rescored(), [&]<bool Rescore>() -> Root::ptr {
      if (ctx.table != nullptr) {
        return search::MakeVectorScored<FilteredWalk, Root::ptr, search::RadiusGate<Inclusive>,
                                Rescore, lead::TwoPhaseScored>(
          query, *query.State().reader, score, query.Threshold(),
          std::move(inner), ctx.table, ctx.fetcher);
      }
      return search::MakeVectorScored<PlainWalk, Root::ptr, search::RadiusGate<Inclusive>,
                              Rescore, lead::TwoPhaseScored>(
        query, *query.State().reader, score, query.Threshold(),
        std::move(inner), utils::Empty{}, ctx.fetcher);
    });
  });
}

Root::ptr Make(const KnnVectorQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const search::ScoreArgs score{.scorer = record.scorer,
                                .stats = record.stats,
                                .fetcher = &ctx.fetcher,
                                .boost = query.Boost()};
  const auto& segment = query.Segment();
  const auto& field = *query.State().reader;

  if (query.Inner() == nullptr) {
    return search::ResolveClusters<search::AcceptAll>(
      query,
      [&]<typename Cluster>(size_t count,
                            const search::ClusterFeed& feed) -> Root::ptr {
        return memory::make_managed<VectorChain<Cluster>>(
          ctx.fetcher, segment, field, score, ctx.k, ctx.table, count, feed);
      });
  }

  auto inner = search::InnerProbe(query);
  if (!inner) {
    return {};
  }
  if (ctx.table != nullptr) {
    return search::MakeVectorScored<FilteredWalk, Root::ptr, search::AcceptAll, false,
                            lead::TwoPhaseScored>(query, field, score,
                                                  search::Unbounded(), std::move(inner),
                                                  ctx.table, ctx.fetcher);
  }
  return search::MakeVectorScored<PlainWalk, Root::ptr, search::AcceptAll, false,
                          lead::TwoPhaseScored>(query, field, score,
                                                search::Unbounded(), std::move(inner),
                                                utils::Empty{}, ctx.fetcher);
}

}  // namespace irs::top
