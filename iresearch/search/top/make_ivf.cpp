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

#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/scorers/score_provider.hpp"
#include "iresearch/search/top/family.hpp"
#include "iresearch/search/top/make.hpp"

namespace irs::top {
namespace {

template<typename Cluster>
class VectorChain : public Root {
 public:
  template<typename Args>
  VectorChain(ColumnArgsFetcher& fetcher, const SubReader& segment,
              const TermReader& field, const irs::detail::ScoreArgs& score,
              uint32_t k, irs::detail::TableFilter* table, size_t count,
              Args&& args)
    : _clusters{count, std::forward<Args>(args)},
      _fetcher{fetcher},
      _table{table},
      _boost{score.boost},
      _k{k} {
    _score = irs::detail::PrepareBlockScorer(score, segment, field.meta(),
                                             _provider, _block);
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    SDB_ASSERT(min == doc_limits::min() && doc_limits::eof(max));
    for (size_t i = 0, n = _clusters.size(); i != n; ++i) {
      auto& cluster = _clusters[i];
      if (_table && i != 0) {
        _table->Rewind();
      }
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
  irs::detail::VectorClusters<Cluster> _clusters;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  score_t _boost;
  uint32_t _k;
};

}  // namespace

Root::ptr Family::Make(const KnnVectorQuery& query, const Context& ctx) {
  const auto record = query.Stats(ScoredOf(ctx));
  const irs::detail::ScoreArgs score{.scorer = record.scorer,
                                     .stats = record.stats,
                                     .fetcher = &ctx.fetcher,
                                     .boost = query.Boost()};
  const auto& segment = query.Segment();
  const auto& field = *query.State().reader;

  if (query.Inner() == nullptr) {
    return irs::detail::ResolveClusters<irs::detail::AcceptAll>(
      query,
      [&]<typename Cluster>(size_t count,
                            const irs::detail::ClusterFeed& feed) -> Root::ptr {
        return memory::make_managed<VectorChain<Cluster>>(
          ctx.fetcher, segment, field, score, ctx.k, ctx.table, count, feed);
      });
  }

  return irs::detail::MakeKnnVector<Family>(query, ctx);
}

}  // namespace irs::top
