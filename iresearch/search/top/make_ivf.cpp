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
              const VectorState& state, Args&& args)
    : _clusters{count, std::forward<Args>(args)},
      _fetcher{fetcher},
      _table{table},
      _boost{score.boost},
      _k{k},
      _top_k{state.top_k},
      _width{state.rescore_width} {
    _score = irs::detail::PrepareBlockScorer(score, segment, field.meta(),
                                             _provider, _block);
    if (_width > 0.f) {
      _clusters.EnableErrors(_width);
      _lower.reserve(2 * size_t{_top_k});
    }
  }

  void Run(doc_id_t min, doc_id_t max, LoserScoreCollector& collector) final {
    SDB_ASSERT(min == doc_limits::min() && doc_limits::eof(max));
    for (size_t i = 0, n = _clusters.size(); i != n; ++i) {
      auto& cluster = _clusters[i];
      if (_table && i != 0) {
        _table->Rewind();
      }
      for (;;) {
        cluster.SetThreshold(_width > 0.f ? _floor : Bar(collector));
        if (!cluster.NextRun()) {
          break;
        }
        auto docs = cluster.RunDocs();
        auto n = static_cast<uint32_t>(docs.size());
        if (_width > 0.f) {
          n = Widen(docs, cluster.RunScores(), cluster.RunErrors());
          docs = {_own, n};
        } else {
          std::copy_n(cluster.RunScores().data(), n, _block);
        }
        _fetcher.Fetch(docs);
        _score.Score(_scores, static_cast<scores_size_t>(n));
        if (_width > 0.f) {
          n = Spread(n);
          collector.AddDocs(_own, n, _scores);
          continue;
        }
        if (_table == nullptr) {
          collector.AddDocs(docs.data(), n, _scores);
          continue;
        }
        if (docs.data() != _own) {
          std::copy_n(docs.data(), n, _own);
        }
        n = _table->Narrow(_own, _scores, n);
        collector.AddDocs(_own, n, _scores);
      }
    }
  }

 private:
  static constexpr uint32_t kRun = Cluster::kRun;

  uint32_t Widen(std::span<const doc_id_t> docs, std::span<const score_t> upper,
                 std::span<const score_t> errors) {
    const auto twice = 2.f * _width;
    uint32_t kept = 0;
    for (size_t i = 0; i < docs.size(); ++i) {
      if (upper[i] < _floor) {
        continue;
      }
      if (const auto lower = upper[i] - twice * errors[i]; lower > _floor) {
        _lower.push_back(lower);
      }
      _own[kept] = docs[i];
      _block[kept] = upper[i] - _width * errors[i];
      _spread[kept] = _width * errors[i];
      ++kept;
    }
    const size_t limit =
      _floor == std::numeric_limits<score_t>::lowest() ? _top_k : 2 * _top_k;
    if (_top_k != 0 && _lower.size() >= limit) {
      const auto kth = _lower.begin() + (_top_k - 1);
      absl::c_nth_element(_lower, kth, std::greater{});
      _floor = *kth;
      _lower.resize(_top_k);
    }
    return kept;
  }

  uint32_t Spread(uint32_t n) {
    if (_table == nullptr) {
      for (uint32_t i = 0; i < n; ++i) {
        _scores[i] += _boost * _spread[i];
      }
      return n;
    }
    std::copy_n(_own, n, _all);
    const auto live = _table->Narrow(_own, _scores, n);
    for (uint32_t i = 0, j = 0; j < live; ++i) {
      if (_all[i] == _own[j]) {
        _scores[j++] += _boost * _spread[i];
      }
    }
    return live;
  }

  score_t Bar(const LoserScoreCollector& collector) const noexcept {
    if (collector.AcceptedCount() < _k || _boost <= 0.f) {
      return std::numeric_limits<score_t>::lowest();
    }
    return collector.ScoreThreshold() / _boost;
  }

  ABSL_CACHELINE_ALIGNED score_t _block[kRun];
  ABSL_CACHELINE_ALIGNED score_t _scores[kRun];
  ABSL_CACHELINE_ALIGNED doc_id_t _own[kRun];
  ABSL_CACHELINE_ALIGNED score_t _spread[kRun];
  ABSL_CACHELINE_ALIGNED doc_id_t _all[kRun];
  irs::detail::VectorClusters<Cluster> _clusters;
  irs::detail::ScaleProvider _provider;
  ScoreFunction _score;
  ColumnArgsFetcher& _fetcher;
  irs::detail::TableFilter* _table;
  score_t _boost;
  uint32_t _k;
  uint32_t _top_k;
  float _width;
  score_t _floor = std::numeric_limits<score_t>::lowest();
  std::vector<score_t> _lower;
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
          ctx.fetcher, segment, field, score, ctx.k, ctx.table, count,
          query.State(), feed);
      });
  }

  return irs::detail::MakeKnnVector<Family>(query, ctx);
}

}  // namespace irs::top
