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

#include "iresearch/search/ann/family.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/scorers/score_provider.hpp"

namespace irs::ann {
namespace {

template<typename Cluster>
class VectorChain : public Root {
 public:
  template<typename Args>
  VectorChain(ColumnArgsFetcher& fetcher, const SubReader& segment,
              const TermReader& field, const irs::detail::ScoreArgs& score,
              irs::detail::TableFilter* table, size_t count,
              const VectorState& state, Args&& args)
    : _clusters{count, std::forward<Args>(args)},
      _fetcher{fetcher},
      _table{table},
      _boost{score.boost},
      _width{state.rescore_width} {
    _score = irs::detail::PrepareBlockScorer(score, segment, field.meta(),
                                             _provider, _block);
    if (_width > 0.f) {
      _clusters.EnableErrors();
    }
  }

  void Run(Collector& collector) final {
    if (_width > 0.f) {
      Scan<true>(collector);
    } else {
      Scan<false>(collector);
    }
  }

 private:
  static constexpr uint32_t kRun = Cluster::kRun;

  template<bool Errors>
  void Scan(Collector& collector) {
    for (size_t i = 0, n = _clusters.size(); i != n; ++i) {
      auto& cluster = _clusters[i];
      if (_table && i != 0) {
        _table->Rewind();
      }
      for (;;) {
        const auto floor = collector.Floor();
        cluster.SetThreshold(floor);
        if (!cluster.NextRun()) {
          break;
        }
        auto n = Widen<Errors>(floor, cluster.RunDocs(), cluster.RunScores(),
                               cluster.RunErrors());
        _fetcher.Fetch(std::span<const doc_id_t>{_own, n});
        _score.Score(_scores, static_cast<scores_size_t>(n));
        n = Spread(n);
        collector.Append(_own, _scores, _spread, n);
      }
    }
  }

  template<bool Errors>
  uint32_t Widen(score_t floor, std::span<const doc_id_t> docs,
                 std::span<const score_t> scores,
                 std::span<const score_t> errors) {
    const auto n = static_cast<uint32_t>(docs.size());
    uint32_t kept = 0;
    uint32_t i = 0;
#ifdef __AVX2__
    const auto edge = _mm256_set1_ps(floor);
    for (; i + 8 <= n; i += 8) {
      const auto score = _mm256_loadu_ps(scores.data() + i);
      auto spread = _mm256_setzero_ps();
      if constexpr (Errors) {
        spread = _mm256_mul_ps(_mm256_set1_ps(_width),
                               _mm256_loadu_ps(errors.data() + i));
      }
      const auto mask = static_cast<uint32_t>(_mm256_movemask_ps(
        _mm256_cmp_ps(_mm256_add_ps(score, spread), edge, _CMP_NLT_UQ)));
      if (mask == 0) {
        continue;
      }
      const auto control = LeftPackControl(mask);
      const auto ids =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs.data() + i));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(_own + kept),
                          _mm256_permutevar8x32_epi32(ids, control));
      _mm256_storeu_ps(_block + kept, _mm256_permutevar8x32_ps(score, control));
      _mm256_storeu_ps(_spread + kept,
                       _mm256_permutevar8x32_ps(spread, control));
      kept += static_cast<uint32_t>(std::popcount(mask));
    }
#endif
    for (; i < n; ++i) {
      score_t spread = 0.f;
      if constexpr (Errors) {
        spread = _width * errors[i];
      }
      _own[kept] = docs[i];
      _block[kept] = scores[i];
      _spread[kept] = spread;
      kept += static_cast<uint32_t>(!(scores[i] + spread < floor));
    }
    return kept;
  }

  uint32_t Spread(uint32_t n) {
    if (_table == nullptr) {
      for (uint32_t i = 0; i < n; ++i) {
        _spread[i] *= _boost;
      }
      return n;
    }
    std::copy_n(_own, n, _all);
    const auto live = _table->Narrow(_own, _scores, n);
    for (uint32_t i = 0, j = 0; j < live; ++i) {
      if (_all[i] == _own[j]) {
        _spread[j++] = _boost * _spread[i];
      }
    }
    return live;
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
  float _width;
};

}  // namespace
}  // namespace irs::ann
namespace irs {

ann::Root::ptr KnnVectorQuery::PlanAnn(const ann::Context& ctx) const {
  if (Inner() != nullptr) {
    return detail::MakeKnnVector<ann::Family>(*this, ctx);
  }
  const auto record = Stats(ann::ScoredOf(ctx));
  const detail::ScoreArgs score{.scorer = record.scorer,
                                .stats = record.stats,
                                .fetcher = &ctx.fetcher,
                                .boost = Boost()};
  const auto& field = *State().reader;
  return detail::ResolveClusters<detail::AcceptAll>(
    *this,
    [&]<typename Cluster>(size_t count,
                          const detail::ClusterFeed& feed) -> ann::Root::ptr {
      return memory::make_managed<ann::VectorChain<Cluster>>(
        ctx.fetcher, Segment(), field, score, ctx.table, count, State(), feed);
    });
}

ann::Root::ptr RangeVectorQuery::PlanAnn(const ann::Context& ctx) const {
  return detail::ResolveBool(Inclusive(), [&]<bool Inclusive>() {
    return detail::MakeVectorScored<ann::Family, detail::RadiusGate<Inclusive>>(
      *this, ctx, Threshold());
  });
}

}  // namespace irs
