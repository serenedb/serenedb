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
#include "iresearch/search/ann/ivf_scan.hpp"
#include "iresearch/search/detail/resolve.hpp"
#include "iresearch/search/detail/vector_of.hpp"
#include "iresearch/search/scorers/score_provider.hpp"

namespace irs::ann {
namespace {

class IvfScanRoot : public Root {
 public:
  IvfScanRoot(const KnnVectorQuery& query, const irs::detail::ScoreArgs& score,
              irs::detail::TableFilter* table)
    : _state{query.State()},
      _scan{MakeQuantizerReader(_state.codebook), score.boost,
            _state.rescore_width} {
    _scan.Score(score, query.Segment(), _state.reader->meta(), table);
    const bool centroids =
      _state.cluster_centroids.size() == _state.cookies.size() * _state.d;
    const auto count = static_cast<uint32_t>(_state.cookies.size());
    _clusters.reserve(count);
    _probes.reserve(count);
    for (uint32_t c = 0; c < count; ++c) {
      _clusters.push_back({.meta = _state.cookies[c], .first = c, .count = 1});
      _probes.push_back(
        {.query = 0,
         .centroid = centroids
                       ? _state.cluster_centroids.data() + size_t{c} * _state.d
                       : nullptr});
    }
  }

  void Run(Collector& collector) final {
    ScanIvfClusters(*irs::detail::DocOf(*_state.reader), *_state.payload,
                    _scan.Reader().BlockSetting(), _clusters, _probes,
                    {&_scan, 1}, {&collector, 1}, nullptr);
  }

 private:
  const VectorState& _state;
  IvfQueryScan _scan;
  std::vector<IvfCluster> _clusters;
  std::vector<IvfProbe> _probes;
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
  return memory::make_managed<ann::IvfScanRoot>(*this, score, ctx.table);
}

ann::Root::ptr RangeVectorQuery::PlanAnn(const ann::Context& ctx) const {
  return detail::ResolveBool(Inclusive(), [&]<bool Inclusive>() {
    return detail::MakeVectorScored<ann::Family, detail::RadiusGate<Inclusive>>(
      *this, ctx, Threshold());
  });
}

}  // namespace irs
