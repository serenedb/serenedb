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

#include <benchmark/benchmark.h>

#include <cstddef>
#include <cstdint>
#include <iresearch/formats/ivf/clustering.hpp>
#include <iresearch/utils/vector.hpp>
#include <random>
#include <vector>

// Compares the clustering primitive (produce k centroids from n sample vectors)
// across three algorithms exposed by irs::TrainCentroids:
//   - Lloyd            (plain k-means, main's baseline)
//   - FlatSuperKMeans  (ADSampling-pruned flat SuperKMeans)
//   - Hskm             (Hierarchical SuperKMeans: meso sqrt(k) + fine)
// Reports wall time plus the k-means objective (mean squared distance of a
// sample of points to their nearest centroid) so speed can be read at ~equal
// quality. Data and cache warm-up happen in SetUp, outside the timed loop.

namespace {

constexpr uint32_t kDim = 1536;
constexpr size_t kN = 16384;
constexpr uint32_t kSeed = 12345;
constexpr size_t kObjSample = 2000;

std::vector<float> MakeMixture(size_t n, uint32_t d, uint32_t seed) {
  std::mt19937 rng{seed};
  constexpr size_t kCenters = 512;
  std::normal_distribution<float> anchor{0.f, 1.f};
  std::normal_distribution<float> noise{0.f, 0.1f};
  std::vector<float> centers(kCenters * d);
  for (auto& x : centers) {
    x = anchor(rng);
  }
  std::vector<float> data(n * d);
  std::uniform_int_distribution<size_t> pick{0, kCenters - 1};
  for (size_t i = 0; i < n; ++i) {
    const size_t c = pick(rng);
    for (uint32_t j = 0; j < d; ++j) {
      data[i * d + j] = centers[c * d + j] + noise(rng);
    }
  }
  return data;
}

double MeanObjective(const std::vector<float>& data, size_t n, uint32_t d,
                     const std::vector<float>& c, uint32_t k, size_t sample) {
  if (c.empty() || k == 0) {
    return 0.0;
  }
  const size_t step = std::max<size_t>(1, n / sample);
  double total = 0.0;
  size_t cnt = 0;
  for (size_t i = 0; i < n; i += step) {
    const auto* x =
      reinterpret_cast<const irs::byte_type*>(data.data() + i * d);
    float best = 0.f;
    for (uint32_t j = 0; j < k; ++j) {
      const auto* cj = reinterpret_cast<const irs::byte_type*>(
        c.data() + static_cast<size_t>(j) * d);
      const float dist = irs::vector::L2Space<float, float, float>::Dist(
        x, cj, static_cast<uint16_t>(d));
      if (j == 0 || dist < best) {
        best = dist;
      }
    }
    total += best;
    ++cnt;
  }
  return cnt ? total / static_cast<double>(cnt) : 0.0;
}

class ClusteringFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State&) override {
    if (data.empty()) {
      data = MakeMixture(kN, kDim, kSeed);
    }
    auto warm = irs::TrainCentroids(
      irs::VectorMetric::L2Sqr, data.data(), std::min<size_t>(kN, 4096),
      /*k=*/64, kDim, kSeed, /*niter=*/3,
      /*nredo=*/1, irs::ClusteringAlgo::FlatSuperKMeans);
    benchmark::DoNotOptimize(warm.data());
  }

  std::vector<float> data;
};

void RunVariant(benchmark::State& state, irs::ClusteringAlgo algo,
                const std::vector<float>& data) {
  const uint32_t k = static_cast<uint32_t>(state.range(0));
  std::vector<float> c;
  for (auto _ : state) {
    c = irs::TrainCentroids(irs::VectorMetric::L2Sqr, data.data(), kN, k, kDim,
                            kSeed, /*niter=*/8, /*nredo=*/1, algo);
    benchmark::DoNotOptimize(c.data());
  }
  state.counters["k"] = static_cast<double>(k);
  state.counters["centroids"] = static_cast<double>(c.size() / kDim);
  state.counters["obj"] = MeanObjective(data, kN, kDim, c, k, kObjSample);
}

BENCHMARK_DEFINE_F(ClusteringFixture, Lloyd)(benchmark::State& state) {
  RunVariant(state, irs::ClusteringAlgo::Lloyd, data);
}
BENCHMARK_DEFINE_F(ClusteringFixture,
                   FlatSuperKMeans)(benchmark::State& state) {
  RunVariant(state, irs::ClusteringAlgo::FlatSuperKMeans, data);
}
#define CLUSTERING_REGISTER(Method)               \
  BENCHMARK_REGISTER_F(ClusteringFixture, Method) \
    ->Arg(256)                                    \
    ->Arg(1024)                                   \
    ->Arg(4096)                                   \
    ->Unit(benchmark::kMillisecond)               \
    ->Iterations(1)

CLUSTERING_REGISTER(Lloyd);
CLUSTERING_REGISTER(FlatSuperKMeans);

}  // namespace

BENCHMARK_MAIN();
