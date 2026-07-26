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

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iresearch/formats/ivf/clustering.hpp>
#include <iresearch/utils/vector.hpp>
#include <limits>
#include <random>
#include <vector>

// Compares the clustering primitive (produce k centroids from n sample vectors)
// across four strategies:
//   - Lloyd            (flat k-means, main's baseline)
//   - FlatSuperKMeans  (flat ADSampling-pruned SuperKMeans)
//   - HierLloyd        (meso sqrt(k) Lloyd + fine Lloyd -- what the IVF tree
//   does
//                       per node)
//   - Hskm             (meso sqrt(k) SuperKMeans + fine Lloyd -- the old H-SKM)
// The two hierarchical variants only differ in the meso clusterer, isolating
// whether SuperKMeans helps at the coarse level. Reports wall time plus the
// k-means objective (mean squared distance of a sample of points to their
// nearest centroid) so speed can be read at ~equal quality. Data and cache
// warm-up happen in SetUp, outside the timed loop.

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

// Two-level k-means: cluster n points into ~sqrt(k) meso centroids with
// meso_algo, then Lloyd-split each meso group into a proportional share of the
// k fine centroids. This is the per-node decomposition the IVF tree performs;
// meso_algo=Lloyd is HierLloyd, meso_algo=FlatSuperKMeans is the old H-SKM.
std::vector<float> RunHierarchical(const std::vector<float>& data, size_t n,
                                   uint32_t k, uint32_t d, uint32_t seed,
                                   irs::ClusteringAlgo meso_algo) {
  const uint32_t k_meso = static_cast<uint32_t>(std::clamp<size_t>(
    static_cast<size_t>(std::lround(std::sqrt(static_cast<double>(k)))),
    size_t{2}, size_t{k}));
  auto meso =
    irs::TrainCentroids(irs::VectorMetric::L2Sqr, data.data(), n, k_meso, d,
                        seed, /*niter=*/8, /*nredo=*/1, meso_algo);
  const uint32_t km = static_cast<uint32_t>(meso.size() / d);

  std::vector<uint32_t> label(n);
  std::vector<size_t> counts(km, 0);
  for (size_t i = 0; i < n; ++i) {
    const auto* x =
      reinterpret_cast<const irs::byte_type*>(data.data() + i * d);
    float best = std::numeric_limits<float>::max();
    uint32_t bg = 0;
    for (uint32_t g = 0; g < km; ++g) {
      const auto* cg = reinterpret_cast<const irs::byte_type*>(
        meso.data() + static_cast<size_t>(g) * d);
      const float dist = irs::vector::L2Space<float, float, float>::Dist(
        x, cg, static_cast<uint16_t>(d));
      if (dist < best) {
        best = dist;
        bg = g;
      }
    }
    label[i] = bg;
    counts[bg]++;
  }

  std::vector<size_t> offset(km + 1, 0);
  for (uint32_t g = 0; g < km; ++g) {
    offset[g + 1] = offset[g] + counts[g];
  }
  std::vector<float> grouped(n * d);
  std::vector<size_t> cursor(offset.begin(), offset.end() - 1);
  for (size_t i = 0; i < n; ++i) {
    const size_t p = cursor[label[i]]++;
    std::memcpy(grouped.data() + p * d, data.data() + i * d, d * sizeof(float));
  }

  std::vector<uint32_t> kg(km, 0);
  uint32_t assigned = 0;
  for (uint32_t g = 0; g < km; ++g) {
    if (counts[g] == 0) {
      continue;
    }
    kg[g] = std::clamp<uint32_t>(
      static_cast<uint32_t>(static_cast<double>(k) *
                            static_cast<double>(counts[g]) / n),
      1u, static_cast<uint32_t>(counts[g]));
    assigned += kg[g];
  }
  for (uint32_t g = 0; assigned < k && g < km; ++g) {
    if (kg[g] != 0 && kg[g] < counts[g]) {
      ++kg[g];
      ++assigned;
    }
  }

  std::vector<float> out;
  out.reserve(static_cast<size_t>(k) * d);
  for (uint32_t g = 0; g < km; ++g) {
    if (kg[g] == 0) {
      continue;
    }
    auto c = irs::TrainCentroids(
      irs::VectorMetric::L2Sqr, grouped.data() + offset[g] * d, counts[g],
      kg[g], d, seed, /*niter=*/5, /*nredo=*/1, irs::ClusteringAlgo::Lloyd);
    out.insert(out.end(), c.begin(), c.end());
  }
  return out;
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

void RunHierVariant(benchmark::State& state, irs::ClusteringAlgo meso_algo,
                    const std::vector<float>& data) {
  const uint32_t k = static_cast<uint32_t>(state.range(0));
  std::vector<float> c;
  for (auto _ : state) {
    c = RunHierarchical(data, kN, k, kDim, kSeed, meso_algo);
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
BENCHMARK_DEFINE_F(ClusteringFixture, HierLloyd)(benchmark::State& state) {
  RunHierVariant(state, irs::ClusteringAlgo::Lloyd, data);
}
BENCHMARK_DEFINE_F(ClusteringFixture, Hskm)(benchmark::State& state) {
  RunHierVariant(state, irs::ClusteringAlgo::FlatSuperKMeans, data);
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
CLUSTERING_REGISTER(HierLloyd);
CLUSTERING_REGISTER(Hskm);

}  // namespace

BENCHMARK_MAIN();
