////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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
#include <iresearch/utils/vector.hpp>

#ifdef ENABLE_FAISS
#include <faiss/utils/distances.h>
#endif

#include <cmath>
#include <memory>
#include <vector>

// Benchmarks comparing:
//   - sdb::pg distance functions (iresearch SIMD-backed)
//   - Velox UDF structs from velox/functions/prestosql/DistanceFunctions.h
//     (only available with VELOX_ENABLE_FAISS; backed by FAISS)
//
// Vector construction is done in the fixture's SetUp, outside the timed loop.
// Parameterized by dimension: 64, 128, 256, 512, 1024, 2048.

#define DISTANCES_BENCHMARK_REGISTER(BaseClass, Method) \
  BENCHMARK_REGISTER_F(BaseClass, Method)               \
    ->Arg(64)                                           \
    ->Arg(128)                                          \
    ->Arg(256)                                          \
    ->Arg(512)                                          \
    ->Arg(1024)                                         \
    ->Arg(2048)

namespace {

std::vector<float> MakeFloatData(int dim, float start, float step = 0.01f) {
  std::vector<float> v(dim);
  for (int i = 0; i < dim; ++i) {
    v[i] = start + static_cast<float>(i) * step;
  }
  return v;
}

class DistanceFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State& state) override {
    const int dim = state.range(0);
    ldata = MakeFloatData(dim, 1.0f);
    rdata = MakeFloatData(dim, 2.0f);
  }

  std::vector<float> ldata;
  std::vector<float> rdata;
};

[[gnu::noinline]] float SdbComputeL2(const float* left, const float* right,
                                     size_t sz) {
  return irs::vector::L2Space<float, float, float>::Dist(
    reinterpret_cast<const irs::byte_type*>(left),
    reinterpret_cast<const irs::byte_type*>(right), static_cast<uint16_t>(sz));
}

[[gnu::noinline]] float SdbComputeL1(const float* left, const float* right,
                                     size_t sz) {
  return irs::vector::L1Space<float, float, float>::Dist(
    reinterpret_cast<const irs::byte_type*>(left),
    reinterpret_cast<const irs::byte_type*>(right), static_cast<uint16_t>(sz));
}

[[gnu::noinline]] float SdbComputeDotProduct(const float* left,
                                             const float* right, size_t sz) {
  return irs::vector::DotProductImpl<float, float>::Compute(
    reinterpret_cast<const irs::byte_type*>(left),
    reinterpret_cast<const irs::byte_type*>(right), static_cast<uint16_t>(sz));
}

[[gnu::noinline]] float SdbComputeCosine(const float* left, const float* right,
                                         size_t sz) {
  const auto [ll, lr, rr] =
    irs::vector::CosineDistanceImpl<float, float, float>::Compute(
      reinterpret_cast<const irs::byte_type*>(left),
      reinterpret_cast<const irs::byte_type*>(right),
      static_cast<uint16_t>(sz));
  return static_cast<float>(lr / std::sqrt(ll * rr));
}

#ifdef ENABLE_FAISS
[[gnu::noinline]] float VeloxComputeCosine(const float* left,
                                           const float* right, size_t sz) {
  float norm_x = 0, norm_y = 0;
  faiss::fvec_norms_L2(&norm_x, left, sz, 1);
  faiss::fvec_norms_L2(&norm_y, right, sz, 1);
  float product = faiss::fvec_inner_product(left, right, sz);
  return static_cast<float>(product / (norm_x * norm_y));
}
#endif

BENCHMARK_DEFINE_F(DistanceFixture, SdbL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = SdbComputeL2(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, SdbL2Squared);

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = faiss::fvec_L2sqr(
      reinterpret_cast<const float*>(ldata.data()),
      reinterpret_cast<const float*>(rdata.data()), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, VeloxL2Squared);

#endif

BENCHMARK_DEFINE_F(DistanceFixture, SdbL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = SdbComputeL1(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

DISTANCES_BENCHMARK_REGISTER(DistanceFixture, SdbL1Distance);

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = faiss::fvec_L1(reinterpret_cast<const float*>(ldata.data()),
                                  reinterpret_cast<const float*>(rdata.data()),
                                  ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, VeloxL1Distance);

#endif

BENCHMARK_DEFINE_F(DistanceFixture, SdbDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result =
      SdbComputeDotProduct(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, SdbDotProduct);

#ifdef ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result = faiss::fvec_inner_product(
      reinterpret_cast<const float*>(ldata.data()),
      reinterpret_cast<const float*>(rdata.data()), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, VeloxDotProduct);

#endif

BENCHMARK_DEFINE_F(DistanceFixture, SdbCosine)(benchmark::State& state) {
  for (auto _ : state) {
    float result = SdbComputeCosine(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, SdbCosine);

#ifdef ENABLE_FAISS
BENCHMARK_DEFINE_F(DistanceFixture, VeloxCosine)(benchmark::State& state) {
  for (auto _ : state) {
    float result = VeloxComputeCosine(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, VeloxCosine);
#endif

}  // namespace

BENCHMARK_MAIN();
