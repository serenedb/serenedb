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

#include <iresearch/utils/vector.hpp>

#include "pg/functions/vector.h"

#ifdef VELOX_ENABLE_FAISS
#include <faiss/utils/distances.h>
#endif

#include <cmath>
#include <memory>
#include <vector>

// Benchmarks comparing:
//   - sdb::pg UDF structs from server/pg/functions/vector.h (full wrapper incl.
//   flat-check)
//   - Velox UDF structs from velox/functions/prestosql/DistanceFunctions.h
//     (only available with VELOX_ENABLE_FAISS; backed by FAISS)
//   - Plain C++ loop equivalents (mirrors velox non-FAISS path)
//
// Vector construction is done in the fixture's SetUp, outside the timed loop.
// Parameterized by dimension: 64, 128, 256, 512, 1024.

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

///////////////////////////////////////////////////////////////////////////////
// L2 Squared
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = irs::vector::L2Space<float, float, float>::Dist(
      reinterpret_cast<const irs::byte_type*>(ldata.data()),
      reinterpret_cast<const irs::byte_type*>(rdata.data()),
      static_cast<uint16_t>(ldata.size()));
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, SdbL2Squared)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

BENCHMARK_DEFINE_F(DistanceFixture, PlainL2Squared)(benchmark::State& state) {
  const int dim = state.range(0);
  for (auto _ : state) {
    float s = 0.0f;
    for (int i = 0; i < dim; ++i) {
      const float d = ldata[i] - rdata[i];
      s += d * d;
    }
    benchmark::DoNotOptimize(s);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, PlainL2Squared)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = faiss::fvec_L2sqr(
      reinterpret_cast<const float*>(ldata.data()),
      reinterpret_cast<const float*>(rdata.data()), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, VeloxL2Squared)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#endif

///////////////////////////////////////////////////////////////////////////////
// L1 (Manhattan) distance
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = irs::vector::L1Space<float, float, float>::Dist(
      reinterpret_cast<const irs::byte_type*>(ldata.data()),
      reinterpret_cast<const irs::byte_type*>(rdata.data()),
      static_cast<uint16_t>(ldata.size()));
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, SdbL1Distance)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

BENCHMARK_DEFINE_F(DistanceFixture, PlainL1Distance)(benchmark::State& state) {
  const int dim = state.range(0);
  for (auto _ : state) {
    float s = 0.0f;
    for (int i = 0; i < dim; ++i) {
      s += std::abs(ldata[i] - rdata[i]);
    }
    benchmark::DoNotOptimize(s);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, PlainL1Distance)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = faiss::fvec_L1(reinterpret_cast<const float*>(ldata.data()),
                                  reinterpret_cast<const float*>(rdata.data()),
                                  ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, VeloxL1Distance)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#endif

///////////////////////////////////////////////////////////////////////////////
// Dot product
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result = irs::vector::DotProductImpl<float, float>::Compute(
      reinterpret_cast<const irs::byte_type*>(ldata.data()),
      reinterpret_cast<const irs::byte_type*>(rdata.data()),
      static_cast<uint16_t>(ldata.size()));
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, SdbDotProduct)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

BENCHMARK_DEFINE_F(DistanceFixture, PlainDotProduct)(benchmark::State& state) {
  const int dim = state.range(0);
  for (auto _ : state) {
    double s = 0.0;
    for (int i = 0; i < dim; ++i) {
      s += static_cast<double>(ldata[i]) * rdata[i];
    }
    benchmark::DoNotOptimize(s);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, PlainDotProduct)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result = faiss::fvec_inner_product(
      reinterpret_cast<const float*>(ldata.data()),
      reinterpret_cast<const float*>(rdata.data()), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, VeloxDotProduct)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#endif

///////////////////////////////////////////////////////////////////////////////
// Cosine similarity
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbCosine)(benchmark::State& state) {
  for (auto _ : state) {
    auto [ll, lr, rr] =
      irs::vector::CosineDistanceImpl<float, float, float>::Compute(
        reinterpret_cast<const irs::byte_type*>(ldata.data()),
        reinterpret_cast<const irs::byte_type*>(rdata.data()),
        static_cast<uint16_t>(ldata.size()));
    float result = static_cast<float>(lr / std::sqrt(ll * rr));
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, SdbCosine)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

// Mirrors the velox non-FAISS cosine path (plain C++ loops with double
// accumulation).
BENCHMARK_DEFINE_F(DistanceFixture, PlainCosine)(benchmark::State& state) {
  const int dim = state.range(0);
  for (auto _ : state) {
    double ll = 0.0, lr = 0.0, rr = 0.0;
    for (int i = 0; i < dim; ++i) {
      const double li = ldata[i];
      const double ri = rdata[i];
      ll += li * li;
      lr += li * ri;
      rr += ri * ri;
    }
    const float result = static_cast<float>(lr / std::sqrt(ll * rr));
    benchmark::DoNotOptimize(result);
  }
}

BENCHMARK_REGISTER_F(DistanceFixture, PlainCosine)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);

#ifdef VELOX_ENABLE_FAISS
BENCHMARK_DEFINE_F(DistanceFixture, VeloxCosine)(benchmark::State& state) {
  for (auto _ : state) {
    float norm_x = 0, norm_y = 0;
    faiss::fvec_norms_L2(&norm_x, ldata.data(), ldata.size(), 1);
    faiss::fvec_norms_L2(&norm_y, rdata.data(), rdata.size(), 1);
    float result = 0.0f;
    float product =
      faiss::fvec_inner_product(ldata.data(), rdata.data(), ldata.size());
    result = static_cast<float>(product / (norm_x * norm_y));
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, VeloxCosine)
  ->Arg(64)
  ->Arg(128)
  ->Arg(256)
  ->Arg(512)
  ->Arg(1024);
#endif

}  // namespace

BENCHMARK_MAIN();
