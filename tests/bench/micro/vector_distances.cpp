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
#include <velox/common/memory/Memory.h>
#include <velox/expression/ComplexViewTypes.h>
#include <velox/expression/UdfTypeResolver.h>
#include <velox/expression/VectorReaders.h>
#include <velox/functions/prestosql/DistanceFunctions.h>
#include <velox/type/CppToType.h>
#include <velox/vector/DecodedVector.h>
#include <velox/vector/FlatVector.h>
#include "pg/functions/vector.h"

#ifdef VELOX_ENABLE_FAISS
#include <faiss/utils/distances.h>
#endif

#include <cmath>
#include <memory>
#include <vector>

// Benchmarks comparing:
//   - sdb::pg UDF structs from server/pg/functions/vector.h (full wrapper incl. flat-check)
//   - Velox UDF structs from velox/functions/prestosql/DistanceFunctions.h
//     (only available with VELOX_ENABLE_FAISS; backed by FAISS)
//   - Plain C++ loop equivalents (mirrors velox non-FAISS path)
//
// Vector construction is done in the fixture's SetUp, outside the timed loop.
// Parameterized by dimension: 64, 128, 256, 512, 1024.

namespace {

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

memory::MemoryPool* pool() {
  static auto leaf = []() {
    if (!memory::MemoryManager::testInstance()) {
      memory::MemoryManager::initialize({});
    }
    return memory::MemoryManager::getInstance()->addLeafPool();
  }();
  return leaf.get();
}

std::vector<float> MakeFloatData(int dim, float start, float step = 0.01f) {
  std::vector<float> v(dim);
  for (int i = 0; i < dim; ++i) {
    v[i] = start + static_cast<float>(i) * step;
  }
  return v;
}

// Owns a FlatVector<float>, its DecodedVector and VectorReader, and exposes an
// ArrayView<false, float> suitable for passing to Velox UDF call/callNullFree.
//
// All internal pointers are stable once constructed; must not be moved or copied.
struct VeloxFloatArray {
  std::shared_ptr<FlatVector<float>> vec;
  DecodedVector decoded;
  std::unique_ptr<VectorReader<float>> reader;
  ArrayView<false, float> view;

  explicit VeloxFloatArray(const std::vector<float>& data)
      : vec{BaseVector::create<FlatVector<float>>(
          CppToType<float>::create(),
          static_cast<vector_size_t>(data.size()),
          pool())},
        view{nullptr, 0, 0} {
    for (vector_size_t i = 0; i < static_cast<vector_size_t>(data.size());
         ++i) {
      vec->set(i, data[i]);
    }
    decoded.decode(*vec);
    reader = std::make_unique<VectorReader<float>>(&decoded);
    view = ArrayView<false, float>{
      reader.get(), 0, static_cast<vector_size_t>(data.size())};
  }

  VeloxFloatArray(const VeloxFloatArray&) = delete;
  VeloxFloatArray& operator=(const VeloxFloatArray&) = delete;
  VeloxFloatArray(VeloxFloatArray&&) = delete;
  VeloxFloatArray& operator=(VeloxFloatArray&&) = delete;
};

// Fixture builds left/right float arrays once per benchmark registration
// (i.e. once per dimension) before any timed iterations run.
class DistanceFixture : public benchmark::Fixture {
 public:
  void SetUp(benchmark::State& state) override {
    const int dim = state.range(0);
    ldata = MakeFloatData(dim, 1.0f);
    rdata = MakeFloatData(dim, 2.0f);
    l = std::make_unique<VeloxFloatArray>(ldata);
    r = std::make_unique<VeloxFloatArray>(rdata);
  }

  void TearDown(benchmark::State& /*state*/) override {
    l.reset();
    r.reset();
  }

  std::vector<float> ldata;
  std::vector<float> rdata;
  std::unique_ptr<VeloxFloatArray> l;
  std::unique_ptr<VeloxFloatArray> r;
};

///////////////////////////////////////////////////////////////////////////////
// L2 Squared
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbL2Squared)(benchmark::State& state) {
  sdb::pg::L2Squared<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, SdbL2Squared)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

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
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

#ifdef VELOX_ENABLE_FAISS
BENCHMARK_DEFINE_F(DistanceFixture, VeloxL2Squared)(benchmark::State& state) {
  L2SquaredFunctionFloatArray<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, VeloxL2Squared)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);
#endif

///////////////////////////////////////////////////////////////////////////////
// L1 (Manhattan) distance
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbL1Distance)(benchmark::State& state) {
  sdb::pg::L1Distance<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, SdbL1Distance)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

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
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

#ifdef VELOX_ENABLE_FAISS
BENCHMARK_DEFINE_F(DistanceFixture, VeloxL1Distance)(benchmark::State& state) {
  L1FunctionFloatArray<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, VeloxL1Distance)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);
#endif

///////////////////////////////////////////////////////////////////////////////
// Dot product
///////////////////////////////////////////////////////////////////////////////

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
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

#ifdef VELOX_ENABLE_FAISS
BENCHMARK_DEFINE_F(DistanceFixture, VeloxDotProduct)(benchmark::State& state) {
  DotProductFloatArray<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, VeloxDotProduct)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);
#endif

///////////////////////////////////////////////////////////////////////////////
// Cosine similarity
///////////////////////////////////////////////////////////////////////////////

BENCHMARK_DEFINE_F(DistanceFixture, SdbCosine)(benchmark::State& state) {
  sdb::pg::CosineSimilarity<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, SdbCosine)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

// Mirrors the velox non-FAISS cosine path (plain C++ loops with double accumulation).
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
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);

#ifdef VELOX_ENABLE_FAISS
BENCHMARK_DEFINE_F(DistanceFixture, VeloxCosine)(benchmark::State& state) {
  CosineSimilarityFunctionFloatArray<VectorExec> fn;
  for (auto _ : state) {
    float result = 0.0f;
    fn.callNullFree(result, l->view, r->view);
    benchmark::DoNotOptimize(result);
  }
}
BENCHMARK_REGISTER_F(DistanceFixture, VeloxCosine)
  ->Arg(64)->Arg(128)->Arg(256)->Arg(512)->Arg(1024);
#endif

}  // namespace

BENCHMARK_MAIN();
