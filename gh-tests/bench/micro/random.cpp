// Copyright 2017 The Abseil Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Benchmarks for absl random distributions as well as a selection of the
// C++ standard library random distributions.

#include <absl/base/macros.h>
#include <absl/random/bernoulli_distribution.h>
#include <absl/random/beta_distribution.h>
#include <absl/random/exponential_distribution.h>
#include <absl/random/gaussian_distribution.h>
#include <absl/random/internal/fast_uniform_bits.h>
#include <absl/random/internal/randen_engine.h>
#include <absl/random/log_uniform_int_distribution.h>
#include <absl/random/poisson_distribution.h>
#include <absl/random/random.h>
#include <absl/random/uniform_int_distribution.h>
#include <absl/random/uniform_real_distribution.h>
#include <absl/random/zipf_distribution.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <iterator>
#include <limits>
#include <random>
#include <type_traits>
#include <vector>

#ifdef USE_PCG_RANDOM
#include "pcg_random.hpp"
#endif

namespace {

// Seed data to avoid reading random_device() for benchmarks.
uint32_t gKSeedData[] = {
  0x1B510052, 0x9A532915, 0xD60F573F, 0xBC9BC6E4, 0x2B60A476, 0x81E67400,
  0x08BA6FB5, 0x571BE91F, 0xF296EC6B, 0x2A0DD915, 0xB6636521, 0xE7B9F9B6,
  0xFF34052E, 0xC5855664, 0x53B02D5D, 0xA99F8FA1, 0x08BA4799, 0x6E85076A,
  0x4B7A70E9, 0xB5B32944, 0xDB75092E, 0xC4192623, 0xAD6EA6B0, 0x49A7DF7D,
  0x9CEE60B8, 0x8FEDB266, 0xECAA8C71, 0x699A18FF, 0x5664526C, 0xC2B19EE1,
  0x193602A5, 0x75094C29, 0xA0591340, 0xE4183A3E, 0x3F54989A, 0x5B429D65,
  0x6B8FE4D6, 0x99F73FD6, 0xA1D29C07, 0xEFE830F5, 0x4D2D38E6, 0xF0255DC1,
  0x4CDD2086, 0x8470EB26, 0x6382E9C6, 0x021ECC5E, 0x09686B3F, 0x3EBAEFC9,
  0x3C971814, 0x6B6A70A1, 0x687F3584, 0x52A0E286, 0x13198A2E, 0x03707344,
};

// PrecompiledSeedSeq provides kSeedData to a conforming
// random engine to speed initialization in the benchmarks.
class PrecompiledSeedSeq {
 public:
  using result_type = uint32_t;

  PrecompiledSeedSeq() = default;

  template<typename Iterator>
  PrecompiledSeedSeq(Iterator, Iterator) {}

  template<typename T>
  PrecompiledSeedSeq(std::initializer_list<T>) {}

  template<typename OutIterator>
  void generate(OutIterator begin, OutIterator end) {
    static size_t gIdx = 0;
    for (; begin != end; begin++) {
      *begin = gKSeedData[gIdx++];
      if (gIdx >= ABSL_ARRAYSIZE(gKSeedData)) {
        gIdx = 0;
      }
    }
  }

  size_t size() const { return ABSL_ARRAYSIZE(gKSeedData); }

  template<typename OutIterator>
  void param(OutIterator out) const {
    std::copy(std::begin(gKSeedData), std::end(gKSeedData), out);
  }
};

// Triggers default constructor initialization.
class DefaultConstructorSeedSeq {};

// make_engine<T, SSeq> returns a random_engine which is initialized,
// either via the default constructor, when use_default_initialization<T>
// is true, or via the indicated seed sequence, SSeq.
template<typename Engine, typename SSeq = DefaultConstructorSeedSeq>
Engine MakeEngine() {
  constexpr bool kUseDefaultInitialization =
    std::is_same_v<SSeq, DefaultConstructorSeedSeq>;
  if constexpr (kUseDefaultInitialization) {
    return Engine();
  } else {
    // Otherwise, use the provided seed sequence.
    SSeq seq(std::begin(gKSeedData), std::end(gKSeedData));
    return Engine(seq);
  }
}

template<typename Engine, typename SSeq>
void BmConstruct(benchmark::State& state) {
  for (auto _ : state) {
    auto rng = MakeEngine<Engine, SSeq>();
    benchmark::DoNotOptimize(rng());
  }
}

template<typename Engine>
void BmDirect(benchmark::State& state) {
  using value_type = typename Engine::result_type;
  // Direct use of the URBG.
  auto rng = MakeEngine<Engine>();
  for (auto _ : state) {
    benchmark::DoNotOptimize(rng());
  }
  state.SetBytesProcessed(sizeof(value_type) * state.iterations());
}

template<typename Engine>
void BmGenerate(benchmark::State& state) {
  // std::generate makes a copy of the RNG; thus this tests the
  // copy-constructor efficiency.
  using value_type = typename Engine::result_type;
  std::vector<value_type> v(64);
  auto rng = MakeEngine<Engine>();
  while (state.KeepRunningBatch(64)) {
    std::generate(std::begin(v), std::end(v), rng);
  }
}

template<typename Engine, size_t Elems>
void BmShuffle(benchmark::State& state) {
  // Direct use of the Engine.
  std::vector<uint32_t> v(Elems);
  while (state.KeepRunningBatch(Elems)) {
    auto rng = MakeEngine<Engine>();
    std::shuffle(std::begin(v), std::end(v), rng);
  }
}

template<typename Engine, size_t Elems>
void BmShuffleReuse(benchmark::State& state) {
  // Direct use of the Engine.
  std::vector<uint32_t> v(Elems);
  auto rng = MakeEngine<Engine>();
  while (state.KeepRunningBatch(Elems)) {
    std::shuffle(std::begin(v), std::end(v), rng);
  }
}

template<typename Engine, typename Dist, typename... Args>
void BmDist(benchmark::State& state, Args&&... args) {
  using value_type = typename Dist::result_type;
  auto rng = MakeEngine<Engine>();
  Dist dis{std::forward<Args>(args)...};
  // Compare the following loop performance:
  for (auto _ : state) {
    benchmark::DoNotOptimize(dis(rng));
  }
  state.SetBytesProcessed(sizeof(value_type) * state.iterations());
}

template<typename Engine, typename Dist>
void BmLarge(benchmark::State& state) {
  using value_type = typename Dist::result_type;
  volatile value_type k_min = 0;
  volatile value_type k_max = std::numeric_limits<value_type>::max() / 2 + 1;
  BmDist<Engine, Dist>(state, k_min, k_max);
}

template<typename Engine, typename Dist>
void BmSmall(benchmark::State& state) {
  using value_type = typename Dist::result_type;
  volatile value_type k_min = 0;
  volatile value_type k_max = std::numeric_limits<value_type>::max() / 64 + 1;
  BmDist<Engine, Dist>(state, k_min, k_max);
}

template<typename Engine, typename Dist, int A>
void BmBernoulli(benchmark::State& state) {
  volatile double a = static_cast<double>(A) / 1000000;
  BmDist<Engine, Dist>(state, a);
}

template<typename Engine, typename Dist, int A, int B>
void BmBeta(benchmark::State& state) {
  using value_type = typename Dist::result_type;
  volatile value_type a = static_cast<value_type>(A) / 100;
  volatile value_type b = static_cast<value_type>(B) / 100;
  BmDist<Engine, Dist>(state, a, b);
}

template<typename Engine, typename Dist, int A>
void BmGamma(benchmark::State& state) {
  using value_type = typename Dist::result_type;
  volatile value_type a = static_cast<value_type>(A) / 100;
  BmDist<Engine, Dist>(state, a);
}

template<typename Engine, typename Dist, int A = 100>
void BmPoisson(benchmark::State& state) {
  volatile double a = static_cast<double>(A) / 100;
  BmDist<Engine, Dist>(state, a);
}

template<typename Engine, typename Dist, int Q = 2, int V = 1>
void BmZipf(benchmark::State& state) {
  using value_type = typename Dist::result_type;
  volatile double q = Q;
  volatile double v = V;
  BmDist<Engine, Dist>(state, std::numeric_limits<value_type>::max(), q, v);
}

template<typename Engine, typename Dist>
void BmThread(benchmark::State& state) {
  using value_type = typename Dist::result_type;
  auto rng = MakeEngine<Engine>();
  Dist dis{};
  for (auto _ : state) {
    benchmark::DoNotOptimize(dis(rng));
  }
  state.SetBytesProcessed(sizeof(value_type) * state.iterations());
}

// NOTES:
//
// std::geometric_distribution is similar to the zipf distributions.
// The algorithm for the geometric_distribution is, basically,
// floor(log(1-X) / log(1-p))

// Normal benchmark suite
#define BM_BASIC(Engine)                                                       \
  BENCHMARK_TEMPLATE(BmConstruct, Engine, DefaultConstructorSeedSeq);          \
  BENCHMARK_TEMPLATE(BmConstruct, Engine, PrecompiledSeedSeq);                 \
  BENCHMARK_TEMPLATE(BmConstruct, Engine, std::seed_seq);                      \
  BENCHMARK_TEMPLATE(BmDirect, Engine);                                        \
  BENCHMARK_TEMPLATE(BmShuffle, Engine, 10);                                   \
  BENCHMARK_TEMPLATE(BmShuffle, Engine, 100);                                  \
  BENCHMARK_TEMPLATE(BmShuffle, Engine, 1000);                                 \
  BENCHMARK_TEMPLATE(BmShuffleReuse, Engine, 100);                             \
  BENCHMARK_TEMPLATE(BmShuffleReuse, Engine, 1000);                            \
  BENCHMARK_TEMPLATE(BmDist, Engine,                                           \
                     absl::random_internal::FastUniformBits<uint32_t>);        \
  BENCHMARK_TEMPLATE(BmDist, Engine,                                           \
                     absl::random_internal::FastUniformBits<uint64_t>);        \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::uniform_int_distribution<int32_t>);  \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::uniform_int_distribution<int64_t>);  \
  BENCHMARK_TEMPLATE(BmDist, Engine, absl::uniform_int_distribution<int32_t>); \
  BENCHMARK_TEMPLATE(BmDist, Engine, absl::uniform_int_distribution<int64_t>); \
  BENCHMARK_TEMPLATE(BmLarge, Engine, std::uniform_int_distribution<int32_t>); \
  BENCHMARK_TEMPLATE(BmLarge, Engine, std::uniform_int_distribution<int64_t>); \
  BENCHMARK_TEMPLATE(BmLarge, Engine,                                          \
                     absl::uniform_int_distribution<int32_t>);                 \
  BENCHMARK_TEMPLATE(BmLarge, Engine,                                          \
                     absl::uniform_int_distribution<int64_t>);                 \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::uniform_real_distribution<float>);   \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::uniform_real_distribution<double>);  \
  BENCHMARK_TEMPLATE(BmDist, Engine, absl::uniform_real_distribution<float>);  \
  BENCHMARK_TEMPLATE(BmDist, Engine, absl::uniform_real_distribution<double>)

#define BM_COPY(Engine) BENCHMARK_TEMPLATE(BmGenerate, Engine)

#define BM_THREAD(Engine)                                          \
  BENCHMARK_TEMPLATE(BmThread, Engine,                             \
                     absl::uniform_int_distribution<int64_t>)      \
    ->ThreadPerCpu();                                              \
  BENCHMARK_TEMPLATE(BmThread, Engine,                             \
                     absl::uniform_real_distribution<double>)      \
    ->ThreadPerCpu();                                              \
  BENCHMARK_TEMPLATE(BmShuffle, Engine, 100)->ThreadPerCpu();      \
  BENCHMARK_TEMPLATE(BmShuffle, Engine, 1000)->ThreadPerCpu();     \
  BENCHMARK_TEMPLATE(BmShuffleReuse, Engine, 100)->ThreadPerCpu(); \
  BENCHMARK_TEMPLATE(BmShuffleReuse, Engine, 1000)->ThreadPerCpu()

#define BM_EXTENDED(Engine)                                                    \
  /* -------------- Extended Uniform -----------------------*/                 \
  BENCHMARK_TEMPLATE(BmSmall, Engine, std::uniform_int_distribution<int32_t>); \
  BENCHMARK_TEMPLATE(BmSmall, Engine, std::uniform_int_distribution<int64_t>); \
  BENCHMARK_TEMPLATE(BmSmall, Engine,                                          \
                     absl::uniform_int_distribution<int32_t>);                 \
  BENCHMARK_TEMPLATE(BmSmall, Engine,                                          \
                     absl::uniform_int_distribution<int64_t>);                 \
  BENCHMARK_TEMPLATE(BmSmall, Engine, std::uniform_real_distribution<float>);  \
  BENCHMARK_TEMPLATE(BmSmall, Engine, std::uniform_real_distribution<double>); \
  BENCHMARK_TEMPLATE(BmSmall, Engine, absl::uniform_real_distribution<float>); \
  BENCHMARK_TEMPLATE(BmSmall, Engine,                                          \
                     absl::uniform_real_distribution<double>);                 \
  /* -------------- Other -----------------------*/                            \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::normal_distribution<double>);        \
  BENCHMARK_TEMPLATE(BmDist, Engine, absl::gaussian_distribution<double>);     \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::exponential_distribution<double>);   \
  BENCHMARK_TEMPLATE(BmDist, Engine, absl::exponential_distribution<double>);  \
  BENCHMARK_TEMPLATE(BmPoisson, Engine, std::poisson_distribution<int64_t>,    \
                     100);                                                     \
  BENCHMARK_TEMPLATE(BmPoisson, Engine, absl::poisson_distribution<int64_t>,   \
                     100);                                                     \
  BENCHMARK_TEMPLATE(BmPoisson, Engine, std::poisson_distribution<int64_t>,    \
                     10 * 100);                                                \
  BENCHMARK_TEMPLATE(BmPoisson, Engine, absl::poisson_distribution<int64_t>,   \
                     10 * 100);                                                \
  BENCHMARK_TEMPLATE(BmPoisson, Engine, std::poisson_distribution<int64_t>,    \
                     13 * 100);                                                \
  BENCHMARK_TEMPLATE(BmPoisson, Engine, absl::poisson_distribution<int64_t>,   \
                     13 * 100);                                                \
  BENCHMARK_TEMPLATE(BmDist, Engine,                                           \
                     absl::log_uniform_int_distribution<int32_t>);             \
  BENCHMARK_TEMPLATE(BmDist, Engine,                                           \
                     absl::log_uniform_int_distribution<int64_t>);             \
  BENCHMARK_TEMPLATE(BmDist, Engine, std::geometric_distribution<int64_t>);    \
  BENCHMARK_TEMPLATE(BmZipf, Engine, absl::zipf_distribution<uint64_t>);       \
  BENCHMARK_TEMPLATE(BmZipf, Engine, absl::zipf_distribution<uint64_t>, 2, 3); \
  BENCHMARK_TEMPLATE(BmBernoulli, Engine, std::bernoulli_distribution,         \
                     257305);                                                  \
  BENCHMARK_TEMPLATE(BmBernoulli, Engine, absl::bernoulli_distribution,        \
                     257305);                                                  \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<double>, 65, 41); \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<double>, 99,      \
                     330);                                                     \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<double>, 150,     \
                     150);                                                     \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<double>, 410,     \
                     580);                                                     \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<float>, 65, 41);  \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<float>, 99, 330); \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<float>, 150,      \
                     150);                                                     \
  BENCHMARK_TEMPLATE(BmBeta, Engine, absl::beta_distribution<float>, 410,      \
                     580);                                                     \
  BENCHMARK_TEMPLATE(BmGamma, Engine, std::gamma_distribution<float>, 199);    \
  BENCHMARK_TEMPLATE(BmGamma, Engine, std::gamma_distribution<double>, 199)

// ABSL Recommended interfaces.
// BM_BASIC(absl::InsecureBitGen);  // === pcg64_2018_engine
// // BM_COPY(absl::InsecureBitGen);
// BM_THREAD(absl::InsecureBitGen);
// BM_EXTENDED(absl::InsecureBitGen);

// BM_BASIC(absl::BitGen);          // === randen_engine<uint64_t>.
// // BM_COPY(absl::BitGen);
// BM_THREAD(absl::BitGen);
// BM_EXTENDED(absl::BitGen);

// Instantiate benchmarks for multiple engines.
using absl_rand64_fast = absl::random_internal::pcg64_2018_engine;
using absl_rand32_fast = absl::random_internal::pcg32_2018_engine;
using absl_rand64 = absl::random_internal::randen_engine<uint64_t>;
using absl_rand32 = absl::random_internal::randen_engine<uint32_t>;

// Comparison interfaces.
BM_BASIC(std::mt19937_64);
BM_COPY(std::mt19937_64);
BM_THREAD(std::mt19937_64);
BM_EXTENDED(std::mt19937_64);

/*
BM_BASIC(std::ranlux48_base);
BM_COPY(std::ranlux48_base);
BM_THREAD(std::ranlux48_base);
BM_EXTENDED(std::ranlux48_base);

BM_BASIC(std::ranlux48);
BM_COPY(std::ranlux48);
BM_THREAD(std::ranlux48);
BM_EXTENDED(std::ranlux48);
*/

BM_BASIC(absl_rand64);
BM_COPY(absl_rand64);
BM_THREAD(absl_rand64);
BM_EXTENDED(absl_rand64);

BM_BASIC(absl_rand64_fast);
BM_COPY(absl_rand64_fast);
BM_THREAD(absl_rand64_fast);
BM_EXTENDED(absl_rand64_fast);

#ifdef USE_PCG_RANDOM
BM_BASIC(pcg64);
BM_COPY(pcg64);
BM_THREAD(pcg64);
BM_EXTENDED(pcg64);

/*
BM_BASIC(pcg64_oneseq);
BM_COPY(pcg64_oneseq);
BM_THREAD(pcg64_oneseq);
BM_EXTENDED(pcg64_oneseq);

BM_BASIC(pcg64_unique);
BM_COPY(pcg64_unique);
BM_THREAD(pcg64_unique);
BM_EXTENDED(pcg64_unique);

BM_BASIC(pcg64_oneseq);
BM_COPY(pcg64_oneseq);
BM_THREAD(pcg64_oneseq);
BM_EXTENDED(pcg64_oneseq);

BM_BASIC(pcg_engines::oneseq_dxsm_128_64);
BM_COPY(pcg_engines::oneseq_dxsm_128_64);
BM_THREAD(pcg_engines::oneseq_dxsm_128_64);
BM_EXTENDED(pcg_engines::oneseq_dxsm_128_64);

BM_BASIC(pcg64_once_insecure);
BM_COPY(pcg64_once_insecure);
BM_THREAD(pcg64_once_insecure);
BM_EXTENDED(pcg64_once_insecure);

BM_BASIC(pcg64_oneseq_once_insecure);
BM_COPY(pcg64_oneseq_once_insecure);
BM_THREAD(pcg64_oneseq_once_insecure);
BM_EXTENDED(pcg64_oneseq_once_insecure);
*/
#endif

/*
BM_BASIC(std::minstd_rand0);
BM_COPY(std::minstd_rand0);
BM_THREAD(std::minstd_rand0);
BM_EXTENDED(std::minstd_rand0);

BM_BASIC(std::minstd_rand);
BM_COPY(std::minstd_rand);
BM_THREAD(std::minstd_rand);
BM_EXTENDED(std::minstd_rand);

BM_BASIC(std::mt19937);
BM_COPY(std::mt19937);
BM_THREAD(std::mt19937);
BM_EXTENDED(std::mt19937);

BM_BASIC(std::ranlux24_base);
BM_COPY(std::ranlux24_base);
BM_THREAD(std::ranlux24_base);
BM_EXTENDED(std::ranlux24_base);

BM_BASIC(std::ranlux24);
BM_COPY(std::ranlux24);
BM_THREAD(std::ranlux24);
BM_EXTENDED(std::ranlux24);

BM_BASIC(std::knuth_b);
BM_COPY(std::knuth_b);
BM_THREAD(std::knuth_b);
BM_EXTENDED(std::knuth_b);

BM_BASIC(randen_engine_32);
BM_COPY(randen_engine_32);
BM_THREAD(randen_engine_32);
BM_EXTENDED(randen_engine_32);

#ifdef USE_PCG_RANDOM
BM_BASIC(pcg32_fast);
BM_COPY(pcg32_fast);
BM_THREAD(pcg32_fast);
BM_EXTENDED(pcg32_fast);

BM_BASIC(pcg32);
BM_COPY(pcg32);
BM_THREAD(pcg32);
BM_EXTENDED(pcg32);
#endif
*/

}  // namespace

BENCHMARK_MAIN();
