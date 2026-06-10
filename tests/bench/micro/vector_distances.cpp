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

#ifdef NUMKONG_DISTANCES_BENCH
#include <numkong/numkong.h>
#endif

#include <cstddef>
#include <iresearch/utils/vector.hpp>

#ifdef ENABLE_FAISS
#include <faiss/utils/distances.h>
#endif

#include <cmath>
#include <memory>
#include <vector>

#include "basics/exceptions.h"

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

#ifdef YDB_DISTANCES_BENCH
// from
// Dot:
// https://github.com/ydb-platform/ydb/blob/main/library/cpp/dot_product/dot_product_avx2.cpp
// L1:
// https://github.com/ydb-platform/ydb/blob/main/library/cpp/l1_distance/l1_distance.h
// L2:
// https://github.com/ydb-platform/ydb/blob/main/library/cpp/l2_distance/l2_distance.cpp
// Cosine:
// https://github.com/ydb-platform/ydb/blob/main/library/cpp/dot_product/dot_product_avx2.cpp

constexpr int64_t Bits(int n) {
  return int64_t(-1) ^ ((int64_t(1) << (64 - n)) - 1);
}

constexpr __m256 BlendMask32[8] = {
  __m256i{Bits(64), Bits(64), Bits(64), Bits(64)},
  __m256i{Bits(32), Bits(64), Bits(64), Bits(64)},
  __m256i{0, Bits(64), Bits(64), Bits(64)},
  __m256i{0, Bits(32), Bits(64), Bits(64)},
  __m256i{0, 0, Bits(64), Bits(64)},
  __m256i{0, 0, Bits(32), Bits(64)},
  __m256i{0, 0, 0, Bits(64)},
  __m256i{0, 0, 0, Bits(32)},
};
// Horizontal sum of eight float values in an avx register
float HsumFloat(__m256 v) {
  __m256 y = _mm256_permute2f128_ps(v, v, 1);
  v = _mm256_add_ps(v, y);
  v = _mm256_hadd_ps(v, v);
  return _mm256_cvtss_f32(_mm256_hadd_ps(v, v));
}

[[gnu::noinline]] float YdbDotProduct(const float* lhs, const float* rhs,
                                      size_t length) noexcept {
  __m256 sum1 = _mm256_setzero_ps();
  __m256 sum2 = _mm256_setzero_ps();
  __m256 a1, b1, a2, b2;

  if (const auto leftover = length % 8; leftover != 0) {
    a1 = _mm256_blendv_ps(_mm256_loadu_ps(lhs), _mm256_setzero_ps(),
                          BlendMask32[leftover]);
    b1 = _mm256_blendv_ps(_mm256_loadu_ps(rhs), _mm256_setzero_ps(),
                          BlendMask32[leftover]);
    sum1 = _mm256_mul_ps(a1, b1);
    lhs += leftover;
    rhs += leftover;
    length -= leftover;
  }

  while (length >= 16) {
    a1 = _mm256_loadu_ps(lhs);
    b1 = _mm256_loadu_ps(rhs);
    a2 = _mm256_loadu_ps(lhs + 8);
    b2 = _mm256_loadu_ps(rhs + 8);

    sum1 = _mm256_fmadd_ps(a1, b1, sum1);
    sum2 = _mm256_fmadd_ps(a2, b2, sum2);

    length -= 16;
    lhs += 16;
    rhs += 16;
  }

  if (length > 0) {
    a1 = _mm256_loadu_ps(lhs);
    b1 = _mm256_loadu_ps(rhs);
    sum1 = _mm256_fmadd_ps(a1, b1, sum1);
  }

  return HsumFloat(_mm256_add_ps(sum1, sum2));
}

template<typename T>
inline T AbsDelta(T a, T b) {
  if (a < b) {
    return b - a;
  }
  return a - b;
}

template<typename Result, typename Number>
inline Result L1DistanceImpl4(const Number* lhs, const Number* rhs,
                              int length) {
  Result s0 = 0;
  Result s1 = 0;
  Result s2 = 0;
  Result s3 = 0;

  while (length >= 4) {
    s0 += AbsDelta(lhs[0], rhs[0]);
    s1 += AbsDelta(lhs[1], rhs[1]);
    s2 += AbsDelta(lhs[2], rhs[2]);
    s3 += AbsDelta(lhs[3], rhs[3]);
    lhs += 4;
    rhs += 4;
    length -= 4;
  }

  while (length--) {
    s0 += AbsDelta(*lhs++, *rhs++);
  }

  return s0 + s1 + s2 + s3;
}

[[gnu::noinline]] float YdbNoSseL1Distance(const float* left,
                                           const float* right, size_t sz) {
  return L1DistanceImpl4<float, float>(left, right, sz);
}

[[gnu::noinline]] float YdbL1Distance(const float* lhs, const float* rhs,
                                      int length) {
  __m128 res = _mm_setzero_ps();
  __m128 absMask = _mm_castsi128_ps(_mm_set1_epi32(0x7fffffff));

  while (length >= 4) {
    __m128 a = _mm_loadu_ps(lhs);
    __m128 b = _mm_loadu_ps(rhs);
    __m128 d = _mm_sub_ps(a, b);
    res = _mm_add_ps(_mm_and_ps(d, absMask), res);
    rhs += 4;
    lhs += 4;
    length -= 4;
  }

  alignas(16) float r[4];
  _mm_store_ps(r, res);
  float sum = r[0] + r[1] + r[2] + r[3];

  while (length) {
    sum += std::abs(*lhs - *rhs);
    ++lhs;
    ++rhs;
    --length;
  }

  return sum;
}

template<typename Result, typename Number>
inline Result SqrDelta(Number a, Number b) {
  Result diff = a < b ? b - a : a - b;
  return diff * diff;
}

template<typename Result, typename Number>
inline Result L2SqrDistanceImpl4(const Number* a, const Number* b, int length) {
  Result s0 = 0;
  Result s1 = 0;
  Result s2 = 0;
  Result s3 = 0;

  while (length >= 4) {
    s0 += SqrDelta<Result, Number>(a[0], b[0]);
    s1 += SqrDelta<Result, Number>(a[1], b[1]);
    s2 += SqrDelta<Result, Number>(a[2], b[2]);
    s3 += SqrDelta<Result, Number>(a[3], b[3]);
    a += 4;
    b += 4;
    length -= 4;
  }

  while (length--) {
    s0 += SqrDelta<Result, Number>(*a++, *b++);
  }

  return s0 + s1 + s2 + s3;
}

[[gnu::noinline]] float YdbNoSseL2Distance(const float* left,
                                           const float* right, size_t sz) {
  return L2SqrDistanceImpl4<float, float>(left, right, sz);
}

template<class T>
static constexpr T Sqr(const T t) noexcept {
  return t * t;
}

[[gnu::noinline]] float YdbL2Distance(const float* lhs, const float* rhs,
                                      int length) {
  __m128 sum = _mm_setzero_ps();

  while (length >= 4) {
    __m128 a = _mm_loadu_ps(lhs);
    __m128 b = _mm_loadu_ps(rhs);
    __m128 delta = _mm_sub_ps(a, b);
    sum = _mm_add_ps(sum, _mm_mul_ps(delta, delta));
    length -= 4;
    rhs += 4;
    lhs += 4;
  }

  alignas(16) float res[4];
  _mm_store_ps(res, sum);

  while (length--) {
    res[0] += Sqr(*rhs++ - *lhs++);
  }

  return res[0] + res[1] + res[2] + res[3];
}

template<typename T>
struct TTriWayDotProduct {
  T LL = 1;
  T LR = 0;
  T RR = 1;
};

template<bool computeRR>
IRS_FORCE_INLINE static void TriWayDotProductIterationAvx2(
  __m256& sumLL, __m256& sumLR, __m256& sumRR, const __m256 a, const __m256 b) {
  sumLL = _mm256_fmadd_ps(a, a, sumLL);
  sumLR = _mm256_fmadd_ps(a, b, sumLR);

  if constexpr (computeRR) {
    sumRR = _mm256_fmadd_ps(b, b, sumRR);
  }
}

template<bool computeRR>
TTriWayDotProduct<float> TriWayDotProductAvx2Impl(const float* lhs,
                                                  const float* rhs,
                                                  size_t length) noexcept {
  __m256 sumLL1 = _mm256_setzero_ps();
  __m256 sumLR1 = _mm256_setzero_ps();
  __m256 sumRR1 = _mm256_setzero_ps();
  __m256 sumLL2 = _mm256_setzero_ps();
  __m256 sumLR2 = _mm256_setzero_ps();
  __m256 sumRR2 = _mm256_setzero_ps();

  while (length >= 16) {
    TriWayDotProductIterationAvx2<computeRR>(sumLL1, sumLR1, sumRR1,
                                             _mm256_loadu_ps(lhs + 0),
                                             _mm256_loadu_ps(rhs + 0));
    TriWayDotProductIterationAvx2<computeRR>(sumLL2, sumLR2, sumRR2,
                                             _mm256_loadu_ps(lhs + 8),
                                             _mm256_loadu_ps(rhs + 8));
    length -= 16;
    lhs += 16;
    rhs += 16;
  }

  if (length >= 8) {
    TriWayDotProductIterationAvx2<computeRR>(sumLL1, sumLR1, sumRR1,
                                             _mm256_loadu_ps(lhs + 0),
                                             _mm256_loadu_ps(rhs + 0));
    length -= 8;
    lhs += 8;
    rhs += 8;
  }

  sumLL1 = _mm256_add_ps(sumLL1, sumLL2);
  sumLR1 = _mm256_add_ps(sumLR1, sumLR2);

  if (computeRR) {
    sumRR1 = _mm256_add_ps(sumRR1, sumRR2);
  }

  if (length) {
    static const int32_t maskTable[14] = {-1, -1, -1, -1, -1, -1, -1,
                                          0,  0,  0,  0,  0,  0,  0};

    const __m256i mask =
      _mm256_loadu_si256((const __m256i*)(maskTable + 7 - length));

    __m256 a = _mm256_maskload_ps(lhs, mask);
    __m256 b = _mm256_maskload_ps(rhs, mask);

    TriWayDotProductIterationAvx2<computeRR>(sumLL1, sumLR1, sumRR1, a, b);
  }

  __m128 sumLL128 = _mm_add_ps(_mm256_castps256_ps128(sumLL1),
                               _mm256_extractf128_ps(sumLL1, 1));
  __m128 sumLR128 = _mm_add_ps(_mm256_castps256_ps128(sumLR1),
                               _mm256_extractf128_ps(sumLR1, 1));

  __m128 t0 = sumLL128;
  __m128 t1 = sumLR128;

  __m128 t2;
  if (computeRR) {
    t2 = _mm_add_ps(_mm256_castps256_ps128(sumRR1),
                    _mm256_extractf128_ps(sumRR1, 1));
  } else {
    t2 = _mm_setzero_ps();
  }

  __m128 t3 = _mm_setzero_ps();

  _MM_TRANSPOSE4_PS(t0, t1, t2, t3);
  t0 = _mm_add_ps(t0, t1);
  t0 = _mm_add_ps(t0, t2);
  t0 = _mm_add_ps(t0, t3);

  alignas(16) float res[4];
  _mm_store_ps(res, t0);
  TTriWayDotProduct<float> result{res[0], res[1], res[2]};
  if (!computeRR) {
    static constexpr const TTriWayDotProduct<float> def;
    result.RR = def.RR;
  }
  return result;
}

#endif

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
  return static_cast<float>(lr / std::sqrt(ll) * std::sqrt(rr));
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

[[gnu::noinline]] float VeloxComputeL2(const float* left, const float* right,
                                       size_t sz) {
  float res = faiss::fvec_L2sqr(left, right, sz);
  return res;
}

[[gnu::noinline]] float VeloxComputeL1(const float* left, const float* right,
                                       size_t sz) {
  float res = faiss::fvec_L1(left, right, sz);
  return res;
}

[[gnu::noinline]] float VeloxComputeDot(const float* left, const float* right,
                                        size_t sz) {
  float res = faiss::fvec_inner_product(left, right, sz);
  return res;
}

#ifdef NUMKONG_DISTANCES_BENCH
[[gnu::noinline]] float NumKongComputeCosine(const float* left,
                                             const float* right, size_t sz) {
  double res;
  nk_angular_f32(left, right, sz, &res);
  return static_cast<float>(res);
}

[[gnu::noinline]] float NumKongComputeL2(const float* left, const float* right,
                                         size_t sz) {
  double res;
  nk_euclidean_f32(left, right, sz, &res);
  return static_cast<float>(res);
}

[[gnu::noinline]] float NumKongComputeDot(const float* left, const float* right,
                                          size_t sz) {
  double res;
  nk_dot_f32(left, right, sz, &res);
  return static_cast<float>(res);
}

#endif

#ifdef YDB_DISTANCES_BENCH
[[gnu::noinline]] float YdbComputeCosine(const float* left, const float* right,
                                         size_t sz) {
  auto p = TriWayDotProductAvx2Impl<true>(left, right, sz);
  return p.LR / (std::sqrt(p.LL) * std::sqrt(p.RR));
}
#endif

BENCHMARK_DEFINE_F(DistanceFixture, SdbL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = SdbComputeL2(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

// DISTANCES_BENCHMARK_REGISTER(DistanceFixture, SdbL2Squared);

#ifdef NUMKONG_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture, NumKongL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = NumKongComputeL2(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

DISTANCES_BENCHMARK_REGISTER(DistanceFixture, NumKongL2Squared);
#endif

#ifdef YDB_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture, YdbL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = YdbL2Distance(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
DISTANCES_BENCHMARK_REGISTER(DistanceFixture, YdbL2Squared);

BENCHMARK_DEFINE_F(DistanceFixture,
                   YdbNoSseL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = YdbNoSseL2Distance(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
DISTANCES_BENCHMARK_REGISTER(DistanceFixture, YdbNoSseL2Squared);

#endif

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxL2Squared)(benchmark::State& state) {
  for (auto _ : state) {
    float result = VeloxComputeL2(ldata.data(), rdata.data(), ldata.size());
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

#ifdef YDB_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture, YdbL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = YdbL1Distance(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
DISTANCES_BENCHMARK_REGISTER(DistanceFixture, YdbL1Distance);

BENCHMARK_DEFINE_F(DistanceFixture,
                   YdbNoSseL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = YdbNoSseL1Distance(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
DISTANCES_BENCHMARK_REGISTER(DistanceFixture, YdbNoSseL1Distance);

#endif

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxL1Distance)(benchmark::State& state) {
  for (auto _ : state) {
    float result = VeloxComputeL1(ldata.data(), rdata.data(), ldata.size());
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

#ifdef NUMKONG_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture,
                   NumKongDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result = NumKongComputeDot(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
DISTANCES_BENCHMARK_REGISTER(DistanceFixture, NumKongDotProduct);

#endif

#ifdef YDB_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture, YdbDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result = YdbDotProduct(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}
DISTANCES_BENCHMARK_REGISTER(DistanceFixture, YdbDotProduct);

#endif

#ifdef VELOX_ENABLE_FAISS

BENCHMARK_DEFINE_F(DistanceFixture, VeloxDotProduct)(benchmark::State& state) {
  for (auto _ : state) {
    float result = VeloxComputeDot(ldata.data(), rdata.data(), ldata.size());
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

#ifdef NUMKONG_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture, NumKongCosine)(benchmark::State& state) {
  for (auto _ : state) {
    float result =
      NumKongComputeCosine(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

DISTANCES_BENCHMARK_REGISTER(DistanceFixture, NumKongCosine);
#endif

#ifdef YDB_DISTANCES_BENCH
BENCHMARK_DEFINE_F(DistanceFixture, YdbCosine)(benchmark::State& state) {
  for (auto _ : state) {
    float result = YdbComputeCosine(ldata.data(), rdata.data(), ldata.size());
    benchmark::DoNotOptimize(result);
  }
}

DISTANCES_BENCHMARK_REGISTER(DistanceFixture, YdbCosine);
#endif

#ifdef VELOX_ENABLE_FAISS
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
