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

#include <faiss/utils/distances.h>
#include <faiss/utils/fp16.h>
#include <immintrin.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <random>
#include <thread>
#include <vector>

#include "iresearch/formats/ivf/clustering.hpp"

extern "C" int sgemv_(const char* trans, const int* m, const int* n,
                      const float* alpha, const float* a, const int* lda,
                      const float* x, const int* incx, const float* beta,
                      float* y, const int* incy);

namespace {

using Fn = void (*)(const float*, const float*, float*, uint32_t);

[[maybe_unused]] void Duck(const float* a, const float* q, float* out,
                           uint32_t d) {
  irs::RotateVector(a, q, out, d);
}

void FaissRow(const float* a, const float* q, float* out, uint32_t d) {
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = faiss::fvec_inner_product(a + size_t{i} * d, q, d);
  }
}

[[maybe_unused]] void FaissBatch4(const float* a, const float* q, float* out,
                                  uint32_t d) {
  uint32_t i = 0;
  for (; i + 4 <= d; i += 4) {
    faiss::fvec_inner_product_batch_4(
      q, a + size_t{i} * d, a + size_t{i + 1} * d, a + size_t{i + 2} * d,
      a + size_t{i + 3} * d, d, out[i], out[i + 1], out[i + 2], out[i + 3]);
  }
  for (; i < d; ++i) {
    out[i] = faiss::fvec_inner_product(a + size_t{i} * d, q, d);
  }
}

[[maybe_unused]] void Blas(const float* a, const float* q, float* out,
                           uint32_t d) {
  const int n = static_cast<int>(d);
  const int one = 1;
  const float alpha = 1.f;
  const float beta = 0.f;
  sgemv_("T", &n, &n, &alpha, a, &n, q, &one, &beta, out, &one);
}

std::vector<uint16_t> gHalf;

float HalfDot2(const uint16_t* row, const float* q, uint32_t d) {
  __m256 acc0 = _mm256_setzero_ps();
  __m256 acc1 = _mm256_setzero_ps();
  uint32_t i = 0;
  for (; i + 16 <= d; i += 16) {
    acc0 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm_loadu_si128(
                             reinterpret_cast<const __m128i*>(row + i))),
                           _mm256_loadu_ps(q + i), acc0);
    acc1 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm_loadu_si128(
                             reinterpret_cast<const __m128i*>(row + i + 8))),
                           _mm256_loadu_ps(q + i + 8), acc1);
  }
  const __m256 v = _mm256_add_ps(acc0, acc1);
  __m128 h = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
  h = _mm_add_ps(h, _mm_movehl_ps(h, h));
  h = _mm_add_ss(h, _mm_movehdup_ps(h));
  return _mm_cvtss_f32(h);
}

[[maybe_unused]] float HalfDot4(const uint16_t* row, const float* q,
                                uint32_t d) {
  __m256 acc0 = _mm256_setzero_ps();
  __m256 acc1 = _mm256_setzero_ps();
  __m256 acc2 = _mm256_setzero_ps();
  __m256 acc3 = _mm256_setzero_ps();
  uint32_t i = 0;
  for (; i + 32 <= d; i += 32) {
    const __m256i h01 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row + i));
    const __m256i h23 =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row + i + 16));
    acc0 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm256_castsi256_si128(h01)),
                           _mm256_loadu_ps(q + i), acc0);
    acc1 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm256_extracti128_si256(h01, 1)),
                           _mm256_loadu_ps(q + i + 8), acc1);
    acc2 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm256_castsi256_si128(h23)),
                           _mm256_loadu_ps(q + i + 16), acc2);
    acc3 = _mm256_fmadd_ps(_mm256_cvtph_ps(_mm256_extracti128_si256(h23, 1)),
                           _mm256_loadu_ps(q + i + 24), acc3);
  }
  const __m256 v =
    _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
  __m128 h = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
  h = _mm_add_ps(h, _mm_movehl_ps(h, h));
  h = _mm_add_ss(h, _mm_movehdup_ps(h));
  return _mm_cvtss_f32(h);
}

__attribute__((target("avx512f,avx512bw,avx512vl"))) float HalfDot512(
  const uint16_t* row, const float* q, uint32_t d) {
  __m512 acc0 = _mm512_setzero_ps();
  __m512 acc1 = _mm512_setzero_ps();
  __m512 acc2 = _mm512_setzero_ps();
  __m512 acc3 = _mm512_setzero_ps();
  uint32_t i = 0;
  for (; i + 64 <= d; i += 64) {
    acc0 = _mm512_fmadd_ps(_mm512_cvtph_ps(_mm256_loadu_si256(
                             reinterpret_cast<const __m256i*>(row + i))),
                           _mm512_loadu_ps(q + i), acc0);
    acc1 = _mm512_fmadd_ps(_mm512_cvtph_ps(_mm256_loadu_si256(
                             reinterpret_cast<const __m256i*>(row + i + 16))),
                           _mm512_loadu_ps(q + i + 16), acc1);
    acc2 = _mm512_fmadd_ps(_mm512_cvtph_ps(_mm256_loadu_si256(
                             reinterpret_cast<const __m256i*>(row + i + 32))),
                           _mm512_loadu_ps(q + i + 32), acc2);
    acc3 = _mm512_fmadd_ps(_mm512_cvtph_ps(_mm256_loadu_si256(
                             reinterpret_cast<const __m256i*>(row + i + 48))),
                           _mm512_loadu_ps(q + i + 48), acc3);
  }
  return _mm512_reduce_add_ps(
    _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3)));
}

template<float (*Dot)(const uint16_t*, const float*, uint32_t)>
void HalfRotate(const float*, const float* q, float* out, uint32_t d) {
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = Dot(gHalf.data() + size_t{i} * d, q, d);
  }
}

std::vector<int8_t> gByte;
std::vector<float> gScale;

__attribute__((target("avx512f,avx512bw,avx512vl"))) __m512
Widen512(const int8_t* p) {
  return _mm512_cvtepi32_ps(
    _mm512_cvtepi8_epi32(_mm_loadu_si128(reinterpret_cast<const __m128i*>(p))));
}

__attribute__((target("avx512f,avx512bw,avx512vl"))) float ByteDot512(
  const int8_t* row, const float* q, uint32_t d) {
  __m512 acc0 = _mm512_setzero_ps();
  __m512 acc1 = _mm512_setzero_ps();
  __m512 acc2 = _mm512_setzero_ps();
  __m512 acc3 = _mm512_setzero_ps();
  uint32_t i = 0;
  for (; i + 64 <= d; i += 64) {
    acc0 = _mm512_fmadd_ps(Widen512(row + i), _mm512_loadu_ps(q + i), acc0);
    acc1 = _mm512_fmadd_ps(Widen512(row + i + 16), _mm512_loadu_ps(q + i + 16),
                           acc1);
    acc2 = _mm512_fmadd_ps(Widen512(row + i + 32), _mm512_loadu_ps(q + i + 32),
                           acc2);
    acc3 = _mm512_fmadd_ps(Widen512(row + i + 48), _mm512_loadu_ps(q + i + 48),
                           acc3);
  }
  return _mm512_reduce_add_ps(
    _mm512_add_ps(_mm512_add_ps(acc0, acc1), _mm512_add_ps(acc2, acc3)));
}

__attribute__((target("avx512f,avx512bw,avx512vl,avx512vnni"))) float
ByteDotVnni(const int8_t* row, const int16_t* q, uint32_t d) {
  __m512i acc0 = _mm512_setzero_si512();
  __m512i acc1 = _mm512_setzero_si512();
  uint32_t i = 0;
  for (; i + 64 <= d; i += 64) {
    const __m512i r0 = _mm512_cvtepi8_epi16(
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row + i)));
    const __m512i r1 = _mm512_cvtepi8_epi16(
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(row + i + 32)));
    acc0 = _mm512_dpwssd_epi32(acc0, r0, _mm512_loadu_si512(q + i));
    acc1 = _mm512_dpwssd_epi32(acc1, r1, _mm512_loadu_si512(q + i + 32));
  }
  return static_cast<float>(
    _mm512_reduce_add_epi32(_mm512_add_epi32(acc0, acc1)));
}

float ByteDot256(const int8_t* row, const float* q, uint32_t d) {
  const auto widen = [](const int8_t* p) {
    return _mm256_cvtepi32_ps(_mm256_cvtepi8_epi32(
      _mm_loadl_epi64(reinterpret_cast<const __m128i*>(p))));
  };
  __m256 acc0 = _mm256_setzero_ps();
  __m256 acc1 = _mm256_setzero_ps();
  __m256 acc2 = _mm256_setzero_ps();
  __m256 acc3 = _mm256_setzero_ps();
  uint32_t i = 0;
  for (; i + 32 <= d; i += 32) {
    acc0 = _mm256_fmadd_ps(widen(row + i), _mm256_loadu_ps(q + i), acc0);
    acc1 =
      _mm256_fmadd_ps(widen(row + i + 8), _mm256_loadu_ps(q + i + 8), acc1);
    acc2 =
      _mm256_fmadd_ps(widen(row + i + 16), _mm256_loadu_ps(q + i + 16), acc2);
    acc3 =
      _mm256_fmadd_ps(widen(row + i + 24), _mm256_loadu_ps(q + i + 24), acc3);
  }
  const __m256 v =
    _mm256_add_ps(_mm256_add_ps(acc0, acc1), _mm256_add_ps(acc2, acc3));
  __m128 h = _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
  h = _mm_add_ps(h, _mm_movehl_ps(h, h));
  h = _mm_add_ss(h, _mm_movehdup_ps(h));
  return _mm_cvtss_f32(h);
}

__attribute__((target("avxvnni"))) float ByteDotVnni256(const int8_t* row,
                                                        const int16_t* q,
                                                        uint32_t d) {
  __m256i acc0 = _mm256_setzero_si256();
  __m256i acc1 = _mm256_setzero_si256();
  uint32_t i = 0;
  for (; i + 32 <= d; i += 32) {
    const __m256i r0 = _mm256_cvtepi8_epi16(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + i)));
    const __m256i r1 = _mm256_cvtepi8_epi16(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + i + 16)));
    acc0 = _mm256_dpwssd_avx_epi32(
      acc0, r0, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i)));
    acc1 = _mm256_dpwssd_avx_epi32(
      acc1, r1,
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i + 16)));
  }
  const __m256i s = _mm256_add_epi32(acc0, acc1);
  __m128i h =
    _mm_add_epi32(_mm256_castsi256_si128(s), _mm256_extracti128_si256(s, 1));
  h = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0x4E));
  h = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0xB1));
  return static_cast<float>(_mm_cvtsi128_si32(h));
}

float ByteDotMadd256(const int8_t* row, const int16_t* q, uint32_t d) {
  __m256i acc0 = _mm256_setzero_si256();
  __m256i acc1 = _mm256_setzero_si256();
  uint32_t i = 0;
  for (; i + 32 <= d; i += 32) {
    const __m256i r0 = _mm256_cvtepi8_epi16(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + i)));
    const __m256i r1 = _mm256_cvtepi8_epi16(
      _mm_loadu_si128(reinterpret_cast<const __m128i*>(row + i + 16)));
    acc0 = _mm256_add_epi32(
      acc0, _mm256_madd_epi16(
              r0, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i))));
    acc1 = _mm256_add_epi32(
      acc1,
      _mm256_madd_epi16(
        r1, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i + 16))));
  }
  const __m256i s = _mm256_add_epi32(acc0, acc1);
  __m128i h =
    _mm_add_epi32(_mm256_castsi256_si128(s), _mm256_extracti128_si256(s, 1));
  h = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0x4E));
  h = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0xB1));
  return static_cast<float>(_mm_cvtsi128_si32(h));
}

thread_local std::vector<int16_t> tQ16m;

void ByteRotateMadd256(const float*, const float* q, float* out, uint32_t d) {
  tQ16m.resize(d);
  float peak = 0;
  for (uint32_t i = 0; i < d; ++i) {
    peak = std::max(peak, std::abs(q[i]));
  }
  const float s = peak > 0 ? 32767.f / peak : 1.f;
  for (uint32_t i = 0; i < d; ++i) {
    tQ16m[i] = static_cast<int16_t>(std::nearbyint(q[i] * s));
  }
  const float inv = 1.f / s;
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = gScale[i] * inv *
             ByteDotMadd256(gByte.data() + size_t{i} * d, tQ16m.data(), d);
  }
}

void ByteRotate256(const float*, const float* q, float* out, uint32_t d) {
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = gScale[i] * ByteDot256(gByte.data() + size_t{i} * d, q, d);
  }
}

thread_local std::vector<int16_t> tQ16;

void ByteRotateVnni256(const float*, const float* q, float* out, uint32_t d) {
  tQ16.resize(d);
  float peak = 0;
  for (uint32_t i = 0; i < d; ++i) {
    peak = std::max(peak, std::abs(q[i]));
  }
  const float s = peak > 0 ? 32767.f / peak : 1.f;
  for (uint32_t i = 0; i < d; ++i) {
    tQ16[i] = static_cast<int16_t>(std::nearbyint(q[i] * s));
  }
  const float inv = 1.f / s;
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = gScale[i] * inv *
             ByteDotVnni256(gByte.data() + size_t{i} * d, tQ16.data(), d);
  }
}

void ByteRotate(const float*, const float* q, float* out, uint32_t d) {
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = gScale[i] * ByteDot512(gByte.data() + size_t{i} * d, q, d);
  }
}

void ByteRotateVnni(const float*, const float* q, float* out, uint32_t d) {
  tQ16.resize(d);
  float peak = 0;
  for (uint32_t i = 0; i < d; ++i) {
    peak = std::max(peak, std::abs(q[i]));
  }
  const float s = peak > 0 ? 32767.f / peak : 1.f;
  for (uint32_t i = 0; i < d; ++i) {
    tQ16[i] = static_cast<int16_t>(std::nearbyint(q[i] * s));
  }
  const float inv = 1.f / s;
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = gScale[i] * inv *
             ByteDotVnni(gByte.data() + size_t{i} * d, tQ16.data(), d);
  }
}

uint32_t gRank = 0;

[[maybe_unused]] void Householder(const float* a, const float* q, float* out,
                                  uint32_t d) {
  std::copy(q, q + d, out);
  const float* tau = a + size_t{d} * d - d;
  for (uint32_t i = 0; i < gRank; ++i) {
    const float* v = a + size_t{i} * d;
    const uint32_t n = d - i - 1;
    float s = out[i] + faiss::fvec_inner_product(v + i + 1, out + i + 1, n);
    s *= tau[i];
    out[i] -= s;
    faiss::fvec_madd(n, out + i + 1, -s, v + i + 1, out + i + 1);
  }
}

double Run(Fn fn, const std::vector<float>& a, uint32_t d, size_t threads,
           size_t reps) {
  std::atomic<size_t> go{0};
  std::vector<std::thread> pool;
  std::vector<double> ns(threads);
  for (size_t t = 0; t < threads; ++t) {
    pool.emplace_back([&, t] {
      std::vector<float> q(d, 1.f / static_cast<float>(t + 1));
      std::vector<float> out(d);
      ++go;
      while (go.load() < threads) {
      }
      const auto t0 = std::chrono::steady_clock::now();
      for (size_t r = 0; r < reps; ++r) {
        q[r % d] += 1e-3f;
        fn(a.data(), q.data(), out.data(), d);
      }
      ns[t] = std::chrono::duration<double, std::nano>(
                std::chrono::steady_clock::now() - t0)
                .count() /
              static_cast<double>(reps);
    });
  }
  for (auto& th : pool) {
    th.join();
  }
  double sum = 0;
  for (double v : ns) {
    sum += v;
  }
  return sum / static_cast<double>(threads);
}

int Main(int argc, char** argv) {
  const uint32_t d =
    argc > 1 ? static_cast<uint32_t>(std::atoi(argv[1])) : 3072;
  if (argc > 2) {
    const size_t n = static_cast<size_t>(std::atoi(argv[2]));
    std::vector<float> x(n * d);
    std::mt19937 g{3};
    std::normal_distribution<float> nd{0.f, 1.f};
    for (size_t i = 0; i < n; ++i) {
      for (uint32_t j = 0; j < d; ++j) {
        x[i * d + j] = nd(g) / (1.f + static_cast<float>(j) / 64.f);
      }
    }
    const auto t0 = std::chrono::steady_clock::now();
    auto pca = irs::TrainPcaRotation(x.data(), n, d);
    const auto t1 = std::chrono::steady_clock::now();
    std::printf("pca d=%u n=%zu %.2f s (A %zu)\n", d, n,
                std::chrono::duration<double>(t1 - t0).count(), pca.A.size());
    return 0;
  }
  std::vector<float> a(size_t{d} * d);
  std::mt19937 rng{1};
  std::uniform_real_distribution<float> u{-1.f, 1.f};
  for (auto& v : a) {
    v = u(rng) * 0.01f;
  }
  gHalf.resize(a.size());
  for (size_t i = 0; i < a.size(); ++i) {
    gHalf[i] = faiss::encode_fp16(a[i]);
  }
  gByte.resize(a.size());
  gScale.resize(d);
  for (uint32_t r = 0; r < d; ++r) {
    float peak = 0;
    for (uint32_t c = 0; c < d; ++c) {
      peak = std::max(peak, std::abs(a[size_t{r} * d + c]));
    }
    gScale[r] = peak / 127.f;
    for (uint32_t c = 0; c < d; ++c) {
      gByte[size_t{r} * d + c] =
        static_cast<int8_t>(std::nearbyint(a[size_t{r} * d + c] / gScale[r]));
    }
  }
  const std::pair<const char*, Fn> fns[] = {{"faiss_row", FaissRow},
                                            {"half2", HalfRotate<HalfDot2>},
                                            {"half512", HalfRotate<HalfDot512>},
                                            {"byte512", ByteRotate},
                                            {"byte256", ByteRotate256},
                                            {"bytevnni", ByteRotateVnni},
                                            {"vnni256", ByteRotateVnni256},
                                            {"madd256", ByteRotateMadd256}};
  for (const size_t threads : {size_t{1}, size_t{8}, size_t{32}, size_t{96}}) {
    for (const auto& [name, fn] : fns) {
      Run(fn, a, d, threads, 5);
      const double ns = Run(fn, a, d, threads, 40);
      std::printf("d=%u threads=%zu %-10s %8.1f us/rotation\n", d, threads,
                  name, ns / 1000.0);
    }
  }
  return 0;
}

}  // namespace

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
