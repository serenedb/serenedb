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
#include <cstdint>
#include <iresearch/index/norm.hpp>
#include <iresearch/search/score_function.hpp>
#include <numeric>
#include <random>
#include <vector>

#include "basics/shared.hpp"

#ifdef __AVX2__
#include <immintrin.h>
#endif

namespace {

// Scalar implementation

template<irs::NormEncoding Encoding>
IRS_FORCE_INLINE static uint32_t ScalarRead(
  const irs::byte_type* IRS_RESTRICT origin, irs::doc_id_t index) noexcept {
  if constexpr (Encoding == irs::NormEncoding::Byte) {
    return origin[index];
  } else if constexpr (Encoding == irs::NormEncoding::Short) {
    return absl::little_endian::Load16(origin + index * sizeof(uint16_t));
  } else if constexpr (Encoding == irs::NormEncoding::Int) {
    return absl::little_endian::Load32(origin + index * sizeof(uint32_t));
  } else {
    static_assert(false);
  }
}

template<irs::NormEncoding Encoding>
[[gnu::noinline]] void GetPostingBlockScalar(
  irs::doc_id_t doc_base, const irs::byte_type* IRS_RESTRICT origin,
  const irs::doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values) {
  for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
    values[i] = ScalarRead<Encoding>(origin, docs[i] - doc_base);
  }
}

template<irs::NormEncoding Encoding>
[[gnu::noinline]] void GetPostingBlockPragma(
  irs::doc_id_t doc_base, const irs::byte_type* IRS_RESTRICT origin,
  const irs::doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values) {
#pragma clang loop unroll(full)
  for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
    values[i] = ScalarRead<Encoding>(origin, docs[i] - doc_base);
  }
}

// AVX2 gather implementation

#ifdef __AVX2__
template<irs::NormEncoding Encoding>
[[gnu::noinline]] void GetPostingBlockGather(
  irs::doc_id_t doc_base, const irs::byte_type* IRS_RESTRICT origin,
  const irs::doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values) {
  static constexpr uint32_t kMask = [] -> uint32_t {
    if constexpr (Encoding == irs::NormEncoding::Byte) {
      return std::numeric_limits<uint8_t>::max();
    } else if constexpr (Encoding == irs::NormEncoding::Short) {
      return std::numeric_limits<uint16_t>::max();
    } else {
      return std::numeric_limits<uint32_t>::max();
    }
  }();
  const auto base = _mm256_set1_epi32(doc_base);
  const auto mask = _mm256_set1_epi32(kMask);

  static_assert(irs::kPostingBlock % 8 == 0);
  for (irs::scores_size_t i = 0; i != irs::kPostingBlock; i += 8) {
    auto indices =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(docs + i));
    indices = _mm256_sub_epi32(indices, base);
    auto gathered =
      _mm256_i32gather_epi32(reinterpret_cast<const int*>(origin), indices,
                             std::to_underlying(Encoding));
    gathered = _mm256_and_si256(gathered, mask);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(values + i), gathered);
  }
}
#endif

// Dense (contiguous) fast-path + scalar fallback

template<irs::NormEncoding Encoding>
[[gnu::noinline]] void GetPostingBlockHybrid(
  irs::doc_id_t doc_base, const irs::byte_type* IRS_RESTRICT origin,
  const irs::doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values) {
  if (docs[irs::kPostingBlock - 1] - docs[0] == irs::kPostingBlock - 1) {
    // Contiguous: sequential reads, compiler will autovectorize
    const auto first = docs[0] - doc_base;
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      values[i] = ScalarRead<Encoding>(origin, first + i);
    }
  } else {
    // Fallback to scalar gather
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      values[i] = ScalarRead<Encoding>(origin, docs[i] - doc_base);
    }
  }
}

// Hybrid with unrolled sparse fallback only
template<irs::NormEncoding Encoding>
[[gnu::noinline]] void GetPostingBlockHybridUnrollSparse(
  irs::doc_id_t doc_base, const irs::byte_type* IRS_RESTRICT origin,
  const irs::doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values) {
  if (docs[irs::kPostingBlock - 1] - docs[0] == irs::kPostingBlock - 1) {
    const auto first = docs[0] - doc_base;
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      values[i] = ScalarRead<Encoding>(origin, first + i);
    }
  } else {
#pragma clang loop unroll(full)
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      values[i] = ScalarRead<Encoding>(origin, docs[i] - doc_base);
    }
  }
}

// Hybrid with both paths unrolled
template<irs::NormEncoding Encoding>
[[gnu::noinline]] void GetPostingBlockHybridUnrollBoth(
  irs::doc_id_t doc_base, const irs::byte_type* IRS_RESTRICT origin,
  const irs::doc_id_t* IRS_RESTRICT docs, uint32_t* IRS_RESTRICT values) {
  if (docs[irs::kPostingBlock - 1] - docs[0] == irs::kPostingBlock - 1) {
    const auto first = docs[0] - doc_base;
#pragma clang loop unroll(full)
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      values[i] = ScalarRead<Encoding>(origin, first + i);
    }
  } else {
#pragma clang loop unroll(full)
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      values[i] = ScalarRead<Encoding>(origin, docs[i] - doc_base);
    }
  }
}

// Benchmark setup

constexpr irs::doc_id_t kDocBase = 1000;
constexpr uint32_t kMaxIndex = 100000;
// https://lemire.me/blog/2026/03/18/how-many-branches-can-your-cpu-predict
// Enough random choices that the CPU branch predictor can't learn the pattern
constexpr size_t kMixedCount = 60000;

struct BenchData {
  std::vector<irs::byte_type> origin;
  std::vector<irs::doc_id_t> dense_docs;
  std::vector<irs::doc_id_t> sparse_docs;
  // Pre-generated random sequences of pointers to dense or sparse docs
  std::vector<const irs::doc_id_t*> mixed_docs;          // 50/50
  std::vector<const irs::doc_id_t*> mostly_sparse_docs;  // 90% sparse
  mutable ABSL_CACHELINE_ALIGNED uint32_t values[irs::kPostingBlock];

  BenchData() {
    // Fill origin with some data (4 bytes per element max)
    origin.resize((kMaxIndex + irs::kPostingBlock) * 4);
    std::mt19937 rng(42);
    std::generate(origin.begin(), origin.end(), [&] { return rng() & 0xFF; });

    // Dense: contiguous doc ids
    dense_docs.resize(irs::kPostingBlock);
    std::iota(dense_docs.begin(), dense_docs.end(), kDocBase + 100);

    // Sparse: sorted but non-contiguous doc ids with gaps
    sparse_docs.resize(irs::kPostingBlock);
    irs::doc_id_t doc = kDocBase + 100;
    std::uniform_int_distribution<uint32_t> gap(1, 20);
    for (irs::scores_size_t i = 0; i != irs::kPostingBlock; ++i) {
      sparse_docs[i] = doc;
      doc += gap(rng);
    }

    // Random mix of dense/sparse to defeat branch prediction
    mixed_docs.resize(kMixedCount);
    std::uniform_int_distribution<uint32_t> coin(0, 1);
    for (size_t i = 0; i != kMixedCount; ++i) {
      mixed_docs[i] = coin(rng) ? dense_docs.data() : sparse_docs.data();
    }

    // 90% sparse, 10% dense
    mostly_sparse_docs.resize(kMixedCount);
    std::uniform_int_distribution<uint32_t> skewed(0, 9);
    for (size_t i = 0; i != kMixedCount; ++i) {
      mostly_sparse_docs[i] =
        skewed(rng) == 0 ? dense_docs.data() : sparse_docs.data();
    }
  }
};

const BenchData& GetData() {
  static const BenchData kData;
  return kData;
}

// Benchmark functions

#define DEFINE_ONE_BENCH(NAME, FUNC, ENCODING, DOCS)                     \
  void BM_##NAME##_##DOCS##_##ENCODING(benchmark::State& state) {        \
    const auto& d = GetData();                                           \
    for (auto _ : state) {                                               \
      FUNC<irs::NormEncoding::ENCODING>(kDocBase, d.origin.data(),       \
                                        d.DOCS##_docs.data(), d.values); \
      benchmark::DoNotOptimize(d.values);                                \
    }                                                                    \
  }                                                                      \
  BENCHMARK(BM_##NAME##_##DOCS##_##ENCODING);

#define DEFINE_ONE_MIXED_BENCH(NAME, FUNC, ENCODING, DOCS, LABEL)  \
  void BM_##NAME##_##LABEL##_##ENCODING(benchmark::State& state) { \
    const auto& d = GetData();                                     \
    size_t idx = 0;                                                \
    for (auto _ : state) {                                         \
      FUNC<irs::NormEncoding::ENCODING>(kDocBase, d.origin.data(), \
                                        d.DOCS[idx++], d.values);  \
      benchmark::DoNotOptimize(d.values);                          \
      if (idx == kMixedCount) {                                    \
        idx = 0;                                                   \
      }                                                            \
    }                                                              \
  }                                                                \
  BENCHMARK(BM_##NAME##_##LABEL##_##ENCODING);

#define DEFINE_BENCH(NAME, FUNC)                                              \
  DEFINE_ONE_BENCH(NAME, FUNC, Byte, dense)                                   \
  DEFINE_ONE_BENCH(NAME, FUNC, Byte, sparse)                                  \
  DEFINE_ONE_MIXED_BENCH(NAME, FUNC, Byte, mixed_docs, Mixed)                 \
  DEFINE_ONE_MIXED_BENCH(NAME, FUNC, Byte, mostly_sparse_docs, MostlySparse)  \
  DEFINE_ONE_BENCH(NAME, FUNC, Short, dense)                                  \
  DEFINE_ONE_BENCH(NAME, FUNC, Short, sparse)                                 \
  DEFINE_ONE_MIXED_BENCH(NAME, FUNC, Short, mixed_docs, Mixed)                \
  DEFINE_ONE_MIXED_BENCH(NAME, FUNC, Short, mostly_sparse_docs, MostlySparse) \
  DEFINE_ONE_BENCH(NAME, FUNC, Int, dense)                                    \
  DEFINE_ONE_BENCH(NAME, FUNC, Int, sparse)                                   \
  DEFINE_ONE_MIXED_BENCH(NAME, FUNC, Int, mixed_docs, Mixed)                  \
  DEFINE_ONE_MIXED_BENCH(NAME, FUNC, Int, mostly_sparse_docs, MostlySparse)

DEFINE_BENCH(Scalar, GetPostingBlockScalar)
DEFINE_BENCH(Pragma, GetPostingBlockPragma)
DEFINE_BENCH(Hybrid, GetPostingBlockHybrid)
DEFINE_BENCH(HybridUS, GetPostingBlockHybridUnrollSparse)
DEFINE_BENCH(HybridUB, GetPostingBlockHybridUnrollBoth)
#ifdef __AVX2__
DEFINE_BENCH(Gather, GetPostingBlockGather)
#endif

}  // namespace

BENCHMARK_MAIN();
