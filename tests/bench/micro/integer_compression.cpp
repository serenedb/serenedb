////////////////////////////////////////////////////////////////////////////////
/// Integer Compression Benchmark: simdcomp vs TurboPFor vs streamvbyte
///
/// Compares packing/unpacking performance for:
/// - Normal (non-differential) encoding
/// - Differential encoding (for sorted/monotonic integers)
/// - Block sizes: 128 and 256
////////////////////////////////////////////////////////////////////////////////

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cassert>
#include <cstdint>
#include <cstdio>
#include <random>

// simdcomp headers
extern "C" {
#include <simdbitpacking.h>
#include <simdcomputil.h>
#include <simdintegratedbitpacking.h>
#ifdef __AVX2__
#include <avxbitpacking.h>
#include <avxdeltabitpacking.h>
#endif
}

#if 0

// TurboPFor headers
extern "C" {
#include <bitpack.h>
}

// streamvbyte headers
extern "C" {
#include <streamvbyte.h>
#include <streamvbytedelta.h>
}

#endif

namespace {

struct alignas(32) TestData {
  uint32_t data128[128];
  uint32_t data256[256];

  // Pre-computed bit widths
  uint32_t bits128;
  uint32_t bits256;
  uint32_t delta_bits128;
  uint32_t delta_bits256;

  TestData() {
    std::mt19937 gen(42);
    std::uniform_int_distribution<uint32_t> dist(1, 100);

    // Generate 256 monotonically increasing integers (128 is subset)
    uint32_t val = 0;
    for (size_t i = 0; i < 256; ++i) {
      val += dist(gen);
      data256[i] = val;
      if (i < 128) {
        data128[i] = val;
      }
    }

    // Pre-compute bit widths for simdcomp
    bits128 = maxbits(data128);
    bits256 = maxbits_length(data256, 256);
    delta_bits128 = simdmaxbitsd1(0, data128);
    delta_bits256 = simdmaxbitsd1_length(0, data256, 256);
  }
};

// Global shared test data
const TestData& GetTestData() {
  static const TestData data;
  return data;
}

#ifdef __AVX2__

// Forward declarations for external delta functions (defined later)
static inline void avx_compute_deltas(uint32_t initvalue, const uint32_t* in,
                                      uint32_t* deltas);
static inline void avx_compute_prefix_sums(uint32_t initvalue,
                                           const uint32_t* deltas,
                                           uint32_t* out);
static inline void sse_compute_deltas(uint32_t initvalue, const uint32_t* in,
                                      uint32_t* deltas);
static inline void sse_compute_prefix_sums(uint32_t initvalue,
                                           const uint32_t* deltas,
                                           uint32_t* out);

bool VerifyExternalDeltaFunctions() {
  alignas(32) uint32_t original[256];
  alignas(32) uint32_t deltas[256];
  alignas(32) uint32_t reconstructed[256];

  // Create monotonically increasing data
  std::mt19937 gen(54321);
  std::uniform_int_distribution<uint32_t> dist(1, 100);

  uint32_t val = 0;
  for (int i = 0; i < 256; ++i) {
    val += dist(gen);
    original[i] = val;
  }

  // Test AVX2 external delta functions
  avx_compute_deltas(0, original, deltas);

  // Verify deltas are correct
  if (deltas[0] != original[0]) {
    std::fprintf(stderr, "AVX delta[0] FAILED: expected %u, got %u\n",
                 original[0], deltas[0]);
    return false;
  }
  for (int i = 1; i < 256; ++i) {
    uint32_t expected_delta = original[i] - original[i - 1];
    if (deltas[i] != expected_delta) {
      std::fprintf(stderr, "AVX delta[%d] FAILED: expected %u, got %u\n", i,
                   expected_delta, deltas[i]);
      return false;
    }
  }

  // Test AVX2 prefix sum reconstruction
  avx_compute_prefix_sums(0, deltas, reconstructed);
  for (int i = 0; i < 256; ++i) {
    if (original[i] != reconstructed[i]) {
      std::fprintf(stderr, "AVX prefix_sum[%d] FAILED: expected %u, got %u\n",
                   i, original[i], reconstructed[i]);
      return false;
    }
  }

  std::fprintf(stderr, "AVX external delta/prefix_sum verification PASSED\n");

  // Test SSE external delta functions
  sse_compute_deltas(0, original, deltas);

  // Verify deltas are correct
  if (deltas[0] != original[0]) {
    std::fprintf(stderr, "SSE delta[0] FAILED: expected %u, got %u\n",
                 original[0], deltas[0]);
    return false;
  }
  for (int i = 1; i < 256; ++i) {
    uint32_t expected_delta = original[i] - original[i - 1];
    if (deltas[i] != expected_delta) {
      std::fprintf(stderr, "SSE delta[%d] FAILED: expected %u, got %u\n", i,
                   expected_delta, deltas[i]);
      return false;
    }
  }

  // Test SSE prefix sum reconstruction
  sse_compute_prefix_sums(0, deltas, reconstructed);
  for (int i = 0; i < 256; ++i) {
    if (original[i] != reconstructed[i]) {
      std::fprintf(stderr, "SSE prefix_sum[%d] FAILED: expected %u, got %u\n",
                   i, original[i], reconstructed[i]);
      return false;
    }
  }

  std::fprintf(stderr, "SSE external delta/prefix_sum verification PASSED\n");
  return true;
}
#endif

////////////////////////////////////////////////////////////////////////////////
// simdcomp benchmarks - 128 integers (SSE)
////////////////////////////////////////////////////////////////////////////////

void BM_SimdComp_Pack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[128];

  for (auto _ : state) {
    simdpackwithoutmask(td.data128, compressed, td.bits128);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_SimdComp_Unpack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[128];
  alignas(16) uint32_t decompressed[128];

  simdpackwithoutmask(td.data128, compressed, td.bits128);

  for (auto _ : state) {
    simdunpack(compressed, decompressed, td.bits128);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_SimdComp_DeltaPack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[128];

  for (auto _ : state) {
    simdpackwithoutmaskd1(0, td.data128, compressed, td.delta_bits128);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_SimdComp_DeltaUnpack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[128];
  alignas(16) uint32_t decompressed[128];

  simdpackwithoutmaskd1(0, td.data128, compressed, td.delta_bits128);

  for (auto _ : state) {
    simdunpackd1(0, compressed, decompressed, td.delta_bits128);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// simdcomp benchmarks - 256 integers using 2x128 SSE calls
////////////////////////////////////////////////////////////////////////////////

void BM_SimdComp_Pack256_2x128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[256];

  for (auto _ : state) {
    simdpackwithoutmask(td.data256, compressed, td.bits256);
    simdpackwithoutmask(td.data256 + 128, compressed + td.bits256, td.bits256);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_Unpack256_2x128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[256];
  alignas(16) uint32_t decompressed[256];

  simdpackwithoutmask(td.data256, compressed, td.bits256);
  simdpackwithoutmask(td.data256 + 128, compressed + td.bits256, td.bits256);

  for (auto _ : state) {
    simdunpack(compressed, decompressed, td.bits256);
    simdunpack(compressed + td.bits256, decompressed + 128, td.bits256);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_DeltaPack256_2x128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[256];

  for (auto _ : state) {
    simdpackwithoutmaskd1(0, td.data256, compressed, td.delta_bits256);
    simdpackwithoutmaskd1(td.data256[127], td.data256 + 128,
                          compressed + td.delta_bits256, td.delta_bits256);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_DeltaUnpack256_2x128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(16) __m128i compressed[256];
  alignas(16) uint32_t decompressed[256];

  simdpackwithoutmaskd1(0, td.data256, compressed, td.delta_bits256);
  simdpackwithoutmaskd1(td.data256[127], td.data256 + 128,
                        compressed + td.delta_bits256, td.delta_bits256);

  for (auto _ : state) {
    simdunpackd1(0, compressed, decompressed, td.delta_bits256);
    simdunpackd1(decompressed[127], compressed + td.delta_bits256,
                 decompressed + 128, td.delta_bits256);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

#ifdef __AVX2__
////////////////////////////////////////////////////////////////////////////////
// simdcomp benchmarks - 256 integers (AVX2 native)
////////////////////////////////////////////////////////////////////////////////

void BM_SimdComp_AVX_Pack256(benchmark::State& state) {
  const auto& td = GetTestData();
  uint32_t b = avxmaxbits(td.data256);
  alignas(32) __m256i compressed[256];

  for (auto _ : state) {
    avxpackwithoutmask(td.data256, compressed, b);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_AVX_Unpack256(benchmark::State& state) {
  const auto& td = GetTestData();
  uint32_t b = avxmaxbits(td.data256);
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];

  avxpackwithoutmask(td.data256, compressed, b);

  for (auto _ : state) {
    avxunpack(compressed, decompressed, b);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// simdcomp benchmarks - 256 integers (AVX2 delta/differential)
////////////////////////////////////////////////////////////////////////////////

void BM_SimdComp_AVX_DeltaPack256(benchmark::State& state) {
  const auto& td = GetTestData();
  uint32_t b = avxmaxbitsd1(0, td.data256);
  alignas(32) __m256i compressed[256];

  for (auto _ : state) {
    avxpackwithoutmaskd1(0, td.data256, compressed, b);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_AVX_DeltaUnpack256(benchmark::State& state) {
  const auto& td = GetTestData();
  uint32_t b = avxmaxbitsd1(0, td.data256);
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];

  avxpackwithoutmaskd1(0, td.data256, compressed, b);

  for (auto _ : state) {
    avxunpackd1(0, compressed, decompressed, b);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// simdcomp benchmarks - 256 integers (AVX2 with external delta encoding)
// This separates delta computation from bitpacking to measure overhead
////////////////////////////////////////////////////////////////////////////////

// Compute deltas for 256 integers using AVX2
// delta[0] = in[0] - initvalue
// delta[i] = in[i] - in[i-1] for i > 0
static inline void avx_compute_deltas(uint32_t initvalue, const uint32_t* in,
                                      uint32_t* deltas) {
  __m256i prev = _mm256_set1_epi32(initvalue);
  const __m256i* vin = reinterpret_cast<const __m256i*>(in);
  __m256i* vout = reinterpret_cast<__m256i*>(deltas);

  // Permutation to shift right by 1: [a7,a0,a1,a2,a3,a4,a5,a6]
  const __m256i perm = _mm256_set_epi32(6, 5, 4, 3, 2, 1, 0, 7);

  for (int i = 0; i < 32; ++i) {
    __m256i curr = _mm256_loadu_si256(vin + i);
    // Shift curr right by 1 position
    __m256i shifted = _mm256_permutevar8x32_epi32(curr, perm);
    // Replace position 0 with last element of prev
    shifted = _mm256_insert_epi32(shifted, _mm256_extract_epi32(prev, 7), 0);
    // Compute delta
    __m256i delta = _mm256_sub_epi32(curr, shifted);
    _mm256_storeu_si256(vout + i, delta);
    prev = curr;
  }
}

// Compute prefix sums (absolute values) for 256 delta-encoded integers using
// AVX2
static inline void avx_compute_prefix_sums(uint32_t initvalue,
                                           const uint32_t* deltas,
                                           uint32_t* out) {
  __m256i initOffset = _mm256_set1_epi32(initvalue);
  const __m256i* vin = reinterpret_cast<const __m256i*>(deltas);
  __m256i* vout = reinterpret_cast<__m256i*>(out);
  const __m256i broadcast7 = _mm256_set_epi32(7, 7, 7, 7, 7, 7, 7, 7);

  for (int i = 0; i < 32; ++i) {
    __m256i curr = _mm256_loadu_si256(vin + i);
    // Prefix sum within 256-bit register
    // Step 1: add neighbors
    __m256i t1 = _mm256_add_epi32(curr, _mm256_slli_si256(curr, 4));
    // Step 2: add pairs
    __m256i t2 = _mm256_add_epi32(t1, _mm256_slli_si256(t1, 8));
    // Step 3: add across 128-bit lanes
    __m256i lane_sum = _mm256_shuffle_epi32(t2, 0xFF);
    __m256i lane_carry =
      _mm256_permute2x128_si256(lane_sum, _mm256_setzero_si256(), 0x03);
    __m256i result = _mm256_add_epi32(t2, lane_carry);
    // Add init offset
    result = _mm256_add_epi32(result, initOffset);
    _mm256_storeu_si256(vout + i, result);
    // Update initOffset to broadcast of last element
    initOffset = _mm256_permutevar8x32_epi32(result, broadcast7);
  }
}

void BM_SimdComp_AVX_ExtDeltaPack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t deltas[256];
  alignas(32) __m256i compressed[256];

  // Compute deltas once to get bit width
  avx_compute_deltas(0, td.data256, deltas);
  uint32_t b = avxmaxbits(deltas);

  for (auto _ : state) {
    // Step 1: compute deltas
    avx_compute_deltas(0, td.data256, deltas);
    // Step 2: pack deltas (non-delta pack)
    avxpackwithoutmask(deltas, compressed, b);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_AVX_ExtDeltaUnpack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t deltas[256];
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];

  // Prepare compressed data
  avx_compute_deltas(0, td.data256, deltas);
  uint32_t b = avxmaxbits(deltas);
  avxpackwithoutmask(deltas, compressed, b);

  for (auto _ : state) {
    // Step 1: unpack to deltas (non-delta unpack)
    avxunpack(compressed, deltas, b);
    // Step 2: compute prefix sums to get absolute values
    avx_compute_prefix_sums(0, deltas, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// Hybrid: SSE delta/prefix-sum + AVX2 pack/unpack
// SSE has no cross-lane penalty, might be faster for delta computation
////////////////////////////////////////////////////////////////////////////////

// Compute deltas for 256 integers using SSE (4 integers at a time)
static inline void sse_compute_deltas(uint32_t initvalue, const uint32_t* in,
                                      uint32_t* deltas) {
  __m128i prev = _mm_set1_epi32(initvalue);
  const __m128i* vin = reinterpret_cast<const __m128i*>(in);
  __m128i* vout = reinterpret_cast<__m128i*>(deltas);

  for (int i = 0; i < 64; ++i) {
    __m128i curr = _mm_loadu_si128(vin + i);
    // Shift curr right by 1: [a3,a0,a1,a2] using alignr
    __m128i shifted = _mm_alignr_epi8(curr, prev, 12);
    __m128i delta = _mm_sub_epi32(curr, shifted);
    _mm_storeu_si128(vout + i, delta);
    prev = curr;
  }
}

// Compute prefix sums for 256 integers using SSE (4 integers at a time)
static inline void sse_compute_prefix_sums(uint32_t initvalue,
                                           const uint32_t* deltas,
                                           uint32_t* out) {
  __m128i initOffset = _mm_set1_epi32(initvalue);
  const __m128i* vin = reinterpret_cast<const __m128i*>(deltas);
  __m128i* vout = reinterpret_cast<__m128i*>(out);

  for (int i = 0; i < 64; ++i) {
    __m128i curr = _mm_loadu_si128(vin + i);
    // Prefix sum within 128-bit register
    // Step 1: add neighbors
    __m128i t1 = _mm_add_epi32(curr, _mm_slli_si128(curr, 4));
    // Step 2: add pairs
    __m128i t2 = _mm_add_epi32(t1, _mm_slli_si128(t1, 8));
    // Add init offset
    __m128i result = _mm_add_epi32(t2, initOffset);
    _mm_storeu_si128(vout + i, result);
    // Update initOffset to broadcast of last element
    initOffset = _mm_shuffle_epi32(result, 0xFF);
  }
}

void BM_SimdComp_Hybrid_SSEDelta_AVXPack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t deltas[256];
  alignas(32) __m256i compressed[256];

  // Compute deltas once to get bit width
  sse_compute_deltas(0, td.data256, deltas);
  uint32_t b = avxmaxbits(deltas);

  for (auto _ : state) {
    // Step 1: compute deltas using SSE
    sse_compute_deltas(0, td.data256, deltas);
    // Step 2: pack deltas using AVX2
    avxpackwithoutmask(deltas, compressed, b);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_Hybrid_AVXUnpack_SSEPrefix256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t deltas[256];
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];

  // Prepare compressed data
  sse_compute_deltas(0, td.data256, deltas);
  uint32_t b = avxmaxbits(deltas);
  avxpackwithoutmask(deltas, compressed, b);

  for (auto _ : state) {
    // Step 1: unpack using AVX2
    avxunpack(compressed, deltas, b);
    // Step 2: compute prefix sums using SSE
    sse_compute_prefix_sums(0, deltas, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

// Run external delta verification at startup (after functions are defined)
static bool external_delta_verified = VerifyExternalDeltaFunctions();

////////////////////////////////////////////////////////////////////////////////
// Scalar loops - let compiler auto-vectorize
////////////////////////////////////////////////////////////////////////////////

// Simple scalar delta computation - compiler can auto-vectorize
static inline void scalar_compute_deltas(uint32_t initvalue, const uint32_t* in,
                                         uint32_t* deltas) {
  uint32_t prev = initvalue;
  for (int i = 0; i < 256; ++i) {
    deltas[i] = in[i] - prev;
    prev = in[i];
  }
}

// Simple scalar prefix sum - compiler can auto-vectorize
static inline void scalar_compute_prefix_sums(uint32_t initvalue,
                                              const uint32_t* deltas,
                                              uint32_t* out) {
  uint32_t sum = initvalue;
  for (int i = 0; i < 256; ++i) {
    sum += deltas[i];
    out[i] = sum;
  }
}

void BM_SimdComp_Scalar_DeltaPack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t deltas[256];
  alignas(32) __m256i compressed[256];

  scalar_compute_deltas(0, td.data256, deltas);
  uint32_t b = avxmaxbits(deltas);

  for (auto _ : state) {
    scalar_compute_deltas(0, td.data256, deltas);
    avxpackwithoutmask(deltas, compressed, b);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_Scalar_DeltaUnpack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t deltas[256];
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];

  scalar_compute_deltas(0, td.data256, deltas);
  uint32_t b = avxmaxbits(deltas);
  avxpackwithoutmask(deltas, compressed, b);

  for (auto _ : state) {
    avxunpack(compressed, deltas, b);
    scalar_compute_prefix_sums(0, deltas, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}
#endif

#if 0

// Constexpr version of streamvbyte_max_compressedbytes formula:
// control_bytes = (n + 3) / 4, data_bytes = n * 4
constexpr size_t kStreamVByteMaxBytes128 = ((128 + 3) / 4) + 128 * 4 + 16;
constexpr size_t kStreamVByteMaxBytes256 = ((256 + 3) / 4) + 256 * 4 + 16;

////////////////////////////////////////////////////////////////////////////////
// TurboPFor benchmarks - 128 integers
// Note: TurboPFor requires non-const input buffers, so we copy from shared data
////////////////////////////////////////////////////////////////////////////////

void BM_TurboPFor_Pack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[128];
  std::copy(std::begin(td.data128), std::end(td.data128), input);
  alignas(32) unsigned char compressed[128 * 4 + 64];

  for (auto _ : state) {
    bitnpack32(input, 128, compressed);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_TurboPFor_Unpack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[128];
  std::copy(std::begin(td.data128), std::end(td.data128), input);
  alignas(32) unsigned char compressed[128 * 4 + 64];
  alignas(32) uint32_t decompressed[128];

  bitnpack32(input, 128, compressed);

  for (auto _ : state) {
    bitnunpack32(compressed, 128, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_TurboPFor_DeltaPack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[128];
  std::copy(std::begin(td.data128), std::end(td.data128), input);
  alignas(32) unsigned char compressed[128 * 4 + 64];

  for (auto _ : state) {
    bitnd1pack32(input, 128, compressed);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_TurboPFor_DeltaUnpack128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[128];
  std::copy(std::begin(td.data128), std::end(td.data128), input);
  alignas(32) unsigned char compressed[128 * 4 + 64];
  alignas(32) uint32_t decompressed[128];

  bitnd1pack32(input, 128, compressed);

  for (auto _ : state) {
    bitnd1unpack32(compressed, 128, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// TurboPFor benchmarks - 256 integers
////////////////////////////////////////////////////////////////////////////////

void BM_TurboPFor_Pack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[256];
  std::copy(std::begin(td.data256), std::end(td.data256), input);
  alignas(32) unsigned char compressed[256 * 4 + 64];

  for (auto _ : state) {
    bitnpack32(input, 256, compressed);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_TurboPFor_Unpack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[256];
  std::copy(std::begin(td.data256), std::end(td.data256), input);
  alignas(32) unsigned char compressed[256 * 4 + 64];
  alignas(32) uint32_t decompressed[256];

  bitnpack32(input, 256, compressed);

  for (auto _ : state) {
    bitnunpack32(compressed, 256, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_TurboPFor_DeltaPack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[256];
  std::copy(std::begin(td.data256), std::end(td.data256), input);
  alignas(32) unsigned char compressed[256 * 4 + 64];

  for (auto _ : state) {
    bitnd1pack32(input, 256, compressed);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_TurboPFor_DeltaUnpack256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint32_t input[256];
  std::copy(std::begin(td.data256), std::end(td.data256), input);
  alignas(32) unsigned char compressed[256 * 4 + 64];
  alignas(32) uint32_t decompressed[256];

  bitnd1pack32(input, 256, compressed);

  for (auto _ : state) {
    bitnd1unpack32(compressed, 256, decompressed);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// streamvbyte benchmarks - 128 integers
////////////////////////////////////////////////////////////////////////////////

void BM_StreamVByte_Encode128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes128];

  for (auto _ : state) {
    streamvbyte_encode(td.data128, 128, compressed);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_StreamVByte_Decode128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes128];
  alignas(32) uint32_t decompressed[128];

  streamvbyte_encode(td.data128, 128, compressed);

  for (auto _ : state) {
    streamvbyte_decode(compressed, decompressed, 128);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_StreamVByte_DeltaEncode128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes128];

  for (auto _ : state) {
    streamvbyte_delta_encode(td.data128, 128, compressed, 0);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_StreamVByte_DeltaDecode128(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes128];
  alignas(32) uint32_t decompressed[128];

  streamvbyte_delta_encode(td.data128, 128, compressed, 0);

  for (auto _ : state) {
    streamvbyte_delta_decode(compressed, decompressed, 128, 0);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

////////////////////////////////////////////////////////////////////////////////
// streamvbyte benchmarks - 256 integers
////////////////////////////////////////////////////////////////////////////////

void BM_StreamVByte_Encode256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes256];

  for (auto _ : state) {
    streamvbyte_encode(td.data256, 256, compressed);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_StreamVByte_Decode256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes256];
  alignas(32) uint32_t decompressed[256];

  streamvbyte_encode(td.data256, 256, compressed);

  for (auto _ : state) {
    streamvbyte_decode(compressed, decompressed, 256);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_StreamVByte_DeltaEncode256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes256];

  for (auto _ : state) {
    streamvbyte_delta_encode(td.data256, 256, compressed, 0);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_StreamVByte_DeltaDecode256(benchmark::State& state) {
  const auto& td = GetTestData();
  alignas(32) uint8_t compressed[kStreamVByteMaxBytes256];
  alignas(32) uint32_t decompressed[256];

  streamvbyte_delta_encode(td.data256, 256, compressed, 0);

  for (auto _ : state) {
    streamvbyte_delta_decode(compressed, decompressed, 256, 0);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

// TurboPFor (128)
BENCHMARK(BM_TurboPFor_Pack128);
BENCHMARK(BM_TurboPFor_Unpack128);
BENCHMARK(BM_TurboPFor_DeltaPack128);
BENCHMARK(BM_TurboPFor_DeltaUnpack128);

// TurboPFor (256)
BENCHMARK(BM_TurboPFor_Pack256);
BENCHMARK(BM_TurboPFor_Unpack256);
BENCHMARK(BM_TurboPFor_DeltaPack256);
BENCHMARK(BM_TurboPFor_DeltaUnpack256);

// TurboPFor parameterized by bit width (256)
void BM_TurboPFor_DeltaPack256_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(32) uint32_t data[256];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 256; ++i) {
    data[i] =
      (i == 0) ? (i % max_val) : (data[i - 1] + (i % std::max(1u, max_val)));
  }
  alignas(32) unsigned char compressed[256 * 4 + 64];
  for (auto _ : state) {
    bitd1pack32(data, 256, compressed, 0, bits);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_TurboPFor_DeltaUnpack256_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(32) uint32_t data[256];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 256; ++i) {
    data[i] =
      (i == 0) ? (i % max_val) : (data[i - 1] + (i % std::max(1u, max_val)));
  }
  alignas(32) unsigned char compressed[256 * 4 + 64];
  alignas(32) uint32_t decompressed[256];
  bitd1pack32(data, 256, compressed, 0, bits);
  for (auto _ : state) {
    bitd1unpack32(compressed, 256, decompressed, 0, bits);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

BENCHMARK(BM_TurboPFor_DeltaPack256_Bits)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(24)
  ->Arg(32);
BENCHMARK(BM_TurboPFor_DeltaUnpack256_Bits)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(24)
  ->Arg(32);

// streamvbyte (128)
BENCHMARK(BM_StreamVByte_Encode128);
BENCHMARK(BM_StreamVByte_Decode128);
BENCHMARK(BM_StreamVByte_DeltaEncode128);
BENCHMARK(BM_StreamVByte_DeltaDecode128);

// streamvbyte (256)
BENCHMARK(BM_StreamVByte_Encode256);
BENCHMARK(BM_StreamVByte_Decode256);
BENCHMARK(BM_StreamVByte_DeltaEncode256);
BENCHMARK(BM_StreamVByte_DeltaDecode256);

#endif

// Register benchmarks
// simdcomp SSE (128)
BENCHMARK(BM_SimdComp_Pack128);
BENCHMARK(BM_SimdComp_Unpack128);
BENCHMARK(BM_SimdComp_DeltaPack128);
BENCHMARK(BM_SimdComp_DeltaUnpack128);

// simdcomp SSE 2x128 (256)
BENCHMARK(BM_SimdComp_Pack256_2x128);
BENCHMARK(BM_SimdComp_Unpack256_2x128);
BENCHMARK(BM_SimdComp_DeltaPack256_2x128);
BENCHMARK(BM_SimdComp_DeltaUnpack256_2x128);

#ifdef __AVX2__
// simdcomp AVX2 (256)
BENCHMARK(BM_SimdComp_AVX_Pack256);
BENCHMARK(BM_SimdComp_AVX_Unpack256);
BENCHMARK(BM_SimdComp_AVX_DeltaPack256);
BENCHMARK(BM_SimdComp_AVX_DeltaUnpack256);
// simdcomp AVX2 with external delta (256)
BENCHMARK(BM_SimdComp_AVX_ExtDeltaPack256);
BENCHMARK(BM_SimdComp_AVX_ExtDeltaUnpack256);
// simdcomp hybrid: SSE delta + AVX2 pack (256)
BENCHMARK(BM_SimdComp_Hybrid_SSEDelta_AVXPack256);
BENCHMARK(BM_SimdComp_Hybrid_AVXUnpack_SSEPrefix256);
// simdcomp scalar delta + AVX2 pack (256) - compiler auto-vectorize
BENCHMARK(BM_SimdComp_Scalar_DeltaPack256);
BENCHMARK(BM_SimdComp_Scalar_DeltaUnpack256);

// Parameterized benchmarks for specific bit widths to compare optimized vs
// regular
void BM_SimdComp_AVX_DeltaPack256_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(32) uint32_t data[256];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 256; ++i) {
    data[i] =
      (i == 0) ? (i % max_val) : (data[i - 1] + (i % std::max(1u, max_val)));
  }
  alignas(32) __m256i compressed[256];
  for (auto _ : state) {
    avxpackwithoutmaskd1(0, data, compressed, bits);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_AVX_DeltaUnpack256_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(32) uint32_t data[256];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 256; ++i) {
    data[i] =
      (i == 0) ? (i % max_val) : (data[i - 1] + (i % std::max(1u, max_val)));
  }
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];
  avxpackwithoutmaskd1(0, data, compressed, bits);
  for (auto _ : state) {
    avxunpackd1(0, compressed, decompressed, bits);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

// Test power-of-2 bit widths and neighbors
BENCHMARK(BM_SimdComp_AVX_DeltaPack256_Bits)
  ->Arg(1)
  ->Arg(2)
  ->Arg(4)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(32);
BENCHMARK(BM_SimdComp_AVX_DeltaUnpack256_Bits)
  ->Arg(1)
  ->Arg(2)
  ->Arg(4)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(32);

// AVX2 non-delta pack/unpack parameterized by bit width
void BM_SimdComp_AVX_Pack256_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(32) uint32_t data[256];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 256; ++i) {
    data[i] = i % (max_val + 1);
  }
  alignas(32) __m256i compressed[256];
  for (auto _ : state) {
    avxpackwithoutmask(data, compressed, bits);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

void BM_SimdComp_AVX_Unpack256_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(32) uint32_t data[256];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 256; ++i) {
    data[i] = i % (max_val + 1);
  }
  alignas(32) __m256i compressed[256];
  alignas(32) uint32_t decompressed[256];
  avxpackwithoutmask(data, compressed, bits);
  for (auto _ : state) {
    avxunpack(compressed, decompressed, bits);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 256 * sizeof(uint32_t));
}

BENCHMARK(BM_SimdComp_AVX_Pack256_Bits)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(24)
  ->Arg(32);
BENCHMARK(BM_SimdComp_AVX_Unpack256_Bits)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(24)
  ->Arg(32);
#endif

// SSE 128-bit delta pack/unpack parameterized by bit width
void BM_SimdComp_DeltaPack128_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(16) uint32_t data[128];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 128; ++i) {
    data[i] =
      (i == 0) ? (i % max_val) : (data[i - 1] + (i % std::max(1u, max_val)));
  }
  alignas(16) __m128i compressed[128];
  for (auto _ : state) {
    simdpackwithoutmaskd1(0, data, compressed, bits);
    benchmark::DoNotOptimize(compressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

void BM_SimdComp_DeltaUnpack128_Bits(benchmark::State& state) {
  const int bits = state.range(0);
  alignas(16) uint32_t data[128];
  const uint32_t max_val = (bits == 32) ? 0xFFFFFFFF : ((1u << bits) - 1);
  for (int i = 0; i < 128; ++i) {
    data[i] =
      (i == 0) ? (i % max_val) : (data[i - 1] + (i % std::max(1u, max_val)));
  }
  alignas(16) __m128i compressed[128];
  alignas(16) uint32_t decompressed[128];
  simdpackwithoutmaskd1(0, data, compressed, bits);
  for (auto _ : state) {
    simdunpackd1(0, compressed, decompressed, bits);
    benchmark::DoNotOptimize(decompressed);
  }
  state.SetBytesProcessed(state.iterations() * 128 * sizeof(uint32_t));
}

BENCHMARK(BM_SimdComp_DeltaPack128_Bits)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(24)
  ->Arg(32);
BENCHMARK(BM_SimdComp_DeltaUnpack128_Bits)
  ->Arg(7)
  ->Arg(8)
  ->Arg(9)
  ->Arg(15)
  ->Arg(16)
  ->Arg(17)
  ->Arg(24)
  ->Arg(32);

}  // namespace

BENCHMARK_MAIN();
