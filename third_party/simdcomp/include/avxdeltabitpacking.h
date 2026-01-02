/**
 * This code is released under a BSD License.
 * AVX2 delta (differential) bitpacking - equivalent of
 * simdintegratedbitpacking.h
 */

#ifndef INCLUDE_AVXINTEGRATEDBITPACKING_H_
#define INCLUDE_AVXINTEGRATEDBITPACKING_H_

#ifdef __AVX2__

#include <immintrin.h>

#include "portability.h"

#ifdef __cplusplus
extern "C" {
#endif

enum { AVXDeltaBlockSize = 256 };

inline __m256i avx_prefix_sum(__m256i curr, __m256i prev) {
  __m256i t = _mm256_slli_si256(curr, 4);
  curr = _mm256_add_epi32(curr, t);
  t = _mm256_slli_si256(curr, 8);
  curr = _mm256_add_epi32(curr, t);
  __m128i low = _mm256_castsi256_si128(curr);
  __m128i low_sum = _mm_shuffle_epi32(low, 0xFF);
  __m256i cross = _mm256_set_m128i(low_sum, _mm_setzero_si128());
  curr = _mm256_add_epi32(curr, cross);
  const __m256i carry = _mm256_set1_epi32(_mm256_extract_epi32(prev, 7));

  return _mm256_add_epi32(curr, carry);
}

inline __m256i avx_delta(__m256i curr, __m256i prev) {
  __m256i shifted = _mm256_slli_si256(curr, 4);
  const __m256i elem3 = _mm256_shuffle_epi32(curr, _MM_SHUFFLE(3, 3, 3, 3));
  const __m256i cross = _mm256_permute2x128_si256(elem3, elem3, 0x08);
  shifted = _mm256_blend_epi32(shifted, cross, 0x10);
  const __m256i prev_bcast = _mm256_set1_epi32(_mm256_extract_epi32(prev, 7));
  shifted = _mm256_blend_epi32(shifted, prev_bcast, 0x01);

  return _mm256_sub_epi32(curr, shifted);
}

/* Compute max bits needed for delta-encoded block of 256 integers */
uint32_t avxmaxbitsd1(uint32_t initvalue, const uint32_t* in);

/* reads 256 values from "in", writes "bit" 256-bit vectors to "out"
   using differential coding. Values assumed to fit in bit bits after delta. */
void avxpackwithoutmaskd1(uint32_t initvalue, const uint32_t* in, __m256i* out,
                          uint32_t bit);

/* reads "bit" 256-bit vectors from "in", writes 256 values to "out"
   using differential decoding starting from initvalue */
void avxunpackd1(uint32_t initvalue, const __m256i* in, uint32_t* out,
                 uint32_t bit);

#ifdef __cplusplus
}
#endif

#endif /* __AVX2__ */

#endif /* INCLUDE_AVXINTEGRATEDBITPACKING_H_ */
