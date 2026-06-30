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

#include "split_by_non_alpha.hpp"

#include <absl/strings/ascii.h>

#include <cstdint>

#if defined(__AVX2__)
#include <immintrin.h>
#endif

namespace irs::analysis {
namespace {

#if defined(__AVX2__)
uint32_t AlnumMask(__m256i v) {
  constexpr char kCaseFoldBit = 'a' - 'A';  // 0x20: maps A-Z to a-z

  const __m256i zero = _mm256_setzero_si256();
  const __m256i digit_lo = _mm256_set1_epi8('0');
  const __m256i digit_hi = _mm256_set1_epi8('9');
  const __m256i alpha_lo = _mm256_set1_epi8('a');
  const __m256i alpha_hi = _mm256_set1_epi8('z');
  const __m256i case_fold = _mm256_set1_epi8(kCaseFoldBit);

  const __m256i folded = _mm256_or_si256(v, case_fold);
  const __m256i is_digit =
    _mm256_and_si256(_mm256_cmpeq_epi8(_mm256_subs_epu8(digit_lo, v), zero),
                     _mm256_cmpeq_epi8(_mm256_subs_epu8(v, digit_hi), zero));
  const __m256i is_alpha = _mm256_and_si256(
    _mm256_cmpeq_epi8(_mm256_subs_epu8(alpha_lo, folded), zero),
    _mm256_cmpeq_epi8(_mm256_subs_epu8(folded, alpha_hi), zero));
  return static_cast<uint32_t>(
    _mm256_movemask_epi8(_mm256_or_si256(is_digit, is_alpha)));
}
#endif

template<bool FindAlnum>
size_t FindBoundary(const char* data, size_t pos, size_t size) noexcept {
#if defined(__AVX2__)
  for (; pos + 32 <= size; pos += 32) {
    const __m256i v =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + pos));
    const uint32_t alnum = AlnumMask(v);
    const uint32_t hit = FindAlnum ? alnum : ~alnum;
    if (hit != 0) {
      return pos + static_cast<size_t>(__builtin_ctz(hit));
    }
  }
#endif
  for (; pos < size; ++pos) {
    if (absl::ascii_isalnum(static_cast<unsigned char>(data[pos])) ==
        FindAlnum) {
      return pos;
    }
  }
  return size;
}

}  // namespace

size_t FindFirstAlnum(const char* data, size_t pos, size_t size) noexcept {
  return FindBoundary<true>(data, pos, size);
}

size_t FindFirstNonAlnum(const char* data, size_t pos, size_t size) noexcept {
  return FindBoundary<false>(data, pos, size);
}

}  // namespace irs::analysis
