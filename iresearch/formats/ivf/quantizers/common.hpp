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

#pragma once

#include <faiss/impl/fast_scan/fast_scan.h>
#include <faiss/utils/AlignedTable.h>
#include <faiss/utils/quantize_lut.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <memory>
#include <span>
#include <vector>

#ifdef __AVX2__
#include <immintrin.h>
#endif

#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs::quantizers {

inline std::span<const byte_type> FloatSpan(
  const std::vector<float>& v) noexcept {
  return {reinterpret_cast<const byte_type*>(v.data()),
          v.size() * sizeof(float)};
}

inline constexpr size_t kFastScanBbs = 32;

inline constexpr size_t kFastScanBits = 4;

inline constexpr size_t kFastScanKsub = size_t{1} << kFastScanBits;

inline size_t FastScanNsq(size_t m) noexcept { return m + (m & 1); }

inline constexpr int64_t kIvfRotationSeed = 0x5a17b17c5eed5eedULL;

inline uint32_t RotatedDim(uint32_t d) noexcept {
  return std::max<uint32_t>(kFastScanBits, std::bit_ceil(d));
}

// In-place Fast Walsh-Hadamard transform; len must be a power of two.
inline void Fwht(float* a, uint32_t len) noexcept {
  for (uint32_t h = 1; h < len; h <<= 1) {
    for (uint32_t i = 0; i < len; i += (h << 1)) {
      for (uint32_t j = i; j < i + h; ++j) {
        const float x = a[j];
        const float y = a[j + h];
        a[j] = x + y;
        a[j + h] = x - y;
      }
    }
  }
}

inline void RotateInto(const float* signs, const float* in, float* out,
                       uint32_t d, uint32_t rotated_d) noexcept {
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = in[i] * signs[i];
  }
  for (uint32_t i = d; i < rotated_d; ++i) {
    out[i] = 0.f;
  }
  Fwht(out, rotated_d);
  const float scale = 1.f / std::sqrt(static_cast<float>(rotated_d));
  for (uint32_t i = 0; i < rotated_d; ++i) {
    out[i] *= scale;
  }
}

template<typename H>
void WritePod(DataOutput& out, const H& h) {
  out.WriteData(reinterpret_cast<const byte_type*>(&h), sizeof(H));
}

template<typename H>
H ReadPodHeader(std::span<const byte_type> in) noexcept {
  H h{};
  if (in.size() >= sizeof(H)) {
    std::memcpy(&h, in.data(), sizeof(H));
  }
  return h;
}

inline constexpr uint32_t kTurboQuantLutChunk = 256;

template<class Codebook, class Reader>
std::unique_ptr<QuantizerReader> MakeReaderT(const Codebook* self) {
  return std::make_unique<Reader>(
    std::static_pointer_cast<const Codebook>(self->shared_from_this()));
}

template<class Stats, class Codebook>
std::shared_ptr<const QuantizerCodebook> MakeCodebookT(
  const Stats* self, std::span<const float> query) {
  return std::make_shared<const Codebook>(
    std::static_pointer_cast<const Stats>(self->shared_from_this()), query);
}

struct FastScanChunk {
  uint32_t m;
  uint32_t nsq;
  uint32_t code_off;
  uint32_t lut_off;
};

inline std::vector<FastScanChunk> MakeFastScanChunks(uint32_t m) {
  std::vector<FastScanChunk> out;
  uint32_t code_off = 0;
  uint32_t lut_off = 0;
  for (uint32_t off = 0; off < m; off += kTurboQuantLutChunk) {
    FastScanChunk c;
    c.m = std::min<uint32_t>(kTurboQuantLutChunk, m - off);
    c.nsq = static_cast<uint32_t>(FastScanNsq(c.m));
    c.code_off = code_off;
    c.lut_off = lut_off;
    code_off += static_cast<uint32_t>(kFastScanBbs) * c.nsq / 2;
    lut_off += c.nsq * static_cast<uint32_t>(kFastScanKsub);
    out.push_back(c);
  }
  return out;
}

/// A distance table quantized and packed for faiss fast scan, together with
/// the affine transform that reads an accumulator back as a float.
///
/// PQ, RaBitQ and TurboQuant differ only in how they fill the float table.
/// From there the steps are the same for all three -- quantize it against its
/// own range, pack it into fast-scan lane order, and undo the scale on
/// whatever the accumulator returns -- so this holds the only copy of them,
/// and so the only copy of the degenerate case below. TurboQuant splits its
/// table into chunks it accumulates in turn, which is why the transform is
/// per chunk; the other two have a single chunk.
class FastScanLut {
 public:
  /// Quantize and pack `table`, laid out as `ksub` entries per sub-quantizer.
  ///
  /// faiss reports a scale `a` with the true value at `code / a + b`. A table
  /// with no range -- every entry equal, which is exactly what an all-zero
  /// query produces, since every inner product with it really is zero -- comes
  /// back as a = 0 with the whole answer already in `b`. Reciprocating that
  /// gives inf, and `0 * inf` is NaN, which loses every `score > threshold`
  /// comparison: the query then returns nothing at all rather than something
  /// merely imprecise. A step of zero is the correct reading of a = 0, and it
  /// leaves `b` as the constant faiss put there.
  void Build(const float* table, std::span<const FastScanChunk> chunks,
             size_t ksub) {
    size_t total = 0;
    for (const auto& c : chunks) {
      total += size_t{c.nsq} * ksub;
    }
    _packed.resize(total);
    _step.resize(chunks.size());
    _bias.resize(chunks.size());
    std::vector<uint8_t> codes;
    size_t moff = 0;
    for (size_t i = 0; i != chunks.size(); ++i) {
      const FastScanChunk& c = chunks[i];
      codes.assign(size_t{c.nsq} * ksub, 0);
      float a = 0.f;
      faiss::quantize_lut::quantize_LUT_and_bias(
        1, c.m, ksub, false, table + moff * ksub, nullptr, codes.data(), c.nsq,
        nullptr, &a, &_bias[i]);
      _step[i] = (std::isfinite(a) && a > 0.f) ? 1.f / a : 0.f;
      faiss::pq4_pack_LUT(1, static_cast<int>(c.nsq), codes.data(),
                          _packed.data() + c.lut_off);
      moff += c.m;
    }
  }

  /// Single-chunk form, for the quantizers that do not split their table.
  void Build(const float* table, size_t m, size_t ksub) {
    const FastScanChunk c{.m = static_cast<uint32_t>(m),
                          .nsq = static_cast<uint32_t>(FastScanNsq(m)),
                          .code_off = 0,
                          .lut_off = 0};
    Build(table, {&c, 1}, ksub);
  }

  const uint8_t* Packed() const noexcept { return _packed.data(); }

  /// Read chunk `c`'s accumulated code back as the float it stood for.
  float Decode(size_t c, uint16_t accu) const noexcept {
    return static_cast<float>(accu) * _step[c] + _bias[c];
  }
  float Decode(uint16_t accu) const noexcept { return Decode(0, accu); }

  /// The distance one code step stands for, zero when the table had no range.
  /// Half a step is the worst rounding error the quantization can introduce.
  float Step(size_t c) const noexcept { return _step[c]; }

 private:
  faiss::AlignedTable<uint8_t> _packed;
  std::vector<float> _step;
  std::vector<float> _bias;
};

inline constexpr size_t kGatherAhead = 4;
inline constexpr uintptr_t kCacheLine = 64;
inline constexpr size_t kGatherLines = 36;

template<typename Score>
void ScoreGatheredRows(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t* out,
                       Score&& score) {
  const size_t n = ids.size();
  const size_t ahead = std::clamp<size_t>(
    kGatherLines * kCacheLine / std::max<size_t>(record_size, 1), 1,
    kGatherAhead);
  const auto row = [&](size_t i) {
    return base + static_cast<size_t>(ids[i]) * record_size;
  };
  for (size_t i = 0; i < n; ++i) {
    __builtin_prefetch(row(i), 0, 3);
    __builtin_prefetch(row(i) + record_size - 1, 0, 3);
  }
  const auto rest = [&](size_t i) {
    const auto begin = reinterpret_cast<uintptr_t>(row(i));
    const auto last = (begin + record_size - 1) & ~(kCacheLine - 1);
    for (auto p = (begin | (kCacheLine - 1)) + 1; p < last; p += kCacheLine) {
      __builtin_prefetch(reinterpret_cast<const void*>(p), 0, 3);
    }
  };
  for (size_t i = 0; i < std::min(n, ahead); ++i) {
    rest(i);
  }
  for (size_t i = 0; i < n; ++i) {
    if (i + ahead < n) {
      rest(i + ahead);
    }
    out[i] = score(row(i));
  }
}

#ifdef __AVX2__
inline int32_t BitPlaneSum(const uint8_t* bits, uint32_t bytes,
                           const int8_t* q) noexcept {
  const __m256i replicate =
    _mm256_setr_epi8(0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2,
                     2, 2, 2, 2, 3, 3, 3, 3, 3, 3, 3, 3);
  const __m256i select =
    _mm256_set1_epi64x(static_cast<int64_t>(0x8040201008040201ULL));
  const __m256i one8 = _mm256_set1_epi8(1);
  const __m256i ones = _mm256_set1_epi16(1);
  const __m256i four = _mm256_set1_epi8(4);
  const std::array<__m256i, 4> spread{
    replicate, _mm256_add_epi8(replicate, four),
    _mm256_add_epi8(replicate, _mm256_add_epi8(four, four)),
    _mm256_add_epi8(replicate,
                    _mm256_add_epi8(four, _mm256_add_epi8(four, four)))};
  __m256i acc = _mm256_setzero_si256();
  uint32_t p = 0;
  const uint32_t whole = bytes & ~15U;
  while (p < whole) {
    const uint32_t end = std::min(whole, p + 256);
    __m256i acc16 = _mm256_setzero_si256();
    for (; p < end; p += 16) {
      const __m256i v = _mm256_broadcastsi128_si256(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(bits + p)));
      for (size_t g = 0; g < spread.size(); ++g) {
        const __m256i set = _mm256_min_epu8(
          _mm256_and_si256(_mm256_shuffle_epi8(v, spread[g]), select), one8);
        const __m256i qv = _mm256_loadu_si256(
          reinterpret_cast<const __m256i*>(q + (size_t{p} + 4 * g) * 8));
        acc16 = _mm256_add_epi16(acc16, _mm256_maddubs_epi16(set, qv));
      }
    }
    acc = _mm256_add_epi32(acc, _mm256_madd_epi16(acc16, ones));
  }
  for (; p + 4 <= bytes; p += 4) {
    uint32_t word = 0;
    std::memcpy(&word, bits + p, sizeof(word));
    const __m256i set = _mm256_min_epu8(
      _mm256_and_si256(_mm256_shuffle_epi8(
                         _mm256_set1_epi32(static_cast<int>(word)), replicate),
                       select),
      one8);
    const __m256i qv =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + size_t{p} * 8));
    acc = _mm256_add_epi32(
      acc, _mm256_madd_epi16(_mm256_maddubs_epi16(set, qv), ones));
  }
  const __m128i h = _mm_add_epi32(_mm256_castsi256_si128(acc),
                                  _mm256_extracti128_si256(acc, 1));
  const __m128i h2 = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0x4E));
  int32_t sum =
    _mm_cvtsi128_si32(_mm_add_epi32(h2, _mm_shuffle_epi32(h2, 0xB1)));
  for (; p < bytes; ++p) {
    for (uint32_t t = 0; t < 8; ++t) {
      if (((bits[p] >> t) & 1U) != 0) {
        sum += q[size_t{p} * 8 + t];
      }
    }
  }
  return sum;
}
#endif

#ifndef __AVX2__
inline int32_t BitPlaneSum(const uint8_t* bits, uint32_t bytes,
                           const int8_t* q) noexcept {
  int32_t sum = 0;
  for (uint32_t p = 0; p < bytes; ++p) {
    for (uint32_t t = 0; t < 8; ++t) {
      if (((bits[p] >> t) & 1U) != 0) {
        sum += q[size_t{p} * 8 + t];
      }
    }
  }
  return sum;
}
#endif

/// Transpose `count` row-major nibble codes into one fast-scan block.
///
/// A row-major record stores its nibbles sequentially; the fast-scan kernels
/// want them interleaved across 32 lanes. Both row-major quantizers pay this
/// transpose to reach the same kernel their grouped layout uses directly, so
/// the scoring below has one shape rather than two.
///
/// `row(k)` returns record k, `code_off` is where its codes start within it,
/// and lanes past `count` are packed as zero.
template<typename Row>
void PackFastScanRows(Row row, size_t count, size_t code_off, size_t nsq,
                      uint8_t* blocks) {
  static_assert(std::endian::native == std::endian::little);
  SDB_ASSERT(count <= kFastScanBbs);
  static constexpr uint8_t kPerm[16] = {0, 8,  1, 9,  2, 10, 3, 11,
                                        4, 12, 5, 13, 6, 14, 7, 15};
  std::array<uint8_t, kFastScanBbs> c;
  std::fill(c.begin() + count, c.end(), uint8_t{0});
  uint8_t* dst = blocks;
  for (size_t sq = 0; sq < nsq; sq += 2) {
    for (size_t k = 0; k < count; ++k) {
      c[k] = row(k)[code_off + sq / 2];
    }
    for (size_t j = 0; j < 16; ++j) {
      const uint8_t lo = c[kPerm[j]];
      const uint8_t hi = c[kPerm[j] + 16];
      dst[j] = static_cast<uint8_t>((lo & 15) | ((hi & 15) << 4));
      dst[j + 16] = static_cast<uint8_t>((lo >> 4) | ((hi >> 4) << 4));
    }
    dst += kFastScanBbs;
  }
}

inline void SetNibble(uint8_t* code, uint32_t sq, uint8_t nib) noexcept {
  uint8_t& dst = code[sq >> 1];
  dst = static_cast<uint8_t>((sq & 1) != 0 ? dst | (nib << 4) : dst | nib);
}

inline uint8_t GetNibble(const uint8_t* code, uint32_t sq) noexcept {
  const uint8_t byte = code[sq >> 1];
  return static_cast<uint8_t>((sq & 1) != 0 ? byte >> 4 : byte & 0x0F);
}

inline void RotateBack(const float* signs, const float* in, float* out,
                       uint32_t d, uint32_t rotated_d,
                       std::vector<float>& scratch) {
  scratch.assign(in, in + rotated_d);
  Fwht(scratch.data(), rotated_d);
  const float scale = 1.f / std::sqrt(static_cast<float>(rotated_d));
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = scratch[i] * scale * signs[i];
  }
}

inline bool BqBit(const uint8_t* bits, uint32_t i) noexcept {
  return ((bits[i / 8] >> (i % 8)) & 1U) != 0;
}

template<template<VectorMetric> class Writer, typename... Args>
std::unique_ptr<QuantizerWriter> MakeWriterWithMetric(VectorMetric metric,
                                                      Args&&... args) {
  switch (EffectiveQuantMetric(metric)) {
    case VectorMetric::L2Sqr:
      return std::make_unique<Writer<VectorMetric::L2Sqr>>(
        std::forward<Args>(args)...);
    case VectorMetric::InnerProduct:
      return std::make_unique<Writer<VectorMetric::InnerProduct>>(
        std::forward<Args>(args)...);
    default:
      SDB_ASSERT(false);
      return nullptr;
  }
}

template<template<VectorMetric> class Stats, typename... Args>
std::shared_ptr<const QuantizerStats> MakeStatsWithMetric(VectorMetric metric,
                                                          Args&&... args) {
  switch (EffectiveQuantMetric(metric)) {
    case VectorMetric::L2Sqr:
      return std::make_shared<const Stats<VectorMetric::L2Sqr>>(
        std::forward<Args>(args)...);
    case VectorMetric::InnerProduct:
      return std::make_shared<const Stats<VectorMetric::InnerProduct>>(
        std::forward<Args>(args)...);
    default:
      SDB_ASSERT(false);
      return nullptr;
  }
}

std::unique_ptr<QuantizerWriter> MakePanoramaWriter(uint32_t d,
                                                    VectorMetric metric);
std::shared_ptr<const QuantizerStats> MakePanoramaStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> blob);

std::unique_ptr<QuantizerWriter> MakeScalarWriter(uint32_t d,
                                                  VectorQuantization quant);
std::shared_ptr<const QuantizerStats> MakeScalarStats(
  VectorMetric metric, uint32_t d, VectorQuantization quant,
  std::span<const byte_type> stats);

std::unique_ptr<QuantizerWriter> MakePqWriter(VectorMetric metric, uint32_t d,
                                              uint32_t pq_m, uint32_t pq_niter,
                                              bool row_major);
std::shared_ptr<const QuantizerStats> MakePqStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats,
  bool row_major);

std::unique_ptr<QuantizerWriter> MakeTurboQuantWriter(VectorMetric metric,
                                                      uint32_t d,
                                                      uint32_t nb_bits,
                                                      bool half_bit,
                                                      bool row_major);
std::shared_ptr<const QuantizerStats> MakeTurboQuantStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats,
  bool row_major);

std::unique_ptr<QuantizerWriter> MakeRaBitQWriter(VectorMetric metric,
                                                  uint32_t d, uint32_t nb_bits,
                                                  bool row_major);
std::shared_ptr<const QuantizerStats> MakeRaBitQStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats,
  bool row_major);

std::unique_ptr<QuantizerWriter> MakeBinaryWriter(VectorMetric metric,
                                                  uint32_t d, uint32_t nb_bits,
                                                  bool half_bit,
                                                  uint32_t query_bits);
std::shared_ptr<const QuantizerStats> MakeBinaryStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats);

std::unique_ptr<QuantizerWriter> MakeOsqWriter(VectorMetric metric, uint32_t d,
                                               uint32_t nb_bits,
                                               uint32_t query_bits);
std::shared_ptr<const QuantizerStats> MakeOsqStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats);

}  // namespace irs::quantizers
