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

#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/utils/random.h>

#include <core_functions/array_kernels.hpp>
#include <limits>
#include <numbers>
#include <numeric>
#include <optional>
#include <pcg_random.hpp>

#include "iresearch/formats/ivf/quantizers/common.hpp"
#include "iresearch/index/vector_distance.hpp"
#include "iresearch/utils/shuffle.hpp"

namespace irs::quantizers {
namespace {

constexpr uint32_t kByteLutSize = 256;
constexpr size_t kPairLutSize = size_t{kByteLutSize} * kByteLutSize;

constexpr uint8_t kTurboQuantQjlFwht = 0;
constexpr uint8_t kTurboQuantLayout = 3;
constexpr uint32_t kTurboQuantDimStep = 16;
constexpr size_t kTurboQuantMixRounds = 2;

struct TurboQuantStatsHeader {
  uint8_t layout;
  uint8_t bits_hi;
  uint8_t bits_lo;
  uint8_t full;
  uint32_t d;
  uint64_t seed;
};

static_assert(sizeof(TurboQuantStatsHeader) == 16);
static_assert(std::has_unique_object_representations_v<TurboQuantStatsHeader>);

struct TurboQuantBits {
  uint32_t hi = 0;
  uint32_t lo = 0;
  bool full = false;
};

std::optional<TurboQuantBits> MakeTurboQuantBits(uint32_t nb_bits,
                                                 bool half_bit) noexcept {
  if (half_bit) {
    if (nb_bits == 1) {
      return TurboQuantBits{.hi = 2, .lo = 1, .full = false};
    }
    return std::nullopt;
  }
  switch (nb_bits) {
    case 1:
      return TurboQuantBits{.hi = 1, .lo = 1, .full = false};
    case 2:
      return TurboQuantBits{.hi = 2, .lo = 2, .full = false};
    case 3:
      return TurboQuantBits{.hi = 4, .lo = 2, .full = false};
    case 4:
      return TurboQuantBits{.hi = 4, .lo = 4, .full = false};
    case 5:
      return TurboQuantBits{.hi = 4, .lo = 4, .full = true};
    default:
      return std::nullopt;
  }
}

std::optional<faiss::ScalarQuantizer::QuantizerType> FaissTurboQuantType(
  uint32_t bits, bool full) noexcept {
  using QT = faiss::ScalarQuantizer::QuantizerType;
  if (full) {
    return bits == 4 ? std::optional{QT::QT_5bit_tq} : std::nullopt;
  }
  switch (bits) {
    case 1:
      return QT::QT_1bit_tqmse;
    case 2:
      return QT::QT_2bit_tqmse;
    case 4:
      return QT::QT_4bit_tqmse;
    default:
      return std::nullopt;
  }
}

void TrainTurboQuant(faiss::ScalarQuantizer& sq, uint64_t seed) {
  using QT = faiss::ScalarQuantizer::QuantizerType;
  sq.turboq_refine.seed = seed;
  sq.turboq_refine.qjl_type = kTurboQuantQjlFwht;
  sq.train(0, nullptr);
  switch (sq.qtype) {
    case QT::QT_1bit_tqmse:
    case QT::QT_2bit_tqmse:
    case QT::QT_3bit_tqmse:
    case QT::QT_4bit_tqmse:
    case QT::QT_8bit_tqmse: {
      const float unit = std::sqrt(static_cast<float>(sq.d));
      for (auto& v : sq.trained) {
        v *= unit;
      }
      break;
    }
    default:
      break;
  }
}

float TurboQuantNorm(const float* v, uint32_t d) {
  return std::max(duckdb::L2NormOp::Operation(v, d),
                  std::numeric_limits<float>::epsilon());
}

uint32_t TurboQuantDim(uint32_t d) noexcept {
  return std::max(
    kTurboQuantDimStep,
    (d + kTurboQuantDimStep - 1) / kTurboQuantDimStep * kTurboQuantDimStep);
}

class TurboQuantRotation {
 public:
  TurboQuantRotation(uint32_t rd, int64_t seed) : _rd{rd}, _signs(rd) {
    faiss::rand_signs(_signs.data(), rd, seed);
    if (std::has_single_bit(rd)) {
      return;
    }
    pcg32 rng{static_cast<uint64_t>(seed)};
    for (auto& round : _rounds) {
      round.perm.resize(rd);
      std::iota(round.perm.begin(), round.perm.end(), uint32_t{0});
      Shuffle<uint32_t>(round.perm, rng);
      round.signs.resize(rd);
      for (auto& s : round.signs) {
        s = (rng() & 1U) != 0 ? 1.f : -1.f;
      }
    }
  }

  void Rotate(const float* in, uint32_t d, float* out,
              std::vector<float>& scratch) const {
    for (uint32_t i = 0; i < d; ++i) {
      out[i] = in[i] * _signs[i];
    }
    std::fill(out + d, out + _rd, 0.f);
    WhtChunks(out);
    if (_rounds[0].perm.empty()) {
      return;
    }
    scratch.resize(_rd);
    for (const auto& round : _rounds) {
      for (uint32_t k = 0; k < _rd; ++k) {
        scratch[k] = out[round.perm[k]] * round.signs[k];
      }
      WhtChunks(scratch.data());
      std::copy_n(scratch.data(), _rd, out);
    }
  }

  void RotateBack(float* v, float* out, uint32_t d,
                  std::vector<float>& scratch) const {
    if (!_rounds[0].perm.empty()) {
      scratch.resize(_rd);
      for (auto round = _rounds.rbegin(); round != _rounds.rend(); ++round) {
        WhtChunks(v);
        for (uint32_t k = 0; k < _rd; ++k) {
          scratch[round->perm[k]] = v[k] * round->signs[k];
        }
        std::copy_n(scratch.data(), _rd, v);
      }
    }
    WhtChunks(v);
    for (uint32_t i = 0; i < d; ++i) {
      out[i] = v[i] * _signs[i];
    }
  }

 private:
  struct Round {
    std::vector<uint32_t> perm;
    std::vector<float> signs;
  };

  void WhtChunks(float* v) const noexcept {
    for (uint32_t off = 0; off < _rd;) {
      const uint32_t n = std::bit_floor(_rd - off);
      Fwht(v + off, n);
      const float scale = 1.f / std::sqrt(static_cast<float>(n));
      for (uint32_t i = off; i < off + n; ++i) {
        v[i] *= scale;
      }
      off += n;
    }
  }

  uint32_t _rd;
  std::vector<float> _signs;
  std::array<Round, kTurboQuantMixRounds> _rounds;
};

struct TurboQuantLayout {
  uint32_t d = 0;
  uint32_t rd = 0;
  uint32_t bits_hi = 0;
  uint32_t bits_lo = 0;
  uint32_t m_hi = 0;
  uint32_t m1 = 0;
  uint32_t nsq1 = 0;
  uint32_t m2 = 0;
  uint32_t nsq2 = 0;
  uint32_t code1_bytes = 0;
  uint32_t code2_bytes = 0;
  uint32_t group1_bytes = 0;
  uint32_t group2_bytes = 0;
  uint32_t record_size = 0;
  bool full = false;
  bool l2 = false;
  bool row_major = false;

  size_t Seg(uint32_t mi) const noexcept { return mi < m_hi ? 0 : 1; }
  uint32_t NibBits(uint32_t mi) const noexcept {
    return mi < m_hi ? bits_hi : bits_lo;
  }
  uint32_t NibDims(uint32_t mi) const noexcept {
    return static_cast<uint32_t>(kFastScanBits) / NibBits(mi);
  }
  uint32_t NibCoord(uint32_t mi) const noexcept {
    return mi < m_hi
             ? mi * (static_cast<uint32_t>(kFastScanBits) / bits_hi)
             : rd / 2 +
                 (mi - m_hi) * (static_cast<uint32_t>(kFastScanBits) / bits_lo);
  }

  uint32_t RowCode2Offset() const noexcept { return code1_bytes; }
  uint32_t RowNormOffset() const noexcept { return code1_bytes + code2_bytes; }
  uint32_t RowGammaOffset() const noexcept {
    return RowNormOffset() + sizeof(float);
  }
  uint32_t RowBiasOffset() const noexcept {
    return RowGammaOffset() + (full ? sizeof(float) : 0);
  }

  uint32_t NormOffset() const noexcept { return group1_bytes + group2_bytes; }
  uint32_t GammaOffset() const noexcept {
    return NormOffset() + kFastScanBbs * sizeof(float);
  }
  uint32_t BiasOffset() const noexcept {
    return GammaOffset() + (full ? kFastScanBbs * sizeof(float) : 0);
  }
  uint32_t GroupBytes() const noexcept { return kFastScanBbs * record_size; }
  PayloadBlockSetting BlockSetting() const noexcept {
    return {
      .group_size = row_major ? 1U : uint32_t{kFastScanBbs},
      .record_size = record_size,
      .alignment = row_major ? 1U : uint32_t{alignof(float)},
    };
  }
};

TurboQuantLayout MakeTurboQuantLayout(uint32_t d, TurboQuantBits bits, bool l2,
                                      bool row_major) noexcept {
  TurboQuantLayout l;
  l.d = d;
  l.rd = TurboQuantDim(d);
  l.full = bits.full;
  l.l2 = l2;
  l.row_major = row_major;
  l.bits_hi = bits.hi;
  l.bits_lo = bits.lo;
  const uint32_t half = l.rd / 2;
  l.m_hi = half / (static_cast<uint32_t>(kFastScanBits) / bits.hi);
  l.m1 = l.m_hi + half / (static_cast<uint32_t>(kFastScanBits) / bits.lo);
  l.nsq1 = static_cast<uint32_t>(FastScanNsq(l.m1));
  l.code1_bytes = (l.m1 + 1) / 2;
  l.group1_bytes = static_cast<uint32_t>(kFastScanBbs) * l.nsq1 / 2;
  if (l.full) {
    l.m2 = l.rd / static_cast<uint32_t>(kFastScanBits);
    l.nsq2 = static_cast<uint32_t>(FastScanNsq(l.m2));
    l.code2_bytes = (l.m2 + 1) / 2;
    l.group2_bytes = static_cast<uint32_t>(kFastScanBbs) * l.nsq2 / 2;
  }
  const uint32_t floats = 2 + (l.full ? 1 : 0);
  l.record_size =
    l.nsq1 / 2 + l.nsq2 / 2 + floats * static_cast<uint32_t>(sizeof(float));
  return l;
}

struct TurboQuantCentroids {
  std::array<const float*, 2> cent{};
  std::array<const float*, 2> bound{};
};

class TurboQuantTables {
 public:
  TurboQuantTables(uint32_t rd, TurboQuantBits bits, uint64_t seed)
    : _hi{rd, *FaissTurboQuantType(bits.hi, bits.full)} {
    TrainTurboQuant(_hi, seed);
    if (bits.lo != bits.hi) {
      _lo.emplace(rd, *FaissTurboQuantType(bits.lo, false));
      TrainTurboQuant(*_lo, seed);
    }
    const auto& lo = _lo ? *_lo : _hi;
    _tc.cent = {_hi.trained.data(), lo.trained.data()};
    _tc.bound = {_hi.trained.data() + (size_t{1} << bits.hi),
                 lo.trained.data() + (size_t{1} << bits.lo)};
  }

  TurboQuantTables(const TurboQuantTables&) = delete;
  TurboQuantTables& operator=(const TurboQuantTables&) = delete;

  const TurboQuantCentroids& Centroids() const noexcept { return _tc; }

 private:
  faiss::ScalarQuantizer _hi;
  std::optional<faiss::ScalarQuantizer> _lo;
  TurboQuantCentroids _tc;
};

struct TurboQuantInt8Seg {
  uint32_t bits = 0;
  uint32_t byte0 = 0;
  uint32_t bytes = 0;
  uint32_t q_off = 0;
  int32_t q_sum = 0;
  float inv_scale = 0.f;
  alignas(16) std::array<int8_t, 16> c8{};
  alignas(16) std::array<uint8_t, 16> a8{};
};

#ifdef __AVX2__
float FillInt8Seg(const TurboQuantLayout& lay, const TurboQuantCentroids& tc,
                  size_t i, TurboQuantInt8Seg& seg) noexcept {
  const bool uniform = lay.bits_hi == lay.bits_lo;
  seg.bits = i == 0 ? lay.bits_hi : lay.bits_lo;
  seg.byte0 = i == 0 ? 0 : lay.m_hi / 2;
  seg.bytes =
    uniform ? lay.m1 / 2 : (i == 0 ? lay.m_hi / 2 : (lay.m1 - lay.m_hi) / 2);
  const float* cent = tc.cent[i];
  const uint32_t k = 1U << seg.bits;
  float maxc = 0.f;
  for (uint32_t c = 0; c < k; ++c) {
    maxc = std::max(maxc, std::fabs(cent[c]));
  }
  const float scale_c = maxc > 0.f ? 127.f / maxc : 0.f;
  seg.c8.fill(0);
  seg.a8.fill(0);
  for (uint32_t c = 0; c < k; ++c) {
    const auto v = static_cast<int8_t>(std::lround(cent[c] * scale_c));
    seg.c8[c] = v;
    seg.a8[c] = static_cast<uint8_t>(v < 0 ? -v : v);
  }
  return scale_c;
}
int32_t TurboQuantDot1(const uint8_t* code, const TurboQuantInt8Seg& seg,
                       const int8_t* q) noexcept {
  const int32_t ones_sum = BitPlaneSum(code, seg.bytes, q);
  return int32_t{seg.c8[0]} * seg.q_sum +
         (int32_t{seg.c8[1]} - int32_t{seg.c8[0]}) * ones_sum;
}

template<uint32_t Bits>
int32_t TurboQuantDot8(const uint8_t* code, const TurboQuantInt8Seg& seg,
                       const int8_t* q) noexcept {
  constexpr uint32_t kPlanes = 8 / Bits;
  constexpr auto kMask = static_cast<uint8_t>((1U << Bits) - 1);
  const uint32_t bytes = seg.bytes;
  const __m256i tc = _mm256_broadcastsi128_si256(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.c8.data())));
  const __m256i ta = _mm256_broadcastsi128_si256(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.a8.data())));
  const __m256i mask = _mm256_set1_epi8(static_cast<char>(kMask));
  const __m256i ones = _mm256_set1_epi16(1);
  __m256i acc = _mm256_setzero_si256();
  uint32_t p = 0;
  for (; p + 32 <= bytes; p += 32) {
    const __m256i v =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(code + p));
    for (uint32_t t = 0; t < kPlanes; ++t) {
      const __m256i idx = _mm256_and_si256(
        _mm256_srli_epi16(v, static_cast<int>(t * Bits)), mask);
      const __m256i c = _mm256_shuffle_epi8(tc, idx);
      const __m256i a = _mm256_shuffle_epi8(ta, idx);
      const __m256i qv = _mm256_loadu_si256(
        reinterpret_cast<const __m256i*>(q + size_t{t} * bytes + p));
      acc = _mm256_add_epi32(
        acc, _mm256_madd_epi16(_mm256_maddubs_epi16(a, _mm256_sign_epi8(qv, c)),
                               ones));
    }
  }
  __m128i h = _mm_add_epi32(_mm256_castsi256_si128(acc),
                            _mm256_extracti128_si256(acc, 1));
  const __m128i tc1 = _mm256_castsi256_si128(tc);
  const __m128i ta1 = _mm256_castsi256_si128(ta);
  const __m128i mask1 = _mm256_castsi256_si128(mask);
  const __m128i ones1 = _mm256_castsi256_si128(ones);
  const auto step = [&](__m128i v, auto load_q) {
    for (uint32_t t = 0; t < kPlanes; ++t) {
      const __m128i idx =
        _mm_and_si128(_mm_srli_epi16(v, static_cast<int>(t * Bits)), mask1);
      const __m128i c = _mm_shuffle_epi8(tc1, idx);
      const __m128i a = _mm_shuffle_epi8(ta1, idx);
      h = _mm_add_epi32(
        h, _mm_madd_epi16(_mm_maddubs_epi16(a, _mm_sign_epi8(load_q(t), c)),
                          ones1));
    }
  };
  if (p + 16 <= bytes) {
    step(_mm_loadu_si128(reinterpret_cast<const __m128i*>(code + p)),
         [&](uint32_t t) {
           return _mm_loadu_si128(
             reinterpret_cast<const __m128i*>(q + size_t{t} * bytes + p));
         });
    p += 16;
  }
  if (p + 8 <= bytes) {
    step(_mm_loadl_epi64(reinterpret_cast<const __m128i*>(code + p)),
         [&](uint32_t t) {
           return _mm_loadl_epi64(
             reinterpret_cast<const __m128i*>(q + size_t{t} * bytes + p));
         });
    p += 8;
  }
  const __m128i h2 = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0x4E));
  int32_t sum =
    _mm_cvtsi128_si32(_mm_add_epi32(h2, _mm_shuffle_epi32(h2, 0xB1)));
  for (; p < bytes; ++p) {
    const uint8_t b = code[p];
    for (uint32_t t = 0; t < kPlanes; ++t) {
      sum += int32_t{q[size_t{t} * bytes + p]} *
             int32_t{seg.c8[(b >> (t * Bits)) & kMask]};
    }
  }
  return sum;
}

#if defined(__x86_64__)
template<uint32_t Bits>
__attribute__((target("avx512f,avx512bw,avx512vnni"))) int32_t
TurboQuantDot8Vnni(const uint8_t* code, const TurboQuantInt8Seg& seg,
                   const int8_t* q) noexcept {
  constexpr uint32_t kPlanes = 8 / Bits;
  constexpr auto kMask = static_cast<uint8_t>((1U << Bits) - 1);
  const uint32_t bytes = seg.bytes;
  const __m512i tc = _mm512_broadcast_i32x4(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.c8.data())));
  const __m512i ta = _mm512_broadcast_i32x4(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.a8.data())));
  const __m512i mask = _mm512_set1_epi8(static_cast<char>(kMask));
  const __m512i zero = _mm512_setzero_si512();
  __m512i acc = zero;
  uint32_t p = 0;
  for (; p < bytes; p += 64) {
    const __mmask64 live =
      bytes - p >= 64 ? ~__mmask64{0} : (__mmask64{1} << (bytes - p)) - 1;
    const __m512i v = _mm512_maskz_loadu_epi8(live, code + p);
    for (uint32_t t = 0; t < kPlanes; ++t) {
      const __m512i idx = _mm512_and_si512(
        _mm512_srli_epi16(v, static_cast<unsigned>(t * Bits)), mask);
      const __m512i a = _mm512_shuffle_epi8(ta, idx);
      const __m512i c = _mm512_shuffle_epi8(tc, idx);
      const __m512i qv =
        _mm512_maskz_loadu_epi8(live, q + size_t{t} * bytes + p);
      acc = _mm512_dpbusd_epi32(
        acc, a, _mm512_mask_sub_epi8(qv, _mm512_movepi8_mask(c), zero, qv));
    }
  }
  return _mm512_reduce_add_epi32(acc);
}

template<uint32_t Bits>
__attribute__((target("avx512f,avx512bw,avx512vnni"))) void TurboQuantDot8Vnni4(
  const uint8_t* const* codes, const TurboQuantInt8Seg& seg, const int8_t* q,
  int32_t* out) noexcept {
  constexpr uint32_t kPlanes = 8 / Bits;
  constexpr auto kMask = static_cast<uint8_t>((1U << Bits) - 1);
  constexpr size_t kRows = 4;
  const uint32_t bytes = seg.bytes;
  const __m512i tc = _mm512_broadcast_i32x4(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.c8.data())));
  const __m512i ta = _mm512_broadcast_i32x4(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.a8.data())));
  const __m512i mask = _mm512_set1_epi8(static_cast<char>(kMask));
  const __m512i zero = _mm512_setzero_si512();
  __m512i acc[kRows] = {zero, zero, zero, zero};
  uint32_t p = 0;
  for (; p < bytes; p += 64) {
    const __mmask64 live =
      bytes - p >= 64 ? ~__mmask64{0} : (__mmask64{1} << (bytes - p)) - 1;
    __m512i v[kRows];
    for (size_t k = 0; k < kRows; ++k) {
      v[k] = _mm512_maskz_loadu_epi8(live, codes[k] + p);
    }
    for (uint32_t t = 0; t < kPlanes; ++t) {
      const __m512i qv =
        _mm512_maskz_loadu_epi8(live, q + size_t{t} * bytes + p);
      for (size_t k = 0; k < kRows; ++k) {
        const __m512i idx = _mm512_and_si512(
          _mm512_srli_epi16(v[k], static_cast<unsigned>(t * Bits)), mask);
        const __m512i a = _mm512_shuffle_epi8(ta, idx);
        const __m512i c = _mm512_shuffle_epi8(tc, idx);
        acc[k] = _mm512_dpbusd_epi32(
          acc[k], a,
          _mm512_mask_sub_epi8(qv, _mm512_movepi8_mask(c), zero, qv));
      }
    }
  }
  for (size_t k = 0; k < kRows; ++k) {
    out[k] = _mm512_reduce_add_epi32(acc[k]);
  }
}
#endif

template<uint32_t Bits>
int32_t TurboQuantPairDot8(const uint8_t* a, const uint8_t* b,
                           const TurboQuantInt8Seg& seg) noexcept {
  constexpr uint32_t kPlanes = 8 / Bits;
  constexpr auto kMask = static_cast<uint8_t>((1U << Bits) - 1);
  const uint32_t bytes = seg.bytes;
  const __m256i tc = _mm256_broadcastsi128_si256(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.c8.data())));
  const __m256i ta = _mm256_broadcastsi128_si256(
    _mm_load_si128(reinterpret_cast<const __m128i*>(seg.a8.data())));
  const __m256i mask = _mm256_set1_epi8(static_cast<char>(kMask));
  const __m256i ones = _mm256_set1_epi16(1);
  __m256i acc = _mm256_setzero_si256();
  uint32_t p = 0;
  for (; p + 32 <= bytes; p += 32) {
    const __m256i va =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + p));
    const __m256i vb =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + p));
    for (uint32_t t = 0; t < kPlanes; ++t) {
      const auto shift = static_cast<int>(t * Bits);
      const __m256i ia = _mm256_and_si256(_mm256_srli_epi16(va, shift), mask);
      const __m256i ib = _mm256_and_si256(_mm256_srli_epi16(vb, shift), mask);
      const __m256i ca = _mm256_shuffle_epi8(tc, ia);
      const __m256i cb = _mm256_shuffle_epi8(tc, ib);
      const __m256i ab = _mm256_shuffle_epi8(ta, ib);
      acc = _mm256_add_epi32(
        acc, _mm256_madd_epi16(
               _mm256_maddubs_epi16(ab, _mm256_sign_epi8(ca, cb)), ones));
    }
  }
  const __m128i h = _mm_add_epi32(_mm256_castsi256_si128(acc),
                                  _mm256_extracti128_si256(acc, 1));
  const __m128i h2 = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0x4E));
  int32_t sum =
    _mm_cvtsi128_si32(_mm_add_epi32(h2, _mm_shuffle_epi32(h2, 0xB1)));
  for (; p < bytes; ++p) {
    for (uint32_t t = 0; t < kPlanes; ++t) {
      const auto shift = t * Bits;
      sum += int32_t{seg.c8[(a[p] >> shift) & kMask]} *
             int32_t{seg.c8[(b[p] >> shift) & kMask]};
    }
  }
  return sum;
}
#endif

template<VectorMetric M>
class TurboQuantizerWriter final : public QuantizerWriter {
 public:
  TurboQuantizerWriter(uint32_t d, TurboQuantBits bits, bool row_major)
    : _lay{MakeTurboQuantLayout(d, bits, M == VectorMetric::L2Sqr, row_major)},
      _bits{bits},
      _tables{_lay.rd, bits, kIvfRotationSeed},
      _sqrt_rd{std::sqrt(static_cast<float>(_lay.rd))},
      _rotation{_lay.rd, ~kIvfRotationSeed} {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    _rot.resize(_lay.rd);
    _res.resize(_lay.rd);
    _code1.assign(kFastScanBbs * size_t{_lay.code1_bytes}, 0);
    _packed1.resize(_lay.group1_bytes);
    if (_lay.full) {
      _projection.emplace(_lay.rd, kIvfRotationSeed);
      _proj.resize(_lay.rd);
      _code2.assign(kFastScanBbs * size_t{_lay.code2_bytes}, 0);
      _packed2.resize(_lay.group2_bytes);
    }
  }

  size_t TrainSamples(size_t /*rows*/) const noexcept final { return 0; }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_lay.rd);
    _rotation.Rotate(centroid, _lay.d, _centroid.data(), _mix);
    if (_lay.full) {
      _cproj.resize(_lay.rd);
      _projection->Rotate(_centroid.data(), _lay.rd, _cproj.data(), _mix);
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return _lay.BlockSetting();
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_centroid.size() == _lay.rd);
    for (size_t i = 0; i < n; ++i) {
      if (_lay.row_major) {
        WriteRecord(out, vecs + i * size_t{_lay.d});
        continue;
      }
      EncodeOne(vecs + i * size_t{_lay.d}, _lane);
      if (++_lane == kFastScanBbs) {
        WriteGroup(out, kFastScanBbs);
      }
    }
  }

  bool EncodeInto(byte_type* dst, const float* vecs, size_t n) final {
    if (!_lay.row_major) {
      return false;
    }
    SDB_ASSERT(_centroid.size() == _lay.rd);
    for (size_t i = 0; i < n; ++i) {
      PackRecord(dst + i * size_t{_lay.record_size}, vecs + i * size_t{_lay.d});
    }
    return true;
  }

  std::unique_ptr<QuantizerWriter> CloneForEncode() const final {
    if (!_lay.row_major) {
      return nullptr;
    }
    auto out =
      std::make_unique<TurboQuantizerWriter>(_lay.d, _bits, _lay.row_major);
    out->_centroid = _centroid;
    out->_cproj = _cproj;
    return out;
  }

  void Finish(IndexOutput& out) final {
    if (!_lay.row_major && _lane != 0) {
      WriteGroup(out, _lane);
    }
  }

  uint32_t PendingLanes() const noexcept final {
    return _lay.row_major ? 0U : static_cast<uint32_t>(_lane);
  }

  void Serialize(DataOutput& out) const final {
    out.WriteU64(sizeof(TurboQuantStatsHeader));
    WritePod(out, TurboQuantStatsHeader{
                    .layout = kTurboQuantLayout,
                    .bits_hi = static_cast<uint8_t>(_bits.hi),
                    .bits_lo = static_cast<uint8_t>(_bits.lo),
                    .full = static_cast<uint8_t>(_bits.full ? 1 : 0),
                    .d = _lay.d,
                    .seed = kIvfRotationSeed});
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::TQ;
  }

 private:
  void EncodeOne(const float* vec, size_t lane) {
    _rotation.Rotate(vec, _lay.d, _rot.data(), _mix);
    float c_dot_r = 0.f;
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _res[j] = _rot[j] - _centroid[j];
      c_dot_r += _centroid[j] * _res[j];
    }
    const float norm = TurboQuantNorm(_res.data(), _lay.rd);
    _norms[lane] = norm;
    const float scale = _sqrt_rd / norm;
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _res[j] *= scale;
    }

    const TurboQuantCentroids& tc = _tables.Centroids();
    const float inv_sqrt_rd = 1.f / _sqrt_rd;
    uint8_t* code = _code1.data() + lane * size_t{_lay.code1_bytes};
    double cn2 = 0.0;
    double cy = 0.0;
    double ct = 0.0;
    for (uint32_t mi = 0; mi < _lay.m1; ++mi) {
      const size_t seg = _lay.Seg(mi);
      const uint32_t bits = _lay.NibBits(mi);
      const uint32_t k = 1U << bits;
      const float* cent = tc.cent[seg];
      const float* bound = tc.bound[seg];
      const uint32_t base = _lay.NibCoord(mi);
      uint8_t nib = 0;
      for (uint32_t t = 0; t < _lay.NibDims(mi); ++t) {
        const uint32_t j = base + t;
        const auto idx = static_cast<uint8_t>(
          std::upper_bound(bound, bound + (k - 1), _res[j]) - bound);
        nib = static_cast<uint8_t>(nib | (idx << (t * bits)));
        const float cr = cent[idx];
        cn2 += static_cast<double>(cr) * cr;
        cy += static_cast<double>(cr) * _res[j];
        ct += static_cast<double>(cr) * _centroid[j];
        if (_lay.full) {
          _res[j] = (_res[j] - cr) * inv_sqrt_rd;
        }
      }
      SetNibble(code, mi, nib);
    }
    if (!_lay.full) {
      const auto cn = static_cast<float>(std::sqrt(cn2));
      if (cy > std::numeric_limits<float>::epsilon()) {
        _norms[lane] =
          norm * static_cast<float>(_lay.rd) / static_cast<float>(cy);
      } else if (cn > std::numeric_limits<float>::epsilon()) {
        _norms[lane] = norm * _sqrt_rd / cn;
      }
      SetBias(lane, norm, c_dot_r,
              _norms[lane] * static_cast<float>(ct) * inv_sqrt_rd);
      return;
    }

    _gammas[lane] = duckdb::L2NormOp::Operation(_res.data(), _lay.rd);
    _projection->Rotate(_res.data(), _lay.rd, _proj.data(), _mix);
    uint8_t* qjl = _code2.data() + lane * size_t{_lay.code2_bytes};
    float c_qjl = 0.f;
    for (uint32_t mi = 0; mi < _lay.m2; ++mi) {
      uint8_t nib = 0;
      for (uint32_t t = 0; t < kFastScanBits; ++t) {
        const uint32_t j = mi * kFastScanBits + t;
        if (_proj[j] > 0.f) {
          nib = static_cast<uint8_t>(nib | (1U << t));
          c_qjl += _cproj[j];
        } else {
          c_qjl -= _cproj[j];
        }
      }
      SetNibble(qjl, mi, nib);
    }
    const float qjl_coeff =
      std::sqrt(std::numbers::pi_v<float> / 2.f) / static_cast<float>(_lay.rd);
    SetBias(lane, norm, c_dot_r,
            norm * (static_cast<float>(ct) * inv_sqrt_rd +
                    qjl_coeff * _gammas[lane] * c_qjl));
  }

  void SetBias(size_t lane, float norm, float c_dot_r, float c_estimate) {
    if constexpr (M == VectorMetric::L2Sqr) {
      _bias[lane] = norm * norm + 2.f * c_estimate;
    } else {
      _bias[lane] = c_dot_r - c_estimate;
    }
  }

  void PackRecord(byte_type* dst, const float* vec) {
    std::fill_n(_code1.begin(), _lay.code1_bytes, uint8_t{0});
    if (_lay.full) {
      std::fill_n(_code2.begin(), _lay.code2_bytes, uint8_t{0});
    }
    EncodeOne(vec, 0);
    std::memcpy(dst, _code1.data(), _lay.code1_bytes);
    if (_lay.full) {
      std::memcpy(dst + _lay.RowCode2Offset(), _code2.data(), _lay.code2_bytes);
    }
    std::memcpy(dst + _lay.RowNormOffset(), &_norms[0], sizeof(float));
    if (_lay.full) {
      std::memcpy(dst + _lay.RowGammaOffset(), &_gammas[0], sizeof(float));
    }
    std::memcpy(dst + _lay.RowBiasOffset(), &_bias[0], sizeof(float));
  }

  void WriteRecord(IndexOutput& out, const float* vec) {
    _record.resize(_lay.record_size);
    PackRecord(_record.data(), vec);
    out.WriteData(_record.data(), _lay.record_size);
  }

  void WriteFloats(IndexOutput& out, std::array<float, kFastScanBbs>& v,
                   size_t count) {
    std::fill_n(v.data() + count, kFastScanBbs - count, 0.f);
    out.WriteData(reinterpret_cast<const byte_type*>(v.data()),
                  kFastScanBbs * sizeof(float));
  }

  void WriteGroup(IndexOutput& out, size_t count) {
    faiss::pq4_pack_codes(_code1.data(), count, _lay.m1, kFastScanBbs,
                          kFastScanBbs, _lay.nsq1, _packed1.data(),
                          _lay.code1_bytes);
    out.WriteData(_packed1.data(), _packed1.size());
    if (_lay.full) {
      faiss::pq4_pack_codes(_code2.data(), count, _lay.m2, kFastScanBbs,
                            kFastScanBbs, _lay.nsq2, _packed2.data(),
                            _lay.code2_bytes);
      out.WriteData(_packed2.data(), _packed2.size());
    }
    WriteFloats(out, _norms, count);
    if (_lay.full) {
      WriteFloats(out, _gammas, count);
    }
    WriteFloats(out, _bias, count);
    std::fill(_code1.begin(), _code1.end(), 0);
    std::fill(_code2.begin(), _code2.end(), 0);
    _lane = 0;
  }

  TurboQuantLayout _lay;
  TurboQuantBits _bits;
  TurboQuantTables _tables;
  float _sqrt_rd;
  size_t _lane = 0;
  TurboQuantRotation _rotation;
  std::optional<TurboQuantRotation> _projection;
  std::vector<float> _mix;
  std::vector<float> _centroid;
  std::vector<float> _cproj;
  std::vector<float> _rot;
  std::vector<float> _res;
  std::vector<float> _proj;
  std::vector<uint8_t> _code1;
  std::vector<uint8_t> _code2;
  std::vector<uint8_t> _packed1;
  std::vector<uint8_t> _packed2;
  std::vector<uint8_t> _record;
  std::array<float, kFastScanBbs> _norms{};
  std::array<float, kFastScanBbs> _gammas{};
  std::array<float, kFastScanBbs> _bias{};
};

template<VectorMetric M>
class TurboQuantizerStats final : public QuantizerStats {
 public:
  TurboQuantizerStats(uint32_t d, std::span<const byte_type> stats,
                      bool row_major) {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    const auto hdr = ReadPodHeader<TurboQuantStatsHeader>(stats);
    const TurboQuantBits bits{
      .hi = hdr.bits_hi, .lo = hdr.bits_lo, .full = hdr.full != 0};
    if (stats.size() < sizeof(TurboQuantStatsHeader) || hdr.d != d ||
        hdr.layout != kTurboQuantLayout ||
        !FaissTurboQuantType(bits.hi, bits.full) ||
        !FaissTurboQuantType(bits.lo, bits.full) ||
        (bits.full && bits.hi != bits.lo)) {
      return;
    }
    _lay = MakeTurboQuantLayout(d, bits, M == VectorMetric::L2Sqr, row_major);
    _tables = std::make_unique<TurboQuantTables>(_lay.rd, bits, hdr.seed);
    const auto seed = static_cast<int64_t>(hdr.seed);
    _rotation.emplace(_lay.rd, ~seed);
    if (bits.full) {
      _projection.emplace(_lay.rd, seed);
    }
    std::memcpy(_key.data(), stats.data(), sizeof(TurboQuantStatsHeader));
    _key[sizeof(TurboQuantStatsHeader)] = static_cast<byte_type>(M);
    _key[sizeof(TurboQuantStatsHeader) + 1] = row_major ? 1 : 0;
    _valid = true;
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::TQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  std::span<const byte_type> CodebookKey() const noexcept final {
    if (!_valid) {
      return {};
    }
    return _key;
  }

  bool Valid() const noexcept { return _valid; }
  const TurboQuantLayout& Layout() const noexcept { return _lay; }
  const TurboQuantCentroids& Centroids() const noexcept {
    return _tables->Centroids();
  }
  const TurboQuantRotation& Rotation() const noexcept { return *_rotation; }
  const TurboQuantRotation& Projection() const noexcept { return *_projection; }

 private:
  TurboQuantLayout _lay;
  bool _valid = false;
  std::array<byte_type, sizeof(TurboQuantStatsHeader) + 2> _key{};
  std::unique_ptr<TurboQuantTables> _tables;
  std::optional<TurboQuantRotation> _rotation;
  std::optional<TurboQuantRotation> _projection;
};

template<VectorMetric M>
class TurboQuantizerCodebook final : public QuantizerCodebook {
 public:
  TurboQuantizerCodebook(std::shared_ptr<const TurboQuantizerStats<M>> stats,
                         std::span<const float> query)
    : _stats{std::move(stats)} {
    SDB_ASSERT(_stats->Valid());
    Rekey(query);
  }

  void Rekey(std::span<const float> query) {
    const TurboQuantLayout& lay = _stats->Layout();
    SDB_ASSERT(query.size() == lay.d);
    _query.assign(query.begin(), query.end());
    _rot_query.resize(lay.rd);
    _stats->Rotation().Rotate(_query.data(), lay.d, _rot_query.data(), _mix);
    BuildMseLut();
    if (lay.full) {
      BuildQjlLut();
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const TurboQuantizerStats<M>& Stats() const noexcept { return *_stats; }
  const std::shared_ptr<const TurboQuantizerStats<M>>& StatsPtr()
    const noexcept {
    return _stats;
  }
  std::span<const float> Query() const noexcept { return _query; }
  const std::vector<FastScanChunk>& Chunks1() const noexcept {
    return _chunks1;
  }
  const std::vector<FastScanChunk>& Chunks2() const noexcept {
    return _chunks2;
  }
  const FastScanLut& Lut1() const noexcept { return _lut1; }
  const FastScanLut& Lut2() const noexcept { return _lut2; }
  const float* ByteLut() const noexcept { return _byte_lut.data(); }
  size_t NumSegs() const noexcept { return _nsegs; }
  const TurboQuantInt8Seg& Seg(size_t i) const noexcept { return _segs[i]; }
  const int8_t* Q8() const noexcept { return _q8.data(); }
  float QjlErrorCoeff() const noexcept { return _qjl_error_coeff; }
  float QjlSum() const noexcept { return _qjl_sum; }
  float MseSlack() const noexcept { return _mse_slack; }

 private:
  void BuildMseLut() {
    const TurboQuantLayout& lay = _stats->Layout();
    const TurboQuantCentroids& tc = _stats->Centroids();
#ifdef __AVX2__
    if (lay.row_major && !lay.full) {
      BuildInt8(lay, tc);
      return;
    }
#endif
    std::vector<float> lut(size_t{lay.m1} * kFastScanKsub);
    for (uint32_t mi = 0; mi < lay.m1; ++mi) {
      const float* cent = tc.cent[lay.Seg(mi)];
      const uint32_t bits = lay.NibBits(mi);
      const uint32_t mask = (1U << bits) - 1;
      const uint32_t base = lay.NibCoord(mi);
      const uint32_t dims = lay.NibDims(mi);
      for (uint32_t code = 0; code < kFastScanKsub; ++code) {
        float s = 0.f;
        for (uint32_t t = 0; t < dims; ++t) {
          s += _rot_query[base + t] * cent[(code >> (t * bits)) & mask];
        }
        lut[size_t{mi} * kFastScanKsub + code] = s;
      }
    }
    if (lay.row_major && !lay.full) {
      _byte_lut.resize(size_t{lay.code1_bytes} * kByteLutSize);
      for (uint32_t p = 0; p < lay.code1_bytes; ++p) {
        const float* lo = lut.data() + size_t{2 * p} * kFastScanKsub;
        float* out = _byte_lut.data() + size_t{p} * kByteLutSize;
        const float* hi = 2 * p + 1 < lay.m1 ? lo + kFastScanKsub : nullptr;
        for (uint32_t h = 0; h < kFastScanKsub; ++h) {
          const float hv = hi ? hi[h] : 0.f;
          for (uint32_t l = 0; l < kFastScanKsub; ++l) {
            out[h * kFastScanKsub + l] = lo[l] + hv;
          }
        }
      }
      return;
    }
    _chunks1 = MakeFastScanChunks(lay.m1);
    _lut1.Build(lut.data(), _chunks1, kFastScanKsub);
    float slack = 0.f;
    for (size_t i = 0; i < _chunks1.size(); ++i) {
      slack += static_cast<float>(_chunks1[i].m) * 0.5f * _lut1.Step(i);
    }
    _mse_slack = slack / std::sqrt(static_cast<float>(lay.rd));
  }

#ifdef __AVX2__
  void BuildInt8(const TurboQuantLayout& lay, const TurboQuantCentroids& tc) {
    float maxq = 0.f;
    for (uint32_t j = 0; j < lay.rd; ++j) {
      maxq = std::max(maxq, std::fabs(_rot_query[j]));
    }
    const float scale_q = maxq > 0.f ? 127.f / maxq : 0.f;
    _qi.resize(lay.rd);
    for (uint32_t j = 0; j < lay.rd; ++j) {
      _qi[j] = static_cast<int8_t>(
        std::clamp(std::nearbyint(_rot_query[j] * scale_q), -127.f, 127.f));
    }
    const bool uniform = lay.bits_hi == lay.bits_lo;
    _nsegs = uniform ? 1 : 2;
    _q8.resize(lay.rd);
    uint32_t q_off = 0;
    for (size_t i = 0; i < _nsegs; ++i) {
      auto& seg = _segs[i];
      const float scale_c = FillInt8Seg(lay, tc, i, seg);
      seg.q_off = q_off;
      seg.inv_scale =
        scale_q > 0.f && scale_c > 0.f ? 1.f / (scale_q * scale_c) : 0.f;
      const uint32_t planes = 8 / seg.bits;
      const uint32_t base = i == 0 ? 0 : lay.rd / 2;
      seg.q_sum = 0;
      for (uint32_t t = 0; t < planes; ++t) {
        for (uint32_t p = 0; p < seg.bytes; ++p) {
          const int8_t v = _qi[base + p * planes + t];
          const uint32_t at =
            seg.bits == 1 ? p * planes + t : t * seg.bytes + p;
          _q8[q_off + at] = v;
          seg.q_sum += v;
        }
      }
      q_off += planes * seg.bytes;
    }
  }
#endif

  void BuildQjlLut() {
    const TurboQuantLayout& lay = _stats->Layout();
    std::vector<float> qproj(lay.rd);
    _stats->Projection().Rotate(_rot_query.data(), lay.rd, qproj.data(), _mix);
    float l1 = 0.f;
    _qjl_sum = 0.f;
    for (uint32_t j = 0; j < lay.rd; ++j) {
      _qjl_sum += qproj[j];
      l1 += std::fabs(qproj[j]);
    }
    _qjl_error_coeff = std::sqrt(std::numbers::pi_v<float> / 2.f) /
                       static_cast<float>(lay.rd) * l1;
    std::vector<float> lut(size_t{lay.m2} * kFastScanKsub);
    for (uint32_t mi = 0; mi < lay.m2; ++mi) {
      const uint32_t base = mi * static_cast<uint32_t>(kFastScanBits);
      for (uint32_t code = 0; code < kFastScanKsub; ++code) {
        float s = 0.f;
        for (uint32_t t = 0; t < kFastScanBits; ++t) {
          if (((code >> t) & 1U) != 0) {
            s += qproj[base + t];
          }
        }
        lut[size_t{mi} * kFastScanKsub + code] = s;
      }
    }
    _chunks2 = MakeFastScanChunks(lay.m2);
    _lut2.Build(lut.data(), _chunks2, kFastScanKsub);
  }

  std::shared_ptr<const TurboQuantizerStats<M>> _stats;
  std::vector<float> _query;
  std::vector<float> _rot_query;
  std::vector<float> _mix;
  std::vector<FastScanChunk> _chunks1;
  std::vector<FastScanChunk> _chunks2;
  FastScanLut _lut1;
  FastScanLut _lut2;
  std::vector<float> _byte_lut;
  std::array<TurboQuantInt8Seg, 2> _segs;
  size_t _nsegs = 0;
  std::vector<int8_t> _qi;
  std::vector<int8_t> _q8;
  float _qjl_error_coeff = 0.f;
  float _qjl_sum = 0.f;
  float _mse_slack = 0.f;
};

template<VectorMetric M>
class TurboQuantizerReader final : public QuantizerReader {
 public:
  explicit TurboQuantizerReader(
    std::shared_ptr<const TurboQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)},
      _cur{_cb.get()},
      _lay{_cb->Stats().Layout()},
      _inv_sqrt_rd{1.f / std::sqrt(static_cast<float>(_lay.rd))},
      _qjl_coeff{std::sqrt(std::numbers::pi_v<float> / 2.f) /
                 static_cast<float>(_lay.rd)} {}

  PayloadBlockSetting BlockSetting() const noexcept final {
    return _lay.BlockSetting();
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid);
    _cluster = centroid;
    _cluster_rot.clear();
    RefreshClusterCorrection();
  }

  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t threshold,
                       score_t* out) final {
    if (!_lay.row_major) {
      QuantizerReader::ComputeGathered(base, record_size, ids, threshold, out);
      return;
    }
    if (!_lay.full) {
#if defined(__x86_64__) && defined(__AVX2__)
      if (Rows4()) {
        ScoreGatheredRows(
          base, record_size, ids, out,
          [this](const byte_type* r) { return ScoreRow(r); },
          [this](const byte_type* const* r, score_t* o) { ScoreRow4(r, o); });
        return;
      }
#endif
      ScoreGatheredRows(base, record_size, ids, out,
                        [this](const byte_type* r) { return ScoreRow(r); });
      return;
    }
    for (size_t off = 0; off < ids.size(); off += kFastScanBbs) {
      const size_t take = std::min<size_t>(ids.size() - off, kFastScanBbs);
      const auto sub = ids.subspan(off, take);
      ScoreRows(
        [base, record_size, sub](size_t k) {
          return base + static_cast<size_t>(sub[k]) * record_size;
        },
        take, threshold, out + off);
    }
  }

  void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                    score_t* out) final {
    if (_lay.row_major && !_lay.full) {
      SDB_ASSERT(block.size() % _lay.record_size == 0);
      const size_t rows = block.size() / _lay.record_size;
      for (size_t i = 0; i < rows; ++i) {
        out[i] = ScoreRow(block.data() + i * _lay.record_size);
      }
      return;
    }
    if (_lay.row_major) {
      SDB_ASSERT(block.size() % _lay.record_size == 0);
      size_t left = block.size() / _lay.record_size;
      const byte_type* rec = block.data();
      while (left != 0) {
        const size_t take = std::min<size_t>(left, kFastScanBbs);
        ScoreRecords(rec, take, threshold, out);
        rec += take * size_t{_lay.record_size};
        out += take;
        left -= take;
      }
      return;
    }
    const size_t group_bytes = _lay.GroupBytes();
    SDB_ASSERT(block.size() % group_bytes == 0);
    for (size_t off = 0; off < block.size();
         off += group_bytes, out += kFastScanBbs) {
      ScoreGroup(block.data() + off, threshold, out);
    }
  }

  bool Decode(const byte_type* code, float* out) const final {
    if (!_lay.row_major || !_cluster) {
      return false;
    }
    const auto& stats = _cb->Stats();
    _scale_scratch.resize(_lay.rd);
    DecodeCentroids(code, _scale_scratch.data());
    const float norm = LoadFloat(code + _lay.RowNormOffset());
    EnsureRotatedCluster();
    float scale = norm * _inv_sqrt_rd;
    if (!_lay.full) {
      if constexpr (M == VectorMetric::L2Sqr) {
        float ct = 0.f;
        float cn2 = 0.f;
        for (uint32_t j = 0; j < _lay.rd; ++j) {
          ct += _scale_scratch[j] * _cluster_rot[j];
          cn2 += _scale_scratch[j] * _scale_scratch[j];
        }
        const float r2 = LoadFloat(code + _lay.RowBiasOffset()) -
                         2.f * norm * ct * _inv_sqrt_rd;
        if (cn2 > std::numeric_limits<float>::epsilon()) {
          scale = std::sqrt(std::max(r2, 0.f) / cn2);
        }
      }
    }
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _scale_scratch[j] = _scale_scratch[j] * scale + _cluster_rot[j];
    }
    stats.Rotation().RotateBack(_scale_scratch.data(), out, _lay.d,
                                _fwht_scratch);
    return true;
  }

  bool SetQuery(std::span<const float> query) final {
    SDB_ASSERT(query.size() == _lay.d);
    if (!_own) {
      _own =
        std::make_unique<TurboQuantizerCodebook<M>>(_cb->StatsPtr(), query);
      _cur = _own.get();
    } else {
      _own->Rekey(query);
    }
    RefreshClusterCorrection();
    return true;
  }

  bool SupportsPairScores() const noexcept final {
    return _lay.row_major && !_lay.full && _cluster;
  }

  bool PreparePairTerms(const byte_type* base, uint32_t record_size,
                        uint64_t rows, std::vector<float>& terms) final {
    if (!SupportsPairScores()) {
      return false;
    }
    EnsurePairState();
    EnsureRotatedCluster();
    terms.assign(2 * rows, 0.f);
    std::vector<float> cent(_lay.rd);
    for (uint64_t row = 0; row < rows; ++row) {
      const byte_type* r = base + row * size_t{record_size};
      DecodeCentroids(r, cent.data());
      float ct = 0.f;
      float cn2 = 0.f;
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        ct += cent[j] * _cluster_rot[j];
        cn2 += cent[j] * cent[j];
      }
      const float norm = LoadFloat(r + _lay.RowNormOffset());
      const float bias = LoadFloat(r + _lay.RowBiasOffset());
      const float estimate = norm * ct * _inv_sqrt_rd;
      if constexpr (M == VectorMetric::L2Sqr) {
        const float r2 = std::max(bias - 2.f * estimate, 0.f);
        terms[2 * row] = cn2 > std::numeric_limits<float>::epsilon()
                           ? std::sqrt(r2 / cn2)
                           : 0.f;
        terms[2 * row + 1] = r2;
      } else {
        terms[2 * row] = norm * _inv_sqrt_rd;
        terms[2 * row + 1] = bias + estimate;
      }
    }
    return true;
  }

  void ScorePairBatch(const byte_type* base, uint32_t record_size,
                      std::span<const float> terms, uint32_t from,
                      std::span<const uint32_t> ids, score_t* out) final {
    SDB_ASSERT(SupportsPairScores());
    SDB_ASSERT(2 * size_t{from} < terms.size());
    EnsurePairState();
    const byte_type* ra = base + size_t{from} * record_size;
    const float sa = terms[2 * size_t{from}];
    const float ta = terms[2 * size_t{from} + 1];
#ifdef __AVX2__
    if (_pair_int) {
      const auto* ua = reinterpret_cast<const uint8_t*>(ra);
      for (size_t i = 0; i < ids.size(); ++i) {
        const byte_type* rb = base + size_t{ids[i]} * record_size;
        const auto* ub = reinterpret_cast<const uint8_t*>(rb);
        float sum = 0.f;
        for (size_t seg = 0; seg < _pair_nsegs; ++seg) {
          const TurboQuantInt8Seg& s = _pair_segs[seg];
          const int32_t d =
            s.bits == 2 ? TurboQuantPairDot8<2>(ua + s.byte0, ub + s.byte0, s)
                        : TurboQuantPairDot8<4>(ua + s.byte0, ub + s.byte0, s);
          sum += static_cast<float>(d) * s.inv_scale;
        }
        const float sb = LoadFloat(rb + _lay.RowNormOffset()) * _inv_sqrt_rd;
        const float tb = terms[2 * size_t{ids[i]} + 1];
        const float dot = sa * sb * sum;
        if constexpr (M == VectorMetric::L2Sqr) {
          out[i] = -(ta + tb - 2.f * dot);
        } else {
          out[i] = _cc + ta + tb + dot;
        }
      }
      return;
    }
#endif
    for (size_t i = 0; i < ids.size(); ++i) {
      SDB_ASSERT(2 * size_t{ids[i]} < terms.size());
      const byte_type* rb = base + size_t{ids[i]} * record_size;
      const float sb = LoadFloat(rb + _lay.RowNormOffset()) * _inv_sqrt_rd;
      const float tb = terms[2 * size_t{ids[i]} + 1];
      const float dot = sa * sb * PairDot(ra, rb);
      if constexpr (M == VectorMetric::L2Sqr) {
        out[i] = -(ta + tb - 2.f * dot);
      } else {
        out[i] = _cc + ta + tb + dot;
      }
    }
  }

 private:
  static float LoadFloat(const byte_type* p) noexcept {
    float v = 0.f;
    std::memcpy(&v, p, sizeof(float));
    return v;
  }

  void EnsurePairState() {
    if (!_pair_lut.empty()) {
      return;
    }
    SDB_ASSERT(_cluster);
    _cc = ComputeDistance<VectorMetric::InnerProduct>(
      _cluster, _cluster, static_cast<uint16_t>(_lay.d));
    const TurboQuantCentroids& tc = _cb->Stats().Centroids();
    const size_t segs = _lay.bits_hi == _lay.bits_lo ? 1 : 2;
#ifdef __AVX2__
    _pair_int = _lay.row_major && !_lay.full;
    for (size_t seg = 0; seg < segs; ++seg) {
      auto& s = _pair_segs[seg];
      const float scale_c = FillInt8Seg(_lay, tc, seg, s);
      s.inv_scale = scale_c > 0.f ? 1.f / (scale_c * scale_c) : 0.f;
      _pair_int = _pair_int && s.bits >= 2;
    }
    _pair_nsegs = segs;
#endif
    constexpr uint32_t kNib = 1U << kFastScanBits;
    std::array<float, size_t{kNib} * kNib> nib;
    _pair_lut.assign(segs * kPairLutSize, 0.f);
    for (size_t seg = 0; seg < segs; ++seg) {
      const float* cent = tc.cent[seg];
      const uint32_t bits = seg == 0 ? _lay.bits_hi : _lay.bits_lo;
      const uint32_t mask = (1U << bits) - 1;
      const uint32_t dims = static_cast<uint32_t>(kFastScanBits) / bits;
      for (uint32_t x = 0; x < kNib; ++x) {
        for (uint32_t y = 0; y < kNib; ++y) {
          float sum = 0.f;
          for (uint32_t t = 0; t < dims; ++t) {
            sum +=
              cent[(x >> (t * bits)) & mask] * cent[(y >> (t * bits)) & mask];
          }
          nib[size_t{x} * kNib + y] = sum;
        }
      }
      float* lut = _pair_lut.data() + seg * kPairLutSize;
      for (uint32_t u = 0; u < kByteLutSize; ++u) {
        for (uint32_t v = 0; v < kByteLutSize; ++v) {
          lut[(size_t{u} << 8) | v] =
            nib[size_t{u & 0x0FU} * kNib + (v & 0x0FU)] +
            nib[size_t{u >> 4U} * kNib + (v >> 4U)];
        }
      }
    }
  }

  float PairDot(const byte_type* a, const byte_type* b) const noexcept {
    const auto* ua = reinterpret_cast<const uint8_t*>(a);
    const auto* ub = reinterpret_cast<const uint8_t*>(b);
    const uint32_t bytes = _lay.m1 / 2;
    const uint32_t split = _lay.m_hi / 2;
    const float* lo = _pair_lut.data();
    const float* hi = _pair_lut.size() > kPairLutSize ? lo + kPairLutSize : lo;
    float s0 = 0.f;
    float s1 = 0.f;
    for (uint32_t i = 0; i < bytes; ++i) {
      const float* lut = i < split ? lo : hi;
      ((i & 1U) != 0 ? s1 : s0) += lut[(size_t{ua[i]} << 8) | ub[i]];
    }
    return s0 + s1;
  }

  void DecodeCentroids(const byte_type* code, float* out) const noexcept {
    const TurboQuantCentroids& tc = _cb->Stats().Centroids();
    for (uint32_t mi = 0; mi < _lay.m1; ++mi) {
      const uint8_t nib = GetNibble(code, mi);
      const float* cent = tc.cent[_lay.Seg(mi)];
      const uint32_t bits = _lay.NibBits(mi);
      const uint32_t mask = (1U << bits) - 1;
      const uint32_t base = _lay.NibCoord(mi);
      for (uint32_t t = 0; t < _lay.NibDims(mi); ++t) {
        out[base + t] = cent[(nib >> (t * bits)) & mask];
      }
    }
  }

  void EnsureRotatedCluster() const {
    if (!_cluster_rot.empty()) {
      return;
    }
    _cluster_rot.resize(_lay.rd);
    _cb->Stats().Rotation().Rotate(_cluster, _lay.d, _cluster_rot.data(),
                                   _fwht_scratch);
  }

  void Accumulate(const byte_type* codes, const FastScanLut& lut,
                  const std::vector<FastScanChunk>& chunks) {
    _sum.fill(0.f);
    for (size_t k = 0; k < chunks.size(); ++k) {
      const FastScanChunk& c = chunks[k];
      faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(c.nsq),
                               codes + c.code_off, lut.Packed() + c.lut_off,
                               _accu.data());
      for (size_t i = 0; i < kFastScanBbs; ++i) {
        _sum[i] += lut.Decode(k, _accu[i]);
      }
    }
  }

  score_t ScoreFrom(float ip, size_t lane, const float* bias) const noexcept {
    if constexpr (M == VectorMetric::L2Sqr) {
      return -(_qc + bias[lane] - 2.f * ip);
    } else {
      return _qc + bias[lane] + ip;
    }
  }

  score_t ScoreRow(const byte_type* r) const noexcept {
    const auto* code = reinterpret_cast<const uint8_t*>(r);
#ifdef __AVX2__
    float sum = 0.f;
    for (size_t i = 0; i < _cur->NumSegs(); ++i) {
      const TurboQuantInt8Seg& seg = _cur->Seg(i);
      const int8_t* q = _cur->Q8() + seg.q_off;
      int32_t dot = 0;
      switch (seg.bits) {
        case 1:
          dot = TurboQuantDot1(code + seg.byte0, seg, q);
          break;
        case 2:
#if defined(__x86_64__)
          if (HasAvx512Vnni()) {
            dot = TurboQuantDot8Vnni<2>(code + seg.byte0, seg, q);
            break;
          }
#endif
          dot = TurboQuantDot8<2>(code + seg.byte0, seg, q);
          break;
        default:
#if defined(__x86_64__)
          if (HasAvx512Vnni()) {
            dot = TurboQuantDot8Vnni<4>(code + seg.byte0, seg, q);
            break;
          }
#endif
          dot = TurboQuantDot8<4>(code + seg.byte0, seg, q);
          break;
      }
      sum += static_cast<float>(dot) * seg.inv_scale;
    }
#else
    const float* lut = _cur->ByteLut();
    const uint32_t bytes = _lay.code1_bytes;
    float a0 = 0.f;
    float a1 = 0.f;
    float a2 = 0.f;
    float a3 = 0.f;
    uint32_t p = 0;
    for (; p + 4 <= bytes; p += 4, lut += 4 * kByteLutSize) {
      a0 += lut[code[p]];
      a1 += lut[kByteLutSize + code[p + 1]];
      a2 += lut[2 * kByteLutSize + code[p + 2]];
      a3 += lut[3 * kByteLutSize + code[p + 3]];
    }
    for (; p < bytes; ++p, lut += kByteLutSize) {
      a0 += lut[code[p]];
    }
    const float sum = (a0 + a1) + (a2 + a3);
#endif
    return FinishRow(r, sum);
  }

  score_t FinishRow(const byte_type* r, float sum) const noexcept {
    const float ip = LoadFloat(r + _lay.RowNormOffset()) * sum * _inv_sqrt_rd;
    const float bias = LoadFloat(r + _lay.RowBiasOffset());
    if constexpr (M == VectorMetric::L2Sqr) {
      return -(_qc + bias - 2.f * ip);
    } else {
      return _qc + bias + ip;
    }
  }

#if defined(__x86_64__) && defined(__AVX2__)
  bool Rows4() const noexcept {
    if (!HasAvx512Vnni()) {
      return false;
    }
    for (size_t i = 0; i < _cur->NumSegs(); ++i) {
      if (_cur->Seg(i).bits < 2) {
        return false;
      }
    }
    return true;
  }

  void ScoreRow4(const byte_type* const* r, score_t* out) const noexcept {
    float sum[4] = {};
    for (size_t i = 0; i < _cur->NumSegs(); ++i) {
      const TurboQuantInt8Seg& seg = _cur->Seg(i);
      const int8_t* q = _cur->Q8() + seg.q_off;
      const uint8_t* codes[4];
      for (size_t k = 0; k < 4; ++k) {
        codes[k] = reinterpret_cast<const uint8_t*>(r[k]) + seg.byte0;
      }
      int32_t dot[4];
      if (seg.bits == 2) {
        TurboQuantDot8Vnni4<2>(codes, seg, q, dot);
      } else {
        TurboQuantDot8Vnni4<4>(codes, seg, q, dot);
      }
      for (size_t k = 0; k < 4; ++k) {
        sum[k] += static_cast<float>(dot[k]) * seg.inv_scale;
      }
    }
    for (size_t k = 0; k < 4; ++k) {
      out[k] = FinishRow(r[k], sum[k]);
    }
  }
#endif

  void ScoreLanes(const byte_type* c1, const byte_type* c2, const float* norms,
                  const float* gammas, const float* bias, size_t count,
                  score_t threshold, score_t* out) {
    Accumulate(c1, _cur->Lut1(), _cur->Chunks1());
    for (size_t i = 0; i < count; ++i) {
      _ip[i] = norms[i] * _sum[i] * _inv_sqrt_rd;
    }
    if (!_lay.full) {
      for (size_t i = 0; i < count; ++i) {
        out[i] = ScoreFrom(_ip[i], i, bias);
      }
      return;
    }

    const float err = _cur->QjlErrorCoeff();
    const float slack = _cur->MseSlack();
    bool refine = false;
    for (size_t i = 0; i < count; ++i) {
      const float bound = norms[i] * (err * gammas[i] + slack);
      if (ScoreFrom(_ip[i] + bound, i, bias) > threshold) {
        refine = true;
        break;
      }
    }
    if (!refine) {
      for (size_t i = 0; i < count; ++i) {
        out[i] = ScoreFrom(_ip[i], i, bias);
      }
      return;
    }

    Accumulate(c2, _cur->Lut2(), _cur->Chunks2());
    const float qjl_sum = _cur->QjlSum();
    for (size_t i = 0; i < count; ++i) {
      const float ip =
        _ip[i] + norms[i] * _qjl_coeff * gammas[i] * (2.f * _sum[i] - qjl_sum);
      out[i] = ScoreFrom(ip, i, bias);
    }
  }

  void ScoreGroup(const byte_type* codes, score_t threshold, score_t* out) {
    const auto* norms =
      reinterpret_cast<const float*>(codes + _lay.NormOffset());
    const auto* bias =
      reinterpret_cast<const float*>(codes + _lay.BiasOffset());
    const auto* gammas =
      _lay.full ? reinterpret_cast<const float*>(codes + _lay.GammaOffset())
                : nullptr;
    ScoreLanes(codes, codes + _lay.group1_bytes, norms, gammas, bias,
               kFastScanBbs, threshold, out);
  }

  template<typename Row>
  void ScoreRows(Row row, size_t count, score_t threshold, score_t* out) {
    _packed1.resize(_lay.group1_bytes);
    PackFastScanRows(row, count, 0, _lay.nsq1, _packed1.data());
    if (_lay.full) {
      _packed2.resize(_lay.group2_bytes);
      PackFastScanRows(row, count, _lay.RowCode2Offset(), _lay.nsq2,
                       _packed2.data());
    }
    for (size_t i = 0; i < count; ++i) {
      const byte_type* r = row(i);
      std::memcpy(&_rm_norms[i], r + _lay.RowNormOffset(), sizeof(float));
      if (_lay.full) {
        std::memcpy(&_rm_gammas[i], r + _lay.RowGammaOffset(), sizeof(float));
      }
      std::memcpy(&_rm_bias[i], r + _lay.RowBiasOffset(), sizeof(float));
    }
    ScoreLanes(_packed1.data(), _packed2.data(), _rm_norms.data(),
               _lay.full ? _rm_gammas.data() : nullptr, _rm_bias.data(), count,
               threshold, out);
  }

  void ScoreRecords(const byte_type* rec, size_t count, score_t threshold,
                    score_t* out) {
    const size_t stride = _lay.record_size;
    ScoreRows([rec, stride](size_t k) { return rec + k * stride; }, count,
              threshold, out);
  }

  void RefreshClusterCorrection() noexcept {
    if (!_cluster) {
      return;
    }
    const std::span<const float> query = _cur->Query();
    if constexpr (M == VectorMetric::L2Sqr) {
      _qc = -ComputeDistance<VectorMetric::L2Sqr>(
        query.data(), _cluster, static_cast<uint16_t>(query.size()));
    } else {
      _qc = ComputeDistance<VectorMetric::InnerProduct>(
        query.data(), _cluster, static_cast<uint16_t>(query.size()));
    }
  }

  std::shared_ptr<const TurboQuantizerCodebook<M>> _cb;
  const TurboQuantizerCodebook<M>* _cur;
  std::unique_ptr<TurboQuantizerCodebook<M>> _own;
  const float* _cluster = nullptr;
  mutable std::vector<float> _cluster_rot;
  mutable std::vector<float> _scale_scratch;
  mutable std::vector<float> _fwht_scratch;
  TurboQuantLayout _lay;
  float _inv_sqrt_rd;
  float _qjl_coeff;
  float _qc = 0.f;
  float _cc = 0.f;
  std::vector<float> _pair_lut;
#ifdef __AVX2__
  std::array<TurboQuantInt8Seg, 2> _pair_segs{};
  size_t _pair_nsegs = 0;
  bool _pair_int = false;
#endif
  std::vector<uint8_t> _packed1;
  std::vector<uint8_t> _packed2;
  std::array<float, kFastScanBbs> _rm_norms{};
  std::array<float, kFastScanBbs> _rm_gammas{};
  std::array<float, kFastScanBbs> _rm_bias{};
  std::array<uint16_t, kFastScanBbs> _accu{};
  std::array<float, kFastScanBbs> _sum{};
  std::array<float, kFastScanBbs> _ip{};
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> TurboQuantizerCodebook<M>::MakeReader() const {
  return MakeReaderT<TurboQuantizerCodebook<M>, TurboQuantizerReader<M>>(this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> TurboQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<TurboQuantizerStats<M>, TurboQuantizerCodebook<M>>(
    this, query);
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeTurboQuantWriter(VectorMetric metric,
                                                      uint32_t d,
                                                      uint32_t nb_bits,
                                                      bool half_bit,
                                                      bool row_major) {
  const auto bits = MakeTurboQuantBits(nb_bits, half_bit);
  SDB_ASSERT(bits);
  return MakeWriterWithMetric<TurboQuantizerWriter>(metric, d, *bits,
                                                    row_major);
}

std::shared_ptr<const QuantizerStats> MakeTurboQuantStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats,
  bool row_major) {
  return MakeStatsWithMetric<TurboQuantizerStats>(metric, d, stats, row_major);
}

}  // namespace irs::quantizers
