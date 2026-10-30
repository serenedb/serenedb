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

#include <limits>

#include "iresearch/formats/ivf/quantizers/common.hpp"

namespace irs::quantizers {
namespace {

struct OsqStatsHeader {
  uint8_t layout;
  uint8_t bits;
  uint8_t query_bits;
  uint8_t reserved;
  uint32_t d;
};

static_assert(sizeof(OsqStatsHeader) == 8);
static_assert(std::has_unique_object_representations_v<OsqStatsHeader>);

constexpr uint8_t kOsqLayout = 1;
constexpr double kOsqLambda = 0.1;
constexpr uint32_t kOsqIters = 5;
constexpr std::array<double, 8> kOsqGrid{0.798, 1.493, 2.051, 2.514,
                                         2.916, 3.278, 3.611, 3.922};

constexpr double OsqStepVariance(uint32_t bits) noexcept {
  switch (bits) {
    case 1:
      return 0.1427;
    case 2:
      return 0.1199;
    default:
      return 1.0 / 12.0;
  }
}

struct OsqTerms {
  float lower = 0.f;
  float step = 0.f;
  float extra = 0.f;
  float sum = 0.f;
};

static_assert(sizeof(OsqTerms) == 4 * sizeof(float));

enum class OsqPacking : uint8_t {
  Planes,
  Nibbles,
  Bytes,
};

struct OsqLayout {
  uint32_t d = 0;
  uint32_t bits = 1;
  uint32_t query_bits = 4;
  OsqPacking packing = OsqPacking::Planes;
  uint32_t plane_bytes = 0;
  uint32_t half = 0;
  uint32_t code_bytes = 0;
  uint32_t record_size = 0;
  uint32_t query_size = 0;
  int32_t query_offset = 0;
};

OsqLayout MakeOsqLayout(uint32_t d, uint32_t bits,
                        uint32_t query_bits) noexcept {
  OsqLayout l;
  l.d = d;
  l.bits = bits;
  l.query_bits = query_bits;
  l.plane_bytes = (d + 7) / 8;
  if (bits <= 2) {
    l.packing = OsqPacking::Planes;
    l.code_bytes = bits * l.plane_bytes;
    l.query_size = l.plane_bytes * 8;
  } else if (bits == 4) {
    l.packing = OsqPacking::Nibbles;
    l.half = (d + 1) / 2;
    l.code_bytes = l.half;
    l.query_size = 2 * l.half;
  } else {
    l.packing = OsqPacking::Bytes;
    l.code_bytes = d;
    l.query_size = d;
  }
  l.record_size = l.code_bytes + static_cast<uint32_t>(sizeof(OsqTerms));
  l.query_offset = query_bits == 8 ? 128 : 0;
  return l;
}

double OsqLoss(const float* v, uint32_t d, double a, double b, double points,
               double norm2) noexcept {
  const double step = (b - a) / (points - 1.0);
  const double inv = 1.0 / step;
  double xe = 0.0;
  double e = 0.0;
  for (uint32_t i = 0; i < d; ++i) {
    const double x = v[i];
    const double xq = a + step * std::round((std::clamp(x, a, b) - a) * inv);
    xe += x * (x - xq);
    e += (x - xq) * (x - xq);
  }
  return (1.0 - kOsqLambda) * xe * xe / norm2 + kOsqLambda * e;
}

OsqTerms OsqQuantize(const float* v, uint32_t d, uint32_t bits,
                     uint8_t* codes) noexcept {
  double mean = 0.0;
  double m2 = 0.0;
  double norm2 = 0.0;
  double lo = std::numeric_limits<double>::max();
  double hi = std::numeric_limits<double>::lowest();
  for (uint32_t i = 0; i < d; ++i) {
    const double x = v[i];
    lo = std::min(lo, x);
    hi = std::max(hi, x);
    norm2 += x * x;
    const double delta = x - mean;
    mean += delta / static_cast<double>(i + 1);
    m2 += delta * (x - mean);
  }
  const double sd = d != 0 ? std::sqrt(m2 / d) : 0.0;
  const double grid = kOsqGrid[bits - 1];
  double a = d != 0 ? std::clamp(mean - grid * sd, lo, hi) : 0.0;
  double b = d != 0 ? std::clamp(mean + grid * sd, lo, hi) : 0.0;
  const double points = static_cast<double>(1U << bits);
  if (b > a && norm2 > 0.0) {
    double loss = OsqLoss(v, d, a, b, points, norm2);
    const double scale = (1.0 - kOsqLambda) / norm2;
    for (uint32_t it = 0; it < kOsqIters; ++it) {
      const double inv = (points - 1.0) / (b - a);
      double daa = 0.0;
      double dab = 0.0;
      double dbb = 0.0;
      double dax = 0.0;
      double dbx = 0.0;
      for (uint32_t i = 0; i < d; ++i) {
        const double x = v[i];
        const double s =
          std::round((std::clamp(x, a, b) - a) * inv) / (points - 1.0);
        daa += (1.0 - s) * (1.0 - s);
        dab += (1.0 - s) * s;
        dbb += s * s;
        dax += x * (1.0 - s);
        dbx += x * s;
      }
      const double m0 = scale * dax * dax + kOsqLambda * daa;
      const double m1 = scale * dax * dbx + kOsqLambda * dab;
      const double mb = scale * dbx * dbx + kOsqLambda * dbb;
      const double det = m0 * mb - m1 * m1;
      if (det == 0.0) {
        break;
      }
      const double na = (mb * dax - m1 * dbx) / det;
      const double nb = (m0 * dbx - m1 * dax) / det;
      if (std::fabs(a - na) < 1e-8 && std::fabs(b - nb) < 1e-8) {
        break;
      }
      if (!(nb > na)) {
        break;
      }
      const double next = OsqLoss(v, d, na, nb, points, norm2);
      if (next > loss) {
        break;
      }
      a = na;
      b = nb;
      loss = next;
    }
  }
  const double step = b > a ? (b - a) / (points - 1.0) : 0.0;
  const double inv = step > 0.0 ? 1.0 / step : 0.0;
  uint32_t sum = 0;
  for (uint32_t i = 0; i < d; ++i) {
    const auto k = static_cast<uint32_t>(
      std::lround((std::clamp(static_cast<double>(v[i]), a, b) - a) * inv));
    codes[i] = static_cast<uint8_t>(k);
    sum += k;
  }
  return {.lower = static_cast<float>(a),
          .step = static_cast<float>(step),
          .extra = static_cast<float>(norm2),
          .sum = static_cast<float>(sum)};
}

#ifdef __AVX2__
int32_t OsqHorizontalSum(__m256i acc) noexcept {
  const __m128i h = _mm_add_epi32(_mm256_castsi256_si128(acc),
                                  _mm256_extracti128_si256(acc, 1));
  const __m128i h2 = _mm_add_epi32(h, _mm_shuffle_epi32(h, 0x4E));
  return _mm_cvtsi128_si32(_mm_add_epi32(h2, _mm_shuffle_epi32(h2, 0xB1)));
}
#endif

int32_t OsqNibbleDot(const uint8_t* code, uint32_t half, const int8_t* qlo,
                     const int8_t* qhi) noexcept {
  int32_t sum = 0;
  uint32_t j = 0;
#ifdef __AVX2__
  const __m256i mask = _mm256_set1_epi8(0x0F);
  const __m256i ones = _mm256_set1_epi16(1);
  __m256i acc = _mm256_setzero_si256();
  for (; j + 32 <= half; j += 32) {
    const __m256i v =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(code + j));
    const __m256i lo = _mm256_and_si256(v, mask);
    const __m256i hi = _mm256_and_si256(_mm256_srli_epi16(v, 4), mask);
    const __m256i plo = _mm256_maddubs_epi16(
      lo, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(qlo + j)));
    const __m256i phi = _mm256_maddubs_epi16(
      hi, _mm256_loadu_si256(reinterpret_cast<const __m256i*>(qhi + j)));
    acc = _mm256_add_epi32(acc,
                           _mm256_madd_epi16(_mm256_add_epi16(plo, phi), ones));
  }
  sum = OsqHorizontalSum(acc);
#endif
  for (; j < half; ++j) {
    sum += int32_t{static_cast<uint8_t>(code[j] & 0x0F)} * qlo[j] +
           int32_t{static_cast<uint8_t>(code[j] >> 4)} * qhi[j];
  }
  return sum;
}

int32_t OsqByteDot(const uint8_t* code, uint32_t d, const int8_t* q,
                   bool wide) noexcept {
  int32_t sum = 0;
  uint32_t i = 0;
#ifdef __AVX2__
  const __m256i ones = _mm256_set1_epi16(1);
  __m256i acc = _mm256_setzero_si256();
  if (wide) {
    for (; i + 16 <= d; i += 16) {
      const __m256i x = _mm256_cvtepu8_epi16(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(code + i)));
      const __m256i y = _mm256_cvtepi8_epi16(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(q + i)));
      acc = _mm256_add_epi32(acc, _mm256_madd_epi16(x, y));
    }
  } else {
    for (; i + 32 <= d; i += 32) {
      const __m256i x =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(code + i));
      const __m256i y =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(q + i));
      acc = _mm256_add_epi32(
        acc, _mm256_madd_epi16(_mm256_maddubs_epi16(x, y), ones));
    }
  }
  sum = OsqHorizontalSum(acc);
#endif
  for (; i < d; ++i) {
    sum += int32_t{code[i]} * q[i];
  }
  return sum;
}

uint32_t OsqAndPopcount(const uint8_t* a, const uint8_t* b,
                        uint32_t bytes) noexcept {
  uint32_t count = 0;
  uint32_t p = 0;
  for (; p + 8 <= bytes; p += 8) {
    uint64_t x = 0;
    uint64_t y = 0;
    std::memcpy(&x, a + p, sizeof(x));
    std::memcpy(&y, b + p, sizeof(y));
    count += static_cast<uint32_t>(std::popcount(x & y));
  }
  for (; p < bytes; ++p) {
    count +=
      static_cast<uint32_t>(std::popcount(static_cast<uint32_t>(a[p] & b[p])));
  }
  return count;
}

int32_t OsqNibblePair(const uint8_t* a, const uint8_t* b,
                      uint32_t half) noexcept {
  int32_t sum = 0;
  uint32_t j = 0;
#ifdef __AVX2__
  const __m256i mask = _mm256_set1_epi8(0x0F);
  const __m256i ones = _mm256_set1_epi16(1);
  __m256i acc = _mm256_setzero_si256();
  for (; j + 32 <= half; j += 32) {
    const __m256i va =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + j));
    const __m256i vb =
      _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + j));
    const __m256i plo = _mm256_maddubs_epi16(_mm256_and_si256(va, mask),
                                             _mm256_and_si256(vb, mask));
    const __m256i phi =
      _mm256_maddubs_epi16(_mm256_and_si256(_mm256_srli_epi16(va, 4), mask),
                           _mm256_and_si256(_mm256_srli_epi16(vb, 4), mask));
    acc = _mm256_add_epi32(acc,
                           _mm256_madd_epi16(_mm256_add_epi16(plo, phi), ones));
  }
  sum = OsqHorizontalSum(acc);
#endif
  for (; j < half; ++j) {
    sum += int32_t{static_cast<uint8_t>(a[j] & 0x0F)} * (b[j] & 0x0F) +
           int32_t{static_cast<uint8_t>(a[j] >> 4)} * (b[j] >> 4);
  }
  return sum;
}

int32_t OsqBytePair(const uint8_t* a, const uint8_t* b, uint32_t d,
                    bool wide) noexcept {
  int32_t sum = 0;
  uint32_t i = 0;
#ifdef __AVX2__
  const __m256i ones = _mm256_set1_epi16(1);
  __m256i acc = _mm256_setzero_si256();
  if (wide) {
    for (; i + 16 <= d; i += 16) {
      const __m256i x = _mm256_cvtepu8_epi16(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(a + i)));
      const __m256i y = _mm256_cvtepu8_epi16(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(b + i)));
      acc = _mm256_add_epi32(acc, _mm256_madd_epi16(x, y));
    }
  } else {
    for (; i + 32 <= d; i += 32) {
      const __m256i x =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(a + i));
      const __m256i y =
        _mm256_loadu_si256(reinterpret_cast<const __m256i*>(b + i));
      acc = _mm256_add_epi32(
        acc, _mm256_madd_epi16(_mm256_maddubs_epi16(x, y), ones));
    }
  }
  sum = OsqHorizontalSum(acc);
#endif
  for (; i < d; ++i) {
    sum += int32_t{a[i]} * b[i];
  }
  return sum;
}

int32_t OsqPairDot(const OsqLayout& lay, const uint8_t* a,
                   const uint8_t* b) noexcept {
  switch (lay.packing) {
    case OsqPacking::Planes: {
      const uint32_t pb = lay.plane_bytes;
      const auto low = static_cast<int32_t>(OsqAndPopcount(a, b, pb));
      if (lay.bits == 1) {
        return low;
      }
      const auto cross = static_cast<int32_t>(OsqAndPopcount(a, b + pb, pb) +
                                              OsqAndPopcount(a + pb, b, pb));
      const auto high =
        static_cast<int32_t>(OsqAndPopcount(a + pb, b + pb, pb));
      return low + 2 * cross + 4 * high;
    }
    case OsqPacking::Nibbles:
      return OsqNibblePair(a, b, lay.half);
    case OsqPacking::Bytes:
      return OsqBytePair(a, b, lay.d, lay.bits == 8);
  }
  return 0;
}

uint32_t OsqCodeAt(const OsqLayout& lay, const uint8_t* code,
                   uint32_t i) noexcept {
  switch (lay.packing) {
    case OsqPacking::Planes: {
      uint32_t k = 0;
      for (uint32_t p = 0; p < lay.bits; ++p) {
        k |= (BqBit(code + p * lay.plane_bytes, i) ? 1U : 0U) << p;
      }
      return k;
    }
    case OsqPacking::Nibbles:
      return i < lay.half ? code[i] & 0x0FU : code[i - lay.half] >> 4;
    case OsqPacking::Bytes:
      return code[i];
  }
  return 0;
}

void OsqPack(const OsqLayout& lay, const uint8_t* k, uint8_t* out) noexcept {
  std::fill_n(out, lay.code_bytes, uint8_t{0});
  switch (lay.packing) {
    case OsqPacking::Planes:
      for (uint32_t i = 0; i < lay.d; ++i) {
        for (uint32_t p = 0; p < lay.bits; ++p) {
          if (((k[i] >> p) & 1U) != 0) {
            uint8_t& byte = out[p * lay.plane_bytes + i / 8];
            byte = static_cast<uint8_t>(byte | (1U << (i % 8)));
          }
        }
      }
      return;
    case OsqPacking::Nibbles:
      for (uint32_t i = 0; i < lay.d; ++i) {
        if (i < lay.half) {
          out[i] = static_cast<uint8_t>(out[i] | k[i]);
        } else {
          out[i - lay.half] =
            static_cast<uint8_t>(out[i - lay.half] | (k[i] << 4));
        }
      }
      return;
    case OsqPacking::Bytes:
      std::copy_n(k, lay.d, out);
      return;
  }
}

template<VectorMetric M>
class OsqQuantizerWriter final : public QuantizerWriter {
 public:
  OsqQuantizerWriter(uint32_t d, uint32_t bits, uint32_t query_bits)
    : _lay{MakeOsqLayout(d, bits, query_bits)},
      _centroid(d, 0.f),
      _res(d),
      _codes(d) {}

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    std::copy_n(centroid, _lay.d, _centroid.data());
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1, .record_size = _lay.record_size};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    _row.resize(_lay.record_size);
    for (size_t r = 0; r < n; ++r) {
      EncodeRow(vecs + r * _lay.d, _row.data());
      out.WriteData(_row.data(), _row.size());
    }
  }

  bool EncodeInto(byte_type* dst, const float* vecs, size_t n) final {
    for (size_t r = 0; r < n; ++r) {
      EncodeRow(vecs + r * _lay.d, dst + r * _lay.record_size);
    }
    return true;
  }

  std::unique_ptr<QuantizerWriter> CloneForEncode() const final {
    auto out =
      std::make_unique<OsqQuantizerWriter>(_lay.d, _lay.bits, _lay.query_bits);
    out->_centroid = _centroid;
    return out;
  }

  void Finish(IndexOutput& /*out*/) final {}

  void Serialize(DataOutput& out) const final {
    out.WriteU64(sizeof(OsqStatsHeader));
    WritePod(out,
             OsqStatsHeader{.layout = kOsqLayout,
                            .bits = static_cast<uint8_t>(_lay.bits),
                            .query_bits = static_cast<uint8_t>(_lay.query_bits),
                            .reserved = 0,
                            .d = _lay.d});
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::OSQ;
  }

 private:
  void EncodeRow(const float* x, byte_type* out) {
    double centroid_dot = 0.0;
    for (uint32_t i = 0; i < _lay.d; ++i) {
      _res[i] = x[i] - _centroid[i];
      centroid_dot += double{x[i]} * _centroid[i];
    }
    OsqTerms terms = OsqQuantize(_res.data(), _lay.d, _lay.bits, _codes.data());
    if constexpr (M == VectorMetric::InnerProduct) {
      terms.extra = static_cast<float>(centroid_dot);
    }
    OsqPack(_lay, _codes.data(), reinterpret_cast<uint8_t*>(out));
    std::memcpy(out + _lay.code_bytes, &terms, sizeof(terms));
  }

  OsqLayout _lay;
  std::vector<float> _centroid;
  std::vector<float> _res;
  std::vector<uint8_t> _codes;
  std::vector<byte_type> _row;
};

template<VectorMetric M>
class OsqQuantizerStats final : public QuantizerStats {
 public:
  OsqQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    const auto hdr = ReadPodHeader<OsqStatsHeader>(stats);
    SDB_ASSERT(hdr.layout == kOsqLayout);
    SDB_ASSERT(hdr.d == d);
    SDB_ASSERT(OSQBitsValid(hdr.bits));
    _lay = MakeOsqLayout(d, hdr.bits, hdr.query_bits);
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::OSQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const OsqLayout& Layout() const noexcept { return _lay; }

 private:
  OsqLayout _lay;
};

template<VectorMetric M>
class OsqQuantizerCodebook final : public QuantizerCodebook {
 public:
  OsqQuantizerCodebook(std::shared_ptr<const OsqQuantizerStats<M>> stats,
                       std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {}

  void Rekey(std::span<const float> query) {
    _query.assign(query.begin(), query.end());
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const std::shared_ptr<const OsqQuantizerStats<M>>& StatsPtr() const noexcept {
    return _stats;
  }
  const OsqQuantizerStats<M>& Stats() const noexcept { return *_stats; }
  std::span<const float> Query() const noexcept { return _query; }

 private:
  std::shared_ptr<const OsqQuantizerStats<M>> _stats;
  std::vector<float> _query;
};

template<VectorMetric M>
class OsqQuantizerReader final : public QuantizerReader {
 public:
  explicit OsqQuantizerReader(std::shared_ptr<const OsqQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)},
      _cur{_cb.get()},
      _lay{_cb->Stats().Layout()},
      _q(_lay.query_size, 0),
      _res(_lay.d),
      _codes(_lay.d) {}

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1, .record_size = _lay.record_size};
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid);
    _cluster = centroid;
    double cc = 0.0;
    for (uint32_t i = 0; i < _lay.d; ++i) {
      cc += double{centroid[i]} * centroid[i];
    }
    _centroid_dot = static_cast<float>(cc);
    KeyQuery();
  }

  void ComputeBlock(std::span<const byte_type> block, score_t /*threshold*/,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _lay.record_size == 0);
    const size_t rows = block.size() / _lay.record_size;
    for (size_t i = 0; i < rows; ++i) {
      out[i] = ScoreRow(block.data() + i * _lay.record_size);
    }
  }

  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t /*threshold*/,
                       score_t* out) final {
    ScoreGatheredRows(base, record_size, ids, out,
                      [this](const byte_type* r) { return ScoreRow(r); });
  }

  void ScoreErrors(const byte_type* base, uint32_t record_size,
                   std::span<const uint32_t> ids, std::span<score_t> /*scores*/,
                   score_t* errors) final {
    Errors(
      [&](size_t i) {
        return base + static_cast<size_t>(ids[i]) * record_size;
      },
      ids.size(), errors);
  }

  void BlockErrors(std::span<const byte_type> block, std::span<score_t> scores,
                   score_t* errors) final {
    Errors([&](size_t i) { return block.data() + i * _lay.record_size; },
           scores.size(), errors);
  }

  bool Decode(const byte_type* code, float* out) const final {
    if (!_cluster) {
      return false;
    }
    const OsqTerms t = RowTerms(code);
    const auto* c = reinterpret_cast<const uint8_t*>(code);
    float scale = 1.f;
    if constexpr (M == VectorMetric::L2Sqr) {
      double norm2 = 0.0;
      for (uint32_t i = 0; i < _lay.d; ++i) {
        const float v =
          t.lower + t.step * static_cast<float>(OsqCodeAt(_lay, c, i));
        norm2 += double{v} * v;
      }
      if (norm2 > 0.0) {
        scale = static_cast<float>(std::sqrt(t.extra / norm2));
      }
    }
    for (uint32_t i = 0; i < _lay.d; ++i) {
      out[i] =
        _cluster[i] +
        scale * (t.lower + t.step * static_cast<float>(OsqCodeAt(_lay, c, i)));
    }
    return true;
  }

  bool SetQuery(std::span<const float> query) final {
    if (!_own) {
      _own = std::make_unique<OsqQuantizerCodebook<M>>(_cb->StatsPtr(), query);
      _cur = _own.get();
    } else {
      _own->Rekey(query);
    }
    if (_cluster) {
      KeyQuery();
    }
    return true;
  }

  bool SupportsPairScores() const noexcept final { return _cluster; }

  bool PreparePairTerms(const byte_type* /*base*/, uint32_t /*record_size*/,
                        uint64_t rows, std::vector<float>& terms) final {
    if (!SupportsPairScores()) {
      return false;
    }
    terms.assign(rows, 0.f);
    return rows != 0;
  }

  void ScorePairBatch(const byte_type* base, uint32_t record_size,
                      std::span<const float> /*terms*/, uint32_t from,
                      std::span<const uint32_t> ids, score_t* out) final {
    const byte_type* a = base + size_t{from} * record_size;
    const OsqTerms ta = RowTerms(a);
    const float d = static_cast<float>(_lay.d);
    for (size_t k = 0; k < ids.size(); ++k) {
      const byte_type* b = base + static_cast<size_t>(ids[k]) * record_size;
      const OsqTerms tb = RowTerms(b);
      const auto both =
        static_cast<float>(OsqPairDot(_lay, reinterpret_cast<const uint8_t*>(a),
                                      reinterpret_cast<const uint8_t*>(b)));
      const float s = d * ta.lower * tb.lower + ta.lower * tb.step * tb.sum +
                      tb.lower * ta.step * ta.sum + ta.step * tb.step * both;
      out[k] = Finish(s, ta.extra, tb.extra);
    }
  }

 private:
  template<typename Row>
  void Errors(Row row, size_t n, score_t* errors) const {
    constexpr float kMetric = M == VectorMetric::L2Sqr ? 2.f : 1.f;
    const auto d = static_cast<float>(_lay.d);
    const auto span = static_cast<float>((1U << _lay.bits) - 1);
    const auto grid = static_cast<float>(kOsqGrid[_lay.bits - 1]);
    const auto row_unit = static_cast<float>(OsqStepVariance(_lay.bits));
    const auto query_norm2 = static_cast<float>(_query_norm2);
    const float query_var = _query.step * _query.step / 12.f;
    for (size_t i = 0; i < n; ++i) {
      const OsqTerms t = RowTerms(row(i));
      float row_norm2 = 0.f;
      if constexpr (M == VectorMetric::L2Sqr) {
        row_norm2 = t.extra;
      } else {
        const float mean = t.lower + t.step * span / 2.f;
        const float sd = t.step * span / (2.f * grid);
        row_norm2 = d * (mean * mean + sd * sd);
      }
      const float row_var = row_unit * t.step * t.step;
      errors[i] =
        kMetric * std::sqrt(row_norm2 * query_var + query_norm2 * row_var);
    }
  }

  OsqTerms RowTerms(const byte_type* code) const noexcept {
    OsqTerms t;
    std::memcpy(&t, code + _lay.code_bytes, sizeof(t));
    return t;
  }

  score_t Finish(float s, float query_extra, float row_extra) const noexcept {
    if constexpr (M == VectorMetric::L2Sqr) {
      return -(query_extra + row_extra - 2.f * s);
    } else {
      return s + query_extra + row_extra - _centroid_dot;
    }
  }

  void KeyQuery() {
    const auto query = _cur->Query();
    double centroid_dot = 0.0;
    double norm2 = 0.0;
    for (uint32_t i = 0; i < _lay.d; ++i) {
      _res[i] = query[i] - _cluster[i];
      centroid_dot += double{query[i]} * _cluster[i];
      norm2 += double{_res[i]} * _res[i];
    }
    _query_norm2 = norm2;
    _query = OsqQuantize(_res.data(), _lay.d, _lay.query_bits, _codes.data());
    if constexpr (M == VectorMetric::InnerProduct) {
      _query.extra = static_cast<float>(centroid_dot);
    }
    std::fill(_q.begin(), _q.end(), int8_t{0});
    for (uint32_t i = 0; i < _lay.d; ++i) {
      _q[i] = static_cast<int8_t>(int32_t{_codes[i]} - _lay.query_offset);
    }
  }

  int32_t CodeDot(const uint8_t* code) const noexcept {
    switch (_lay.packing) {
      case OsqPacking::Planes: {
        int32_t dot = BitPlaneSum(code, _lay.plane_bytes, _q.data());
        if (_lay.bits == 2) {
          dot += 2 * BitPlaneSum(code + _lay.plane_bytes, _lay.plane_bytes,
                                 _q.data());
        }
        return dot;
      }
      case OsqPacking::Nibbles:
        return OsqNibbleDot(code, _lay.half, _q.data(), _q.data() + _lay.half);
      case OsqPacking::Bytes:
        return OsqByteDot(code, _lay.d, _q.data(), _lay.bits == 8);
    }
    return 0;
  }

  score_t ScoreRow(const byte_type* r) const noexcept {
    const OsqTerms t = RowTerms(r);
    const auto dot =
      static_cast<float>(CodeDot(reinterpret_cast<const uint8_t*>(r)) +
                         _lay.query_offset * static_cast<int32_t>(t.sum));
    const float s = static_cast<float>(_lay.d) * t.lower * _query.lower +
                    _query.lower * t.step * t.sum +
                    t.lower * _query.step * _query.sum +
                    t.step * _query.step * dot;
    return Finish(s, _query.extra, t.extra);
  }

  std::shared_ptr<const OsqQuantizerCodebook<M>> _cb;
  const OsqQuantizerCodebook<M>* _cur;
  std::unique_ptr<OsqQuantizerCodebook<M>> _own;
  OsqLayout _lay;
  const float* _cluster = nullptr;
  float _centroid_dot = 0.f;
  double _query_norm2 = 0.0;
  OsqTerms _query;
  std::vector<int8_t> _q;
  std::vector<float> _res;
  std::vector<uint8_t> _codes;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> OsqQuantizerCodebook<M>::MakeReader() const {
  return MakeReaderT<OsqQuantizerCodebook<M>, OsqQuantizerReader<M>>(this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> OsqQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<OsqQuantizerStats<M>, OsqQuantizerCodebook<M>>(this,
                                                                      query);
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeOsqWriter(VectorMetric metric, uint32_t d,
                                               uint32_t nb_bits,
                                               uint32_t query_bits) {
  const uint32_t bits = nb_bits == 0 ? kOSQDefaultBits : nb_bits;
  return MakeWriterWithMetric<OsqQuantizerWriter>(
    metric, d, bits, query_bits == 0 ? OSQDefaultQueryBits(bits) : query_bits);
}

std::shared_ptr<const QuantizerStats> MakeOsqStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> stats) {
  return MakeStatsWithMetric<OsqQuantizerStats>(metric, d, stats);
}

}  // namespace irs::quantizers
