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

#include <faiss/MetricType.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/impl/scalar_quantizer/sq8_batch.h>

#include <limits>

#include "iresearch/formats/ivf/quantizers/common.hpp"

namespace irs::quantizers {
namespace {

faiss::ScalarQuantizer::QuantizerType FaissScalarType(
  VectorQuantization quant) {
  switch (quant) {
    case VectorQuantization::SQ4:
      return faiss::ScalarQuantizer::QuantizerType::QT_4bit;
    case VectorQuantization::USQ8:
      return faiss::ScalarQuantizer::QuantizerType::QT_8bit_uniform;
    case VectorQuantization::USQ4:
      return faiss::ScalarQuantizer::QuantizerType::QT_4bit_uniform;
    case VectorQuantization::FP16:
      return faiss::ScalarQuantizer::QuantizerType::QT_fp16;
    case VectorQuantization::BF16:
      return faiss::ScalarQuantizer::QuantizerType::QT_bf16;
    default:
      return faiss::ScalarQuantizer::QuantizerType::QT_8bit;
  }
}

constexpr size_t kEncodeTile = kFastScanBbs;

constexpr uint32_t kSq8LevelsAhead = 2;
constexpr size_t kSq8RowsAhead = 4;
constexpr size_t kSq8RowLines = 4;

struct Sq8Levels {
  const float* a;
  uint32_t d;
  uint32_t width;
  uint32_t query_step;
  float bias;
  const float* level_bias;
  const float* query;
};

#if defined(__x86_64__) && defined(__AVX2__)
__attribute__((target("avx512f,avx512bw,avx512vl"))) score_t
Sq8ScoreLevels512(const Sq8Levels& lv, const uint8_t* code, const float* tails,
                  score_t threshold) noexcept {
  __m512 acc0 = _mm512_setzero_ps();
  __m512 acc1 = _mm512_setzero_ps();
  uint32_t b = 0;
  for (uint32_t k = 0;; ++k) {
    const uint32_t end = std::min(lv.d, b + lv.width);
    const uint32_t ahead = b + kSq8LevelsAhead * lv.width;
    for (uint32_t o = ahead; o < std::min(lv.d, ahead + lv.width); o += 64) {
      __builtin_prefetch(code + o, 0, 3);
    }
    for (; b + 32 <= end; b += 32) {
      const __m512 c0 = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(code + b))));
      const __m512 c1 = _mm512_cvtepi32_ps(_mm512_cvtepu8_epi32(
        _mm_loadu_si128(reinterpret_cast<const __m128i*>(code + b + 16))));
      acc0 = _mm512_fmadd_ps(c0, _mm512_loadu_ps(lv.a + b), acc0);
      acc1 = _mm512_fmadd_ps(c1, _mm512_loadu_ps(lv.a + b + 16), acc1);
    }
    for (; b < end; b += 16) {
      const __mmask16 live = end - b >= 16
                               ? __mmask16{0xFFFF}
                               : static_cast<__mmask16>((1U << (end - b)) - 1);
      const __m512 c = _mm512_cvtepi32_ps(
        _mm512_cvtepu8_epi32(_mm_maskz_loadu_epi8(live, code + b)));
      acc0 = _mm512_fmadd_ps(c, _mm512_maskz_loadu_ps(live, lv.a + b), acc0);
    }
    b = end;
    const float dot = _mm512_reduce_add_ps(_mm512_add_ps(acc0, acc1));
    if (b == lv.d) {
      return lv.bias + dot;
    }
    if (dot + lv.level_bias[k + 1] + lv.query[b / lv.query_step] * tails[k] <
        threshold) {
      return std::numeric_limits<score_t>::lowest();
    }
  }
}

score_t Sq8ScoreLevels(const Sq8Levels& lv, const uint8_t* code,
                       const float* tails, score_t threshold) noexcept {
  __m256 acc0 = _mm256_setzero_ps();
  __m256 acc1 = _mm256_setzero_ps();
  uint32_t b = 0;
  for (uint32_t k = 0;; ++k) {
    const uint32_t end = std::min(lv.d, b + lv.width);
    const uint32_t ahead = b + kSq8LevelsAhead * lv.width;
    for (uint32_t o = ahead; o < std::min(lv.d, ahead + lv.width); o += 64) {
      __builtin_prefetch(code + o, 0, 3);
    }
    for (; b + 16 <= end; b += 16) {
      const __m256 c0 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(code + b))));
      const __m256 c1 = _mm256_cvtepi32_ps(_mm256_cvtepu8_epi32(
        _mm_loadl_epi64(reinterpret_cast<const __m128i*>(code + b + 8))));
      acc0 = _mm256_fmadd_ps(c0, _mm256_loadu_ps(lv.a + b), acc0);
      acc1 = _mm256_fmadd_ps(c1, _mm256_loadu_ps(lv.a + b + 8), acc1);
    }
    const __m256 v = _mm256_add_ps(acc0, acc1);
    __m128 h =
      _mm_add_ps(_mm256_castps256_ps128(v), _mm256_extractf128_ps(v, 1));
    h = _mm_add_ps(h, _mm_movehl_ps(h, h));
    h = _mm_add_ss(h, _mm_movehdup_ps(h));
    float dot = _mm_cvtss_f32(h);
    for (; b < end; ++b) {
      dot += lv.a[b] * static_cast<float>(code[b]);
    }
    if (b == lv.d) {
      return lv.bias + dot;
    }
    if (dot + lv.level_bias[k + 1] + lv.query[b / lv.query_step] * tails[k] <
        threshold) {
      return std::numeric_limits<score_t>::lowest();
    }
  }
}
#endif

class ScalarQuantizerWriter final : public QuantizerWriter {
 public:
  ScalarQuantizerWriter(uint32_t d, VectorQuantization quant)
    : _d{d},
      _quant{quant},
      _sq{d, FaissScalarType(quant)},
      _vmin(d, std::numeric_limits<float>::max()),
      _vmax(d, std::numeric_limits<float>::lowest()) {
    // faiss lays `trained` out per quantizer: [vmin(d), vdiff(d)] for the
    // per-dimension types, [vmin, vdiff] for the uniform ones.
    _sq.trained.assign(TrainedSize(_quant, _d), 0.f);
  }

  /// Floats faiss expects in `trained` for this quantizer.
  static size_t TrainedSize(VectorQuantization quant, uint32_t d) noexcept {
    if (IsHalfFloat(quant)) {
      return 0;
    }
    return IsUniformScalar(quant) ? 2 : 2 * static_cast<size_t>(d);
  }

  size_t TrainSamples(size_t /*rows*/) const noexcept final {
    return IsHalfFloat(_quant) ? 0 : kTrainStreaming;
  }

  void Train(const float* vecs, size_t n) final {
    if (n == 0 || IsHalfFloat(_quant)) {
      return;
    }
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _vmin[j] = std::min(_vmin[j], v[j]);
        _vmax[j] = std::max(_vmax[j], v[j]);
      }
    }
    if (IsUniformScalar(_quant)) {
      // One range for the whole vector: the widest span any dimension needs.
      const float lo = *std::min_element(_vmin.begin(), _vmin.end());
      const float hi = *std::max_element(_vmax.begin(), _vmax.end());
      _sq.trained[0] = lo;
      _sq.trained[1] = hi - lo;
      return;
    }
    for (uint32_t j = 0; j < _d; ++j) {
      _sq.trained[j] = _vmin[j];
      _sq.trained[_d + j] = _vmax[j] - _vmin[j];
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1,
            .record_size = static_cast<uint32_t>(_sq.code_size)};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    if (_codes.empty()) {
      _codes.resize(kEncodeTile * _sq.code_size);
    }
    for (size_t off = 0; off < n; off += kEncodeTile) {
      const size_t count = std::min(kEncodeTile, n - off);
      _sq.compute_codes(vecs + off * _d, _codes.data(), count);
      out.WriteData(_codes.data(), count * _sq.code_size);
    }
  }

  bool EncodeInto(byte_type* dst, const float* vecs, size_t n) final {
    _sq.compute_codes(vecs, dst, n);
    return true;
  }

  void Finish(IndexOutput& /*out*/) final {}

  void Serialize(DataOutput& out) const final {
    const auto bytes = FloatSpan(_sq.trained);
    out.WriteU64(bytes.size());
    if (!bytes.empty()) {
      out.WriteData(bytes.data(), bytes.size());
    }
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

 private:
  uint32_t _d;
  VectorQuantization _quant;
  faiss::ScalarQuantizer _sq;
  std::vector<float> _vmin;
  std::vector<float> _vmax;
  std::vector<uint8_t> _codes;
};

class ScalarQuantizerCodeBound final : public QuantizerCodeBound {
 public:
  ScalarQuantizerCodeBound(const faiss::ScalarQuantizer& sq,
                           std::span<const float> query, VectorMetric metric)
    : _l2{metric == VectorMetric::L2Sqr} {
    faiss::scalar_quantizer::sq8_batch_train(sq, query.data(), _l2,
                                             /*integer=*/false, _weights);
    const size_t d = sq.d;
    const bool uniform =
      sq.qtype == faiss::ScalarQuantizer::QuantizerType::QT_8bit_uniform;
    const float* vmin = sq.trained.data();
    const float* vdiff = sq.trained.data() + (uniform ? 1 : d);
    const size_t stride = uniform ? 0 : 1;
    double coded = std::abs(static_cast<double>(_weights.bias));
    double stored = 0;
    double error = 0;
    double radius = 0;
    for (size_t i = 0; i < d; ++i) {
      const double lo = vmin[i * stride];
      const double span = vdiff[i * stride];
      const double half = span / 510 + 3 * kUnit * span;
      coded += 255 * std::abs(static_cast<double>(_weights.a[i]));
      if (_l2) {
        coded += span * span;
        radius += half * half;
      } else {
        const double q = std::abs(static_cast<double>(query[i]));
        stored += q * (std::max(std::abs(lo), std::abs(lo + span)) + half);
        error += q * half;
      }
    }
    if (_l2) {
      _round = Gamma(d + 4) * coded;
      _radius = std::sqrt(radius);
      _relative = Gamma(d + 4);
      return;
    }
    _margin = error + Gamma(d + 4) * coded + Gamma(d + 2) * stored;
    if (metric == VectorMetric::Cosine) {
      _margin += 2 * Gamma(d + 4) * stored + 3 * Gamma(2 * d + 8);
    }
  }

  void Score(const byte_type* base, uint32_t record_size,
             std::span<const uint32_t> ids, score_t* out) const final {
    faiss::scalar_quantizer::sq8_batch_score_n(
      _weights,
      [&](size_t j) {
        return base + static_cast<size_t>(ids[j]) * record_size;
      },
      ids.size(), out, 0);
  }

  void Bounds(std::span<const score_t> scores, score_t* lower,
              score_t* upper) const noexcept final {
    for (size_t i = 0; i < scores.size(); ++i) {
      const double s = scores[i];
      if (std::isnan(s)) {
        lower[i] = -std::numeric_limits<score_t>::infinity();
        upper[i] = std::numeric_limits<score_t>::infinity();
        continue;
      }
      if (!_l2) {
        lower[i] = Down(s - _margin);
        upper[i] = Up(s + _margin);
        continue;
      }
      const double near =
        std::max(0.0, std::sqrt(std::max(0.0, -s - _round)) - _radius);
      const double far = std::sqrt(std::max(0.0, -s + _round)) + _radius;
      lower[i] = Down(-(far * far) * (1 + _relative));
      upper[i] = Up(-(near * near) * (1 - _relative));
    }
  }

 private:
  static constexpr double kUnit = std::numeric_limits<float>::epsilon() / 2;

  static double Gamma(size_t n) noexcept {
    const double nu = static_cast<double>(n) * kUnit;
    return nu / (1 - nu);
  }

  static score_t Down(double v) noexcept {
    return std::nextafter(static_cast<score_t>(v),
                          -std::numeric_limits<score_t>::infinity());
  }

  static score_t Up(double v) noexcept {
    return std::nextafter(static_cast<score_t>(v),
                          std::numeric_limits<score_t>::infinity());
  }

  faiss::scalar_quantizer::SQ8BatchWeights _weights;
  double _margin = 0;
  double _round = 0;
  double _radius = 0;
  double _relative = 0;
  bool _l2;
};

template<VectorMetric M>
class ScalarQuantizerStats final : public QuantizerStats {
 public:
  ScalarQuantizerStats(uint32_t d, VectorQuantization quant,
                       std::span<const byte_type> stats)
    : _sq{d, FaissScalarType(quant)}, _quant{quant} {
    _sq.trained.assign(ScalarQuantizerWriter::TrainedSize(quant, d), 0.f);
    const size_t want = _sq.trained.size() * sizeof(float);
    SDB_ASSERT(stats.size() >= want);
    std::memcpy(_sq.trained.data(), stats.data(), want);
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  std::shared_ptr<const QuantizerCodeBound> MakeCodeBound(
    std::span<const float> query, VectorMetric metric) const final {
    const auto qt = _sq.qtype;
    if ((qt != faiss::ScalarQuantizer::QuantizerType::QT_8bit &&
         qt != faiss::ScalarQuantizer::QuantizerType::QT_8bit_uniform) ||
        _sq.code_size != _sq.d || query.size() != _sq.d ||
        (metric != VectorMetric::L2Sqr &&
         metric != VectorMetric::InnerProduct &&
         metric != VectorMetric::Cosine)) {
      return nullptr;
    }
    return std::make_shared<const ScalarQuantizerCodeBound>(_sq, query, metric);
  }

  const faiss::ScalarQuantizer& Sq() const noexcept { return _sq; }

 private:
  faiss::ScalarQuantizer _sq;
  VectorQuantization _quant;
};

template<VectorMetric M>
class ScalarQuantizerCodebook final : public QuantizerCodebook {
 public:
  ScalarQuantizerCodebook(std::shared_ptr<const ScalarQuantizerStats<M>> stats,
                          std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {}

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const faiss::ScalarQuantizer& Sq() const noexcept { return _stats->Sq(); }
  std::span<const float> Query() const noexcept { return _query; }

 private:
  std::shared_ptr<const ScalarQuantizerStats<M>> _stats;
  std::vector<float> _query;
};

template<VectorMetric M>
class ScalarQuantizerReader final : public QuantizerReader {
 public:
  ScalarQuantizerReader(std::shared_ptr<const ScalarQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)} {
    _dc.reset(_cb->Sq().get_distance_computer(
      M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                               : faiss::MetricType::METRIC_INNER_PRODUCT));
    _dc->code_size = _cb->Sq().code_size;
    _dc->set_query(_cb->Query().data());
    PrepareFast(_cb->Query(), /*integer=*/true);
    _query = _cb->Query();
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = 1,
            .record_size = static_cast<uint32_t>(_cb->Sq().code_size)};
  }

  void StartCluster(const float* /*centroid*/) final {}

  void ComputeBlock(std::span<const byte_type> block, score_t /*threshold*/,
                    score_t* out) final {
    SDB_ASSERT(_dc);
    const size_t cs = _cb->Sq().code_size;
    SDB_ASSERT(block.size() % cs == 0);
    const size_t n = block.size() / cs;
    const byte_type* c = block.data();
    if (_fast) {
      faiss::scalar_quantizer::sq8_batch_score_n(
        _weights, [&](size_t j) { return c + j * cs; }, n, out, 0);
      return;
    }
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
      _dc->distance_to_code_batch_4(c + i * cs, c + (i + 1) * cs,
                                    c + (i + 2) * cs, c + (i + 3) * cs, out[i],
                                    out[i + 1], out[i + 2], out[i + 3]);
    }
    for (; i < n; ++i) {
      out[i] = _dc->distance_to_code(c + i * cs);
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      for (i = 0; i < n; ++i) {
        out[i] = -out[i];
      }
    }
  }

  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t /*threshold*/,
                       score_t* out) final {
    SDB_ASSERT(_dc);
    const auto row = [&](size_t j) {
      return base + static_cast<size_t>(ids[j]) * record_size;
    };
    const size_t n = ids.size();
    if (_fast) {
      faiss::scalar_quantizer::sq8_batch_score_n(_weights, row, n, out, 0);
      return;
    }
    for (size_t j = 0; j < n; ++j) {
      __builtin_prefetch(row(j), 0, 3);
      __builtin_prefetch(row(j) + record_size - 1, 0, 3);
    }
    size_t i = 0;
    for (; i + 4 <= n; i += 4) {
      _dc->distance_to_code_batch_4(row(i), row(i + 1), row(i + 2), row(i + 3),
                                    out[i], out[i + 1], out[i + 2], out[i + 3]);
    }
    for (; i < n; ++i) {
      out[i] = _dc->distance_to_code(row(i));
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      for (i = 0; i < n; ++i) {
        out[i] = -out[i];
      }
    }
  }

  bool ComputeGatheredBounded(const byte_type* base, uint32_t record_size,
                              std::span<const uint32_t> ids,
                              const QuantizerLevelBound& bound,
                              score_t threshold, score_t* out) final {
#if defined(__x86_64__) && defined(__AVX2__)
    if constexpr (M != VectorMetric::InnerProduct) {
      return false;
    }
    const auto& sq = _cb->Sq();
    if (!_fast || _weights.l2 || sq.qtype != faiss::ScalarQuantizer::QT_8bit ||
        bound.width == 0 || bound.width % 32 != 0 || bound.query_step == 0 ||
        bound.width % bound.query_step != 0 || bound.rows == nullptr) {
      return false;
    }
    const auto d = static_cast<uint32_t>(sq.d);
    const uint32_t levels = (d + bound.width - 1) / bound.width;
    if (_level_bias.size() != levels) {
      const float* vmin = sq.trained.data();
      const float* vdiff = sq.trained.data() + d;
      const auto q = _query;
      _level_bias.assign(levels, 0.f);
      double acc = 0;
      for (uint32_t j = 0; j < d; ++j) {
        if (j % bound.width == 0) {
          _level_bias[j / bound.width] = static_cast<float>(acc);
        }
        acc += double{q[j]} * (vmin[j] + 0.5f * (vdiff[j] / 255.f));
      }
    }
    const auto row = [&](size_t i) {
      return reinterpret_cast<const uint8_t*>(base) +
             static_cast<size_t>(ids[i]) * record_size;
    };
    const auto tails = [&](size_t i) {
      return bound.rows + static_cast<size_t>(ids[i]) * bound.row_stride;
    };
    const auto prefetch = [&](size_t i) {
      if (i < ids.size()) {
        for (size_t l = 0; l < kSq8RowLines; ++l) {
          __builtin_prefetch(row(i) + 64 * l, 0, 3);
        }
        __builtin_prefetch(tails(i), 0, 3);
      }
    };
    for (size_t i = 0; i < kSq8RowsAhead; ++i) {
      prefetch(i);
    }
    const Sq8Levels lv{.a = _weights.a.data(),
                       .d = d,
                       .width = bound.width,
                       .query_step = bound.query_step,
                       .bias = _weights.bias,
                       .level_bias = _level_bias.data(),
                       .query = bound.query.data()};
    const bool wide = HasAvx512Vnni();
    for (size_t i = 0; i < ids.size(); ++i) {
      prefetch(i + kSq8RowsAhead);
      out[i] = wide ? Sq8ScoreLevels512(lv, row(i), tails(i), threshold)
                    : Sq8ScoreLevels(lv, row(i), tails(i), threshold);
    }
    return true;
#else
    return false;
#endif
  }

  bool Decode(const byte_type* code, float* out) const final {
    _cb->Sq().decode(code, out, 1);
    return true;
  }

  bool SetQuery(std::span<const float> query) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(query.size() == _cb->Sq().d);
    _dc->set_query(query.data());
    PrepareFast(query, /*integer=*/false);
    _own_query.assign(query.begin(), query.end());
    _query = _own_query;
    _level_bias.clear();
    return true;
  }

 private:
  void PrepareFast(std::span<const float> query, bool integer) {
    // faiss dispatches the kernel by runtime SIMD level, so there is no CPU
    // probe here any more: AVX-512 where the machine has it, AVX2 or NEON
    // otherwise, and a scalar loop on anything else. Nothing here is x86, and
    // gating it on x86 is what made the NEON kernel we wrote unreachable.
    const auto qt = _cb->Sq().qtype;
    _fast = (qt == faiss::ScalarQuantizer::QT_8bit ||
             qt == faiss::ScalarQuantizer::QT_8bit_uniform) &&
            _cb->Sq().code_size == _cb->Sq().d;
    if (_fast) {
      faiss::scalar_quantizer::sq8_batch_train(
        _cb->Sq(), query.data(), M == VectorMetric::L2Sqr, integer, _weights);
    }
  }

  std::shared_ptr<const ScalarQuantizerCodebook<M>> _cb;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  faiss::scalar_quantizer::SQ8BatchWeights _weights;
  std::vector<float> _own_query;
  std::span<const float> _query;
  std::vector<float> _level_bias;
  bool _fast = false;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> ScalarQuantizerCodebook<M>::MakeReader()
  const {
  return MakeReaderT<ScalarQuantizerCodebook<M>, ScalarQuantizerReader<M>>(
    this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> ScalarQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<ScalarQuantizerStats<M>, ScalarQuantizerCodebook<M>>(
    this, query);
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeScalarWriter(uint32_t d,
                                                  VectorQuantization quant) {
  return std::make_unique<ScalarQuantizerWriter>(d, quant);
}

std::shared_ptr<const QuantizerStats> MakeScalarStats(
  VectorMetric metric, uint32_t d, VectorQuantization quant,
  std::span<const byte_type> stats) {
  return MakeStatsWithMetric<ScalarQuantizerStats>(metric, d, quant, stats);
}

}  // namespace irs::quantizers
