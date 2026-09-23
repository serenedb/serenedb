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

#include "iresearch/formats/ivf/quantizer.hpp"

#include <absl/random/distributions.h>
#include <absl/random/random.h>
#include <faiss/MetricType.h>
#include <faiss/impl/Panorama.h>
#include <faiss/impl/ProductQuantizer.h>
#include <faiss/impl/RaBitQUtils.h>
#include <faiss/impl/RaBitQuantizerMultiBit.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/impl/scalar_quantizer/sq8_batch.h>
#include <faiss/impl/fast_scan/fast_scan.h>
#include <faiss/utils/AlignedTable.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/ordered_key_value.h>
#include <faiss/utils/quantize_lut.h>
#include <faiss/utils/random.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <numbers>
#include <optional>
#include <random>
#include <vector>

#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/misc.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {

void GenerateSigns(uint32_t rotated_d, int64_t seed,
                   std::vector<float>& signs) {
  signs.resize(rotated_d);
  std::mt19937_64 rng{static_cast<uint64_t>(seed)};
  for (uint32_t i = 0; i < rotated_d;) {
    const uint64_t num = rng();
    for (size_t b = 0; b < sizeof(num) * 8 && i < rotated_d; ++b) {
      signs[i] = 1.f - 2.f * ((num >> b) & 1);
      i++;
    }
  }
}

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
    default:
      return faiss::ScalarQuantizer::QuantizerType::QT_8bit;
  }
}

std::span<const byte_type> FloatSpan(const std::vector<float>& v) noexcept {
  return {reinterpret_cast<const byte_type*>(v.data()),
          v.size() * sizeof(float)};
}

constexpr size_t kFastScanBbs = 32;
constexpr uint32_t kPqNbits = 4;
constexpr size_t kFastScanBits = 4;

constexpr size_t kEncodeTile = kFastScanBbs;
constexpr size_t kFastScanKsub = size_t{1} << kFastScanBits;
constexpr uint32_t kPanoramaMinDim = 64;
constexpr uint32_t kPanoramaLevelWidth = 32;
constexpr size_t kPqTrainResiduals = kFastScanKsub * 256;
constexpr size_t kPcaMinRows = 1024;
constexpr size_t kPcaTrainRows = 4096;

size_t FastScanNsq(size_t m) noexcept { return m + (m & 1); }

constexpr int64_t kIvfRotationSeed = 0x5a17b17c5eed5eedULL;

uint32_t RotatedDim(uint32_t d) noexcept {
  return std::max<uint32_t>(kFastScanBits, std::bit_ceil(d));
}

constexpr size_t SignBytes(uint32_t rotated_d) noexcept {
  return (static_cast<size_t>(rotated_d) + 7) / 8;
}

std::vector<byte_type> PackSigns(const std::vector<float>& signs) {
  std::vector<byte_type> out(SignBytes(static_cast<uint32_t>(signs.size())), 0);
  for (size_t i = 0; i < signs.size(); ++i) {
    if (signs[i] > 0.f) {
      out[i / 8] = static_cast<byte_type>(out[i / 8] | (1U << (i % 8)));
    }
  }
  return out;
}

void LoadSigns(std::span<const byte_type> stats, size_t offset, uint32_t rd,
               std::vector<float>& signs) {
  SDB_ASSERT(stats.size() >= offset + SignBytes(rd));
  const byte_type* p = stats.data() + offset;
  signs.resize(rd);
  for (uint32_t i = 0; i < rd; ++i) {
    signs[i] = ((p[i / 8] >> (i % 8)) & 1U) != 0 ? 1.f : -1.f;
  }
}

// In-place Fast Walsh-Hadamard transform; len must be a power of two.
void Fwht(float* a, uint32_t len) noexcept {
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

void RotateInto(const float* signs, const float* in, float* out, uint32_t d,
                uint32_t rotated_d) noexcept {
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

struct PqStatsHeader {
  uint32_t m;
  uint32_t ksub;
};

struct RaBitQStatsHeader {
  uint32_t nb_bits;
  uint32_t d;
};

constexpr uint8_t kRaBitQQueryBits = 8;
constexpr bool kRaBitQCentered = false;

constexpr uint8_t kTurboQuantQjlFwht = 0;
constexpr uint8_t kTurboQuantLayout = 0;
constexpr uint32_t kTurboQuantLutChunk = 256;
constexpr size_t kTurboQuantRefineSample = 65536;
constexpr size_t kTurboQuantEcQuantileRows = 8192;
constexpr size_t kTurboQuantEcStride =
  kTurboQuantRefineSample / kTurboQuantEcQuantileRows;
constexpr size_t kTurboQuantEcMinRows = 32;
constexpr bool kTurboQuantErrorCorrection = false;
constexpr float kTurboQuantMinQuantileWidth = 1e-3f;

struct TurboQuantStatsHeader {
  uint8_t layout;
  uint8_t nb_bits;
  uint8_t full;
  uint8_t qjl_type;
  uint32_t d;
  uint64_t seed;
};

static_assert(sizeof(TurboQuantStatsHeader) == 16);
static_assert(std::has_unique_object_representations_v<TurboQuantStatsHeader>);

std::optional<faiss::ScalarQuantizer::QuantizerType> FaissTurboQuantType(
  bool full, uint32_t nb_bits) noexcept {
  using QT = faiss::ScalarQuantizer::QuantizerType;
  if (full) {
    switch (nb_bits) {
      case 2:
        return QT::QT_2bit_tq;
      case 3:
        return QT::QT_3bit_tq;
      case 5:
        return QT::QT_5bit_tq;
      default:
        return std::nullopt;
    }
  }
  switch (nb_bits) {
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

struct PanoramaStatsHeader {
  uint32_t n_levels;
  uint32_t d;
};

void RotateQuery(const byte_type* rotation, const float* q, float* out,
                 uint32_t d) {
  const auto* qb = reinterpret_cast<const byte_type*>(q);
  const auto width = static_cast<uint16_t>(d);
  const size_t stride = size_t{d} * sizeof(float);
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = vector::DotProductImpl<float, float>::Compute(
      rotation + i * stride, qb, width);
  }
}
constexpr uint32_t PanoramaLevels(uint32_t d) noexcept {
  return (d + kPanoramaLevelWidth - 1) / kPanoramaLevelWidth;
}

constexpr uint32_t PanoramaRecordSize(uint32_t d, uint32_t n_levels) noexcept {
  return (d + (n_levels != 0 ? n_levels + 1 : 0)) *
         static_cast<uint32_t>(sizeof(float));
}

faiss::Panorama MakePanorama(uint32_t d, size_t batch_size) {
  return faiss::Panorama{size_t{d} * sizeof(float), PanoramaLevels(d),
                         batch_size};
}

class PanoramaQuantizerWriter final : public QuantizerWriter {
 public:
  PanoramaQuantizerWriter(uint32_t d, VectorMetric metric)
    : _d{d}, _metric{metric} {}

  size_t TrainSamples(size_t rows) const noexcept final {
    if (!PanoramaApplies(_metric, _d) ||
        rows < std::max<size_t>(kPcaMinRows, size_t{8} * _d)) {
      return 0;
    }
    return std::max<size_t>(kPcaTrainRows, size_t{8} * _d);
  }

  void Train(const float* vecs, size_t n) final {
    if (!PanoramaApplies(_metric, _d) || n < _d) {
      return;
    }
    _rotation = TrainPcaRotation(vecs, n, _d);
    SDB_ASSERT(!_rotation.A.empty());
    _pano = MakePanorama(_d, faiss::Panorama::kDefaultBatchSize);
    SDB_ASSERT(_pano->n_levels == PanoramaLevels(_d));
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {
      .group_size =
        _pano ? static_cast<uint32_t>(_pano->batch_size) : uint32_t{1},
      .record_size = PanoramaRecordSize(_d, _pano ? PanoramaLevels(_d) : 0)};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    if (n == 0) {
      return;
    }
    if (!_pano) {
      out.WriteData(reinterpret_cast<const byte_type*>(vecs),
                    n * size_t{_d} * sizeof(float));
      return;
    }
    const size_t batch = _pano->batch_size;
    if (_stage.empty()) {
      _stage.resize(batch * size_t{_d});
    }
    for (size_t off = 0; off < n;) {
      const size_t take = std::min(batch - _pending, n - off);
      _rotation.apply_noalloc(static_cast<faiss::idx_t>(take), vecs + off * _d,
                              _stage.data() + _pending * size_t{_d});
      _pending += take;
      off += take;
      if (_pending == batch) {
        WriteBatch(out, _stage.data(), batch);
        _pending = 0;
      }
    }
  }

  void Finish(IndexOutput& out) final {
    if (_pending == 0) {
      return;
    }
    const size_t batch = _pano->batch_size;
    std::fill(_stage.begin() + _pending * size_t{_d}, _stage.end(), 0.f);
    WriteBatch(out, _stage.data(), batch);
    _pending = 0;
  }

  uint32_t PendingLanes() const noexcept final {
    return static_cast<uint32_t>(_pending);
  }

  void Serialize(DataOutput& out) const final {
    if (!_pano) {
      out.WriteU64(0);
      return;
    }
    const auto a = FloatSpan(_rotation.A);
    out.WriteU64(sizeof(PanoramaStatsHeader) + a.size());
    WritePod(out, PanoramaStatsHeader{PanoramaLevels(_d), _d});
    out.WriteData(a.data(), a.size());
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::None;
  }

 private:
  void WriteBatch(IndexOutput& out, const float* src, size_t count) {
    auto pano = MakePanorama(_d, count);
    _cums.assign(count * (pano.n_levels + 1), 0.f);
    _codes.resize(count * size_t{_d});
    pano.compute_cumulative_sums(_cums.data(), 0, count, src);
    pano.copy_codes_to_level_layout(reinterpret_cast<uint8_t*>(_codes.data()),
                                    0, count,
                                    reinterpret_cast<const uint8_t*>(src));
    out.WriteData(reinterpret_cast<const byte_type*>(_cums.data()),
                  _cums.size() * sizeof(float));
    out.WriteData(reinterpret_cast<const byte_type*>(_codes.data()),
                  _codes.size() * sizeof(float));
  }

  uint32_t _d;
  VectorMetric _metric;
  faiss::PCAMatrix _rotation;
  std::optional<faiss::Panorama> _pano;
  size_t _pending = 0;
  std::vector<float> _stage;
  std::vector<float> _cums;
  std::vector<float> _codes;
};

template<VectorMetric M>
class PanoramaQuantizerStats final : public QuantizerStats {
 public:
  PanoramaQuantizerStats(uint32_t d, std::span<const byte_type> stats) : _d{d} {
    const size_t want = size_t{d} * d * sizeof(float);
    if (stats.size() < sizeof(PanoramaStatsHeader) + want) {
      return;
    }
    const auto header = ReadPodHeader<PanoramaStatsHeader>(stats);
    if (header.n_levels != PanoramaLevels(d) || header.d != d) {
      return;
    }
    _rotation = stats.data() + sizeof(PanoramaStatsHeader);
    _levels = header.n_levels;
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::None;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  uint32_t Dim() const noexcept { return _d; }
  uint32_t Levels() const noexcept { return _levels; }
  const byte_type* Rotation() const noexcept { return _rotation; }

 private:
  uint32_t _d;
  uint32_t _levels = 0;
  const byte_type* _rotation = nullptr;
};

template<VectorMetric M>
class PanoramaQuantizerCodebook final : public QuantizerCodebook {
 public:
  PanoramaQuantizerCodebook(
    std::shared_ptr<const PanoramaQuantizerStats<M>> stats,
    std::span<const float> query)
    : _stats{std::move(stats)}, _levels{_stats->Levels()} {
    const auto d = _stats->Dim();
    SDB_ASSERT(query.size() == d);
    if (_levels == 0) {
      _query.assign(query.begin(), query.end());
      return;
    }
    _query.resize(d);
    RotateQuery(_stats->Rotation(), query.data(), _query.data(), d);
    _pano = MakePanorama(d, faiss::Panorama::kDefaultBatchSize);
    _cums.resize(_levels + 1);
    _pano->compute_query_cum_sums(_query.data(), _cums.data());
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  uint32_t Dim() const noexcept { return _stats->Dim(); }
  uint32_t Levels() const noexcept { return _levels; }
  const byte_type* Query() const noexcept {
    return reinterpret_cast<const byte_type*>(_query.data());
  }
  const float* QueryData() const noexcept { return _query.data(); }
  const float* QueryCums() const noexcept { return _cums.data(); }
  const faiss::Panorama& Panorama() const noexcept {
    SDB_ASSERT(_pano);
    return *_pano;
  }

 private:
  std::shared_ptr<const PanoramaQuantizerStats<M>> _stats;
  uint32_t _levels;
  std::vector<float> _query;
  std::vector<float> _cums;
  std::optional<faiss::Panorama> _pano;
};

template<VectorMetric M>
class PanoramaQuantizerReader final : public QuantizerReader {
 public:
  explicit PanoramaQuantizerReader(
    std::shared_ptr<const PanoramaQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)},
      _d{_cb->Dim()},
      _levels{_cb->Levels()},
      _record_size{PanoramaRecordSize(_d, _levels)} {
    if (_levels == 0) {
      return;
    }
    const auto batch = _cb->Panorama().batch_size;
    _group_size = static_cast<uint32_t>(batch);
    _active.resize(batch);
    _byteset.resize(batch);
    _exact.resize(batch);
    _dots.resize(batch);
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = _group_size, .record_size = _record_size};
  }

  void StartCluster(const float* /*centroid*/) final {}

  void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _record_size == 0);
    size_t left = block.size() / _record_size;
    if (_levels == 0) {
      const byte_type* q = _cb->Query();
      for (size_t i = 0; i < left; ++i) {
        out[i] = ComputeDistance<M>(q, block.data() + i * _record_size,
                                    static_cast<uint16_t>(_d));
      }
      return;
    }
    const byte_type* p = block.data();
    const size_t batch_size = _cb->Panorama().batch_size;
    while (left != 0) {
      const size_t len = std::min(batch_size, left);
      ScoreBatch(p, len, threshold, out);
      p += len * _record_size;
      out += len;
      left -= len;
    }
  }

 private:
  static constexpr auto kMetric =
    M == VectorMetric::L2Sqr ? faiss::METRIC_L2 : faiss::METRIC_INNER_PRODUCT;

  void ScoreBatch(const byte_type* p, size_t len, score_t score, score_t* out) {
    const auto& full = _cb->Panorama();
    const faiss::Panorama pano{full.code_size, full.n_levels, len};
    const auto* cums = reinterpret_cast<const float*>(p);
    const byte_type* codes = p + len * (_levels + 1) * sizeof(float);
    const float threshold = std::nextafter(
      kMetric == faiss::METRIC_L2 ? -score : score,
      kMetric == faiss::METRIC_L2 ? std::numeric_limits<float>::max()
                                  : std::numeric_limits<float>::lowest());
    std::fill_n(out, len, std::numeric_limits<score_t>::lowest());
    faiss::PanoramaStats stats;
    using C = std::conditional_t<kMetric == faiss::METRIC_L2,
                                 faiss::CMax<float, int64_t>,
                                 faiss::CMin<float, int64_t>>;
    const size_t alive = pano.template progressive_filter_batch<C, kMetric>(
      codes, cums, _cb->QueryData(), _cb->QueryCums(), 0, len,
      /*sel=*/nullptr, /*ids=*/nullptr, /*use_sel=*/false, _active, _byteset,
      _exact, _dots, threshold, stats);
    for (size_t i = 0; i < alive; ++i) {
      const auto idx = _active[i];
      SDB_ASSERT(idx < len);
      if constexpr (M == VectorMetric::L2Sqr) {
        out[idx] = -_exact[idx];
      } else {
        out[idx] = _exact[idx];
      }
    }
  }

  std::shared_ptr<const PanoramaQuantizerCodebook<M>> _cb;
  uint32_t _d;
  uint32_t _levels;
  uint32_t _record_size;
  uint32_t _group_size = 1;
  std::vector<uint32_t> _active;
  std::vector<uint8_t> _byteset;
  std::vector<float> _exact;
  std::vector<float> _dots;
};

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
    return IsUniformScalar(quant) ? 2 : 2 * static_cast<size_t>(d);
  }

  size_t TrainSamples(size_t /*rows*/) const noexcept final {
    return kTrainStreaming;
  }

  void Train(const float* vecs, size_t n) final {
    if (n == 0) {
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
    out.WriteData(bytes.data(), bytes.size());
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

template<VectorMetric M>
class ScalarQuantizerStats final : public QuantizerStats {
 public:
  ScalarQuantizerStats(uint32_t d, VectorQuantization quant,
                       std::span<const byte_type> stats)
    : _sq{d, FaissScalarType(quant)}, _quant{quant} {
    _sq.trained.assign(
      ScalarQuantizerWriter::TrainedSize(quant, d), 0.f);
    const size_t want = _sq.trained.size() * sizeof(float);
    SDB_ASSERT(stats.size() >= want);
    std::memcpy(_sq.trained.data(), stats.data(), want);
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

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
    PrepareFast(_cb->Query());
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

  bool Decode(const byte_type* code, float* out) const final {
    _cb->Sq().decode(code, out, 1);
    return true;
  }

  bool SetQuery(std::span<const float> query) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(query.size() == _cb->Sq().d);
    _dc->set_query(query.data());
    PrepareFast(query);
    return true;
  }

 private:
  void PrepareFast(std::span<const float> query) {
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
        _cb->Sq(), query.data(), M == VectorMetric::L2Sqr, _weights);
    }
  }

  std::shared_ptr<const ScalarQuantizerCodebook<M>> _cb;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  faiss::scalar_quantizer::SQ8BatchWeights _weights;
  bool _fast = false;
};

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

template<VectorMetric M>
std::unique_ptr<QuantizerReader> PanoramaQuantizerCodebook<M>::MakeReader()
  const {
  return MakeReaderT<PanoramaQuantizerCodebook<M>, PanoramaQuantizerReader<M>>(
    this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook>
PanoramaQuantizerStats<M>::MakeCodebook(std::span<const float> query) const {
  return MakeCodebookT<PanoramaQuantizerStats<M>, PanoramaQuantizerCodebook<M>>(
    this, query);
}

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

float TurboQuantNorm(const float* v, uint32_t d) {
  return std::max(
    std::sqrt(vector::L2Space<float, float, float>::Norm(
      reinterpret_cast<const byte_type*>(v), static_cast<uint16_t>(d))),
    std::numeric_limits<float>::epsilon());
}

struct TurboQuantLayout {
  uint32_t d = 0;
  uint32_t rd = 0;
  uint32_t mse_bits = 0;
  uint32_t dims_per_nibble = 0;
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

  uint32_t RowCode2Offset() const noexcept { return code1_bytes; }
  uint32_t RowNormOffset() const noexcept { return code1_bytes + code2_bytes; }
  uint32_t RowGammaOffset() const noexcept {
    return RowNormOffset() + sizeof(float);
  }
  uint32_t RowXNorm2Offset() const noexcept {
    return RowGammaOffset() + (full ? sizeof(float) : 0);
  }

  uint32_t NormOffset() const noexcept { return group1_bytes + group2_bytes; }
  uint32_t GammaOffset() const noexcept {
    return NormOffset() + kFastScanBbs * sizeof(float);
  }
  uint32_t XNorm2Offset() const noexcept {
    return GammaOffset() + (full ? kFastScanBbs * sizeof(float) : 0);
  }
  uint32_t GroupBytes() const noexcept { return kFastScanBbs * record_size; }
};

TurboQuantLayout MakeTurboQuantLayout(uint32_t d, bool full, uint32_t nb_bits,
                                      bool l2, bool row_major) noexcept {
  TurboQuantLayout l;
  l.d = d;
  l.rd = RotatedDim(d);
  l.full = full;
  l.l2 = l2;
  l.row_major = row_major;
  l.mse_bits = full ? nb_bits - 1 : nb_bits;
  l.dims_per_nibble = static_cast<uint32_t>(kFastScanBits) / l.mse_bits;
  l.m1 = l.rd / l.dims_per_nibble;
  l.nsq1 = static_cast<uint32_t>(FastScanNsq(l.m1));
  l.code1_bytes = (l.m1 + 1) / 2;
  l.group1_bytes = static_cast<uint32_t>(kFastScanBbs) * l.nsq1 / 2;
  if (full) {
    l.m2 = l.rd / static_cast<uint32_t>(kFastScanBits);
    l.nsq2 = static_cast<uint32_t>(FastScanNsq(l.m2));
    l.code2_bytes = (l.m2 + 1) / 2;
    l.group2_bytes = static_cast<uint32_t>(kFastScanBbs) * l.nsq2 / 2;
  }
  const uint32_t floats = 1 + (full ? 1 : 0) + (l2 ? 1 : 0);
  l.record_size =
    l.nsq1 / 2 + l.nsq2 / 2 + floats * static_cast<uint32_t>(sizeof(float));
  return l;
}

struct FastScanChunk {
  uint32_t m;
  uint32_t nsq;
  uint32_t code_off;
  uint32_t lut_off;
};

std::vector<FastScanChunk> MakeFastScanChunks(uint32_t m) {
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

void SetNibble(uint8_t* code, uint32_t sq, uint8_t nib) noexcept {
  uint8_t& dst = code[sq >> 1];
  dst = static_cast<uint8_t>((sq & 1) != 0 ? dst | (nib << 4) : dst | nib);
}

uint8_t GetNibble(const uint8_t* code, uint32_t sq) noexcept {
  const uint8_t byte = code[sq >> 1];
  return static_cast<uint8_t>((sq & 1) != 0 ? byte >> 4 : byte & 0x0F);
}

void RotateBack(const float* signs, const float* in, float* out, uint32_t d,
                uint32_t rotated_d, std::vector<float>& scratch) {
  scratch.assign(in, in + rotated_d);
  Fwht(scratch.data(), rotated_d);
  const float scale = 1.f / std::sqrt(static_cast<float>(rotated_d));
  for (uint32_t i = 0; i < d; ++i) {
    out[i] = scratch[i] * scale * signs[i];
  }
}

// faiss keeps TurboQuantRefine a small config struct -- there is a
// static_assert forbidding projection buffers in it -- so the FWHT sign vector
// lives in the quantizer and is derived from the seed. Rebuilt here the same
// way: RandomGenerator(seed), one sign per padded dimension.
std::vector<float> MakeTurboQuantFwhtSigns(uint64_t seed, uint32_t d) {
  size_t padded_d = 1;
  while (padded_d < d) {
    padded_d <<= 1;
  }
  std::vector<float> signs(padded_d);
  faiss::RandomGenerator rng(static_cast<int64_t>(seed));
  for (size_t i = 0; i < padded_d; ++i) {
    signs[i] = (rng.rand_int(2) == 0) ? 1.f : -1.f;
  }
  return signs;
}

void TurboQuantProject(const std::vector<float>& fwht_signs, const float* in,
                       float* out, uint32_t rd) noexcept {
  for (uint32_t j = 0; j < rd; ++j) {
    out[j] = in[j] * fwht_signs[j];
  }
  Fwht(out, rd);
}

template<VectorMetric M>
class TurboQuantizerWriter final : public QuantizerWriter {
 public:
  TurboQuantizerWriter(uint32_t d, bool full, uint32_t nb_bits,
                       faiss::ScalarQuantizer::QuantizerType qtype,
                       bool row_major)
    : _lay{MakeTurboQuantLayout(d, full, nb_bits, M == VectorMetric::L2Sqr,
                                row_major)},
      _nb_bits{nb_bits},
      _sq{_lay.rd, qtype},
      _sqrt_rd{std::sqrt(static_cast<float>(_lay.rd))} {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    TrainTurboQuant(_sq, kIvfRotationSeed);
    _fwht_signs = MakeTurboQuantFwhtSigns(kIvfRotationSeed, _lay.rd);
    GenerateSigns(_lay.rd, kIvfRotationSeed, _signs);
    _centroids = _sq.trained.data();
    _boundaries = _sq.trained.data() + (size_t{1} << _lay.mse_bits);
    _rot.resize(_lay.rd);
    _res.resize(_lay.rd);
    _code1.assign(kFastScanBbs * size_t{_lay.code1_bytes}, 0);
    _packed1.resize(_lay.group1_bytes);
    if (_lay.full) {
      _proj.resize(_lay.rd);
      _code2.assign(kFastScanBbs * size_t{_lay.code2_bytes}, 0);
      _packed2.resize(_lay.group2_bytes);
    }
  }

  size_t TrainSamples(size_t /*rows*/) const noexcept final { return 0; }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_lay.rd);
    RotateInto(_signs.data(), centroid, _centroid.data(), _lay.d, _lay.rd);
  }

  size_t RefineSamples(size_t rows) const noexcept final {
    if (!kTurboQuantErrorCorrection) {
      return 0;
    }
    return _lay.full ? 0 : std::min<size_t>(rows, kTurboQuantRefineSample);
  }

  void Refine(const float* vecs, size_t n) final {
    if (_lay.full) {
      return;
    }
    _ec_samples.resize(size_t{_lay.rd} * kTurboQuantEcQuantileRows);
    for (size_t i = 0; i < n; ++i) {
      const bool keep = _ec_n % kTurboQuantEcStride == 0 &&
                        _ec_kept < kTurboQuantEcQuantileRows;
      ++_ec_n;
      if (!keep) {
        continue;
      }
      RotateInto(_signs.data(), vecs + i * size_t{_lay.d}, _rot.data(), _lay.d,
                 _lay.rd);
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        _res[j] = _rot[j] - (_centroid.empty() ? 0.f : _centroid[j]);
      }
      const float norm = TurboQuantNorm(_res.data(), _lay.rd);
      const float rescale = _sqrt_rd / norm;
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        _ec_samples[size_t{j} * kTurboQuantEcQuantileRows + _ec_kept] =
          _res[j] * rescale;
      }
      ++_ec_kept;
    }
  }

  void RefineDone() final {
    FitErrorCorrection();
    _ec_samples.clear();
    _ec_samples.shrink_to_fit();
  }

  void FitErrorCorrection() {
    if (_lay.full || _ec_kept < kTurboQuantEcMinRows) {
      return;
    }
    const uint32_t k = 1U << _lay.mse_bits;
    float c_outer = 0.f;
    for (uint32_t i = 0; i < k; ++i) {
      c_outer = std::max(c_outer, std::abs(_centroids[i]));
    }
    const double p_outer =
      0.5 * std::erfc(-double{c_outer} / std::numbers::sqrt2);
    const auto span = static_cast<double>(_ec_kept - 1);
    const auto lo = static_cast<size_t>(std::llround((1.0 - p_outer) * span));
    const auto hi = static_cast<size_t>(std::llround(p_outer * span));
    if (hi <= lo) {
      return;
    }
    _ec_scale.assign(_lay.rd, 1.f);
    _ec_shift.assign(_lay.rd, 0.f);
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      float* col = _ec_samples.data() + size_t{j} * kTurboQuantEcQuantileRows;
      float* end = col + _ec_kept;
      std::nth_element(col, col + lo, end);
      const float q_lo = col[lo];
      std::nth_element(col + lo + 1, col + hi, end);
      const float q_hi = col[hi];
      const float denom = q_hi - q_lo;
      if (denom > kTurboQuantMinQuantileWidth) {
        _ec_shift[j] = -(q_lo + q_hi) / 2.f;
        _ec_scale[j] = 2.f * c_outer / denom;
      }
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = _lay.row_major ? 1U : uint32_t{kFastScanBbs},
            .record_size = _lay.record_size};
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
    auto out = std::make_unique<TurboQuantizerWriter>(
      _lay.d, _lay.full, _nb_bits, _sq.qtype, _lay.row_major);
    out->_centroid = _centroid;
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
    const uint64_t ec_bytes =
      (_ec_scale.size() + _ec_shift.size()) * sizeof(float);
    out.WriteU64(sizeof(TurboQuantStatsHeader) + ec_bytes);
    WritePod(out, TurboQuantStatsHeader{
                    .layout = kTurboQuantLayout,
                    .nb_bits = static_cast<uint8_t>(_nb_bits),
                    .full = static_cast<uint8_t>(_lay.full ? 1 : 0),
                    .qjl_type = kTurboQuantQjlFwht,
                    .d = _lay.d,
                    .seed = kIvfRotationSeed});
    if (ec_bytes != 0) {
      out.WriteData(reinterpret_cast<const byte_type*>(_ec_scale.data()),
                    _ec_scale.size() * sizeof(float));
      out.WriteData(reinterpret_cast<const byte_type*>(_ec_shift.data()),
                    _ec_shift.size() * sizeof(float));
    }
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::TQ;
  }

 private:
  void EncodeOne(const float* vec, size_t lane) {
    RotateInto(_signs.data(), vec, _rot.data(), _lay.d, _lay.rd);
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _res[j] = _rot[j] - _centroid[j];
    }
    const float norm = TurboQuantNorm(_res.data(), _lay.rd);
    _norms[lane] = norm;
    if constexpr (M == VectorMetric::L2Sqr) {
      _xnorm2[lane] = vector::L2Space<float, float, float>::Norm(
        reinterpret_cast<const byte_type*>(vec), static_cast<uint16_t>(_lay.d));
    }
    const float scale = _sqrt_rd / norm;
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _res[j] *= scale;
    }
    if (!_ec_scale.empty()) {
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        _res[j] = (_res[j] + _ec_shift[j]) * _ec_scale[j];
      }
    }

    const uint32_t k = 1U << _lay.mse_bits;
    const auto mask = static_cast<uint8_t>(k - 1);
    const float inv_sqrt_rd = 1.f / _sqrt_rd;
    uint8_t* code = _code1.data() + lane * size_t{_lay.code1_bytes};
    double cn2 = 0.0;
    for (uint32_t mi = 0; mi < _lay.m1; ++mi) {
      uint8_t nib = 0;
      for (uint32_t t = 0; t < _lay.dims_per_nibble; ++t) {
        const uint32_t j = mi * _lay.dims_per_nibble + t;
        const auto idx = static_cast<uint8_t>(
          std::upper_bound(_boundaries, _boundaries + (k - 1), _res[j]) -
          _boundaries);
        nib = static_cast<uint8_t>(nib | ((idx & mask) << (t * _lay.mse_bits)));
        const float cr = _ec_scale.empty()
                           ? _centroids[idx]
                           : _centroids[idx] / _ec_scale[j] - _ec_shift[j];
        cn2 += static_cast<double>(cr) * cr;
        if (_lay.full) {
          _res[j] = (_res[j] - _centroids[idx]) * inv_sqrt_rd;
        }
      }
      SetNibble(code, mi, nib);
    }
    if (!_lay.full) {
      const auto cn = static_cast<float>(std::sqrt(cn2));
      if (cn > std::numeric_limits<float>::epsilon()) {
        _norms[lane] = norm * _sqrt_rd / cn;
      }
      return;
    }

    _gammas[lane] = std::sqrt(vector::L2Space<float, float, float>::Norm(
      reinterpret_cast<const byte_type*>(_res.data()),
      static_cast<uint16_t>(_lay.rd)));
    TurboQuantProject(_fwht_signs, _res.data(), _proj.data(),
                      _lay.rd);
    uint8_t* qjl = _code2.data() + lane * size_t{_lay.code2_bytes};
    for (uint32_t mi = 0; mi < _lay.m2; ++mi) {
      uint8_t nib = 0;
      for (uint32_t t = 0; t < kFastScanBits; ++t) {
        if (_proj[mi * kFastScanBits + t] > 0.f) {
          nib = static_cast<uint8_t>(nib | (1U << t));
        }
      }
      SetNibble(qjl, mi, nib);
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
    if constexpr (M == VectorMetric::L2Sqr) {
      std::memcpy(dst + _lay.RowXNorm2Offset(), &_xnorm2[0], sizeof(float));
    }
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
    if constexpr (M == VectorMetric::L2Sqr) {
      WriteFloats(out, _xnorm2, count);
    }
    std::fill(_code1.begin(), _code1.end(), 0);
    std::fill(_code2.begin(), _code2.end(), 0);
    _lane = 0;
  }

  std::vector<float> _ec_scale;
  std::vector<float> _ec_shift;
  std::vector<float> _ec_samples;
  size_t _ec_kept = 0;
  uint64_t _ec_n = 0;
  TurboQuantLayout _lay;
  uint32_t _nb_bits;
  faiss::ScalarQuantizer _sq;
  float _sqrt_rd;
  const float* _centroids = nullptr;
  const float* _boundaries = nullptr;
  size_t _lane = 0;
  std::vector<float> _signs;
  // Derived from turboq_refine.seed; faiss no longer stores it.
  std::vector<float> _fwht_signs;
  std::vector<float> _centroid;
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
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::array<float, kFastScanBbs>> _xnorm2;
};

template<VectorMetric M>
class TurboQuantizerStats final : public QuantizerStats {
 public:
  TurboQuantizerStats(uint32_t d, std::span<const byte_type> stats,
                      bool row_major) {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    const auto hdr = ReadPodHeader<TurboQuantStatsHeader>(stats);
    const bool full = hdr.full != 0;
    _full = full;
    const auto qtype = FaissTurboQuantType(full, hdr.nb_bits);
    if (stats.size() < sizeof(TurboQuantStatsHeader) || hdr.d != d ||
        hdr.layout != kTurboQuantLayout || hdr.qjl_type != kTurboQuantQjlFwht ||
        !qtype) {
      return;
    }
    _lay = MakeTurboQuantLayout(d, full, hdr.nb_bits, M == VectorMetric::L2Sqr,
                                row_major);
    _sq = std::make_unique<faiss::ScalarQuantizer>(_lay.rd, *qtype);
    TrainTurboQuant(*_sq, hdr.seed);
    _fwht_signs = MakeTurboQuantFwhtSigns(hdr.seed, _lay.rd);
    GenerateSigns(_lay.rd, static_cast<int64_t>(hdr.seed), _signs);
    const size_t ec_bytes = size_t{_lay.rd} * sizeof(float);
    if (stats.size() >= sizeof(TurboQuantStatsHeader) + 2 * ec_bytes) {
      _ec_scale.resize(_lay.rd);
      _ec_shift.resize(_lay.rd);
      std::memcpy(_ec_scale.data(),
                  stats.data() + sizeof(TurboQuantStatsHeader), ec_bytes);
      std::memcpy(_ec_shift.data(),
                  stats.data() + sizeof(TurboQuantStatsHeader) + ec_bytes,
                  ec_bytes);
    } else if (stats.size() >= sizeof(TurboQuantStatsHeader) + ec_bytes) {
      _ec_scale.resize(_lay.rd);
      _ec_shift.assign(_lay.rd, 0.f);
      std::memcpy(_ec_scale.data(),
                  stats.data() + sizeof(TurboQuantStatsHeader), ec_bytes);
    }
    _valid = true;
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::TQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  bool Valid() const noexcept { return _valid; }
  const TurboQuantLayout& Layout() const noexcept { return _lay; }
  const float* Centroids() const noexcept { return _sq->trained.data(); }
  const std::vector<float>& Signs() const noexcept { return _signs; }
  const std::vector<float>& FwhtSigns() const noexcept {
    return _fwht_signs;
  }
  const std::vector<float>& EcScale() const noexcept { return _ec_scale; }
  const std::vector<float>& EcShift() const noexcept { return _ec_shift; }

 private:
  TurboQuantLayout _lay;
  bool _full;
  bool _valid = false;
  std::unique_ptr<faiss::ScalarQuantizer> _sq;
  std::vector<float> _signs;
  // Derived from turboq_refine.seed; faiss no longer stores it.
  std::vector<float> _fwht_signs;
  std::vector<float> _ec_scale;
  std::vector<float> _ec_shift;
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
    RotateInto(_stats->Signs().data(), _query.data(), _rot_query.data(), lay.d,
               lay.rd);
    const auto& ec = _stats->EcScale();
    _qm_block.clear();
    if (!ec.empty()) {
      const auto& sh = _stats->EcShift();
      _qm_block.assign(lay.m1, 0.f);
      for (uint32_t mi = 0; mi < lay.m1; ++mi) {
        const uint32_t base = mi * lay.dims_per_nibble;
        float acc = 0.f;
        for (uint32_t t = 0; t < lay.dims_per_nibble; ++t) {
          acc -= _rot_query[base + t] * sh[base + t];
        }
        _qm_block[mi] = acc;
      }
      for (uint32_t j = 0; j < lay.rd; ++j) {
        _rot_query[j] /= ec[j];
      }
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      _query_norm2 = vector::L2Space<float, float, float>::Norm(
        reinterpret_cast<const byte_type*>(_query.data()),
        static_cast<uint16_t>(lay.d));
    }
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
  float QjlErrorCoeff() const noexcept { return _qjl_error_coeff; }
  float QjlSum() const noexcept { return _qjl_sum; }
  float MseSlack() const noexcept { return _mse_slack; }
  float QueryNorm2() const noexcept { return _query_norm2; }

 private:
  void BuildMseLut() {
    const TurboQuantLayout& lay = _stats->Layout();
    const float* cent = _stats->Centroids();
    const auto mask = static_cast<uint32_t>((1U << lay.mse_bits) - 1);
    std::vector<float> lut(size_t{lay.m1} * kFastScanKsub);
    for (uint32_t mi = 0; mi < lay.m1; ++mi) {
      const uint32_t base = mi * lay.dims_per_nibble;
      for (uint32_t code = 0; code < kFastScanKsub; ++code) {
        float s = 0.f;
        for (uint32_t t = 0; t < lay.dims_per_nibble; ++t) {
          const uint32_t idx = (code >> (t * lay.mse_bits)) & mask;
          s += _rot_query[base + t] * cent[idx];
        }
        lut[size_t{mi} * kFastScanKsub + code] =
          _qm_block.empty() ? s : s + _qm_block[mi];
      }
    }
    _chunks1 = MakeFastScanChunks(lay.m1);
    _lut1.Build(lut.data(), _chunks1, kFastScanKsub);
    float slack = 0.f;
    for (size_t i = 0; i < _chunks1.size(); ++i) {
      slack += static_cast<float>(_chunks1[i].m) * 0.5f * _lut1.Step(i);
    }
    _mse_slack = slack / std::sqrt(static_cast<float>(lay.rd));
  }

  void BuildQjlLut() {
    const TurboQuantLayout& lay = _stats->Layout();
    std::vector<float> qproj(lay.rd);
    TurboQuantProject(_stats->FwhtSigns(), _rot_query.data(), qproj.data(),
                      lay.rd);
    const float inv_sqrt_rd = 1.f / std::sqrt(static_cast<float>(lay.rd));
    float l1 = 0.f;
    _qjl_sum = 0.f;
    for (uint32_t j = 0; j < lay.rd; ++j) {
      qproj[j] *= inv_sqrt_rd;
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
  std::vector<float> _qm_block;
  std::vector<FastScanChunk> _chunks1;
  std::vector<FastScanChunk> _chunks2;
  FastScanLut _lut1;
  FastScanLut _lut2;
  float _qjl_error_coeff = 0.f;
  float _qjl_sum = 0.f;
  float _mse_slack = 0.f;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr, float>
    _query_norm2;
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
    return {.group_size = _lay.row_major ? 1U : uint32_t{kFastScanBbs},
            .record_size = _lay.record_size};
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid != nullptr);
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
    if (!_lay.row_major || _cluster == nullptr) {
      return false;
    }
    const auto& stats = _cb->Stats();
    const float* cent = stats.Centroids();
    const auto mask = static_cast<uint32_t>((1U << _lay.mse_bits) - 1);
    _scale_scratch.resize(_lay.rd);
    for (uint32_t mi = 0; mi < _lay.m1; ++mi) {
      const uint8_t nib = GetNibble(code, mi);
      for (uint32_t t = 0; t < _lay.dims_per_nibble; ++t) {
        const uint32_t idx = (nib >> (t * _lay.mse_bits)) & mask;
        _scale_scratch[mi * _lay.dims_per_nibble + t] = cent[idx];
      }
    }
    if (const auto& ec = stats.EcScale(); !ec.empty()) {
      const auto& sh = stats.EcShift();
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        _scale_scratch[j] = _scale_scratch[j] / ec[j] - sh[j];
      }
    }
    float norm = 0.f;
    std::memcpy(&norm, code + _lay.RowNormOffset(), sizeof(float));
    EnsureRotatedCluster();
    const float scale = norm / std::sqrt(static_cast<float>(_lay.rd));
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _scale_scratch[j] = _scale_scratch[j] * scale + _cluster_rot[j];
    }
    RotateBack(stats.Signs().data(), _scale_scratch.data(), out, _lay.d,
               _lay.rd, _fwht_scratch);
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
    return _lay.row_major && _cluster != nullptr &&
           _cb->Stats().EcScale().empty();
  }

  bool PreparePairTerms(const byte_type* base, uint32_t record_size,
                        uint64_t rows, std::vector<float>& terms) final {
    if (!SupportsPairScores()) {
      return false;
    }
    EnsurePairState();
    const TurboQuantizerCodebook<M> cb{
      _cb->StatsPtr(), std::span<const float>{_cluster, _lay.d}};
    terms.assign(rows, 0.f);
    _packed1.resize(_lay.group1_bytes);
    for (uint64_t off = 0; off < rows; off += kFastScanBbs) {
      const auto take =
        static_cast<size_t>(std::min<uint64_t>(rows - off, kFastScanBbs));
      PackFastScanRows(
        [base, record_size, off](size_t k) {
          return base + (off + k) * size_t{record_size};
        },
        take, 0, _lay.nsq1, _packed1.data());
      Accumulate(_packed1.data(), cb.Lut1(), cb.Chunks1());
      for (size_t k = 0; k < take; ++k) {
        terms[off + k] = _sum[k];
      }
    }
    return true;
  }

  void ScorePairBatch(const byte_type* base, uint32_t record_size,
                      std::span<const float> terms, uint32_t from,
                      std::span<const uint32_t> ids, score_t* out) final {
    SDB_ASSERT(SupportsPairScores());
    SDB_ASSERT(from < terms.size());
    EnsurePairState();
    const byte_type* ra = base + size_t{from} * record_size;
    const float na = LoadFloat(ra + _lay.RowNormOffset());
    const float ta = terms[from] * na;
    const float inv_rd = _inv_sqrt_rd * _inv_sqrt_rd;
    const auto ip_with = [&](const byte_type* rb, float nb, float tb) {
      return _cc + (ta + tb * nb) * _inv_sqrt_rd +
             na * nb * inv_rd * PairDot(ra, rb);
    };
    float a_norm2 = 0.f;
    if constexpr (M == VectorMetric::L2Sqr) {
      a_norm2 = ip_with(ra, na, terms[from]);
    }
    for (size_t i = 0; i < ids.size(); ++i) {
      SDB_ASSERT(ids[i] < terms.size());
      const byte_type* rb = base + size_t{ids[i]} * record_size;
      const float nb = LoadFloat(rb + _lay.RowNormOffset());
      const float ip = ip_with(rb, nb, terms[ids[i]]);
      if constexpr (M == VectorMetric::L2Sqr) {
        out[i] = -(a_norm2 + LoadFloat(rb + _lay.RowXNorm2Offset()) - 2.f * ip);
      } else {
        out[i] = ip;
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
    SDB_ASSERT(_cluster != nullptr);
    _cc = ComputeDistance<VectorMetric::InnerProduct>(
      _cluster, _cluster, static_cast<uint16_t>(_lay.d));
    const float* cent = _cb->Stats().Centroids();
    const auto mask = static_cast<uint32_t>((1U << _lay.mse_bits) - 1);
    const uint32_t k = 1U << kFastScanBits;
    _nib_lut.assign(size_t{k} * k, 0.f);
    for (uint32_t x = 0; x < k; ++x) {
      for (uint32_t y = 0; y < k; ++y) {
        float sum = 0.f;
        for (uint32_t t = 0; t < _lay.dims_per_nibble; ++t) {
          const uint32_t xi = (x >> (t * _lay.mse_bits)) & mask;
          const uint32_t yi = (y >> (t * _lay.mse_bits)) & mask;
          sum += cent[xi] * cent[yi];
        }
        _nib_lut[size_t{x} * k + y] = sum;
      }
    }
    _pair_lut.assign(size_t{256} * 256, 0.f);
    for (uint32_t u = 0; u < 256; ++u) {
      for (uint32_t v = 0; v < 256; ++v) {
        _pair_lut[(size_t{u} << 8) | v] =
          _nib_lut[size_t{u & 0x0FU} * k + (v & 0x0FU)] +
          _nib_lut[size_t{u >> 4U} * k + (v >> 4U)];
      }
    }
  }

  float PairDot(const byte_type* a, const byte_type* b) const noexcept {
    const uint32_t whole = _lay.m1 / 2;
    const auto* ua = reinterpret_cast<const uint8_t*>(a);
    const auto* ub = reinterpret_cast<const uint8_t*>(b);
    float s0 = 0.f;
    float s1 = 0.f;
    uint32_t i = 0;
    for (; i + 2 <= whole; i += 2) {
      s0 += _pair_lut[(size_t{ua[i]} << 8) | ub[i]];
      s1 += _pair_lut[(size_t{ua[i + 1]} << 8) | ub[i + 1]];
    }
    for (; i < whole; ++i) {
      s0 += _pair_lut[(size_t{ua[i]} << 8) | ub[i]];
    }
    float s = s0 + s1;
    if ((_lay.m1 & 1U) != 0) {
      const uint32_t k = 1U << kFastScanBits;
      s += _nib_lut[size_t{ua[whole] & 0x0FU} * k + (ub[whole] & 0x0FU)];
    }
    return s;
  }

  void EnsureRotatedCluster() const {
    if (!_cluster_rot.empty()) {
      return;
    }
    _cluster_rot.resize(_lay.rd);
    RotateInto(_cb->Stats().Signs().data(), _cluster, _cluster_rot.data(),
               _lay.d, _lay.rd);
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

  score_t ScoreFrom(float ip, size_t lane, const float* xnorm2) const noexcept {
    if constexpr (M == VectorMetric::L2Sqr) {
      return -(_cur->QueryNorm2() + xnorm2[lane] - 2.f * (ip + _qc));
    } else {
      return ip + _qc;
    }
  }

  void ScoreLanes(const byte_type* c1, const byte_type* c2, const float* norms,
                  const float* gammas, const float* xnorm2, size_t count,
                  score_t threshold, score_t* out) {
    Accumulate(c1, _cur->Lut1(), _cur->Chunks1());
    for (size_t i = 0; i < count; ++i) {
      _ip[i] = norms[i] * _sum[i] * _inv_sqrt_rd;
    }
    if (!_lay.full) {
      for (size_t i = 0; i < count; ++i) {
        out[i] = ScoreFrom(_ip[i], i, xnorm2);
      }
      return;
    }

    const float err = _cur->QjlErrorCoeff();
    const float slack = _cur->MseSlack();
    bool refine = false;
    for (size_t i = 0; i < count; ++i) {
      const float bound = norms[i] * (err * gammas[i] + slack);
      if (ScoreFrom(_ip[i] + bound, i, xnorm2) > threshold) {
        refine = true;
        break;
      }
    }
    if (!refine) {
      for (size_t i = 0; i < count; ++i) {
        out[i] = ScoreFrom(_ip[i], i, xnorm2);
      }
      return;
    }

    Accumulate(c2, _cur->Lut2(), _cur->Chunks2());
    const float qjl_sum = _cur->QjlSum();
    for (size_t i = 0; i < count; ++i) {
      const float ip =
        _ip[i] + norms[i] * _qjl_coeff * gammas[i] * (2.f * _sum[i] - qjl_sum);
      out[i] = ScoreFrom(ip, i, xnorm2);
    }
  }

  void ScoreGroup(const byte_type* codes, score_t threshold, score_t* out) {
    const auto* norms =
      reinterpret_cast<const float*>(codes + _lay.NormOffset());
    const float* xnorm2 = nullptr;
    if constexpr (M == VectorMetric::L2Sqr) {
      xnorm2 = reinterpret_cast<const float*>(codes + _lay.XNorm2Offset());
    }
    const auto* gammas =
      _lay.full ? reinterpret_cast<const float*>(codes + _lay.GammaOffset())
                : nullptr;
    ScoreLanes(codes, codes + _lay.group1_bytes, norms, gammas, xnorm2,
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
      if constexpr (M == VectorMetric::L2Sqr) {
        std::memcpy(&_rm_xnorm2[i], r + _lay.RowXNorm2Offset(), sizeof(float));
      }
    }
    const float* xnorm2 = nullptr;
    if constexpr (M == VectorMetric::L2Sqr) {
      xnorm2 = _rm_xnorm2.data();
    }
    ScoreLanes(_packed1.data(), _packed2.data(), _rm_norms.data(),
               _lay.full ? _rm_gammas.data() : nullptr, xnorm2, count,
               threshold, out);
  }

  void ScoreRecords(const byte_type* rec, size_t count, score_t threshold,
                    score_t* out) {
    const size_t stride = _lay.record_size;
    ScoreRows([rec, stride](size_t k) { return rec + k * stride; }, count,
              threshold, out);
  }

  void RefreshClusterCorrection() noexcept {
    if (_cluster == nullptr) {
      return;
    }
    const std::span<const float> query = _cur->Query();
    _qc = ComputeDistance<VectorMetric::InnerProduct>(
      query.data(), _cluster, static_cast<uint16_t>(query.size()));
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
  std::vector<float> _nib_lut;
  std::vector<uint8_t> _packed1;
  std::vector<uint8_t> _packed2;
  std::array<float, kFastScanBbs> _rm_norms{};
  std::array<float, kFastScanBbs> _rm_gammas{};
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::array<float, kFastScanBbs>> _rm_xnorm2;
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

template<VectorMetric M>
class ProductQuantizerWriter final : public QuantizerWriter {
 public:
  ProductQuantizerWriter(uint32_t d, uint32_t m, uint32_t niter)
    : _d{d}, _pq{d, m == 0 ? 1 : m, kPqNbits} {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    if (niter != 0) {
      _pq.cp.niter = static_cast<int>(niter);
    }
    _packed.resize(kFastScanBbs * FastScanNsq(_pq.M) / 2);
    _res_tile.resize(kFastScanBbs * size_t{_d});
    _codes_tile.resize(kFastScanBbs * _pq.code_size);
    if constexpr (M == VectorMetric::L2Sqr) {
      _dec.resize(_d);
    }
  }

  size_t TrainSamples(size_t rows) const noexcept final {
    return std::min<size_t>(rows, kPqTrainResiduals);
  }

  void Train(const float* vecs, size_t n) final {
    if (n == 0) {
      return;
    }
    const size_t ksub = _pq.ksub;
    if (n >= ksub) {
      _pq.train(n, vecs);
    } else {
      std::vector<float> padded(ksub * _d);
      for (size_t i = 0; i < ksub; ++i) {
        std::memcpy(padded.data() + i * _d, vecs + (i % n) * _d,
                    _d * sizeof(float));
      }
      _pq.train(ksub, padded.data());
    }
    _trained = true;
  }

  void SetClusterCentroid(const float* centroid) final {
    CodePending();
    _centroid.assign(centroid, centroid + _d);
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = kFastScanBbs,
            .record_size = static_cast<uint32_t>(
              FastScanNsq(_pq.M) / 2 +
              (M == VectorMetric::L2Sqr ? sizeof(float) : 0))};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_trained);
    SDB_ASSERT(_centroid.size() == _d);
    for (size_t i = 0; i < n; ++i) {
      const float* vec = vecs + i * _d;
      float* res = _res_tile.data() + _lane * size_t{_d};
      for (uint32_t j = 0; j < _d; ++j) {
        res[j] = vec[j] - _centroid[j];
      }
      if (++_lane == kFastScanBbs) {
        WriteGroup(out, kFastScanBbs);
      }
    }
  }

  void Finish(IndexOutput& out) final {
    if (_lane != 0) {
      WriteGroup(out, _lane);
    }
  }

  uint32_t PendingLanes() const noexcept final {
    return static_cast<uint32_t>(_lane);
  }

  void Serialize(DataOutput& out) const final {
    if (!_trained) {
      out.WriteU64(0);
      return;
    }
    const auto cents = FloatSpan(_pq.centroids);
    out.WriteU64(sizeof(PqStatsHeader) + cents.size());
    WritePod(out, PqStatsHeader{static_cast<uint32_t>(_pq.M),
                                static_cast<uint32_t>(_pq.ksub)});
    out.WriteData(cents.data(), cents.size());
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

 private:
  void CodePending() {
    if (_lane == _coded) {
      return;
    }
    const size_t count = _lane - _coded;
    uint8_t* codes = _codes_tile.data() + _coded * _pq.code_size;
    _pq.compute_codes(_res_tile.data() + _coded * size_t{_d}, codes, count);
    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < count; ++i) {
        _pq.decode(codes + i * _pq.code_size, _dec.data());
        for (uint32_t j = 0; j < _d; ++j) {
          _dec[j] += _centroid[j];
        }
        _norms[_coded + i] = vector::L2Space<float, float, float>::Norm(
          reinterpret_cast<const byte_type*>(_dec.data()),
          static_cast<uint16_t>(_d));
      }
    }
    _coded = _lane;
  }

  void WriteGroup(IndexOutput& out, size_t count) {
    CodePending();
    faiss::pq4_pack_codes(_codes_tile.data(), count, _pq.M, kFastScanBbs,
                          kFastScanBbs, FastScanNsq(_pq.M), _packed.data(),
                          _pq.code_size);
    out.WriteData(_packed.data(), _packed.size());
    if constexpr (M == VectorMetric::L2Sqr) {
      std::fill_n(_norms.data() + count, kFastScanBbs - count, 0.f);
      out.WriteData(reinterpret_cast<const byte_type*>(_norms.data()),
                    kFastScanBbs * sizeof(float));
    }
    _lane = 0;
    _coded = 0;
  }

  uint32_t _d;
  faiss::ProductQuantizer _pq;
  bool _trained = false;
  size_t _lane = 0;
  size_t _coded = 0;
  std::vector<float> _centroid;
  std::vector<float> _res_tile;
  std::vector<uint8_t> _codes_tile;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::array<float, kFastScanBbs>> _norms;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr,
                                    std::vector<float>> _dec;
  std::vector<uint8_t> _packed;
};

template<VectorMetric M>
class ProductQuantizerStats final : public QuantizerStats {
 public:
  ProductQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    static_assert(M == VectorMetric::L2Sqr || M == VectorMetric::InnerProduct);
    const PqStatsHeader hdr = ReadPodHeader<PqStatsHeader>(stats);
    SDB_ASSERT(hdr.m != 0);
    SDB_ASSERT(d % hdr.m == 0);
    SDB_ASSERT(hdr.ksub != 0);
    _pq.d = d;
    _pq.M = hdr.m;
    _pq.nbits = kPqNbits;
    _pq.set_derived_values();
    const size_t want = _pq.centroids.size() * sizeof(float);
    SDB_ASSERT(hdr.ksub == static_cast<uint32_t>(_pq.ksub));
    SDB_ASSERT(stats.size() >= sizeof(PqStatsHeader) + want);
    std::memcpy(_pq.centroids.data(), stats.data() + sizeof(PqStatsHeader),
                want);
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _pq; }

 private:
  faiss::ProductQuantizer _pq;
};

template<VectorMetric M>
class ProductQuantizerCodebook final : public QuantizerCodebook {
 public:
  ProductQuantizerCodebook(
    std::shared_ptr<const ProductQuantizerStats<M>> stats,
    std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {
    // IP(q, c + r) = IP(q, c) + IP(q, r); the packed LUT for IP(q, r) is
    // query-only and precomputed once per query here.
    const faiss::ProductQuantizer& pq = _stats->Pq();
    const size_t ksub = pq.ksub;
    std::vector<float> ip_table(static_cast<size_t>(pq.M) * ksub);
    pq.compute_inner_prod_table(_query.data(), ip_table.data());
    _lut.Build(ip_table.data(), pq.M, ksub);
    if constexpr (M == VectorMetric::L2Sqr) {
      _query_norm2 = vector::L2Space<float, float, float>::Norm(
        reinterpret_cast<const byte_type*>(_query.data()),
        static_cast<uint16_t>(_query.size()));
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _stats->Pq(); }
  std::span<const float> Query() const noexcept { return _query; }
  const FastScanLut& Lut() const noexcept { return _lut; }
  float QueryNorm() const noexcept
    requires(M == VectorMetric::L2Sqr)
  {
    return _query_norm2;
  }

 private:
  std::shared_ptr<const ProductQuantizerStats<M>> _stats;
  std::vector<float> _query;
  FastScanLut _lut;
  [[no_unique_address]] utils::Need<M == VectorMetric::L2Sqr, float>
    _query_norm2;
};

template<VectorMetric M>
class ProductQuantizerReader final : public QuantizerReader {
 public:
  explicit ProductQuantizerReader(
    std::shared_ptr<const ProductQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)},
      _nsq{FastScanNsq(_cb->Pq().M)},
      _code_bytes{kFastScanBbs * _nsq / 2},
      _group_bytes{_code_bytes + (M == VectorMetric::L2Sqr
                                    ? kFastScanBbs * sizeof(float)
                                    : 0)} {}

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = kFastScanBbs,
            .record_size = static_cast<uint32_t>(_group_bytes / kFastScanBbs)};
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid != nullptr);
    const std::span<const float> query = _cb->Query();
    // IP(q, c + r) = IP(q, c) + IP(q, r); the packed LUT for IP(q, r) is
    // query-only and precomputed once per query in the codebook.
    _qc = ComputeDistance<VectorMetric::InnerProduct>(
      query.data(), centroid, static_cast<uint16_t>(query.size()));
  }

  void ComputeBlock(std::span<const byte_type> block, score_t /*threshold*/,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _group_bytes == 0);
    const FastScanLut& lut = _cb->Lut();
    for (size_t off = 0; off < block.size();
         off += _group_bytes, out += kFastScanBbs) {
      const byte_type* codes = block.data() + off;
      faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_nsq), codes,
                               lut.Packed(), _accu.data());
      if constexpr (M == VectorMetric::L2Sqr) {
        const float* norms =
          reinterpret_cast<const float*>(codes + _code_bytes);
        const float q2 = _cb->QueryNorm();
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          const float ip = lut.Decode(_accu[i]);
          out[i] = -(q2 - 2.f * _qc - 2.f * ip + norms[i]);
        }
      } else {
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          out[i] = lut.Decode(_accu[i]) + _qc;
        }
      }
    }
  }

 private:
  std::shared_ptr<const ProductQuantizerCodebook<M>> _cb;
  size_t _nsq;
  size_t _code_bytes;
  size_t _group_bytes;
  std::array<uint16_t, kFastScanBbs> _accu;
  float _qc = 0.f;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> ProductQuantizerCodebook<M>::MakeReader()
  const {
  return MakeReaderT<ProductQuantizerCodebook<M>, ProductQuantizerReader<M>>(
    this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> ProductQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<ProductQuantizerStats<M>, ProductQuantizerCodebook<M>>(
    this, query);
}

/// Where RaBitQ puts a vector's parts, in either of the two layouts.
///
/// Grouped (IVF): 32 lanes interleaved for fast scan -- every sign code, then
/// every aux record, then every centroid correction. Row-major (HNSW): one
/// self-contained record per vector, because a graph walk visits nodes in no
/// order anyone chose and cannot read a lane out of a block it never fetched.
/// Both hold the same numbers per vector and cost the same per vector; only
/// the interleaving differs, so the grouped layout on disk is unchanged.
struct RaBitQLayout {
  uint32_t d = 0;
  uint32_t rd = 0;
  uint32_t nb_bits = 0;
  uint32_t ex_bits = 0;
  /// Sign nibbles per vector, four rotated dimensions each.
  uint32_t m = 0;
  /// m padded to the even count fast scan wants.
  uint32_t nsq = 0;
  /// Sign bytes in a row-major record. The nibbles sit in dimension order, so
  /// these bytes are already the sign bitmap the refine step reads.
  uint32_t row_code_bytes = 0;
  /// Sign bytes in a whole fast-scan group.
  uint32_t group_code_bytes = 0;
  uint32_t storage = 0;
  uint32_t ex_code_size = 0;
  uint32_t record_size = 0;
  bool row_major = false;

  uint32_t RowAuxOffset() const noexcept { return row_code_bytes; }
  uint32_t RowCsOffset() const noexcept { return row_code_bytes + storage; }
  /// Only the row-major record stores the residual norm. A graph build has to
  /// decode a code back to a vector to re-key on it, and the stored factors
  /// cannot give the norm back under inner product, where or_minus_c_l2sqr is
  /// already net of the vector's own norm. TurboQuant's row-major record keeps
  /// one for the same reason. The grouped layout has no decode step and so
  /// stays exactly as it is on disk.
  uint32_t RowNormOffset() const noexcept {
    return RowCsOffset() + static_cast<uint32_t>(sizeof(float));
  }

  uint32_t AuxOffset() const noexcept { return group_code_bytes; }
  uint32_t CsOffset() const noexcept {
    return AuxOffset() + static_cast<uint32_t>(kFastScanBbs) * storage;
  }
  uint32_t GroupBytes() const noexcept {
    return static_cast<uint32_t>(kFastScanBbs) * record_size;
  }
};

RaBitQLayout MakeRaBitQLayout(uint32_t d, uint32_t nb_bits,
                              bool row_major) noexcept {
  RaBitQLayout l;
  l.d = d;
  l.rd = RotatedDim(d);
  l.nb_bits = nb_bits;
  l.ex_bits = nb_bits - 1;
  l.m = l.rd / static_cast<uint32_t>(kFastScanBits);
  l.nsq = static_cast<uint32_t>(FastScanNsq(l.m));
  l.row_code_bytes = (l.m + 1) / 2;
  l.group_code_bytes = static_cast<uint32_t>(kFastScanBbs) * l.nsq / 2;
  l.storage = static_cast<uint32_t>(
    faiss::rabitq_utils::compute_per_vector_storage_size(nb_bits, l.rd));
  l.ex_code_size = (l.rd * l.ex_bits + 7) / 8;
  l.row_major = row_major;
  l.record_size = (row_major ? l.row_code_bytes : l.nsq / 2) + l.storage +
                  static_cast<uint32_t>(sizeof(float)) *
                    (row_major ? 2U : 1U);
  return l;
}

template<VectorMetric M>
class RaBitQuantizerWriter final : public QuantizerWriter {
 public:
  RaBitQuantizerWriter(uint32_t d, uint32_t nb_bits, bool row_major)
    : _lay{MakeRaBitQLayout(d, nb_bits, row_major)},
      _inv_rd_sqrt{1.f / std::sqrt(static_cast<float>(_lay.rd))} {
    GenerateSigns(_lay.rd, kIvfRotationSeed, _signs);
    _rotated.resize(_lay.rd);
    _residual.resize(_lay.rd);
    if (_lay.row_major) {
      _row.resize(_lay.record_size);
    } else {
      _packed.resize(_lay.group_code_bytes);
      _aux.resize(kFastScanBbs * size_t{_lay.storage});
    }
    SDB_ASSERT(_lay.rd % kFastScanBits == 0);
  }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_lay.rd);
    RotateInto(_signs.data(), centroid, _centroid.data(), _lay.d, _lay.rd);
    _centroid_sum = 0.f;
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _centroid_sum += _centroid[j];
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = _lay.row_major ? 1U : uint32_t{kFastScanBbs},
            .record_size = _lay.record_size};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_centroid.size() == _lay.rd);
    for (size_t i = 0; i < n; ++i) {
      const float* vec = vecs + i * size_t{_lay.d};
      if (_lay.row_major) {
        PackRecord(_row.data(), vec);
        out.WriteData(_row.data(), _row.size());
        continue;
      }
      EncodeGrouped(vec, _lane);
      if (++_lane == kFastScanBbs) {
        WriteGroup(out, kFastScanBbs);
      }
    }
  }

  /// HNSW encodes straight into its own code array, in parallel, from a clone
  /// per thread -- it never sees an IndexOutput. Only the row-major layout can
  /// answer, since a fast-scan group is not addressable one record at a time.
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
    auto clone = std::make_unique<RaBitQuantizerWriter>(_lay.d, _lay.nb_bits,
                                                        _lay.row_major);
    // The centroid is already rotated, so it is copied rather than re-set.
    clone->_centroid = _centroid;
    clone->_centroid_sum = _centroid_sum;
    return clone;
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
    const auto packed = PackSigns(_signs);
    out.WriteU64(sizeof(RaBitQStatsHeader) + packed.size());
    WritePod(out, RaBitQStatsHeader{_lay.nb_bits, _lay.d});
    out.WriteData(packed.data(), packed.size());
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

 private:
  static constexpr faiss::MetricType kMetric =
    M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                             : faiss::MetricType::METRIC_INNER_PRODUCT;

  /// Sign-encode one vector and write its aux record, handing each sign nibble
  /// to `set(sq, nib)` so the caller decides where it lands. Returns the
  /// centroid correction. This is everything the two layouts share, which is
  /// every number either of them stores.
  template<typename SetNib>
  float EncodeVector(const float* vec, uint8_t* aux, float* norm_out,
                     SetNib&& set) {
    RotateInto(_signs.data(), vec, _rotated.data(), _lay.d, _lay.rd);
    for (uint32_t j = 0; j < _lay.rd; ++j) {
      _residual[j] = _rotated[j] - _centroid[j];
    }
    float cs_sum = 0.f;
    for (uint32_t sq = 0; sq < _lay.m; ++sq) {
      uint8_t nib = 0;
      for (uint32_t b = 0; b < kFastScanBits; ++b) {
        const uint32_t j = sq * static_cast<uint32_t>(kFastScanBits) + b;
        if (_residual[j] > 0.f) {
          nib = static_cast<uint8_t>(nib | (1U << b));
          cs_sum += _centroid[j];
        }
      }
      set(sq, nib);
    }
    const faiss::rabitq_utils::SignBitFactorsWithError f =
      faiss::rabitq_utils::compute_vector_factors(
        _rotated.data(), _lay.rd, _centroid.data(), kMetric,
        /*compute_error=*/_lay.ex_bits > 0);
    if (_lay.ex_bits == 0) {
      std::memcpy(aux, &f, sizeof(faiss::rabitq_utils::SignBitFactors));
    } else {
      std::memcpy(aux, &f,
                  sizeof(faiss::rabitq_utils::SignBitFactorsWithError));
      uint8_t* ex_code =
        aux + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
      faiss::rabitq_utils::ExtraBitsFactors ex;
      faiss::rabitq_multibit::quantize_ex_bits(_residual.data(), _lay.rd,
                                               _lay.nb_bits, ex_code, ex,
                                               kMetric, _centroid.data());
      std::memcpy(ex_code + _lay.ex_code_size, &ex,
                  sizeof(faiss::rabitq_utils::ExtraBitsFactors));
    }
    if (norm_out != nullptr) {
      float sum = 0.f;
      for (uint32_t j = 0; j < _lay.rd; ++j) {
        sum += _residual[j] * _residual[j];
      }
      *norm_out = std::sqrt(sum);
    }
    return (2.f * cs_sum - _centroid_sum) * _inv_rd_sqrt;
  }

  void EncodeGrouped(const float* vec, size_t lane) {
    _cs[lane] = EncodeVector(vec, _aux.data() + lane * size_t{_lay.storage},
                             nullptr,
                             [this, lane](uint32_t sq, uint8_t nib) {
                               faiss::pq4_set_packed_element(
                                 _packed.data(), nib, kFastScanBbs, _lay.nsq,
                                 lane, sq);
                             });
  }

  /// One self-contained record: sign bytes in dimension order, then the aux
  /// record, then the correction.
  void PackRecord(byte_type* dst, const float* vec) {
    std::fill_n(dst, _lay.record_size, byte_type{0});
    uint8_t* code = reinterpret_cast<uint8_t*>(dst);
    float norm = 0.f;
    const float cs = EncodeVector(
      vec, code + _lay.RowAuxOffset(), &norm,
      // SetNibble ORs into a zeroed record, which this one is.
      [code](uint32_t sq, uint8_t nib) { SetNibble(code, sq, nib); });
    std::memcpy(dst + _lay.RowCsOffset(), &cs, sizeof(float));
    std::memcpy(dst + _lay.RowNormOffset(), &norm, sizeof(float));
  }

  void WriteGroup(IndexOutput& out, size_t count) {
    const size_t storage = _lay.storage;
    std::memset(_aux.data() + count * storage, 0,
                (kFastScanBbs - count) * storage);
    std::fill_n(_cs.data() + count, kFastScanBbs - count, 0.f);
    out.WriteData(_packed.data(), _packed.size());
    out.WriteData(_aux.data(), _aux.size());
    out.WriteData(reinterpret_cast<const byte_type*>(_cs.data()),
                  kFastScanBbs * sizeof(float));
    std::memset(_packed.data(), 0, _packed.size());
    _lane = 0;
  }

  RaBitQLayout _lay;
  float _inv_rd_sqrt;
  float _centroid_sum = 0.f;
  size_t _lane = 0;
  std::vector<float> _signs;
  std::vector<float> _centroid;
  std::vector<float> _rotated;
  std::vector<float> _residual;
  std::vector<uint8_t> _packed;
  std::vector<uint8_t> _aux;
  std::array<float, kFastScanBbs> _cs{};
  /// One row-major record, staged before it goes to an IndexOutput.
  std::vector<byte_type> _row;
};

template<VectorMetric M>
class RaBitQuantizerStats final : public QuantizerStats {
 public:
  RaBitQuantizerStats(uint32_t d, std::span<const byte_type> stats,
                      bool row_major) {
    const RaBitQStatsHeader hdr = ReadPodHeader<RaBitQStatsHeader>(stats);
    SDB_ASSERT(hdr.nb_bits >= kRaBitQMinBits);
    SDB_ASSERT(hdr.nb_bits <= kRaBitQMaxBits);
    SDB_ASSERT(hdr.d == d);
    SDB_ASSERT(stats.size() >=
               sizeof(RaBitQStatsHeader) + SignBytes(RotatedDim(d)));
    _lay = MakeRaBitQLayout(d, hdr.nb_bits, row_major);
    LoadSigns(stats, sizeof(RaBitQStatsHeader), _lay.rd, _signs);
  }
  uint32_t NbBits() const noexcept { return _lay.nb_bits; }
  const RaBitQLayout& Layout() const noexcept { return _lay; }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const std::vector<float>& Signs() const noexcept { return _signs; }
  uint32_t SrcDim() const noexcept { return _lay.d; }
  uint32_t RotDim() const noexcept { return _lay.rd; }

 private:
  RaBitQLayout _lay;
  std::vector<float> _signs;
};

template<VectorMetric M>
class RaBitQuantizerCodebook final : public QuantizerCodebook {
 public:
  RaBitQuantizerCodebook(std::shared_ptr<const RaBitQuantizerStats<M>> stats,
                         std::span<const float> query)
    : _stats{std::move(stats)} {
    Rekey(query);
  }

  /// Point this codebook at another query, reusing what it already allocated.
  /// The graph build re-keys once per inserted node, so this is a hot path.
  void Rekey(std::span<const float> query) {
    _query.assign(query.begin(), query.end());
    static_assert(!kRaBitQCentered);
    const size_t rd = _stats->RotDim();
    _rotated_query.resize(rd);
    RotateInto(_stats->Signs().data(), _query.data(), _rotated_query.data(),
               _stats->SrcDim(), static_cast<uint32_t>(rd));

    std::vector<float> tmp_q;
    std::vector<uint8_t> qq;
    _qf = faiss::rabitq_utils::compute_query_factors(
      _rotated_query.data(), rd, /*centroid=*/nullptr, kRaBitQQueryBits,
      kRaBitQCentered,
      M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                               : faiss::MetricType::METRIC_INNER_PRODUCT,
      tmp_q, qq);

    const size_t m = rd / kFastScanBits;
    std::vector<float> lut(m * kFastScanKsub);
    for (size_t mi = 0; mi < m; ++mi) {
      const size_t dim_start = mi * kFastScanBits;
      for (size_t code = 0; code < kFastScanKsub; ++code) {
        float ip = 0.f;
        int pc = 0;
        for (size_t off = 0; off < kFastScanBits; ++off) {
          if ((code >> off) & 1) {
            ip += qq[dim_start + off];
            ++pc;
          }
        }
        lut[mi * kFastScanKsub + code] =
          _qf.c1 * ip + _qf.c2 * static_cast<float>(pc);
      }
    }
    _lut.Build(lut.data(), m, kFastScanKsub);
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const std::shared_ptr<const RaBitQuantizerStats<M>>& StatsPtr()
    const noexcept {
    return _stats;
  }
  const std::vector<float>& Signs() const noexcept { return _stats->Signs(); }
  const std::vector<float>& RotatedQuery() const noexcept {
    return _rotated_query;
  }
  std::span<const float> Query() const noexcept { return _query; }
  uint32_t SrcDim() const noexcept { return _stats->SrcDim(); }
  uint32_t RotDim() const noexcept { return _stats->RotDim(); }
  uint32_t NbBits() const noexcept { return _stats->NbBits(); }
  const RaBitQLayout& Layout() const noexcept { return _stats->Layout(); }
  const faiss::rabitq_utils::QueryFactorsData& QueryFactors() const noexcept {
    return _qf;
  }
  const FastScanLut& Lut() const noexcept { return _lut; }

 private:
  std::shared_ptr<const RaBitQuantizerStats<M>> _stats;
  std::vector<float> _query;
  std::vector<float> _rotated_query;
  faiss::rabitq_utils::QueryFactorsData _qf;
  FastScanLut _lut;
};

template<VectorMetric M>
class RaBitQuantizerReader final : public QuantizerReader {
 public:
  explicit RaBitQuantizerReader(
    std::shared_ptr<const RaBitQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)}, _cur{_cb.get()}, _lay{_cb->Layout()}, _rd{_lay.rd} {
    if (_lay.ex_bits > 0) {
      _sign_bits.resize((_rd + 7) / 8);
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = _lay.row_major ? 1U : uint32_t{kFastScanBbs},
            .record_size = _lay.record_size};
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid);
    _cluster = centroid;
    RefreshQuery();
  }

  /// The graph build decodes a node's code and searches with it, so the reader
  /// has to take a new query without rebuilding the stats or the rotation.
  bool SetQuery(std::span<const float> query) final {
    SDB_ASSERT(query.size() == _lay.d);
    if (!_own) {
      _own =
        std::make_unique<RaBitQuantizerCodebook<M>>(_cb->StatsPtr(), query);
      _cur = _own.get();
    } else {
      _own->Rekey(query);
    }
    RefreshQuery();
    return true;
  }

  /// Reconstruct the vector a row-major code stands for: the sign bits give
  /// the residual's direction, the stored norm its length, and the rotation is
  /// orthogonal so it inverts by transpose.
  bool Decode(const byte_type* code, float* out) const final {
    if (!_lay.row_major || _cluster == nullptr) {
      return false;
    }
    float norm = 0.f;
    std::memcpy(&norm, code + _lay.RowNormOffset(), sizeof(float));
    const float step = norm / std::sqrt(static_cast<float>(_rd));
    const auto* bits = reinterpret_cast<const uint8_t*>(code);
    _dec_scratch.resize(_rd);
    for (uint32_t j = 0; j < _rd; ++j) {
      const bool one = ((bits[j >> 3] >> (j & 7)) & 1) != 0;
      _dec_scratch[j] = _rot_centroid[j] + (one ? step : -step);
    }
    RotateBack(_cb->Signs().data(), _dec_scratch.data(), out, _lay.d,
               static_cast<uint32_t>(_rd), _fwht_scratch);
    return true;
  }

 private:
  void RefreshQuery() {
    SDB_ASSERT(_cluster);
    const std::span<const float> query = _cur->Query();
    const auto d = static_cast<uint16_t>(query.size());
    const float* centroid = _cluster;

    // Decode rebuilds around the rotated centroid, so it is kept whether or
    // not there are refine bits to spend it on.
    _rot_centroid.resize(_rd);
    RotateInto(_cb->Signs().data(), centroid, _rot_centroid.data(), _lay.d,
               static_cast<uint32_t>(_rd));

    _qf = _cur->QueryFactors();
    _qf.qr_to_c_L2sqr =
      -ComputeDistance<VectorMetric::L2Sqr>(query.data(), centroid, d);
    _qf.g_error = std::sqrt(_qf.qr_to_c_L2sqr);
    if constexpr (M == VectorMetric::InnerProduct) {
      _qf.q_dot_c =
        ComputeDistance<VectorMetric::InnerProduct>(query.data(), centroid, d);
    }
    if (_lay.ex_bits == 0) {
      return;
    }
    const std::vector<float>& rq = _cur->RotatedQuery();
    _q_res.resize(_rd);
    for (size_t j = 0; j < _rd; ++j) {
      _q_res[j] = rq[j] - _rot_centroid[j];
    }
  }

  /// HNSW reaches codes by node id, never as a run, so the gathered form is
  /// the one it uses.
  void ComputeGathered(const byte_type* base, uint32_t record_size,
                       std::span<const uint32_t> ids, score_t threshold,
                       score_t* out) final {
    if (!_lay.row_major) {
      QuantizerReader::ComputeGathered(base, record_size, ids, threshold, out);
      return;
    }
    const float mt = MetricThreshold(threshold);
    for (size_t off = 0; off < ids.size(); off += kFastScanBbs) {
      const size_t take = std::min<size_t>(ids.size() - off, kFastScanBbs);
      const auto sub = ids.subspan(off, take);
      ScoreRows(
        [base, record_size, sub](size_t k) {
          return base + static_cast<size_t>(sub[k]) * record_size;
        },
        take, mt, out + off);
    }
  }

  void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                    score_t* out) final {
    const float mt = MetricThreshold(threshold);
    if (_lay.row_major) {
      SDB_ASSERT(block.size() % _lay.record_size == 0);
      size_t left = block.size() / _lay.record_size;
      const byte_type* rec = block.data();
      const size_t stride = _lay.record_size;
      while (left != 0) {
        const size_t take = std::min<size_t>(left, kFastScanBbs);
        ScoreRows([rec, stride](size_t k) { return rec + k * stride; }, take,
                  mt, out);
        rec += take * stride;
        out += take;
        left -= take;
      }
      return;
    }
    const size_t group_bytes = _lay.GroupBytes();
    SDB_ASSERT(block.size() % group_bytes == 0);
    for (size_t off = 0; off < block.size();
         off += group_bytes, out += kFastScanBbs) {
      ScoreGroup(block.data() + off, mt, out);
    }
  }

 private:
  float MetricThreshold(score_t threshold) const noexcept {
    return M == VectorMetric::L2Sqr ? -threshold : threshold;
  }

  /// A group is already in fast-scan order and its aux records sit in one run.
  void ScoreGroup(const byte_type* codes, float mt, score_t* out) {
    const byte_type* aux = codes + _lay.AuxOffset();
    const auto* cs = reinterpret_cast<const float*>(codes + _lay.CsOffset());
    const size_t storage = _lay.storage;
    ScoreLanes(
      codes, [aux, storage](size_t i) { return aux + i * storage; },
      [this, codes](size_t i) {
        // A group interleaves the sign bits across lanes, so the flat bitmap
        // compute_full_multibit_distance wants has to be rebuilt per lane.
        faiss::rabitq_utils::unpack_sign_bits_from_packed(
          codes, kFastScanBbs, _lay.nsq, i, _lay.group_code_bytes,
          _sign_bits.data());
        return const_cast<const uint8_t*>(_sign_bits.data());
      },
      cs, kFastScanBbs, mt, out);
  }

  /// Row-major records are transposed into one fast-scan block so the same
  /// kernel scores them; their aux records stay where they are, one per row.
  template<typename Row>
  void ScoreRows(Row row, size_t count, float mt, score_t* out) {
    _packed.resize(_lay.group_code_bytes);
    PackFastScanRows(row, count, 0, _lay.nsq, _packed.data());
    for (size_t i = 0; i < count; ++i) {
      std::memcpy(&_rm_cs[i], row(i) + _lay.RowCsOffset(), sizeof(float));
    }
    const uint32_t aux_off = _lay.RowAuxOffset();
    ScoreLanes(
      _packed.data(), [row, aux_off](size_t i) { return row(i) + aux_off; },
      // The nibbles sit in dimension order, so a record's sign bytes already
      // are the bitmap the refine wants -- packing them into a fast-scan block
      // and unpacking them back out again was work that cancelled itself.
      [row](size_t i) { return reinterpret_cast<const uint8_t*>(row(i)); },
      _rm_cs.data(), count, mt, out);
  }

  template<typename Aux, typename Signs>
  void ScoreLanes(const byte_type* packed, Aux aux, Signs signs,
                  const float* cs, size_t count, float mt, score_t* out) {
    const FastScanLut& lut = _cur->Lut();
    faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_lay.nsq),
                             packed, lut.Packed(), _accu.data());
    for (size_t i = 0; i < count; ++i) {
      const float normalized = lut.Decode(_accu[i]) - cs[i];
      const auto* fac =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactors*>(aux(i));
      out[i] = faiss::rabitq_utils::compute_1bit_adjusted_distance(
        normalized, *fac, _qf, kRaBitQCentered, kRaBitQQueryBits, _rd);
    }
    if (_lay.ex_bits > 0) {
      Refine(aux, signs, count, mt, out);
    }
    if constexpr (M == VectorMetric::L2Sqr) {
      for (size_t i = 0; i < count; ++i) {
        out[i] = -out[i];
      }
    }
  }

  template<typename Aux, typename Signs>
  void Refine(Aux aux, Signs signs, size_t count, float threshold,
              score_t* out) {
    const float qr_base =
      M == VectorMetric::L2Sqr ? _qf.qr_to_c_L2sqr : _qf.q_dot_c;
    for (size_t i = 0; i < count; ++i) {
      const byte_type* rec = aux(i);
      const auto* fe =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactorsWithError*>(
          rec);
      if (!faiss::rabitq_utils::should_refine_candidate(
            out[i], fe->f_error, _qf.g_error, threshold,
            M != VectorMetric::L2Sqr)) {
        continue;
      }
      const uint8_t* ex_code =
        rec + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
      const auto* ex_fac =
        reinterpret_cast<const faiss::rabitq_utils::ExtraBitsFactors*>(
          ex_code + _lay.ex_code_size);
      out[i] = faiss::rabitq_utils::compute_full_multibit_distance(
        signs(i), ex_code, *ex_fac, _q_res.data(), qr_base, _rd,
        _lay.ex_bits,
        M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                                 : faiss::MetricType::METRIC_INNER_PRODUCT);
    }
  }

  std::shared_ptr<const RaBitQuantizerCodebook<M>> _cb;
  /// The codebook in force: the shared one until SetQuery makes a private one.
  const RaBitQuantizerCodebook<M>* _cur;
  std::unique_ptr<RaBitQuantizerCodebook<M>> _own;
  RaBitQLayout _lay;
  size_t _rd;
  const float* _cluster = nullptr;
  faiss::rabitq_utils::QueryFactorsData _qf;
  std::vector<float> _rot_centroid;
  std::vector<float> _q_res;
  std::vector<uint8_t> _sign_bits;
  std::vector<uint8_t> _packed;
  mutable std::vector<float> _dec_scratch;
  mutable std::vector<float> _fwht_scratch;
  std::array<float, kFastScanBbs> _rm_cs{};
  std::array<uint16_t, kFastScanBbs> _accu;
};

template<VectorMetric M>
std::unique_ptr<QuantizerReader> RaBitQuantizerCodebook<M>::MakeReader() const {
  return MakeReaderT<RaBitQuantizerCodebook<M>, RaBitQuantizerReader<M>>(this);
}

template<VectorMetric M>
std::shared_ptr<const QuantizerCodebook> RaBitQuantizerStats<M>::MakeCodebook(
  std::span<const float> query) const {
  return MakeCodebookT<RaBitQuantizerStats<M>, RaBitQuantizerCodebook<M>>(
    this, query);
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

std::shared_ptr<const QuantizerStats> MakePanoramaStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> blob) {
  std::shared_ptr<const QuantizerStats> stats;
  ResolveEnum<VectorMetric>(EffectiveQuantMetric(metric), [&]<VectorMetric M> {
    stats = std::make_shared<const PanoramaQuantizerStats<M>>(d, blob);
  });
  return stats;
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

}  // namespace

bool PanoramaApplies(VectorMetric metric, uint32_t d) noexcept {
  return metric != VectorMetric::L1 && d >= kPanoramaMinDim;
}

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits, bool row_major) {
  switch (quant) {
    case VectorQuantization::None:
      return std::make_unique<PanoramaQuantizerWriter>(d, metric);
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
    case VectorQuantization::USQ8:
    case VectorQuantization::USQ4:
      return std::make_unique<ScalarQuantizerWriter>(d, quant);
    case VectorQuantization::PQ:
      return MakeWriterWithMetric<ProductQuantizerWriter>(metric, d, pq_m,
                                                          pq_niter);
    case VectorQuantization::TQ: {
      const bool full = TQFullBits(nb_bits);
      const auto qtype = FaissTurboQuantType(full, nb_bits);
      SDB_ASSERT(qtype);
      return MakeWriterWithMetric<TurboQuantizerWriter>(
        metric, d, full, nb_bits, *qtype, row_major);
    }
    case VectorQuantization::RaBitQ:
      return MakeWriterWithMetric<RaBitQuantizerWriter>(metric, d, nb_bits,
                                                        row_major);
  }
  return nullptr;
}

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric, bool row_major) {
  switch (quant) {
    case VectorQuantization::None:
      return MakePanoramaStats(metric, d, stats);
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
    case VectorQuantization::USQ8:
    case VectorQuantization::USQ4:
      return MakeStatsWithMetric<ScalarQuantizerStats>(metric, d, quant, stats);
    case VectorQuantization::PQ:
      return MakeStatsWithMetric<ProductQuantizerStats>(metric, d, stats);
    case VectorQuantization::TQ:
      return MakeStatsWithMetric<TurboQuantizerStats>(metric, d, stats,
                                                      row_major);
    case VectorQuantization::RaBitQ:
      return MakeStatsWithMetric<RaBitQuantizerStats>(metric, d, stats,
                                                      row_major);
  }
  return nullptr;
}

void QuantizerReader::ComputeGathered(const byte_type* base,
                                      uint32_t record_size,
                                      std::span<const uint32_t> ids,
                                      score_t threshold, score_t* out) {
  _gather.resize(ids.size() * static_cast<size_t>(record_size));
  for (size_t i = 0; i < ids.size(); ++i) {
    std::memcpy(_gather.data() + i * record_size,
                base + static_cast<size_t>(ids[i]) * record_size, record_size);
  }
  ComputeBlock(_gather, threshold, out);
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook) {
  return codebook ? codebook->MakeReader() : nullptr;
}

}  // namespace irs
