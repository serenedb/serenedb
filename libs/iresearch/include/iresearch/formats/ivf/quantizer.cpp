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

#include <faiss/MetricType.h>
#include <faiss/impl/Panorama.h>
#include <faiss/impl/ProductQuantizer.h>
#include <faiss/impl/RaBitQUtils.h>
#include <faiss/impl/RaBitQuantizerMultiBit.h>
#include <faiss/impl/ScalarQuantizer.h>
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
#include <cstring>
#include <limits>
#include <optional>
#include <vector>

#include "basics/assert.h"
#include "basics/misc.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

faiss::ScalarQuantizer::QuantizerType FaissScalarType(
  VectorQuantization quant) {
  switch (quant) {
    case VectorQuantization::SQ4:
      return faiss::ScalarQuantizer::QuantizerType::QT_4bit;
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

#if defined(__AVX512F__)
constexpr uint32_t kSimdFloatLanes = 16;
#elif defined(__AVX2__)
constexpr uint32_t kSimdFloatLanes = 8;
#elif defined(__SSE2__) || defined(__ARM_NEON) || defined(__aarch64__)
constexpr uint32_t kSimdFloatLanes = 4;
#else
constexpr uint32_t kSimdFloatLanes = 1;
#endif

constexpr uint32_t kSq8OpsPerDim = 8;
constexpr uint32_t kSq4OpsPerDim = 9;

constexpr uint32_t kStreamBytesPerCycle = 12;

constexpr int64_t kRaBitQRotationSeed = 0x5a17b17c5eed5eedULL;

uint32_t RotatedDim(uint32_t d) noexcept {
  return std::max<uint32_t>(kFastScanBits, std::bit_ceil(d));
}

void GenerateSigns(uint32_t rotated_d, int64_t seed,
                   std::vector<float>& signs) {
  signs.resize(rotated_d);
  faiss::float_randn(signs.data(), signs.size(), seed);
  for (uint32_t i = 0; i < rotated_d; ++i) {
    signs[i] = signs[i] < 0.f ? -1.f : 1.f;
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

  uint32_t ScanCostBytes() const noexcept final {
    return static_cast<uint32_t>(sizeof(float)) * _d;
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
    _sq.trained.assign(2 * static_cast<size_t>(_d), 0.f);
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

  void Finish(IndexOutput& /*out*/) final {}

  void Serialize(DataOutput& out) const final {
    const auto bytes = FloatSpan(_sq.trained);
    out.WriteU64(bytes.size());
    out.WriteData(bytes.data(), bytes.size());
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

  uint32_t ScanCostBytes() const noexcept final {
    const uint32_t ops =
      _quant == VectorQuantization::SQ4 ? kSq4OpsPerDim : kSq8OpsPerDim;
    return ops * _d * kStreamBytesPerCycle / kSimdFloatLanes;
  }

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
    _sq.trained.assign(2 * static_cast<size_t>(d), 0.f);
    const size_t want = _sq.trained.size() * sizeof(float);
    if (stats.size() >= want) {
      std::memcpy(_sq.trained.data(), stats.data(), want);
    }
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

 private:
  std::shared_ptr<const ScalarQuantizerCodebook<M>> _cb;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
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

  uint32_t ScanCostBytes() const noexcept final {
    const size_t nsq = FastScanNsq(_pq.M);
    return static_cast<uint32_t>(_pq.code_size +
                                 nsq * kFastScanKsub / kFastScanBbs);
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
    if (hdr.m != 0 && d % hdr.m == 0 && hdr.ksub != 0) {
      _pq.d = d;
      _pq.M = hdr.m;
      _pq.nbits = kPqNbits;
      _pq.set_derived_values();
      const size_t want = _pq.centroids.size() * sizeof(float);
      if (hdr.ksub == static_cast<uint32_t>(_pq.ksub) &&
          stats.size() >= sizeof(PqStatsHeader) + want) {
        std::memcpy(_pq.centroids.data(), stats.data() + sizeof(PqStatsHeader),
                    want);
        _valid = true;
      }
    }
  }

  bool Valid() const noexcept { return _valid; }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _pq; }

 private:
  faiss::ProductQuantizer _pq;
  bool _valid = false;
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
    const size_t nsq = FastScanNsq(pq.M);
    std::vector<float> ip_table(static_cast<size_t>(pq.M) * ksub);
    pq.compute_inner_prod_table(_query.data(), ip_table.data());
    std::vector<byte_type> lutq(nsq * ksub);
    faiss::quantize_lut::quantize_LUT_and_bias(
      1, pq.M, ksub, false, ip_table.data(), nullptr, lutq.data(), nsq, nullptr,
      &_ip_a, &_ip_b);
    _packed_ip_lut.resize(nsq * ksub);
    faiss::pq4_pack_LUT(1, static_cast<int>(nsq), lutq.data(),
                        _packed_ip_lut.data());
    if constexpr (M == VectorMetric::L2Sqr) {
      _query_norm2 = vector::L2Space<float, float, float>::Norm(
        reinterpret_cast<const byte_type*>(_query.data()),
        static_cast<uint16_t>(_query.size()));
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _stats->Pq(); }
  std::span<const float> Query() const noexcept { return _query; }
  const uint8_t* PackedIpLut() const noexcept { return _packed_ip_lut.data(); }
  float IpA() const noexcept { return _ip_a; }
  float IpB() const noexcept { return _ip_b; }
  float QueryNorm() const noexcept
    requires(M == VectorMetric::L2Sqr)
  {
    return _query_norm2;
  }

 private:
  std::shared_ptr<const ProductQuantizerStats<M>> _stats;
  std::vector<float> _query;
  faiss::AlignedTable<uint8_t> _packed_ip_lut;
  float _ip_a = 1.f;
  float _ip_b = 0.f;
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
    const float inv_a = 1.f / _cb->IpA();
    const float b = _cb->IpB();
    for (size_t off = 0; off < block.size();
         off += _group_bytes, out += kFastScanBbs) {
      const byte_type* codes = block.data() + off;
      faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_nsq), codes,
                               _cb->PackedIpLut(), _accu.data());
      if constexpr (M == VectorMetric::L2Sqr) {
        const float* norms =
          reinterpret_cast<const float*>(codes + _code_bytes);
        const float q2 = _cb->QueryNorm();
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          const float ip = static_cast<float>(_accu[i]) * inv_a + b;
          out[i] = -(q2 - 2.f * _qc - 2.f * ip + norms[i]);
        }
      } else {
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          out[i] = static_cast<float>(_accu[i]) * inv_a + b + _qc;
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

template<VectorMetric M>
class RaBitQuantizerWriter final : public QuantizerWriter {
 public:
  RaBitQuantizerWriter(uint32_t d, uint32_t nb_bits)
    : _d{d},
      _rd{RotatedDim(d)},
      _nb_bits{nb_bits},
      _ex_bits{nb_bits - 1},
      _storage{
        faiss::rabitq_utils::compute_per_vector_storage_size(nb_bits, _rd)},
      _ex_code_size{(static_cast<size_t>(_rd) * _ex_bits + 7) / 8},
      _sign_stride{FastScanNsq(_rd / kFastScanBits) / 2},
      _inv_rd_sqrt{1.f / std::sqrt(static_cast<float>(_rd))} {
    GenerateSigns(_rd, kRaBitQRotationSeed, _signs);
    _rotated.resize(_rd);
    _residual.resize(_rd);
    _packed.resize(kFastScanBbs * _sign_stride);
    _aux.resize(kFastScanBbs * _storage);
    SDB_ASSERT(_rd % kFastScanBits == 0);
  }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_rd);
    RotateInto(_signs.data(), centroid, _centroid.data(), _d, _rd);
    _centroid_sum = 0.f;
    for (uint32_t j = 0; j < _rd; ++j) {
      _centroid_sum += _centroid[j];
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = kFastScanBbs,
            .record_size =
              static_cast<uint32_t>(_sign_stride + _storage + sizeof(float))};
  }

  void Encode(IndexOutput& out, const float* vecs, size_t n) final {
    SDB_ASSERT(_centroid.size() == _rd);
    for (size_t i = 0; i < n; ++i) {
      EncodeOne(vecs + i * _d, _lane);
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
    out.WriteU64(sizeof(RaBitQStatsHeader));
    WritePod(out, RaBitQStatsHeader{_nb_bits, _d});
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  uint32_t ScanCostBytes() const noexcept final {
    const size_t nsq = _sign_stride * 2;
    return static_cast<uint32_t>(_sign_stride + _storage + sizeof(float) +
                                 nsq * kFastScanKsub / kFastScanBbs);
  }

 private:
  static constexpr faiss::MetricType kMetric =
    M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                             : faiss::MetricType::METRIC_INNER_PRODUCT;

  void EncodeOne(const float* vec, size_t lane) {
    RotateInto(_signs.data(), vec, _rotated.data(), _d, _rd);
    const size_t m = _rd / kFastScanBits;
    const size_t nsq = FastScanNsq(m);
    for (uint32_t j = 0; j < _rd; ++j) {
      _residual[j] = _rotated[j] - _centroid[j];
    }
    float cs_sum = 0.f;
    for (size_t sq = 0; sq < m; ++sq) {
      uint8_t nib = 0;
      for (uint32_t b = 0; b < kFastScanBits; ++b) {
        const uint32_t j = static_cast<uint32_t>(sq) * kFastScanBits + b;
        if (_residual[j] > 0.f) {
          nib = static_cast<uint8_t>(nib | (1U << b));
          cs_sum += _centroid[j];
        }
      }
      faiss::pq4_set_packed_element(_packed.data(), nib, kFastScanBbs, nsq,
                                    lane, sq);
    }
    _cs[lane] = (2.f * cs_sum - _centroid_sum) * _inv_rd_sqrt;
    uint8_t* aux = _aux.data() + lane * _storage;
    const faiss::rabitq_utils::SignBitFactorsWithError f =
      faiss::rabitq_utils::compute_vector_factors(
        _rotated.data(), _rd, _centroid.data(), kMetric,
        /*compute_error=*/_ex_bits > 0);
    if (_ex_bits == 0) {
      std::memcpy(aux, &f, sizeof(faiss::rabitq_utils::SignBitFactors));
      return;
    }
    std::memcpy(aux, &f, sizeof(faiss::rabitq_utils::SignBitFactorsWithError));
    uint8_t* ex_code =
      aux + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
    faiss::rabitq_utils::ExtraBitsFactors ex;
    faiss::rabitq_multibit::quantize_ex_bits(
      _residual.data(), _rd, _nb_bits, ex_code, ex, kMetric, _centroid.data());
    std::memcpy(ex_code + _ex_code_size, &ex,
                sizeof(faiss::rabitq_utils::ExtraBitsFactors));
  }

  void WriteGroup(IndexOutput& out, size_t count) {
    std::memset(_aux.data() + count * _storage, 0,
                (kFastScanBbs - count) * _storage);
    std::fill_n(_cs.data() + count, kFastScanBbs - count, 0.f);
    out.WriteData(_packed.data(), _packed.size());
    out.WriteData(_aux.data(), _aux.size());
    out.WriteData(reinterpret_cast<const byte_type*>(_cs.data()),
                  kFastScanBbs * sizeof(float));
    std::memset(_packed.data(), 0, _packed.size());
    _lane = 0;
  }

  uint32_t _d;
  uint32_t _rd;
  uint32_t _nb_bits;
  uint32_t _ex_bits;
  size_t _storage;
  size_t _ex_code_size;
  size_t _sign_stride;
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
};

template<VectorMetric M>
class RaBitQuantizerStats final : public QuantizerStats {
 public:
  RaBitQuantizerStats(uint32_t d, std::span<const byte_type> stats) {
    const RaBitQStatsHeader hdr = ReadPodHeader<RaBitQStatsHeader>(stats);
    if (hdr.nb_bits >= kRaBitQMinBits && hdr.nb_bits <= kRaBitQMaxBits &&
        hdr.d == d && stats.size() >= sizeof(RaBitQStatsHeader)) {
      _d = d;
      _rd = RotatedDim(d);
      _nb_bits = hdr.nb_bits;
      GenerateSigns(_rd, kRaBitQRotationSeed, _signs);
      _valid = true;
    }
  }

  bool Valid() const noexcept { return _valid; }
  uint32_t NbBits() const noexcept { return _nb_bits; }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  std::shared_ptr<const QuantizerCodebook> MakeCodebook(
    std::span<const float> query) const final;

  const std::vector<float>& Signs() const noexcept { return _signs; }
  uint32_t SrcDim() const noexcept { return _d; }
  uint32_t RotDim() const noexcept { return _rd; }

 private:
  uint32_t _d = 0;
  uint32_t _rd = 0;
  uint32_t _nb_bits = 0;
  std::vector<float> _signs;
  bool _valid = false;
};

template<VectorMetric M>
class RaBitQuantizerCodebook final : public QuantizerCodebook {
 public:
  RaBitQuantizerCodebook(std::shared_ptr<const RaBitQuantizerStats<M>> stats,
                         std::span<const float> query)
    : _stats{std::move(stats)}, _query(query.begin(), query.end()) {
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
    const size_t nsq = FastScanNsq(m);
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
    std::vector<uint8_t> lutq(nsq * kFastScanKsub);
    faiss::quantize_lut::quantize_LUT_and_bias(1, m, kFastScanKsub, false,
                                               lut.data(), nullptr, lutq.data(),
                                               nsq, nullptr, &_a, &_b);
    _packed_lut.resize(nsq * kFastScanKsub);
    faiss::pq4_pack_LUT(1, static_cast<int>(nsq), lutq.data(),
                        _packed_lut.data());
  }

  std::unique_ptr<QuantizerReader> MakeReader() const final;

  const std::vector<float>& Signs() const noexcept { return _stats->Signs(); }
  const std::vector<float>& RotatedQuery() const noexcept {
    return _rotated_query;
  }
  std::span<const float> Query() const noexcept { return _query; }
  uint32_t SrcDim() const noexcept { return _stats->SrcDim(); }
  uint32_t RotDim() const noexcept { return _stats->RotDim(); }
  uint32_t NbBits() const noexcept { return _stats->NbBits(); }
  const faiss::rabitq_utils::QueryFactorsData& QueryFactors() const noexcept {
    return _qf;
  }
  const uint8_t* PackedLut() const noexcept { return _packed_lut.data(); }
  float A() const noexcept { return _a; }
  float B() const noexcept { return _b; }

 private:
  std::shared_ptr<const RaBitQuantizerStats<M>> _stats;
  std::vector<float> _query;
  std::vector<float> _rotated_query;
  faiss::rabitq_utils::QueryFactorsData _qf;
  faiss::AlignedTable<uint8_t> _packed_lut;
  float _a = 1.f;
  float _b = 0.f;
};

template<VectorMetric M>
class RaBitQuantizerReader final : public QuantizerReader {
 public:
  explicit RaBitQuantizerReader(
    std::shared_ptr<const RaBitQuantizerCodebook<M>> cb)
    : _cb{std::move(cb)},
      _rd{_cb->RotDim()},
      _nsq{FastScanNsq(_rd / kFastScanBits)},
      _ex_bits{_cb->NbBits() - 1},
      _storage{faiss::rabitq_utils::compute_per_vector_storage_size(
        _cb->NbBits(), _rd)},
      _ex_code_size{(static_cast<size_t>(_rd) * _ex_bits + 7) / 8},
      _code_bytes{kFastScanBbs * _nsq / 2},
      _aux_bytes{kFastScanBbs * _storage},
      _group_bytes{_code_bytes + _aux_bytes + kFastScanBbs * sizeof(float)} {
    if (_ex_bits > 0) {
      _sign_bits.resize((_rd + 7) / 8);
    }
  }

  PayloadBlockSetting BlockSetting() const noexcept final {
    return {.group_size = kFastScanBbs,
            .record_size = static_cast<uint32_t>(_group_bytes / kFastScanBbs)};
  }

  void StartCluster(const float* centroid) final {
    SDB_ASSERT(centroid);
    const std::span<const float> query = _cb->Query();
    const auto d = static_cast<uint16_t>(query.size());

    _qf = _cb->QueryFactors();
    _qf.qr_to_c_L2sqr =
      -ComputeDistance<VectorMetric::L2Sqr>(query.data(), centroid, d);
    _qf.g_error = std::sqrt(_qf.qr_to_c_L2sqr);
    if constexpr (M == VectorMetric::InnerProduct) {
      _qf.q_dot_c =
        ComputeDistance<VectorMetric::InnerProduct>(query.data(), centroid, d);
    }
    if (_ex_bits == 0) {
      return;
    }
    _rot_centroid.resize(_rd);
    RotateInto(_cb->Signs().data(), centroid, _rot_centroid.data(),
               _cb->SrcDim(), static_cast<uint32_t>(_rd));
    const std::vector<float>& rq = _cb->RotatedQuery();
    _q_res.resize(_rd);
    for (size_t j = 0; j < _rd; ++j) {
      _q_res[j] = rq[j] - _rot_centroid[j];
    }
  }

  void ComputeBlock(std::span<const byte_type> block, score_t threshold,
                    score_t* out) final {
    SDB_ASSERT(block.size() % _group_bytes == 0);
    const float metric_threshold =
      M == VectorMetric::L2Sqr ? -threshold : threshold;
    for (size_t off = 0; off < block.size();
         off += _group_bytes, out += kFastScanBbs) {
      const byte_type* codes = block.data() + off;
      ScoreSignBits(codes, out);
      if (_ex_bits > 0) {
        Refine(codes, metric_threshold, out);
      }
      if constexpr (M == VectorMetric::L2Sqr) {
        for (size_t i = 0; i < kFastScanBbs; ++i) {
          out[i] = -out[i];
        }
      }
    }
  }

 private:
  void ScoreSignBits(const byte_type* codes, score_t* out) {
    const byte_type* aux = codes + _code_bytes;
    const auto* cs = reinterpret_cast<const float*>(aux + _aux_bytes);
    faiss::accumulate_to_mem(1, kFastScanBbs, static_cast<int>(_nsq), codes,
                             _cb->PackedLut(), _accu.data());
    const float inv_a = 1.f / _cb->A();
    const float b = _cb->B();
    for (size_t i = 0; i < kFastScanBbs; ++i) {
      const float normalized = static_cast<float>(_accu[i]) * inv_a + b - cs[i];
      const auto* fac =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactors*>(
          aux + i * _storage);
      out[i] = faiss::rabitq_utils::compute_1bit_adjusted_distance(
        normalized, *fac, _qf, kRaBitQCentered, kRaBitQQueryBits, _rd);
    }
  }

  void Refine(const byte_type* codes, float threshold, score_t* out) {
    const byte_type* aux = codes + _code_bytes;
    const float qr_base =
      M == VectorMetric::L2Sqr ? _qf.qr_to_c_L2sqr : _qf.q_dot_c;
    for (size_t i = 0; i < kFastScanBbs; ++i) {
      const byte_type* rec = aux + i * _storage;
      const auto* fe =
        reinterpret_cast<const faiss::rabitq_utils::SignBitFactorsWithError*>(
          rec);
      if (!faiss::rabitq_utils::should_refine_candidate(
            out[i], fe->f_error, _qf.g_error, threshold,
            M != VectorMetric::L2Sqr)) {
        continue;
      }
      faiss::rabitq_utils::unpack_sign_bits_from_packed(
        codes, kFastScanBbs, _nsq, i, _code_bytes, _sign_bits.data());
      const uint8_t* ex_code =
        rec + sizeof(faiss::rabitq_utils::SignBitFactorsWithError);
      const auto* ex_fac =
        reinterpret_cast<const faiss::rabitq_utils::ExtraBitsFactors*>(
          ex_code + _ex_code_size);
      out[i] = faiss::rabitq_utils::compute_full_multibit_distance(
        _sign_bits.data(), ex_code, *ex_fac, _q_res.data(), qr_base, _rd,
        _ex_bits,
        M == VectorMetric::L2Sqr ? faiss::MetricType::METRIC_L2
                                 : faiss::MetricType::METRIC_INNER_PRODUCT);
    }
  }

  std::shared_ptr<const RaBitQuantizerCodebook<M>> _cb;
  size_t _rd;
  size_t _nsq;
  uint32_t _ex_bits;
  size_t _storage;
  size_t _ex_code_size;
  size_t _code_bytes;
  size_t _aux_bytes;
  size_t _group_bytes;
  faiss::rabitq_utils::QueryFactorsData _qf;
  std::vector<float> _rot_centroid;
  std::vector<float> _q_res;
  std::vector<uint8_t> _sign_bits;
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
  uint32_t pq_niter, uint32_t nb_bits) {
  switch (quant) {
    case VectorQuantization::None:
      return std::make_unique<PanoramaQuantizerWriter>(d, metric);
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return std::make_unique<ScalarQuantizerWriter>(d, quant);
    case VectorQuantization::PQ:
      return MakeWriterWithMetric<ProductQuantizerWriter>(metric, d, pq_m,
                                                          pq_niter);
    case VectorQuantization::RaBitQ:
      return MakeWriterWithMetric<RaBitQuantizerWriter>(metric, d, nb_bits);
  }
  return nullptr;
}

std::shared_ptr<const QuantizerStats> MakeQuantizerStats(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  VectorMetric metric) {
  switch (quant) {
    case VectorQuantization::None:
      return MakePanoramaStats(metric, d, stats);
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return MakeStatsWithMetric<ScalarQuantizerStats>(metric, d, quant, stats);
    case VectorQuantization::PQ:
      return MakeStatsWithMetric<ProductQuantizerStats>(metric, d, stats);
    case VectorQuantization::RaBitQ:
      return MakeStatsWithMetric<RaBitQuantizerStats>(metric, d, stats);
  }
  return nullptr;
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook) {
  return codebook ? codebook->MakeReader() : nullptr;
}

}  // namespace irs
