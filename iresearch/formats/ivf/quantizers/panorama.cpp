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
#include <faiss/impl/Panorama.h>
#include <faiss/utils/ordered_key_value.h>

#include <core_functions/array_kernels.hpp>
#include <limits>
#include <optional>

#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/quantizers/common.hpp"
#include "iresearch/index/vector_distance.hpp"
#include "iresearch/utils/misc.hpp"

namespace irs::quantizers {
namespace {

constexpr uint32_t kPanoramaLevelWidth = 32;

constexpr size_t kPcaMinRows = 1024;
constexpr size_t kPcaTrainRows = 4096;

struct PanoramaStatsHeader {
  uint32_t n_levels;
  uint32_t d;
};

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
    RotateVector(reinterpret_cast<const float*>(_stats->Rotation()),
                 query.data(), _query.data(), d);
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

}  // namespace

std::shared_ptr<const QuantizerStats> MakePanoramaStats(
  VectorMetric metric, uint32_t d, std::span<const byte_type> blob) {
  std::shared_ptr<const QuantizerStats> stats;
  ResolveEnum<VectorMetric>(EffectiveQuantMetric(metric), [&]<VectorMetric M> {
    stats = std::make_shared<const PanoramaQuantizerStats<M>>(d, blob);
  });
  return stats;
}

std::unique_ptr<QuantizerWriter> MakePanoramaWriter(uint32_t d,
                                                    VectorMetric metric) {
  return std::make_unique<PanoramaQuantizerWriter>(d, metric);
}

}  // namespace irs::quantizers
