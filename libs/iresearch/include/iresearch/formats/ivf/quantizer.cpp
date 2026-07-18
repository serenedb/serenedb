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

#include <faiss/impl/ProductQuantizer.h>
#include <faiss/impl/RaBitQuantizer.h>
#include <faiss/impl/ScalarQuantizer.h>
#include <faiss/impl/pq4_fast_scan.h>
#include <faiss/utils/AlignedTable.h>
#include <faiss/utils/distances.h>
#include <faiss/utils/quantize_lut.h>
#include <faiss/utils/random.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <vector>

#include "basics/assert.h"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

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

faiss::MetricType FaissMetric(VectorMetric metric) {
  SDB_ASSERT(metric == VectorMetric::L2Sqr ||
             metric == VectorMetric::InnerProduct);
  return metric == VectorMetric::L2Sqr
           ? faiss::MetricType::METRIC_L2
           : faiss::MetricType::METRIC_INNER_PRODUCT;
}

std::span<const byte_type> FloatSpan(const std::vector<float>& v) noexcept {
  return {reinterpret_cast<const byte_type*>(v.data()),
          v.size() * sizeof(float)};
}

constexpr size_t kFastScanBbs = 32;
constexpr uint32_t kPqNbits = 4;

size_t RoundUp(size_t n, size_t multiple) noexcept {
  return (n + multiple - 1) / multiple * multiple;
}

size_t FastScanNsq(size_t m) noexcept { return m + (m & 1); }

constexpr int64_t kRaBitQRotationSeed = 0x5a17b17c5eed5eedULL;

uint32_t RotatedDim(uint32_t d) noexcept { return std::bit_ceil(d); }

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
void WritePodHeader(const H& h, byte_type* out) noexcept {
  std::memcpy(out, &h, sizeof(H));
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

  void Train(const float* vecs, size_t n) final {
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _vmin[j] = std::min(_vmin[j], v[j]);
        _vmax[j] = std::max(_vmax[j], v[j]);
      }
    }
    for (uint32_t j = 0; j < _d; ++j) {
      const bool seen = _vmin[j] <= _vmax[j];
      _sq.trained[j] = seen ? _vmin[j] : 0.f;
      _sq.trained[_d + j] = seen ? (_vmax[j] - _vmin[j]) : 0.f;
    }
  }

  void EncodeCluster(IndexOutput& out, const float* vecs,
                     size_t n) const final {
    if (n == 0) {
      return;
    }
    _code.resize(n * _sq.code_size);
    _sq.compute_codes(vecs, _code.data(), n);
    out.WriteData(_code.data(), _code.size());
  }

  std::span<const byte_type> StatsBytes() const final {
    return FloatSpan(_sq.trained);
  }

  VectorQuantization Kind() const noexcept final { return _quant; }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_sq.code_size);
  }

 private:
  uint32_t _d;
  VectorQuantization _quant;
  faiss::ScalarQuantizer _sq;
  std::vector<float> _vmin;
  std::vector<float> _vmax;
  mutable std::vector<uint8_t> _code;
};

class ScalarQuantizerCodebook final : public QuantizerCodebook {
 public:
  ScalarQuantizerCodebook(uint32_t d, VectorQuantization quant,
                          std::span<const byte_type> stats,
                          std::span<const float> query, VectorMetric metric)
    : _sq{d, FaissScalarType(quant)},
      _metric{metric},
      _query(query.begin(), query.end()) {
    _sq.trained.assign(2 * static_cast<size_t>(d), 0.f);
    const size_t want = _sq.trained.size() * sizeof(float);
    if (stats.size() >= want) {
      std::memcpy(_sq.trained.data(), stats.data(), want);
    }
  }

  std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in) const final;

  const faiss::ScalarQuantizer& Sq() const noexcept { return _sq; }
  std::span<const float> Query() const noexcept { return _query; }
  VectorMetric Metric() const noexcept { return _metric; }

 private:
  faiss::ScalarQuantizer _sq;
  VectorMetric _metric;
  std::vector<float> _query;
};

class ScalarQuantizerReader final : public QuantizerReader {
 public:
  ScalarQuantizerReader(std::shared_ptr<const ScalarQuantizerCodebook> cb,
                        std::unique_ptr<IndexInput> pay_in)
    : _cb{std::move(cb)},
      _pay_in{std::move(pay_in)},
      _codes_reader{*_pay_in, static_cast<uint32_t>(_cb->Sq().code_size)} {
    _dc.reset(_cb->Sq().get_distance_computer(FaissMetric(_cb->Metric())));
    _dc->code_size = _cb->Sq().code_size;
    _dc->set_query(_cb->Query().data());
  }

  void StartCluster(uint64_t pay_start, size_t num_docs,
                    const float* /*centroid*/) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    _codes_reader.Reset(pay_start);
  }

  void ComputeBlock(size_t offset, size_t count, score_t* out) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(offset + count <= _n);
    const byte_type* block = _codes_reader.Read(offset, count);
    _dc->codes = block;
    const size_t cs = _cb->Sq().code_size;
    const bool is_l2 = _cb->Metric() == VectorMetric::L2Sqr;
    size_t i = 0;
    for (; i + 3 < count; i += 4) {
      _dc->distances_batch_4(i, i + 1, i + 2, i + 3, out[i], out[i + 1],
                             out[i + 2], out[i + 3]);
      if (is_l2) {
        out[i] = -out[i];
        out[i + 1] = -out[i + 1];
        out[i + 2] = -out[i + 2];
        out[i + 3] = -out[i + 3];
      }
    }
    for (; i < count; i++) {
      const auto d = _dc->distance_to_code(block + i * cs);
      out[i] = is_l2 ? -d : d;
    }
  }

 private:
  std::shared_ptr<const ScalarQuantizerCodebook> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  VectorBlockReader _codes_reader;
  size_t _n = 0;
};

template<class Codebook, class Reader>
std::unique_ptr<QuantizerReader> MakeReaderT(const Codebook* self,
                                             std::unique_ptr<IndexInput> in) {
  return std::make_unique<Reader>(
    std::static_pointer_cast<const Codebook>(self->shared_from_this()),
    std::move(in));
}

std::unique_ptr<QuantizerReader> ScalarQuantizerCodebook::MakeReader(
  std::unique_ptr<IndexInput> pay_in) const {
  return MakeReaderT<ScalarQuantizerCodebook, ScalarQuantizerReader>(
    this, std::move(pay_in));
}

class ProductQuantizerWriter final : public QuantizerWriter {
 public:
  ProductQuantizerWriter(uint32_t d, uint32_t m, uint32_t niter)
    : _d{d}, _pq{d, m == 0 ? 1 : m, kPqNbits} {
    if (niter != 0) {
      _pq.cp.niter = static_cast<int>(niter);
    }
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
    BuildStats();
    _trained = true;
  }

  void SetClusterCentroid(const float* centroid) final {
    _centroid.assign(centroid, centroid + _d);
  }

  void BeginCluster(size_t total_docs) final {
    _cluster_codes.assign(total_docs * _pq.code_size, 0);
    _cluster_filled = 0;
  }

  void EncodeCluster(IndexOutput& /*out*/, const float* vecs,
                     size_t n) const final {
    if (n == 0) {
      return;
    }
    SDB_ASSERT(_trained);
    SDB_ASSERT(_centroid.size() == _d);
    SDB_ASSERT((_cluster_filled + n) * _pq.code_size <= _cluster_codes.size());
    _res.resize(_d);
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _res[j] = v[j] - _centroid[j];
      }
      _pq.compute_code(_res.data(), _cluster_codes.data() +
                                      (_cluster_filled + i) * _pq.code_size);
    }
    _cluster_filled += n;
  }

  void FinishCluster(IndexOutput& out) final {
    if (_cluster_filled == 0) {
      _cluster_codes.clear();
      return;
    }
    const size_t m = _pq.M;
    const size_t nsq = FastScanNsq(m);
    const size_t nb = RoundUp(_cluster_filled, kFastScanBbs);
    _packed.assign(nb * nsq / 2, 0);
    faiss::pq4_pack_codes(_cluster_codes.data(), _cluster_filled, m, nb,
                          kFastScanBbs, nsq, _packed.data());
    out.WriteData(_packed.data(), _packed.size());
    _cluster_codes.clear();
  }

  std::span<const byte_type> StatsBytes() const final {
    return {_stats.data(), _stats.size()};
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::PQ;
  }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_pq.code_size);
  }

 private:
  void BuildStats() {
    const uint32_t m = static_cast<uint32_t>(_pq.M);
    const uint32_t ksub = static_cast<uint32_t>(_pq.ksub);
    _stats.resize(sizeof(PqStatsHeader) + _pq.centroids.size() * sizeof(float));
    WritePodHeader(PqStatsHeader{m, ksub}, _stats.data());
    std::memcpy(_stats.data() + sizeof(PqStatsHeader), _pq.centroids.data(),
                _pq.centroids.size() * sizeof(float));
  }

  uint32_t _d;
  faiss::ProductQuantizer _pq;
  bool _trained = false;
  std::vector<byte_type> _stats;
  std::vector<float> _centroid;
  mutable std::vector<uint8_t> _cluster_codes;
  mutable size_t _cluster_filled = 0;
  mutable std::vector<float> _res;
  std::vector<uint8_t> _packed;
};

class ProductQuantizerCodebook final : public QuantizerCodebook {
 public:
  ProductQuantizerCodebook(uint32_t d, std::span<const byte_type> stats,
                           std::span<const float> query, VectorMetric metric)
    : _metric{metric}, _query(query.begin(), query.end()) {
    SDB_ASSERT(metric == VectorMetric::L2Sqr ||
               metric == VectorMetric::InnerProduct);
    const PqStatsHeader hdr = ReadHeader(stats);
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
        if (_metric == VectorMetric::InnerProduct) {
          const size_t ksub = _pq.ksub;
          const size_t nsq = FastScanNsq(_pq.M);
          std::vector<float> ip_table(static_cast<size_t>(_pq.M) * ksub);
          _pq.compute_inner_prod_table(_query.data(), ip_table.data());
          std::vector<byte_type> lutq(nsq * ksub);
          faiss::quantize_lut::quantize_LUT_and_bias(
            1, _pq.M, ksub, false, ip_table.data(), nullptr, lutq.data(), nsq,
            nullptr, &_ip_a, &_ip_b);
          _packed_ip_lut.resize(nsq * ksub);
          faiss::pq4_pack_LUT(1, static_cast<int>(nsq), lutq.data(),
                              _packed_ip_lut.data());
        }
      }
    }
  }

  bool Valid() const noexcept { return _valid; }

  std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in) const final;

  const faiss::ProductQuantizer& Pq() const noexcept { return _pq; }
  std::span<const float> Query() const noexcept { return _query; }
  VectorMetric Metric() const noexcept { return _metric; }
  const uint8_t* PackedIpLut() const noexcept { return _packed_ip_lut.data(); }
  float IpA() const noexcept { return _ip_a; }
  float IpB() const noexcept { return _ip_b; }

 private:
  static PqStatsHeader ReadHeader(std::span<const byte_type> stats) noexcept {
    return ReadPodHeader<PqStatsHeader>(stats);
  }

  faiss::ProductQuantizer _pq;
  VectorMetric _metric;
  std::vector<float> _query;
  faiss::AlignedTable<uint8_t> _packed_ip_lut;
  float _ip_a = 1.f;
  float _ip_b = 0.f;
  bool _valid = false;
};

class ProductQuantizerReader final : public QuantizerReader {
 public:
  ProductQuantizerReader(std::shared_ptr<const ProductQuantizerCodebook> cb,
                         std::unique_ptr<IndexInput> pay_in)
    : _cb{std::move(cb)}, _pay_in{std::move(pay_in)} {}

  void StartCluster(uint64_t pay_start, size_t num_docs,
                    const float* centroid) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    SDB_ASSERT(centroid != nullptr);
    const faiss::ProductQuantizer& pq = _cb->Pq();
    const size_t m = pq.M;
    const size_t ksub = pq.ksub;
    const size_t nsq = FastScanNsq(m);
    const std::span<const float> query = _cb->Query();

    const uint8_t* packed_lut;
    float a;
    float b;
    float ip_offset = 0.f;
    const bool is_l2 = _cb->Metric() == VectorMetric::L2Sqr;
    if (is_l2) {
      _qr.resize(query.size());
      for (size_t j = 0; j < query.size(); ++j) {
        _qr[j] = query[j] - centroid[j];
      }
      _table.resize(m * ksub);
      pq.compute_distance_table(_qr.data(), _table.data());

      _lutq.resize(nsq * ksub);
      faiss::quantize_lut::quantize_LUT_and_bias(
        1, m, ksub, false, _table.data(), nullptr, _lutq.data(), nsq, nullptr,
        &a, &b);
      _packed_lut.resize(nsq * ksub);
      faiss::pq4_pack_LUT(1, static_cast<int>(nsq), _lutq.data(),
                          _packed_lut.data());
      packed_lut = _packed_lut.data();
    } else {
      // IP(q, c + r) = IP(q, c) + IP(q, r); the packed LUT for IP(q, r) is
      // query-only and precomputed once per query in the codebook.
      packed_lut = _cb->PackedIpLut();
      a = _cb->IpA();
      b = _cb->IpB();
      for (size_t j = 0; j < query.size(); ++j) {
        ip_offset += query[j] * centroid[j];
      }
    }

    const size_t nb = RoundUp(_n, kFastScanBbs);
    const size_t packed_bytes = nb * nsq / 2;
    const byte_type* codes = _pay_in->ReadStable(pay_start, packed_bytes);
    if (!codes) {
      _codes_buf.resize(packed_bytes);
      _pay_in->ReadData(pay_start, _codes_buf.data(), packed_bytes);
      codes = _codes_buf.data();
    }
    _accu.resize(nb);
    faiss::accumulate_to_mem(1, nb, static_cast<int>(nsq), codes, packed_lut,
                             _accu.data());

    _scores.resize(_n);
    const float inv_a = 1.f / a;
    for (size_t i = 0; i < _n; ++i) {
      const float dist = static_cast<float>(_accu[i]) * inv_a + b;
      _scores[i] = is_l2 ? -dist : dist + ip_offset;
    }
  }

  void ComputeBlock(size_t offset, size_t count, score_t* out) final {
    SDB_ASSERT(offset + count <= _n);
    std::memcpy(out, _scores.data() + offset, count * sizeof(score_t));
  }

 private:
  std::shared_ptr<const ProductQuantizerCodebook> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  std::vector<float> _table;
  std::vector<float> _qr;
  std::vector<uint8_t> _lutq;
  faiss::AlignedTable<uint8_t> _packed_lut;
  std::vector<byte_type> _codes_buf;
  std::vector<uint16_t> _accu;
  std::vector<score_t> _scores;
  size_t _n = 0;
};

std::unique_ptr<QuantizerReader> ProductQuantizerCodebook::MakeReader(
  std::unique_ptr<IndexInput> pay_in) const {
  return MakeReaderT<ProductQuantizerCodebook, ProductQuantizerReader>(
    this, std::move(pay_in));
}

class RaBitQuantizerWriter final : public QuantizerWriter {
 public:
  RaBitQuantizerWriter(uint32_t d, VectorMetric metric, uint32_t nb_bits)
    : _d{d}, _rd{RotatedDim(d)}, _rabitq{_rd, FaissMetric(metric), nb_bits} {
    GenerateSigns(_rd, kRaBitQRotationSeed, _signs);
    _stats.resize(sizeof(RaBitQStatsHeader));
    WritePodHeader(RaBitQStatsHeader{nb_bits, d}, _stats.data());
  }

  void Train(const float* /*vecs*/, size_t /*n*/) final {}

  void SetClusterCentroid(const float* centroid) final {
    _centroid.resize(_rd);
    RotateInto(_signs.data(), centroid, _centroid.data(), _d, _rd);
  }

  void EncodeCluster(IndexOutput& out, const float* vecs,
                     size_t n) const final {
    if (n == 0) {
      return;
    }
    SDB_ASSERT(_centroid.size() == _rd);
    _rotated.resize(n * _rd);
    for (size_t i = 0; i < n; ++i) {
      RotateInto(_signs.data(), vecs + i * _d, _rotated.data() + i * _rd, _d,
                 _rd);
    }
    _code.resize(n * _rabitq.code_size);
    _rabitq.compute_codes_core(_rotated.data(), _code.data(), n,
                               _centroid.data());
    out.WriteData(_code.data(), _code.size());
  }

  std::span<const byte_type> StatsBytes() const final {
    return {_stats.data(), _stats.size()};
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::RaBitQ;
  }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_rabitq.code_size);
  }

 private:
  uint32_t _d;
  uint32_t _rd;
  faiss::RaBitQuantizer _rabitq;
  std::vector<float> _signs;
  std::vector<float> _centroid;
  std::vector<byte_type> _stats;
  mutable std::vector<float> _rotated;
  mutable std::vector<uint8_t> _code;
};

class RaBitQuantizerCodebook final : public QuantizerCodebook {
 public:
  RaBitQuantizerCodebook(uint32_t d, std::span<const byte_type> stats,
                         std::span<const float> query, VectorMetric metric) {
    const RaBitQStatsHeader hdr = ReadHeader(stats);
    if (hdr.nb_bits >= kRaBitQMinBits && hdr.nb_bits <= kRaBitQMaxBits &&
        hdr.d == d && stats.size() >= 2 * sizeof(uint32_t)) {
      _d = d;
      _rd = RotatedDim(d);
      _rabitq = std::make_unique<faiss::RaBitQuantizer>(
        _rd, FaissMetric(metric), hdr.nb_bits);
      GenerateSigns(_rd, kRaBitQRotationSeed, _signs);
      _rotated_query.resize(_rd);
      RotateInto(_signs.data(), query.data(), _rotated_query.data(), _d, _rd);
      _metric = metric;
      _valid = true;
    }
  }

  bool Valid() const noexcept { return _valid; }

  std::unique_ptr<QuantizerReader> MakeReader(
    std::unique_ptr<IndexInput> pay_in) const final;

  const faiss::RaBitQuantizer& Rabitq() const noexcept { return *_rabitq; }
  const std::vector<float>& Signs() const noexcept { return _signs; }
  const std::vector<float>& RotatedQuery() const noexcept {
    return _rotated_query;
  }
  uint32_t SrcDim() const noexcept { return _d; }
  uint32_t RotDim() const noexcept { return _rd; }
  VectorMetric Metric() const noexcept { return _metric; }

 private:
  static RaBitQStatsHeader ReadHeader(
    std::span<const byte_type> stats) noexcept {
    return ReadPodHeader<RaBitQStatsHeader>(stats);
  }

  uint32_t _d = 0;
  uint32_t _rd = 0;
  std::unique_ptr<faiss::RaBitQuantizer> _rabitq;
  std::vector<float> _signs;
  std::vector<float> _rotated_query;
  VectorMetric _metric = VectorMetric::L2Sqr;
  bool _valid = false;
};

class RaBitQuantizerReader final : public QuantizerReader {
 public:
  RaBitQuantizerReader(std::shared_ptr<const RaBitQuantizerCodebook> cb,
                       std::unique_ptr<IndexInput> pay_in)
    : _cb{std::move(cb)},
      _pay_in{std::move(pay_in)},
      _codes_reader{*_pay_in, static_cast<uint32_t>(_cb->Rabitq().code_size)} {}

  void StartCluster(uint64_t pay_start, size_t num_docs,
                    const float* centroid) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    SDB_ASSERT(centroid != nullptr);
    _rotated_centroid.resize(_cb->RotDim());
    RotateInto(_cb->Signs().data(), centroid, _rotated_centroid.data(),
               _cb->SrcDim(), _cb->RotDim());
    _dc.reset(
      _cb->Rabitq().get_distance_computer(0, _rotated_centroid.data(), false));
    _dc->set_query(_cb->RotatedQuery().data());
    _codes_reader.Reset(pay_start);
  }

  void ComputeBlock(size_t offset, size_t count, score_t* out) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(offset + count <= _n);
    const byte_type* block = _codes_reader.Read(offset, count);
    const size_t cs = _cb->Rabitq().code_size;
    const bool is_l2 = _cb->Metric() == VectorMetric::L2Sqr;
    for (size_t i = 0; i < count; ++i) {
      const auto d = _dc->distance_to_code(block + i * cs);
      out[i] = is_l2 ? -d : d;
    }
  }

 private:
  std::shared_ptr<const RaBitQuantizerCodebook> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  std::unique_ptr<faiss::FlatCodesDistanceComputer> _dc;
  VectorBlockReader _codes_reader;
  std::vector<float> _rotated_centroid;
  size_t _n = 0;
};

std::unique_ptr<QuantizerReader> RaBitQuantizerCodebook::MakeReader(
  std::unique_ptr<IndexInput> pay_in) const {
  return MakeReaderT<RaBitQuantizerCodebook, RaBitQuantizerReader>(
    this, std::move(pay_in));
}

}  // namespace

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(
  VectorQuantization quant, uint32_t d, VectorMetric metric, uint32_t pq_m,
  uint32_t pq_niter, uint32_t nb_bits) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return std::make_unique<ScalarQuantizerWriter>(d, quant);
    case VectorQuantization::PQ:
      return std::make_unique<ProductQuantizerWriter>(d, pq_m, pq_niter);
    case VectorQuantization::RaBitQ:
      return std::make_unique<RaBitQuantizerWriter>(d, metric, nb_bits);
  }
  return nullptr;
}

std::shared_ptr<const QuantizerCodebook> MakeQuantizerCodebook(
  VectorQuantization quant, uint32_t d, std::span<const byte_type> stats,
  std::span<const float> query, VectorMetric metric) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return std::make_shared<ScalarQuantizerCodebook>(d, quant, stats, query,
                                                       metric);
    case VectorQuantization::PQ: {
      auto cb =
        std::make_shared<ProductQuantizerCodebook>(d, stats, query, metric);
      if (!cb->Valid()) {
        return nullptr;
      }
      return cb;
    }
    case VectorQuantization::RaBitQ: {
      auto cb =
        std::make_shared<RaBitQuantizerCodebook>(d, stats, query, metric);
      if (!cb->Valid()) {
        return nullptr;
      }
      return cb;
    }
  }
  return nullptr;
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook,
  std::unique_ptr<IndexInput> pay_in) {
  return codebook ? codebook->MakeReader(std::move(pay_in)) : nullptr;
}

}  // namespace irs
