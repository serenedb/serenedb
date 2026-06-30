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
#include <faiss/impl/ScalarQuantizer.h>

#include <algorithm>
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

class ScalarQuantizerReader final : public QuantizerReader {
 public:
  ScalarQuantizerReader(std::unique_ptr<IndexInput> pay_in, uint32_t d,
                        VectorQuantization quant,
                        std::span<const byte_type> stats)
    : _pay_in{std::move(pay_in)},
      _d{d},
      _sq{d, FaissScalarType(quant)},
      _codes_reader{*_pay_in, static_cast<uint32_t>(_sq.code_size)} {
    _sq.trained.assign(2 * static_cast<size_t>(_d), 0.f);
    const size_t want = _sq.trained.size() * sizeof(float);
    if (stats.size() >= want) {
      std::memcpy(_sq.trained.data(), stats.data(), want);
    }
  }

  void SetQuery(std::span<const float> query, VectorMetric metric) final {
    _query.assign(query.begin(), query.end());
    _dc.reset(_sq.get_distance_computer(FaissMetric(metric)));
    _dc->code_size = _sq.code_size;
    _dc->set_query(_query.data());
  }

  void StartCluster(uint64_t pay_start, size_t num_docs,
                    const float* /*centroid*/) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    _codes_reader.Reset(pay_start);
  }

  void ComputeBlock(size_t offset, size_t count, score_t /*boost*/,
                    score_t* out) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(offset + count <= _n);
    const byte_type* block = _codes_reader.Read(offset, count);
    _dc->codes = block;
    const size_t cs = _sq.code_size;
    size_t i = 0;
    for (; i + 3 < count; i += 4) {
      _dc->distances_batch_4(i, i + 1, i + 2, i + 3, out[i], out[i + 1],
                             out[i + 2], out[i + 3]);
    }
    for (; i < count; i++) {
      out[i] = _dc->distance_to_code(block + i * cs);
    }
  }

 private:
  std::unique_ptr<IndexInput> _pay_in;
  uint32_t _d;
  std::vector<float> _query;
  faiss::ScalarQuantizer _sq;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  VectorBlockReader _codes_reader;
  size_t _n = 0;
};

class ProductQuantizerWriter final : public QuantizerWriter {
 public:
  ProductQuantizerWriter(uint32_t d, uint32_t m)
    : _d{d}, _pq{d, m == 0 ? 1 : m, 8} {}

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

  void EncodeCluster(IndexOutput& out, const float* vecs,
                     size_t n) const final {
    if (n == 0) {
      return;
    }
    SDB_ASSERT(_trained);
    SDB_ASSERT(_centroid.size() == _d);
    _code.resize(n * _pq.code_size);
    std::vector<float> res(_d);
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        res[j] = v[j] - _centroid[j];
      }
      _pq.compute_code(res.data(), _code.data() + i * _pq.code_size);
    }
    out.WriteData(_code.data(), _code.size());
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
    _stats.resize(2 * sizeof(uint32_t) + _pq.centroids.size() * sizeof(float));
    std::memcpy(_stats.data(), &m, sizeof(m));
    std::memcpy(_stats.data() + sizeof(m), &ksub, sizeof(ksub));
    std::memcpy(_stats.data() + 2 * sizeof(uint32_t), _pq.centroids.data(),
                _pq.centroids.size() * sizeof(float));
  }

  uint32_t _d;
  faiss::ProductQuantizer _pq;
  bool _trained = false;
  std::vector<byte_type> _stats;
  std::vector<float> _centroid;
  mutable std::vector<uint8_t> _code;
};

class ProductQuantizerReader final : public QuantizerReader {
 public:
  ProductQuantizerReader(std::unique_ptr<IndexInput> pay_in, uint32_t d,
                         std::span<const byte_type> stats)
    : _pay_in{std::move(pay_in)},
      _hdr{ReadHeader(stats)},
      _codes_reader{*_pay_in, _hdr.m == 0 ? 1 : _hdr.m} {
    if (_hdr.m != 0 && d % _hdr.m == 0 && _hdr.ksub != 0) {
      _pq.d = d;
      _pq.M = _hdr.m;
      _pq.nbits = 8;
      _pq.set_derived_values();
      const size_t want = _pq.centroids.size() * sizeof(float);
      if (_hdr.ksub == static_cast<uint32_t>(_pq.ksub) &&
          stats.size() >= 2 * sizeof(uint32_t) + want) {
        std::memcpy(_pq.centroids.data(), stats.data() + 2 * sizeof(uint32_t),
                    want);
        _valid = true;
      }
    }
  }

  bool Valid() const noexcept { return _valid; }

  void SetQuery(std::span<const float> query, VectorMetric metric) final {
    _query.assign(query.begin(), query.end());
    _metric = metric;
    SDB_ASSERT(metric == VectorMetric::L2Sqr ||
               metric == VectorMetric::InnerProduct);
  }

  void StartCluster(uint64_t pay_start, size_t num_docs,
                    const float* centroid) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    SDB_ASSERT(centroid != nullptr);
    _table.resize(static_cast<size_t>(_pq.M) * _pq.ksub);
    if (_metric == VectorMetric::L2Sqr) {
      // ||q - (c + r)||^2 = ||(q - c) - r||^2: ADC table for the residual
      // query.
      std::vector<float> qr(_query.size());
      for (size_t j = 0; j < _query.size(); ++j) {
        qr[j] = _query[j] - centroid[j];
      }
      _pq.compute_distance_table(qr.data(), _table.data());
      _ip_offset = 0.f;
    } else {
      // IP(q, c + r) = IP(q, c) + IP(q, r).
      _pq.compute_inner_prod_table(_query.data(), _table.data());
      float off = 0.f;
      for (size_t j = 0; j < _query.size(); ++j) {
        off += _query[j] * centroid[j];
      }
      _ip_offset = off;
    }
    _codes_reader.Reset(pay_start);
  }

  void ComputeBlock(size_t offset, size_t count, score_t /*boost*/,
                    score_t* out) final {
    SDB_ASSERT(offset + count <= _n);
    const byte_type* block = _codes_reader.Read(offset, count);
    const size_t m = _pq.M;
    const size_t ksub = _pq.ksub;
    const float* table = _table.data();
    for (size_t i = 0; i < count; ++i) {
      const byte_type* code = block + i * m;
      float acc = _ip_offset;
      for (size_t j = 0; j < m; ++j) {
        acc += table[j * ksub + code[j]];
      }
      out[i] = acc;
    }
  }

 private:
  struct Header {
    uint32_t m;
    uint32_t ksub;
  };

  static Header ReadHeader(std::span<const byte_type> stats) noexcept {
    Header h{0, 0};
    if (stats.size() >= 2 * sizeof(uint32_t)) {
      std::memcpy(&h.m, stats.data(), sizeof(h.m));
      std::memcpy(&h.ksub, stats.data() + sizeof(h.m), sizeof(h.ksub));
    }
    return h;
  }

  std::unique_ptr<IndexInput> _pay_in;
  Header _hdr;
  faiss::ProductQuantizer _pq;
  VectorBlockReader _codes_reader;
  bool _valid = false;
  VectorMetric _metric = VectorMetric::L2Sqr;
  std::vector<float> _query;
  std::vector<float> _table;
  float _ip_offset = 0.f;
  size_t _n = 0;
};

}  // namespace

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(VectorQuantization quant,
                                                     uint32_t d,
                                                     VectorMetric /*metric*/,
                                                     uint32_t pq_m) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return std::make_unique<ScalarQuantizerWriter>(d, quant);
    case VectorQuantization::PQ:
      return std::make_unique<ProductQuantizerWriter>(d, pq_m);
  }
  return nullptr;
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  VectorQuantization quant, std::unique_ptr<IndexInput> pay_in, uint32_t d,
  std::span<const byte_type> stats) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
    case VectorQuantization::SQ4:
      return std::make_unique<ScalarQuantizerReader>(std::move(pay_in), d,
                                                     quant, stats);
    case VectorQuantization::PQ: {
      auto reader =
        std::make_unique<ProductQuantizerReader>(std::move(pay_in), d, stats);
      if (!reader->Valid()) {
        return nullptr;
      }
      return reader;
    }
  }
  return nullptr;
}

}  // namespace irs
