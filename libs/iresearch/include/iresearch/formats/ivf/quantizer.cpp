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

  void ComputeBlock(size_t offset, size_t count, score_t /*boost*/,
                    score_t* out) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(offset + count <= _n);
    const byte_type* block = _codes_reader.Read(offset, count);
    _dc->codes = block;
    const size_t cs = _cb->Sq().code_size;
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
  std::shared_ptr<const ScalarQuantizerCodebook> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  VectorBlockReader _codes_reader;
  size_t _n = 0;
};

std::unique_ptr<QuantizerReader> ScalarQuantizerCodebook::MakeReader(
  std::unique_ptr<IndexInput> pay_in) const {
  return std::make_unique<ScalarQuantizerReader>(
    std::static_pointer_cast<const ScalarQuantizerCodebook>(shared_from_this()),
    std::move(pay_in));
}

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

class ProductQuantizerCodebook final : public QuantizerCodebook {
 public:
  ProductQuantizerCodebook(uint32_t d, std::span<const byte_type> stats,
                           std::span<const float> query, VectorMetric metric)
    : _metric{metric}, _query(query.begin(), query.end()) {
    SDB_ASSERT(metric == VectorMetric::L2Sqr ||
               metric == VectorMetric::InnerProduct);
    const Header hdr = ReadHeader(stats);
    if (hdr.m != 0 && d % hdr.m == 0 && hdr.ksub != 0) {
      _pq.d = d;
      _pq.M = hdr.m;
      _pq.nbits = 8;
      _pq.set_derived_values();
      const size_t want = _pq.centroids.size() * sizeof(float);
      if (hdr.ksub == static_cast<uint32_t>(_pq.ksub) &&
          stats.size() >= 2 * sizeof(uint32_t) + want) {
        std::memcpy(_pq.centroids.data(), stats.data() + 2 * sizeof(uint32_t),
                    want);
        _valid = true;
        if (_metric == VectorMetric::InnerProduct) {
          _ip_table.resize(static_cast<size_t>(_pq.M) * _pq.ksub);
          _pq.compute_inner_prod_table(_query.data(), _ip_table.data());
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
  const float* IpTable() const noexcept { return _ip_table.data(); }

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

  faiss::ProductQuantizer _pq;
  VectorMetric _metric;
  std::vector<float> _query;
  std::vector<float> _ip_table;
  bool _valid = false;
};

class ProductQuantizerReader final : public QuantizerReader {
 public:
  ProductQuantizerReader(std::shared_ptr<const ProductQuantizerCodebook> cb,
                         std::unique_ptr<IndexInput> pay_in)
    : _cb{std::move(cb)},
      _pay_in{std::move(pay_in)},
      _codes_reader{*_pay_in, static_cast<uint32_t>(_cb->Pq().code_size)} {}

  void StartCluster(uint64_t pay_start, size_t num_docs,
                    const float* centroid) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    SDB_ASSERT(centroid != nullptr);
    const faiss::ProductQuantizer& pq = _cb->Pq();
    const std::span<const float> query = _cb->Query();
    if (_cb->Metric() == VectorMetric::L2Sqr) {
      // ||q - (c + r)||^2 = ||(q - c) - r||^2: ADC table for the residual
      // query.
      _table.resize(static_cast<size_t>(pq.M) * pq.ksub);
      _qr.resize(query.size());
      for (size_t j = 0; j < query.size(); ++j) {
        _qr[j] = query[j] - centroid[j];
      }
      pq.compute_distance_table(_qr.data(), _table.data());
      _table_ptr = _table.data();
      _ip_offset = 0.f;
    } else {
      // IP(q, c + r) = IP(q, c) + IP(q, r).
      _table_ptr = _cb->IpTable();
      float off = 0.f;
      for (size_t j = 0; j < query.size(); ++j) {
        off += query[j] * centroid[j];
      }
      _ip_offset = off;
    }
    _codes_reader.Reset(pay_start);
  }

  void ComputeBlock(size_t offset, size_t count, score_t /*boost*/,
                    score_t* out) final {
    SDB_ASSERT(offset + count <= _n);
    const byte_type* block = _codes_reader.Read(offset, count);
    const size_t m = _cb->Pq().M;
    const size_t ksub = _cb->Pq().ksub;
    const float* table = _table_ptr;
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
  std::shared_ptr<const ProductQuantizerCodebook> _cb;
  std::unique_ptr<IndexInput> _pay_in;
  VectorBlockReader _codes_reader;
  std::vector<float> _table;
  std::vector<float> _qr;
  const float* _table_ptr = nullptr;
  float _ip_offset = 0.f;
  size_t _n = 0;
};

std::unique_ptr<QuantizerReader> ProductQuantizerCodebook::MakeReader(
  std::unique_ptr<IndexInput> pay_in) const {
  return std::make_unique<ProductQuantizerReader>(
    std::static_pointer_cast<const ProductQuantizerCodebook>(
      shared_from_this()),
    std::move(pay_in));
}

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
  }
  return nullptr;
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  const std::shared_ptr<const QuantizerCodebook>& codebook,
  std::unique_ptr<IndexInput> pay_in) {
  return codebook ? codebook->MakeReader(std::move(pay_in)) : nullptr;
}

}  // namespace irs
