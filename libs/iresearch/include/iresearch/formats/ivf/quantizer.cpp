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

#include <faiss/impl/ScalarQuantizer.h>

#include <algorithm>
#include <cstdint>
#include <limits>
#include <vector>

#include "basics/assert.h"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

class ScalarQuantizerWriter final : public QuantizerWriter {
 public:
  explicit ScalarQuantizerWriter(uint32_t d)
    : _d{d}, _sq{d, faiss::ScalarQuantizer::QuantizerType::QT_8bit} {
    ResetAccum();
  }

  void UpdateStats(const float* vecs, size_t n) final {
    for (size_t i = 0; i < n; ++i) {
      const float* v = vecs + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _vmin[j] = std::min(_vmin[j], v[j]);
        _vmax[j] = std::max(_vmax[j], v[j]);
      }
    }
    _sq.trained.resize(2 * static_cast<size_t>(_d));
    for (uint32_t j = 0; j < _d; ++j) {
      _sq.trained[j] = _vmin[j];
      _sq.trained[_d + j] = _vmax[j] - _vmin[j];
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

  void Finalize(IndexOutput& out) final {
    out.WriteData(reinterpret_cast<const byte_type*>(_sq.trained.data()),
                  _sq.trained.size() * sizeof(float));
    ResetAccum();
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::SQ8;
  }

  uint32_t CodeSize() const noexcept final {
    return static_cast<uint32_t>(_sq.code_size);
  }

 private:
  void ResetAccum() {
    _vmin.assign(_d, std::numeric_limits<float>::max());
    _vmax.assign(_d, std::numeric_limits<float>::lowest());
    _sq.trained.assign(2 * static_cast<size_t>(_d), 0.f);
  }

  uint32_t _d;
  faiss::ScalarQuantizer _sq;
  std::vector<float> _vmin;
  std::vector<float> _vmax;
  mutable std::vector<uint8_t> _code;
};

class ScalarQuantizerReader final : public QuantizerReader {
 public:
  ScalarQuantizerReader(std::unique_ptr<IndexInput> pay_in, uint32_t d)
    : _pay_in{std::move(pay_in)},
      _d{d},
      _sq{d, faiss::ScalarQuantizer::QuantizerType::QT_8bit} {}

  void SetQuery(std::span<const float> query, VectorMetric metric) final {
    _query.assign(query.begin(), query.end());
    SDB_ASSERT(metric == VectorMetric::L2Sqr ||
               metric == VectorMetric::InnerProduct);
    _faiss_metric = metric == VectorMetric::L2Sqr
                      ? faiss::MetricType::METRIC_L2
                      : faiss::MetricType::METRIC_INNER_PRODUCT;
  }

  void StartCluster(uint64_t pay_start, size_t num_docs) final {
    _n = num_docs;
    if (_n == 0) {
      return;
    }
    _codes.resize(_n * _d);
    _stat.resize(2 * static_cast<size_t>(_d));
    _pay_in->ReadData(pay_start, _codes.data(), _codes.size());
    _pay_in->ReadData(pay_start + _codes.size(),
                      reinterpret_cast<byte_type*>(_stat.data()),
                      _stat.size() * sizeof(float));
    _sq.trained = _stat;
    _dc.reset(_sq.get_distance_computer(_faiss_metric));
    _dc->codes = _codes.data();
    _dc->code_size = _d;
    _dc->set_query(_query.data());
  }

  void ComputeBlock(size_t offset, size_t count, score_t boost,
                    score_t* out) final {
    SDB_ASSERT(_dc);
    SDB_ASSERT(offset + count <= _n);
    size_t i = 0;
    for (; i + 3 < count; i += 4) {
      _dc->distances_batch_4(offset + i, offset + i + 1, offset + i + 2,
                             offset + i + 3, out[i], out[i + 1], out[i + 2],
                             out[i + 3]);
    }

    for (; i < count; i++) {
      out[i] = _dc->distance_to_code(_codes.data() + (offset + i) * _d);
    }
  }

 private:
  std::unique_ptr<IndexInput> _pay_in;
  uint32_t _d;
  std::vector<float> _query;
  faiss::MetricType _faiss_metric = faiss::MetricType::METRIC_L2;
  faiss::ScalarQuantizer _sq;
  std::unique_ptr<faiss::ScalarQuantizer::SQDistanceComputer> _dc;
  std::vector<uint8_t> _codes;
  std::vector<float> _stat;
  size_t _n = 0;
};

}  // namespace

std::unique_ptr<QuantizerWriter> MakeQuantizerWriter(VectorQuantization quant,
                                                     uint32_t d,
                                                     VectorMetric /*metric*/) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
      return std::make_unique<ScalarQuantizerWriter>(d);
  }
  return nullptr;
}

std::unique_ptr<QuantizerReader> MakeQuantizerReader(
  VectorQuantization quant, std::unique_ptr<IndexInput> pay_in, uint32_t d) {
  switch (quant) {
    case VectorQuantization::None:
      return nullptr;
    case VectorQuantization::SQ8:
      return std::make_unique<ScalarQuantizerReader>(std::move(pay_in), d);
  }
  return nullptr;
}

}  // namespace irs
