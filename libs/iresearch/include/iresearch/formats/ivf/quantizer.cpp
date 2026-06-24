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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <duckdb/common/types.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <limits>
#include <vector>

#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

class ScalarQuantizerWriter final : public QuantizerWriter {
 public:
  explicit ScalarQuantizerWriter(uint32_t d) : _d{d} {}

  void Train(const float* matrix, uint64_t rows, uint32_t d,
             std::span<const float> /*centroids*/,
             std::span<const uint32_t> /*assign*/) final {
    SDB_ASSERT(d == _d);
    _offset.assign(_d, std::numeric_limits<float>::max());
    std::vector<float> dmax(_d, std::numeric_limits<float>::lowest());
    for (uint64_t i = 0; i < rows; ++i) {
      const float* v = matrix + i * _d;
      for (uint32_t j = 0; j < _d; ++j) {
        _offset[j] = std::min(_offset[j], v[j]);
        dmax[j] = std::max(dmax[j], v[j]);
      }
    }
    _scale.assign(_d, 1.f);
    for (uint32_t j = 0; j < _d; ++j) {
      const float range = dmax[j] - _offset[j];
      _scale[j] = range > 0.f ? range / 255.f : 1.f;
    }
  }

  void Encode(doc_id_t doc, const float* vec) final {
    _docs.push_back(doc);
    const size_t base = _codes.size();
    _codes.resize(base + _d);
    for (uint32_t j = 0; j < _d; ++j) {
      const float q = (vec[j] - _offset[j]) / _scale[j];
      const int32_t u = std::clamp<int32_t>(std::lround(q), 0, 255);
      _codes[base + j] = static_cast<int8_t>(u - 128);
    }
  }

  void Serialize(ColWriter& cw, field_id sq_id, uint64_t total_rows) final {
    if (total_rows == 0 || _d == 0) {
      return;
    }
    const auto type = duckdb::LogicalType::ARRAY(duckdb::LogicalType::TINYINT,
                                                 static_cast<int64_t>(_d));
    auto& writer =
      cw.OpenColumn(sq_id, type, /*skip_validity=*/true, DEFAULT_ROW_GROUP_SIZE,
                    duckdb::CompressionType::COMPRESSION_AUTO,
                    /*hyperloglog=*/false);

    std::vector<int8_t> aligned(static_cast<size_t>(total_rows) * _d, 0);
    for (size_t i = 0; i < _docs.size(); ++i) {
      const uint64_t row = static_cast<uint64_t>(_docs[i]) - doc_limits::min();
      std::memcpy(aligned.data() + row * _d, _codes.data() + i * _d, _d);
    }

    for (uint64_t off = 0; off < total_rows;) {
      const uint64_t batch =
        std::min<uint64_t>(STANDARD_VECTOR_SIZE, total_rows - off);
      duckdb::Vector vec{type, static_cast<duckdb::idx_t>(batch)};
      auto& child = duckdb::ArrayVector::GetEntry(vec);
      std::memcpy(duckdb::FlatVector::GetDataMutable<int8_t>(child),
                  aligned.data() + off * _d, static_cast<size_t>(batch) * _d);
      writer.Append(off, vec, static_cast<duckdb::idx_t>(batch));
      off += batch;
    }
    writer.Finalize();
  }

  VectorQuantization Kind() const noexcept final {
    return VectorQuantization::SQ8;
  }

 private:
  uint32_t _d;
  std::vector<float> _scale;
  std::vector<float> _offset;
  std::vector<int8_t> _codes;
  std::vector<doc_id_t> _docs;
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
  VectorQuantization /*quant*/, const ColumnReader& /*store*/,
  ReadContext& /*ctx*/) {
  return nullptr;
}

}  // namespace irs
