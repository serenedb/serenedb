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

#pragma once

#include <cstdint>
#include <duckdb/common/types/vector.hpp>
#include <memory>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/ivf/vector_block_reader.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"

namespace irs {

class ReadContext;
class ColReader;

inline constexpr size_t kCentroidTermWidth = 4;

inline void EncodeCentroidTerm(uint32_t id, byte_type* out) noexcept {
  out[0] = static_cast<byte_type>(id >> 24);
  out[1] = static_cast<byte_type>(id >> 16);
  out[2] = static_cast<byte_type>(id >> 8);
  out[3] = static_cast<byte_type>(id);
}

using VectorDistanceFn = float (*)(const byte_type*, const byte_type*,
                                   uint16_t);

VectorDistanceFn ResolveVectorDistance(VectorMetric metric);

bool VectorMetricNearestIsLargest(VectorMetric metric) noexcept;

class IvfVectorReader {
 public:
  IvfVectorReader(const ColumnReader& vector_column, ReadContext& ctx);

  uint32_t Dimension() const noexcept { return _d; }

  const float* ReadDoc(doc_id_t doc);

 private:
  uint32_t _d;
  ColumnReader::RangeScan _scan;
  duckdb::Vector _buf;
  std::vector<float> _scratch;
};

std::unique_ptr<VectorBlockReader> MakeRawVectorReader(
  const ColumnReader& vector_column, const ColReader& col_reader, uint32_t d);

}  // namespace irs
