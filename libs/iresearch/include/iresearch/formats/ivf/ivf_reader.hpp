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
#include <duckdb/common/types/vector_cache.hpp>
#include <memory>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/search/ann_index.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/bytes_utils.hpp"

namespace irs {

class ReadContext;
class ColReader;

inline constexpr size_t kCentroidTermWidth = 4;

inline void EncodeCentroidTerm(uint32_t id, byte_type* out) noexcept {
  auto* p = out;
  irs::WriteBE<uint32_t>(id, p);
}

using VectorDistanceFn = float (*)(const byte_type*, const byte_type*,
                                   uint16_t);

VectorDistanceFn ResolveScoringDistance(VectorMetric metric);

bool VectorMetricIsAngular(VectorMetric metric) noexcept;

class IvfVectorReader {
 public:
  IvfVectorReader(const ColumnReader& vector_column, ReadContext& ctx);

  uint32_t Dimension() const noexcept { return _d; }

  const float* ReadDocBatch(doc_id_t first, size_t count);

 private:
  void Seek(uint64_t row);

  uint32_t _d;
  const ColumnReader* _reader;
  ReadContext* _ctx;
  ColumnReader::ScanState _scan;
  duckdb::VectorCache _cache;
  size_t _cap = 0;
};

class IvfIndex final : public AnnIndex {
 public:
  explicit IvfIndex(CentroidsTree&& tree) noexcept : _tree{std::move(tree)} {}

  CentroidsTree& Tree() noexcept { return _tree; }
  const CentroidsTree& Tree() const noexcept { return _tree; }

  AnnKind Kind() const noexcept final { return AnnKind::Ivf; }

  uint32_t Dim() const noexcept final {
    return static_cast<uint32_t>(_tree.Dim());
  }

  bool Empty() const noexcept final { return _tree.Empty(); }

  bool HasQuantStats() const noexcept final { return _tree.HasQuantStats(); }

  bool SupportsFilter() const noexcept final { return true; }

  bool SupportsRange() const noexcept final { return true; }

  QueryBuilder::ptr PrepareKnn(const SubReader& segment,
                               const PrepareContext& ctx,
                               const VectorFilterOptions& opts,
                               uint32_t effort) const final;

  QueryBuilder::ptr PrepareRange(const SubReader& segment,
                                 const PrepareContext& ctx,
                                 const VectorFilterOptions& opts, float radius,
                                 bool inclusive, uint32_t effort) const final;

 private:
  CentroidsTree _tree;
};

}  // namespace irs
