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

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace duckdb {

class Vector;
}

namespace irs {

class ColReader;
class ColumnReader;
class ReadContext;
class IvfTermIterator;

struct BuiltIvf {
  bool empty = true;
  uint32_t d = 0;
  std::vector<doc_id_t> cluster_docs;
  std::vector<uint64_t> cluster_offsets;
  std::vector<float> fine_centroids;

  std::vector<float> l1_centroids;
  std::vector<uint32_t> cell_fine_base;
  std::vector<uint32_t> cell_n_l2;
  std::vector<float> radii;
  uint32_t n_l1 = 0;
  CentroidShapeKind shape_kind = CentroidShapeKind::TwoLayer;
  bstring stats;
};

class QuantizerWriter;

struct CentroidShape {
  CentroidShapeKind kind;
  uint32_t total_target;
  uint32_t n_l1;
  uint32_t n_l2_target;
};

class IvfBuilder {
 public:
  explicit IvfBuilder(IvfInfo info) : _info{std::move(info)} {}

  BuiltIvf Compute(const ColumnReader& vector_column, ReadContext& ctx,
                   QuantizerWriter* qw) const;

  static CentroidShape ResolveCentroidShape(uint64_t valid_count,
                                            const IvfInfo& info);

 private:
  IvfInfo _info;
};

void WriteIvfCentroidBody(IndexOutput& out, VectorMetric metric,
                          const BuiltIvf& built);

class IvfTermReader final : public BasicTermReader, public TermPayloadWriter {
 public:
  IvfTermReader(field_id postings_id, std::span<const doc_id_t> cluster_docs,
                std::span<const uint64_t> cluster_offsets, QuantizerWriter* qw,
                const ColumnReader* vectors, ReadContext* ctx, uint32_t d,
                std::span<const float> fine_centroids);
  ~IvfTermReader() final;

  TermIterator::ptr iterator() const final;
  field_id id() const final { return _meta.id; }
  FieldProperties properties() const final { return _meta; }
  bytes_view(min)() const final { return _min; }
  bytes_view(max)() const final { return _max; }
  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  TermPayloadWriter* PayloadWriter() const final {
    return _qw != nullptr ? const_cast<IvfTermReader*>(this) : nullptr;
  }

  void WriteTermPayload(IndexOutput& out, std::span<const doc_id_t> docs) final;
  void Finish(IndexOutput& out) final;

 private:
  std::span<const doc_id_t> _cluster_docs;
  std::span<const uint64_t> _cluster_offsets;
  QuantizerWriter* _qw;
  const ColumnReader* _vectors;
  ReadContext* _ctx;
  uint32_t _d;
  std::span<const float> _fine_centroids;
  size_t _count;
  size_t _term_idx = 0;
  FieldMeta _meta;
  std::array<byte_type, 4> _min_buf;
  std::array<byte_type, 4> _max_buf;
  bytes_view _min;
  bytes_view _max;
  mutable std::unique_ptr<IvfTermIterator> _it;
};

class IvfWriter {
 public:
  explicit IvfWriter(IvfInfo info);

  ~IvfWriter();

  void Compute(const ColumnReader& col, ReadContext& ctx);

  field_id ColumnId() const noexcept { return _info.centroids_id; }
  VectorMetric Metric() const noexcept { return _info.metric; }
  std::shared_ptr<const BuiltIvf> Built() const noexcept {
    return _built ? _result.built : nullptr;
  }

  bool Empty() const noexcept { return !_built; }

  const BasicTermReader* ClusterReader(ReadContext& ctx,
                                       const ColReader& col_reader);

 private:
  struct Result {
    field_id postings_id;
    std::unique_ptr<QuantizerWriter> qw;
    std::shared_ptr<const BuiltIvf> built;
  };

  IvfInfo _info;
  Result _result;
  bool _built = false;
  std::unique_ptr<IvfTermReader> _reader;
};

inline std::vector<const BasicTermReader*> PrepareIvfClusterReaders(
  std::span<const std::unique_ptr<IvfWriter>> writers, ColReader* col_reader,
  std::optional<ReadContext>& ctx) {
  std::vector<const BasicTermReader*> out;
  if (col_reader == nullptr || writers.empty()) {
    return out;
  }
  ctx.emplace(*col_reader);
  out.reserve(writers.size());
  for (const auto& w : writers) {
    if (!w) {
      continue;
    }
    if (const auto* r = w->ClusterReader(ctx.value(), *col_reader)) {
      out.push_back(r);
    }
  }
  return out;
}

}  // namespace irs
