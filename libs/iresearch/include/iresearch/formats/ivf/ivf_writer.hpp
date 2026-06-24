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
#include <span>
#include <utility>
#include <vector>

#include "iresearch/formats/formats.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/index/column_info.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/iterators.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

class ColumnReader;
class ColWriter;
class ReadContext;
class IvfTermIterator;

struct BuiltIvf {
  std::vector<float> centroids;
  uint32_t nlist = 0;
  uint32_t d = 0;

  std::vector<std::vector<doc_id_t>> clusters;

  std::vector<float> cluster_radii;
};

class QuantizerWriter;

class IvfBuilder {
 public:
  explicit IvfBuilder(IvfInfo info) : _info{std::move(info)} {}

  BuiltIvf Build(const ColumnReader& vector_column, ReadContext& ctx) const;

 private:
  IvfInfo _info;
};

class IvfTermReader final : public BasicTermReader, public TermPayloadWriter {
 public:
  IvfTermReader(field_id postings_id,
                const std::vector<std::vector<doc_id_t>>& clusters,
                QuantizerWriter* qw, const ColumnReader* vectors,
                ReadContext* ctx, uint32_t d);
  ~IvfTermReader() final;

  TermIterator::ptr iterator() const final;
  field_id id() const final { return _meta.id; }
  FieldProperties properties() const final { return _meta; }
  bytes_view(min)() const final { return _min; }
  bytes_view(max)() const final { return _max; }
  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

  // Quantized fields stream their codes into ".pay" via this payload writer.
  TermPayloadWriter* PayloadWriter() const final {
    return _qw != nullptr ? const_cast<IvfTermReader*>(this) : nullptr;
  }

  // TermPayloadWriter: re-reads one cluster's vectors and streams its codes.
  void WriteTermPayload(IndexOutput& out, std::span<const doc_id_t> docs) final;
  void Finish(IndexOutput& out) final;

 private:
  const std::vector<std::vector<doc_id_t>>* _clusters;
  QuantizerWriter* _qw;
  const ColumnReader* _vectors;
  ReadContext* _ctx;
  uint32_t _d;
  std::vector<float> _vec_buf;  // scratch for re-read cluster vectors
  FieldMeta _meta;
  std::array<byte_type, 4> _min_buf{};
  std::array<byte_type, 4> _max_buf{};
  bytes_view _min;
  bytes_view _max;
  mutable std::unique_ptr<IvfTermIterator> _it;
};

struct BuiltCentroids {
  field_id centroids_id;
  FlatCentroids index;
};

class IvfWriter {
 public:
  explicit IvfWriter(const ColumnOptionsProvider* column_options) noexcept;

  ~IvfWriter();

  void OnCommit(ColWriter& cw, std::span<const field_id> column_ids);

  bool Empty() const noexcept { return _results.empty(); }

  std::span<const BasicTermReader* const> ClusterReaders();

  std::vector<BuiltCentroids> TakeBuiltCentroids() noexcept {
    return std::move(_centroids);
  }

 private:
  struct Result {
    field_id postings_id;
    std::vector<std::vector<doc_id_t>> clusters;
    // Retained until flush so the cluster codes can be (re-)read and streamed
    // into ".pay"; null when the field is not quantized.
    std::unique_ptr<QuantizerWriter> qw;
    std::unique_ptr<ColumnReader> vector_column;
    uint32_t d = 0;
  };

  const ColumnOptionsProvider* _column_options;
  ReadContext* _read_ctx = nullptr;  // borrowed; valid through flush
  std::vector<Result> _results;
  std::vector<BuiltCentroids> _centroids;
  std::vector<std::unique_ptr<IvfTermReader>> _readers;
  std::vector<const BasicTermReader*> _reader_ptrs;
};

}  // namespace irs
