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

  BuiltIvf Build(const ColumnReader& vector_column, ReadContext& ctx,
                 QuantizerWriter* qw) const;

 private:
  IvfInfo _info;
};

class IvfTermReader final : public BasicTermReader {
 public:
  IvfTermReader(field_id postings_id,
                const std::vector<std::vector<doc_id_t>>& clusters);
  ~IvfTermReader() final;

  TermIterator::ptr iterator() const final;
  field_id id() const final { return _meta.id; }
  FieldProperties properties() const final { return _meta; }
  bytes_view(min)() const final { return _min; }
  bytes_view(max)() const final { return _max; }
  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

 private:
  const std::vector<std::vector<doc_id_t>>* _clusters;
  FieldMeta _meta;
  std::array<byte_type, 4> _min_buf{};
  std::array<byte_type, 4> _max_buf{};
  bytes_view _min;
  bytes_view _max;
  mutable std::unique_ptr<IvfTermIterator> _it;
};

struct BuiltCentroids {
  field_id centroids_id;
  VectorMetric metric;
  uint32_t nlist;
  uint32_t d;
  std::vector<float> centroids;  // nlist x d row-major
};

class IvfWriter {
 public:
  explicit IvfWriter(const ColumnOptionsProvider* column_options) noexcept
    : _column_options{column_options} {}

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
  };

  const ColumnOptionsProvider* _column_options;
  std::vector<Result> _results;
  std::vector<BuiltCentroids> _centroids;
  std::vector<std::unique_ptr<IvfTermReader>> _readers;
  std::vector<const BasicTermReader*> _reader_ptrs;
};

}  // namespace irs
