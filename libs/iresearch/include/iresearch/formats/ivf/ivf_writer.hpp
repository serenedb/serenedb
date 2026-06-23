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

// Result of training the IVF codebook and assigning every row of one vector
// column in one segment. `clusters` becomes the IVF posting field; `centroids`
// is routed into the segment `.idx`. Per-doc quantization is handled separately
// through the QuantizerWriter (see quantizer.hpp).
struct BuiltIvf {
  // nlist x d row-major centroid matrix.
  std::vector<float> centroids;
  uint32_t nlist = 0;
  uint32_t d = 0;

  // clusters[c] = sorted doc-ids assigned to centroid c (ascending by
  // construction, since rows are scanned in order). size == nlist.
  std::vector<std::vector<doc_id_t>> clusters;

  // Per-cluster max member distance (Euclidean) for ByRadius cell pruning.
  // size == nlist.
  std::vector<float> cluster_radii;
};

class QuantizerWriter;

class IvfBuilder {
 public:
  explicit IvfBuilder(IvfInfo info) : _info{std::move(info)} {}

  // Trains the codebook on `vector_column` (an ARRAY(FLOAT) column) and assigns
  // every non-null row to a cluster. Null rows are excluded from training and
  // from every cluster posting list. doc-ids follow row order + doc_limits::min.
  // When `qw` is non-null it is trained on the same matrix and fed every valid
  // doc's vector via Encode for later Serialize.
  BuiltIvf Build(const ColumnReader& vector_column, ReadContext& ctx,
                 QuantizerWriter* qw) const;

 private:
  IvfInfo _info;
};

// Adapts the per-cluster doc-id lists from BuiltIvf into a BasicTermReader so
// they can be written as an ordinary doc-only posting field through
// burst_trie::FieldWriter::write. Each centroid id is a fixed-width big-endian
// uint32 term whose posting list is the cluster's ascending doc-ids; FST term
// order then matches numeric centroid order. The referenced `clusters` must
// outlive the reader.
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

// Trained centroids of one field, handed to the IdxWriter for serialization
// into the segment `.idx` body.
struct BuiltCentroids {
  field_id centroids_id;
  VectorMetric metric;
  uint32_t nlist;
  uint32_t d;
  std::vector<float> centroids;  // nlist x d row-major
};

// Drives IVF index construction at segment flush. Registered as a ColWriter
// commit hook (OnCommit): for every committed column whose ColumnOptions carry
// an IvfInfo, it trains the codebook and records the per-cluster doc-id lists
// plus the trained centroids. The SegmentWriter then merges ClusterReaders()
// into the term dictionary as the IVF posting fields and routes
// TakeBuiltCentroids() into the `.idx` via IdxWriter::AddIvfCentroids. This
// keeps ColWriter index-agnostic -- all IVF logic lives here.
class IvfWriter {
 public:
  explicit IvfWriter(const ColumnOptionsProvider* column_options) noexcept
    : _column_options{column_options} {}

  void OnCommit(ColWriter& cw, std::span<const field_id> column_ids);

  bool Empty() const noexcept { return _results.empty(); }

  // Posting-field readers for the trained clusters; built lazily on first call
  // and valid until *this is destroyed.
  std::span<const BasicTermReader* const> ClusterReaders();

  // Trained centroids for every IVF field of the segment; moved out for the
  // IdxWriter handoff.
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
