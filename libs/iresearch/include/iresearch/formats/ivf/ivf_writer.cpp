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

#include "iresearch/formats/ivf/ivf_writer.hpp"

#include <superkmeans/superkmeans.h>

#include <algorithm>
#include <cmath>
#include <cstring>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <limits>
#include <span>

#include "basics/assert.h"
#include "basics/exceptions.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

// Reads the full rows x d float matrix backing an ARRAY(FLOAT) column by
// scanning its flat element child in STANDARD_VECTOR_SIZE batches.
std::vector<float> ReadMatrix(const ColumnReader& child, uint64_t total,
                              ReadContext& ctx) {
  std::vector<float> data(total);
  duckdb::Vector batch{duckdb::LogicalType::FLOAT, STANDARD_VECTOR_SIZE};
  ColumnReader::RangeScan scan{child, ctx};
  uint64_t produced = 0;
  while (produced < total) {
    const auto take =
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, total - produced);
    scan.Scan(produced, take, batch, /*out_offset=*/0);
    const float* p = duckdb::FlatVector::GetData<float>(batch);
    std::memcpy(data.data() + produced, p, take * sizeof(float));
    produced += take;
  }
  return data;
}

// Per-row validity of an ARRAY column (true == present). Empty fast path when
// the column has no nulls.
std::vector<bool> ReadValidity(const ColumnReader& vector_column, uint64_t rows,
                               ReadContext& ctx) {
  std::vector<bool> valid(rows, true);
  if (!vector_column.HasValidity()) {
    return valid;
  }
  duckdb::Vector vbatch{vector_column.Type(), /*capacity=*/0};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  ColumnReader::RangeScan vscan{vector_column, ctx, /*validity_side=*/true};
  for (uint64_t start = 0; start < rows; start += STANDARD_VECTOR_SIZE) {
    const auto take =
      std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows - start);
    vscan.Scan(start, take, vbatch, /*out_offset=*/0);
    const auto& mask = vbatch.Buffer().GetValidityMask();
    for (uint64_t k = 0; k < take; ++k) {
      valid[start + k] = mask.RowIsValid(k);
    }
  }
  return valid;
}

float SquaredL2(const float* a, const float* b, uint32_t d) noexcept {
  float acc = 0.f;
  for (uint32_t j = 0; j < d; ++j) {
    const float diff = a[j] - b[j];
    acc += diff * diff;
  }
  return acc;
}

uint32_t ResolveNlist(const IvfInfo& info, uint64_t valid_count) {
  uint32_t nlist =
    info.nlist != 0
      ? info.nlist
      : static_cast<uint32_t>(std::max<int64_t>(
          1, std::llround(std::sqrt(static_cast<double>(valid_count)))));
  return static_cast<uint32_t>(
    std::min<uint64_t>(nlist, std::max<uint64_t>(valid_count, 1)));
}

}  // namespace

BuiltIvf IvfBuilder::Build(const ColumnReader& vector_column, ReadContext& ctx,
                           QuantizerWriter* qw) const {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto d = static_cast<uint32_t>(vector_column.ArraySize());
  const auto rows = vector_column.RowCount();

  BuiltIvf out;
  out.d = d;
  if (rows == 0 || d == 0) {
    return out;
  }

  const auto data = ReadMatrix(*child, rows * d, ctx);
  const auto valid = ReadValidity(vector_column, rows, ctx);

  // Compact the valid rows into a contiguous training matrix and remember each
  // compacted row's doc-id.
  std::vector<float> compact;
  std::vector<doc_id_t> compact_doc;
  compact.reserve(rows * d);
  compact_doc.reserve(rows);
  for (uint64_t r = 0; r < rows; ++r) {
    if (!valid[r]) {
      continue;
    }
    const float* v = data.data() + r * d;
    compact.insert(compact.end(), v, v + d);
    compact_doc.push_back(static_cast<doc_id_t>(r + doc_limits::min()));
  }
  const uint64_t valid_count = compact_doc.size();
  if (valid_count == 0) {
    return out;
  }

  const uint32_t nlist = ResolveNlist(_info, valid_count);

  // Train the codebook. Tiny segments collapse to a single mean centroid so we
  // never feed SuperKMeans a degenerate problem.
  std::vector<float> centroids;
  std::vector<uint32_t> assign(valid_count, 0);
  const bool single_cluster = nlist <= 1 || valid_count < 4ull * nlist;
  if (single_cluster) {
    out.nlist = 1;
    centroids.assign(d, 0.f);
    for (uint64_t i = 0; i < valid_count; ++i) {
      const float* v = compact.data() + i * d;
      for (uint32_t j = 0; j < d; ++j) {
        centroids[j] += v[j];
      }
    }
    for (uint32_t j = 0; j < d; ++j) {
      centroids[j] /= static_cast<float>(valid_count);
    }
  } else {
    out.nlist = nlist;
    skmeans::SuperKMeansConfig cfg;
    // Single-threaded: avoids the omp_get_max_threads() path in the ctor and
    // matches the OpenMP-disabled build.
    cfg.n_threads = 1;
    if (_info.train_sample != 0) {
      cfg.max_points_per_cluster =
        std::max<uint32_t>(1, _info.train_sample / nlist);
    }
    skmeans::SuperKMeans<skmeans::Quantization::f32,
                         skmeans::DistanceFunction::l2>
      km{nlist, d, cfg};
    centroids = km.Train(compact.data(), valid_count);
    assign = km.Assign(compact.data(), centroids.data(), valid_count, nlist);
  }

  out.centroids = std::move(centroids);
  out.clusters.assign(out.nlist, {});
  out.cluster_radii.assign(out.nlist, 0.f);

  if (qw) {
    qw->Train(compact.data(), valid_count, d, out.centroids, assign);
  }

  for (uint64_t i = 0; i < valid_count; ++i) {
    const uint32_t c = assign[i];
    SDB_ASSERT(c < out.nlist);
    const float* v = compact.data() + i * d;
    out.clusters[c].push_back(compact_doc[i]);
    const float dist2 = SquaredL2(v, out.centroids.data() + c * d, d);
    out.cluster_radii[c] = std::max(out.cluster_radii[c], dist2);
    if (qw) {
      qw->Encode(compact_doc[i], v);
    }
  }
  for (auto& r : out.cluster_radii) {
    r = std::sqrt(r);
  }

  return out;
}

namespace {

void EncodeBE(uint32_t v, byte_type* out) noexcept {
  out[0] = static_cast<byte_type>(v >> 24);
  out[1] = static_cast<byte_type>(v >> 16);
  out[2] = static_cast<byte_type>(v >> 8);
  out[3] = static_cast<byte_type>(v);
}

}  // namespace

class IvfTermIterator final : public TermIterator {
 public:
  static constexpr size_t kWidth = 4;

  explicit IvfTermIterator(const std::vector<std::vector<doc_id_t>>& clusters)
    : _clusters{&clusters}, _terms(clusters.size() * kWidth) {
    for (size_t c = 0; c < clusters.size(); ++c) {
      EncodeBE(static_cast<uint32_t>(c), _terms.data() + c * kWidth);
    }
  }

  bytes_view value() const noexcept final {
    return {_terms.data() + _cur * kWidth, kWidth};
  }

  bool next() final {
    if (_next >= _clusters->size()) {
      return false;
    }
    _cur = _next++;
    return true;
  }

  void read() noexcept final {}

  DocIterator::ptr postings(IndexFeatures /*features*/) const final {
    _doc_itr.Reset((*_clusters)[_cur]);
    return memory::to_managed<DocIterator>(_doc_itr);
  }

  Attribute* GetMutable(TypeInfo::type_id) noexcept final { return nullptr; }

 private:
  class DocIter final : public DocIterator {
   public:
    void Reset(std::span<const doc_id_t> docs) noexcept {
      _docs = docs;
      _pos = 0;
      _doc = doc_limits::invalid();
      _notified = false;
    }

    doc_id_t advance() noexcept final {
      if (!_notified) {
        _notified = true;
        _change(*this);
      }
      if (_pos >= _docs.size()) {
        return _doc = doc_limits::eof();
      }
      return _doc = _docs[_pos++];
    }

    doc_id_t seek(doc_id_t target) noexcept final {
      if (doc_limits::eof(target)) {
        return _doc = doc_limits::eof();
      }
      while (_doc < target) {
        if (doc_limits::eof(advance())) {
          break;
        }
      }
      return _doc;
    }

    uint32_t GetFreq() const final { return 1; }

    Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
      return type == irs::Type<AttrProviderChangeAttr>::id() ? &_change
                                                             : nullptr;
    }

   private:
    std::span<const doc_id_t> _docs;
    size_t _pos = 0;
    bool _notified = false;
    AttrProviderChangeAttr _change;
  };

  const std::vector<std::vector<doc_id_t>>* _clusters;
  std::vector<byte_type> _terms;
  size_t _cur = 0;
  size_t _next = 0;
  mutable DocIter _doc_itr;
};

IvfTermReader::IvfTermReader(field_id postings_id,
                             const std::vector<std::vector<doc_id_t>>& clusters)
  : _clusters{&clusters}, _meta{postings_id, IndexFeatures::None} {
  bool found = false;
  for (size_t c = 0; c < clusters.size(); ++c) {
    if (clusters[c].empty()) {
      continue;
    }
    EncodeBE(static_cast<uint32_t>(c), _max_buf.data());
    _max = {_max_buf.data(), _max_buf.size()};
    if (!found) {
      EncodeBE(static_cast<uint32_t>(c), _min_buf.data());
      _min = {_min_buf.data(), _min_buf.size()};
      found = true;
    }
  }
}

IvfTermReader::~IvfTermReader() = default;

TermIterator::ptr IvfTermReader::iterator() const {
  _it = std::make_unique<IvfTermIterator>(*_clusters);
  return memory::to_managed<TermIterator>(*_it);
}

void IvfWriter::OnCommit(ColWriter& cw, std::span<const field_id> column_ids) {
  if (!_column_options || !*_column_options) {
    return;
  }
  for (const field_id id : column_ids) {
    const auto opts = (*_column_options)(id);
    if (!opts.ivf_info) {
      continue;
    }
    auto col = cw.ReopenColumn(id);
    if (!col) {
      continue;
    }
    const auto d = static_cast<uint32_t>(col->ArraySize());
    auto qw =
      MakeQuantizerWriter(opts.ivf_info->quant, d, opts.ivf_info->metric);

    IvfBuilder builder{*opts.ivf_info};
    BuiltIvf built = builder.Build(*col, cw.CommitReadContext(), qw.get());
    if (built.nlist == 0) {
      continue;
    }
    if (qw && field_limits::valid(opts.ivf_info->sq_id)) {
      qw->Serialize(cw, opts.ivf_info->sq_id, col->RowCount());
    }
    _centroids.push_back(
      BuiltCentroids{.centroids_id = opts.ivf_info->centroids_id,
                     .metric = opts.ivf_info->metric,
                     .nlist = built.nlist,
                     .d = built.d,
                     .centroids = std::move(built.centroids)});
    _results.push_back(
      Result{opts.ivf_info->postings_id, std::move(built.clusters)});
  }
}

std::span<const BasicTermReader* const> IvfWriter::ClusterReaders() {
  if (_reader_ptrs.empty() && !_results.empty()) {
    _readers.reserve(_results.size());
    _reader_ptrs.reserve(_results.size());
    for (auto& r : _results) {
      _readers.push_back(
        std::make_unique<IvfTermReader>(r.postings_id, r.clusters));
      _reader_ptrs.push_back(_readers.back().get());
    }
  }
  return _reader_ptrs;
}

}  // namespace irs
