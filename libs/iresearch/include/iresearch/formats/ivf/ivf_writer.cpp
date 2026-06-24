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
#include <random>
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

// Streams the flat vector column in row-aligned chunks (no full matrix in RAM),
// invoking `sink(first_row, n_rows, data)` where `data` points at `n_rows * d`
// contiguous floats. The chunk holds whole rows so callers can map each row to
// its validity / doc id.
template<typename Sink>
void StreamRowBatches(const ColumnReader& child, uint64_t rows, uint32_t d,
                      ReadContext& ctx, Sink&& sink) {
  const auto rows_per_batch =
    static_cast<duckdb::idx_t>(std::max<uint64_t>(1, STANDARD_VECTOR_SIZE / d));
  duckdb::Vector batch{duckdb::LogicalType::FLOAT,
                       static_cast<duckdb::idx_t>(rows_per_batch) * d};
  ColumnReader::RangeScan scan{child, ctx};
  for (uint64_t first = 0; first < rows; first += rows_per_batch) {
    const auto n = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(rows_per_batch, rows - first));
    scan.Scan(first * d, n * d, batch, /*out_offset=*/0);
    sink(first, n, duckdb::FlatVector::GetData<float>(batch));
  }
}

uint64_t ResolveTrainSample(const IvfInfo& info, uint64_t valid_count,
                            uint32_t nlist) {
  uint64_t n_train;
  if (info.train_sample != 0) {
    n_train = std::min<uint64_t>(valid_count, info.train_sample);
  } else {
    // Mirror SuperKMeans' default GetNVectorsToSample policy so behaviour and
    // the training set size match what it would otherwise pick internally.
    const auto by_fraction =
      static_cast<uint64_t>(0.3 * static_cast<double>(valid_count));
    const uint64_t by_clusters = static_cast<uint64_t>(nlist) * 256;
    n_train =
      std::min<uint64_t>(valid_count, std::min(by_fraction, by_clusters));
  }
  // Train requires at least `nlist` points; nlist is already <= valid_count.
  return std::clamp<uint64_t>(n_train, nlist, valid_count);
}

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

BuiltIvf IvfBuilder::Build(const ColumnReader& vector_column,
                           ReadContext& ctx) const {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto d = static_cast<uint32_t>(vector_column.ArraySize());
  const auto rows = vector_column.RowCount();

  BuiltIvf out;
  out.d = d;
  if (rows == 0 || d == 0) {
    return out;
  }

  // Pass 0: stream the validity mask (no vector data) to size the index.
  const auto valid = ReadValidity(vector_column, rows, ctx);
  uint64_t valid_count = 0;
  for (uint64_t r = 0; r < rows; ++r) {
    valid_count += valid[r] ? 1 : 0;
  }
  if (valid_count == 0) {
    return out;
  }

  const uint32_t nlist = ResolveNlist(_info, valid_count);
  out.nlist = nlist;
  const uint64_t n_train = ResolveTrainSample(_info, valid_count, nlist);

  // Pass 1: reservoir-sample `n_train` valid vectors (algorithm R) into a
  // contiguous buffer, streaming the column so the full matrix is never
  // resident.
  std::vector<float> sample(static_cast<size_t>(n_train) * d);
  {
    std::mt19937_64 rng{skmeans::SuperKMeansConfig{}.seed};
    uint64_t seen = 0;
    StreamRowBatches(
      *child, rows, d, ctx,
      [&](uint64_t first, duckdb::idx_t n, const float* p) {
        for (duckdb::idx_t k = 0; k < n; ++k) {
          if (!valid[first + k]) {
            continue;
          }
          const float* v = p + static_cast<size_t>(k) * d;
          if (seen < n_train) {
            std::memcpy(sample.data() + seen * d, v, d * sizeof(float));
          } else {
            const uint64_t j =
              std::uniform_int_distribution<uint64_t>{0, seen}(rng);
            if (j < n_train) {
              std::memcpy(sample.data() + j * d, v, d * sizeof(float));
            }
          }
          ++seen;
        }
      });
    SDB_ASSERT(seen == valid_count);
  }

  skmeans::SuperKMeansConfig cfg;
  cfg.n_threads = 1;
  cfg.sampling_fraction = 1.0f;  // already sampled; cluster on all we pass in
  skmeans::SuperKMeans<skmeans::Quantization::f32,
                       skmeans::DistanceFunction::l2>
    km{nlist, d, cfg};
  out.centroids = km.Train(sample.data(), n_train);
  std::vector<float>{}.swap(sample);

  out.clusters.assign(nlist, {});
  out.cluster_radii.assign(nlist, 0.f);

  // Pass 2: stream the column again and assign each valid vector to its nearest
  // centroid. `Assign` is brute-force and stateless, so assigning a gathered
  // chunk yields the same result as a single full call; we accumulate across
  // scan batches into `kAssignChunk`-row chunks to amortize the GEMM setup.
  constexpr size_t kAssignChunk = 4096;
  std::vector<float> gather;
  std::vector<doc_id_t> gather_doc;
  gather.reserve(kAssignChunk * d);
  gather_doc.reserve(kAssignChunk);
  const auto flush = [&]() {
    if (gather_doc.empty()) {
      return;
    }
    const auto assign =
      km.Assign(gather.data(), out.centroids.data(), gather_doc.size(), nlist);
    for (size_t i = 0; i < gather_doc.size(); ++i) {
      const uint32_t c = assign[i];
      SDB_ASSERT(c < nlist);
      out.clusters[c].push_back(gather_doc[i]);
      const float dist2 =
        SquaredL2(gather.data() + i * d,
                  out.centroids.data() + static_cast<size_t>(c) * d, d);
      out.cluster_radii[c] = std::max(out.cluster_radii[c], dist2);
    }
    gather.clear();
    gather_doc.clear();
  };
  StreamRowBatches(*child, rows, d, ctx,
                   [&](uint64_t first, duckdb::idx_t n, const float* p) {
                     for (duckdb::idx_t k = 0; k < n; ++k) {
                       if (!valid[first + k]) {
                         continue;
                       }
                       const float* v = p + static_cast<size_t>(k) * d;
                       gather.insert(gather.end(), v, v + d);
                       gather_doc.push_back(
                         static_cast<doc_id_t>(first + k + doc_limits::min()));
                       if (gather_doc.size() >= kAssignChunk) {
                         flush();
                       }
                     }
                   });
  flush();

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
                             const std::vector<std::vector<doc_id_t>>& clusters,
                             QuantizerWriter* qw, const ColumnReader* vectors,
                             ReadContext* ctx, uint32_t d)
  : _clusters{&clusters},
    _qw{qw},
    _vectors{vectors},
    _ctx{ctx},
    _d{d},
    _meta{postings_id,
          qw != nullptr ? IndexFeatures::Pay : IndexFeatures::None} {
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

void IvfTermReader::WriteTermPayload(IndexOutput& out,
                                     std::span<const doc_id_t> docs) {
  SDB_ASSERT(_qw && _vectors && _vectors->Child() && _ctx);
  const size_t n = docs.size();
  if (n != 0) {
    constexpr size_t kBatch = STANDARD_VECTOR_SIZE;
    _vec_buf.resize(std::min(n, kBatch) * _d);
    auto scan_batches = [&](auto&& sink) {
      ColumnReader::RangeScan scan{*_vectors->Child(), *_ctx};
      duckdb::Vector row{duckdb::LogicalType::FLOAT,
                         static_cast<duckdb::idx_t>(_d)};
      size_t filled = 0;
      for (size_t k = 0; k < n; ++k) {
        const uint64_t r = static_cast<uint64_t>(docs[k]) - doc_limits::min();
        scan.Scan(r * _d, _d, row, /*out_offset=*/0);
        std::memcpy(_vec_buf.data() + filled * _d,
                    duckdb::FlatVector::GetData<float>(row),
                    static_cast<size_t>(_d) * sizeof(float));
        if (++filled == kBatch) {
          sink(_vec_buf.data(), filled);
          filled = 0;
        }
      }
      if (filled != 0) {
        sink(_vec_buf.data(), filled);
      }
    };
    scan_batches([&](const float* p, size_t c) { _qw->UpdateStats(p, c); });
    scan_batches(
      [&](const float* p, size_t c) { _qw->EncodeCluster(out, p, c); });
  }
  _qw->Finalize(out);
}

void IvfTermReader::Finish(IndexOutput& /*out*/) { SDB_ASSERT(_qw); }

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
    BuiltIvf built = builder.Build(*col, cw.CommitReadContext());
    if (built.nlist == 0) {
      continue;
    }
    // Retained for flush-time payload streaming (see IvfTermReader): the
    // quantized codes are (re-)read from the vector column and written into
    // ".pay" per cluster.
    _read_ctx = &cw.CommitReadContext();
    _centroids.push_back(BuiltCentroids{
      .centroids_id = opts.ivf_info->centroids_id,
      .index = FlatCentroids{opts.ivf_info->metric, built.nlist, built.d,
                             std::move(built.centroids)}});
    _results.push_back(Result{.postings_id = opts.ivf_info->postings_id,
                              .clusters = std::move(built.clusters),
                              .qw = std::move(qw),
                              .vector_column = std::move(col),
                              .d = d});
  }
}

std::span<const BasicTermReader* const> IvfWriter::ClusterReaders() {
  if (_reader_ptrs.empty() && !_results.empty()) {
    _readers.reserve(_results.size());
    _reader_ptrs.reserve(_results.size());
    for (auto& r : _results) {
      _readers.push_back(
        std::make_unique<IvfTermReader>(r.postings_id, r.clusters, r.qw.get(),
                                        r.vector_column.get(), _read_ctx, r.d));
      _reader_ptrs.push_back(_readers.back().get());
    }
  }
  return _reader_ptrs;
}

IvfWriter::IvfWriter(const ColumnOptionsProvider* column_options) noexcept
  : _column_options{column_options} {}

IvfWriter::~IvfWriter() = default;

}  // namespace irs
