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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <duckdb/common/types/validity_mask.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>
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
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

constexpr uint32_t kClusterSeed = 42;

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
                           IdxWriter& idx, QuantizerWriter* qw) const {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto d = static_cast<uint32_t>(vector_column.ArraySize());
  const auto rows = vector_column.RowCount();

  BuiltIvf out;
  out.d = d;
  if (rows == 0 || d == 0) {
    return out;
  }

  const auto valid = ReadValidity(vector_column, rows, ctx);
  uint64_t valid_count = 0;
  for (uint64_t r = 0; r < rows; ++r) {
    valid_count += valid[r] ? 1 : 0;
  }
  if (valid_count == 0) {
    return out;
  }

  const uint32_t total_target = ResolveNlist(_info, valid_count);
  const uint32_t n_l1 = static_cast<uint32_t>(std::clamp<uint64_t>(
    static_cast<uint64_t>(
      std::llround(std::ceil(std::sqrt(static_cast<double>(total_target))))),
    1, valid_count));
  const uint32_t n_l2_target = static_cast<uint32_t>(std::max<uint64_t>(
    1, (static_cast<uint64_t>(total_target) + n_l1 - 1) / n_l1));
  uint64_t n_train = ResolveTrainSample(_info, valid_count, total_target);
  if (_info.quant.kind == VectorQuantization::PQ) {
    n_train = std::min<uint64_t>(valid_count, std::max<uint64_t>(n_train, 256));
  }

  std::vector<float> sample(static_cast<size_t>(n_train) * d);
  {
    std::mt19937_64 rng{kClusterSeed};
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

  const VectorMetric m = _info.metric;
  const bool pq = _info.quant.kind == VectorQuantization::PQ;
  if (m == VectorMetric::Cosine) {
    NormalizeRows(sample.data(), n_train, d);
  }

  const uint32_t base_seed = kClusterSeed;
  const std::vector<float> l1_centroids =
    TrainCentroids(m, sample.data(), n_train, n_l1, d, base_seed);

  std::vector<uint32_t> doc_cell;
  doc_cell.reserve(valid_count);
  {
    std::vector<float> gather;
    gather.reserve(STANDARD_VECTOR_SIZE * d);
    size_t gathered = 0;
    const bool sq_train = qw != nullptr && !pq;
    const auto flush = [&]() {
      if (gathered == 0) {
        return;
      }
      AssignNearest(m, gather.data(), gathered, l1_centroids.data(), n_l1, d,
                    doc_cell);
      // Scalar quantizers accumulate their global [vmin, vdiff] over every
      // vector here (streamed), not just the centroid-training sample.
      if (sq_train) {
        qw->Train(gather.data(), gathered);
      }
      gather.clear();
      gathered = 0;
    };
    StreamRowBatches(*child, rows, d, ctx,
                     [&](uint64_t first, duckdb::idx_t n, const float* p) {
                       for (duckdb::idx_t k = 0; k < n; ++k) {
                         if (!valid[first + k]) {
                           continue;
                         }
                         const float* v = p + static_cast<size_t>(k) * d;
                         gather.insert(gather.end(), v, v + d);
                         if (++gathered >= STANDARD_VECTOR_SIZE) {
                           flush();
                         }
                       }
                     });
    flush();
    SDB_ASSERT(doc_cell.size() == valid_count);
  }

  std::vector<size_t> l1_offsets(n_l1 + 1, 0);
  for (const uint32_t cell : doc_cell) {
    ++l1_offsets[cell + 1];
  }
  for (uint32_t c = 0; c < n_l1; ++c) {
    l1_offsets[c + 1] += l1_offsets[c];
  }
  std::vector<doc_id_t> l1_member_docs(valid_count);
  {
    std::vector<size_t> cursor(l1_offsets.begin(), l1_offsets.begin() + n_l1);
    size_t seen = 0;
    for (uint64_t r = 0; r < rows; ++r) {
      if (!valid[r]) {
        continue;
      }
      const uint32_t cell = doc_cell[seen++];
      l1_member_docs[cursor[cell]++] =
        static_cast<doc_id_t>(r + doc_limits::min());
    }
    SDB_ASSERT(seen == valid_count);
  }

  std::vector<std::vector<uint32_t>> sample_groups(n_l1);
  {
    std::vector<uint32_t> sample_assign;
    AssignNearest(m, sample.data(), n_train, l1_centroids.data(), n_l1, d,
                  sample_assign);
    for (uint64_t i = 0; i < n_train; ++i) {
      SDB_ASSERT(sample_assign[i] < n_l1);
      sample_groups[sample_assign[i]].push_back(static_cast<uint32_t>(i));
    }
  }

  IndexOutput& bout = idx.BlocksOut();
  std::vector<uint64_t> body_offsets;
  body_offsets.reserve(n_l1);
  out.cluster_offsets.push_back(0);
  uint32_t next_fine_id = 0;
  std::vector<float> l2_centroids;
  std::vector<float> cell_sample;
  std::vector<uint32_t> fine_ids;
  std::vector<float> residual_sample;

  for (uint32_t c = 0; c < n_l1; ++c) {
    body_offsets.push_back(bout.Position());
    const float* l1c = l1_centroids.data() + static_cast<size_t>(c) * d;
    const auto& sidx = sample_groups[c];
    const size_t ms = sidx.size();

    uint32_t n_l2;
    if (ms == 0) {
      n_l2 = 1;
      l2_centroids.assign(l1c, l1c + d);
    } else {
      n_l2 = static_cast<uint32_t>(std::clamp<size_t>(n_l2_target, 1, ms));
      if (n_l2 == 1) {
        l2_centroids.assign(d, 0.f);
        for (const uint32_t si : sidx) {
          const float* v = sample.data() + static_cast<size_t>(si) * d;
          for (uint32_t j = 0; j < d; ++j) {
            l2_centroids[j] += v[j];
          }
        }
        const float inv = 1.f / static_cast<float>(ms);
        for (uint32_t j = 0; j < d; ++j) {
          l2_centroids[j] *= inv;
        }
        if (m == VectorMetric::InnerProduct || m == VectorMetric::Cosine) {
          NormalizeRows(l2_centroids.data(), 1, d);
        }
      } else {
        cell_sample.resize(ms * d);
        for (size_t i = 0; i < ms; ++i) {
          std::memcpy(cell_sample.data() + i * d,
                      sample.data() + static_cast<size_t>(sidx[i]) * d,
                      static_cast<size_t>(d) * sizeof(float));
        }
        l2_centroids =
          TrainCentroids(m, cell_sample.data(), ms, n_l2, d, base_seed + c);
      }
    }

    bout.WriteU32(n_l2);
    bout.WriteData(reinterpret_cast<const byte_type*>(l2_centroids.data()),
                   static_cast<size_t>(n_l2) * d * sizeof(float));
    fine_ids.resize(n_l2);
    for (uint32_t s = 0; s < n_l2; ++s) {
      fine_ids[s] = next_fine_id + s;
    }
    bout.WriteData(reinterpret_cast<const byte_type*>(fine_ids.data()),
                   static_cast<size_t>(n_l2) * sizeof(uint32_t));

    out.fine_centroids.insert(
      out.fine_centroids.end(), l2_centroids.data(),
      l2_centroids.data() + static_cast<size_t>(n_l2) * d);
    if (pq) {
      for (const uint32_t si : sidx) {
        const float* v = sample.data() + static_cast<size_t>(si) * d;
        const uint32_t best =
          NearestCentroid(m, v, l2_centroids.data(), n_l2, d);
        const float* cen = l2_centroids.data() + static_cast<size_t>(best) * d;
        const size_t base = residual_sample.size();
        residual_sample.resize(base + d);
        for (uint32_t j = 0; j < d; ++j) {
          residual_sample[base + j] = v[j] - cen[j];
        }
      }
    }

    std::vector<std::vector<doc_id_t>> sub(n_l2);
    {
      ColumnReader::RangeScan member_scan{*child, ctx};
      duckdb::Vector row{duckdb::LogicalType::FLOAT,
                         static_cast<duckdb::idx_t>(d)};
      for (size_t mi = l1_offsets[c]; mi < l1_offsets[c + 1]; ++mi) {
        const doc_id_t docid = l1_member_docs[mi];
        const uint64_t r = static_cast<uint64_t>(docid) - doc_limits::min();
        member_scan.Scan(r * d, d, row, /*out_offset=*/0);
        const float* v = duckdb::FlatVector::GetData<float>(row);
        const uint32_t best =
          NearestCentroid(m, v, l2_centroids.data(), n_l2, d);
        sub[best].push_back(docid);
      }
    }
    for (uint32_t s = 0; s < n_l2; ++s) {
      out.cluster_docs.insert(out.cluster_docs.end(), sub[s].begin(),
                              sub[s].end());
      out.cluster_offsets.push_back(out.cluster_docs.size());
    }
    next_fine_id += n_l2;
  }

  std::span<const byte_type> stats;
  if (qw != nullptr) {
    if (pq) {
      qw->Train(residual_sample.data(), residual_sample.size() / d);
    }
    stats = qw->StatsBytes();
  }

  out.resident_offset = bout.Position();
  TwoLayerCentroids::WriteFooter(
    bout, _info.metric, d, n_l1, std::span<const float>{l1_centroids},
    std::span<const uint64_t>{body_offsets}, stats);
  out.resident_size = bout.Position() - out.resident_offset;
  SDB_ASSERT(out.resident_size ==
             TwoLayerCentroids::FooterSize(d, n_l1, stats.size()));
  out.empty = false;
  return out;
}

class IvfTermIterator final : public TermIterator {
 public:
  static constexpr size_t kWidth = kCentroidTermWidth;

  IvfTermIterator(std::span<const doc_id_t> cluster_docs,
                  std::span<const uint64_t> cluster_offsets)
    : _cluster_docs{cluster_docs},
      _cluster_offsets{cluster_offsets},
      _count{cluster_offsets.empty() ? 0 : cluster_offsets.size() - 1},
      _terms(_count * kWidth) {
    for (size_t c = 0; c < _count; ++c) {
      EncodeCentroidTerm(static_cast<uint32_t>(c), _terms.data() + c * kWidth);
    }
  }

  bytes_view value() const noexcept final {
    return {_terms.data() + _cur * kWidth, kWidth};
  }

  bool next() final {
    if (_next >= _count) {
      return false;
    }
    _cur = _next++;
    return true;
  }

  void read() noexcept final {}

  DocIterator::ptr postings(IndexFeatures /*features*/) const final {
    const doc_id_t* p = _cluster_docs.data() + _cluster_offsets[_cur];
    const size_t len = _cluster_offsets[_cur + 1] - _cluster_offsets[_cur];
    _doc_itr.Reset({p, len});
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

  std::span<const doc_id_t> _cluster_docs;
  std::span<const uint64_t> _cluster_offsets;
  size_t _count;
  std::vector<byte_type> _terms;
  size_t _cur = 0;
  size_t _next = 0;
  mutable DocIter _doc_itr;
};

IvfTermReader::IvfTermReader(field_id postings_id,
                             std::span<const doc_id_t> cluster_docs,
                             std::span<const uint64_t> cluster_offsets,
                             QuantizerWriter* qw, const ColumnReader* vectors,
                             ReadContext* ctx, uint32_t d,
                             std::span<const float> fine_centroids)
  : _cluster_docs{cluster_docs},
    _cluster_offsets{cluster_offsets},
    _qw{qw},
    _vectors{vectors},
    _ctx{ctx},
    _d{d},
    _fine_centroids{fine_centroids},
    _count{cluster_offsets.empty() ? 0 : cluster_offsets.size() - 1},
    _meta{postings_id,
          qw != nullptr ? IndexFeatures::Pay : IndexFeatures::None} {
  bool found = false;
  for (size_t c = 0; c < _count; ++c) {
    if (cluster_offsets[c + 1] == cluster_offsets[c]) {
      continue;
    }
    EncodeCentroidTerm(static_cast<uint32_t>(c), _max_buf.data());
    _max = {_max_buf.data(), _max_buf.size()};
    if (!found) {
      EncodeCentroidTerm(static_cast<uint32_t>(c), _min_buf.data());
      _min = {_min_buf.data(), _min_buf.size()};
      found = true;
    }
  }
}

IvfTermReader::~IvfTermReader() = default;

TermIterator::ptr IvfTermReader::iterator() const {
  _it = std::make_unique<IvfTermIterator>(_cluster_docs, _cluster_offsets);
  return memory::to_managed<TermIterator>(*_it);
}

void IvfTermReader::WriteTermPayload(IndexOutput& out,
                                     std::span<const doc_id_t> docs) {
  SDB_ASSERT(_qw && _vectors && _vectors->Child() && _ctx);
  const size_t cluster = _term_idx++;
  if (cluster < _count && (cluster + 1) * _d <= _fine_centroids.size()) {
    _qw->SetClusterCentroid(_fine_centroids.data() + cluster * _d);
  }
  const size_t n = docs.size();
  if (n == 0) {
    return;
  }
  constexpr size_t kBatch = STANDARD_VECTOR_SIZE;
  _vec_buf.resize(std::min(n, kBatch) * _d);
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
      _qw->EncodeCluster(out, _vec_buf.data(), filled);
      filled = 0;
    }
  }
  if (filled != 0) {
    _qw->EncodeCluster(out, _vec_buf.data(), filled);
  }
}

void IvfTermReader::Finish(IndexOutput& /*out*/) { SDB_ASSERT(_qw); }

void IvfWriter::OnCommit(ColWriter& cw, IdxWriter& idx,
                         std::span<const field_id> column_ids,
                         const IndexFieldOptions* field_options) {
  if (!field_options) {
    return;
  }
  for (const field_id id : column_ids) {
    const auto opts = field_options->GetColumnOptions(id);
    if (!opts.ivf_info) {
      continue;
    }
    auto col = cw.ReopenColumn(id);
    if (!col) {
      continue;
    }
    const auto d = static_cast<uint32_t>(col->ArraySize());
    auto qw =
      MakeQuantizerWriter(opts.ivf_info->quant.kind, d, opts.ivf_info->metric,
                          opts.ivf_info->quant.pq_m);

    IvfBuilder builder{*opts.ivf_info};
    BuiltIvf built = builder.Build(*col, cw.CommitReadContext(), idx, qw.get());
    if (built.empty) {
      continue;
    }
    idx.AddCentroidsEntry(opts.ivf_info->centroids_id, built.resident_offset,
                          built.resident_size);
    _results.push_back(
      Result{.postings_id = opts.ivf_info->postings_id,
             .cluster_docs = std::move(built.cluster_docs),
             .cluster_offsets = std::move(built.cluster_offsets),
             .fine_centroids = std::move(built.fine_centroids),
             .qw = std::move(qw),
             .vector_column = std::move(col),
             .d = d});
  }
}

std::span<const BasicTermReader* const> IvfWriter::ClusterReaders(
  ReadContext& ctx) {
  if (_reader_ptrs.empty() && !_results.empty()) {
    _readers.reserve(_results.size());
    _reader_ptrs.reserve(_results.size());
    for (auto& r : _results) {
      _readers.push_back(std::make_unique<IvfTermReader>(
        r.postings_id, r.cluster_docs, r.cluster_offsets, r.qw.get(),
        r.vector_column.get(), &ctx, r.d, r.fine_centroids));
      _reader_ptrs.push_back(_readers.back().get());
    }
  }
  return _reader_ptrs;
}

IvfWriter::IvfWriter() noexcept = default;

IvfWriter::~IvfWriter() = default;

}  // namespace irs
