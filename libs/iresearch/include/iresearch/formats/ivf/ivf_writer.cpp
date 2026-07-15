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
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <span>
#include <utility>

#include "basics/assert.h"
#include "basics/memory.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/column/col_writer.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/formats/ivf/centroids.hpp"
#include "iresearch/formats/ivf/clustering.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/formats/ivf/quantizer.hpp"
#include "iresearch/index/index_features.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/type_limits.hpp"
#include "pg/sql_exception_macro.h"

namespace irs {
namespace {

constexpr uint32_t kDefaultClusterIters = 25;
constexpr uint64_t kMinPqTrainSample = 256;

}  // namespace

BuiltIvf IvfBuilder::Compute(const ColumnReader& vector_column,
                             ReadContext& ctx, QuantizerWriter* qw) const {
  const auto* child = vector_column.Child();
  SDB_ASSERT(child);
  const auto d = static_cast<uint32_t>(vector_column.ArraySize());
  const auto rows = vector_column.RowCount();

  BuiltIvf result;
  result.d = d;
  if (rows == 0 || d == 0) {
    return result;
  }

  const bool pq = _info.quant.kind == VectorQuantization::PQ;
  const bool sq_train =
    qw != nullptr && (_info.quant.kind == VectorQuantization::SQ8 ||
                      _info.quant.kind == VectorQuantization::SQ4);

  auto centroids = CentroidsBuilder::Create(
    vector_column, ctx, rows, _info.metric, d, pq ? kMinPqTrainSample : 0);
  centroids.Finish();
  const auto leaf = centroids.LeafCentroids();
  const uint32_t n_leaf = static_cast<uint32_t>(leaf.size() / d);
  if (n_leaf == 0) {
    return result;
  }

  if (pq && qw != nullptr) {
    const auto sample = centroids.TrainingSample();
    const size_t n_sample = sample.size() / d;
    auto ids = centroids.AssignCentroids(sample, d);
    SDB_ASSERT(ids.size() == n_sample);
    std::vector<float> residuals(sample.size());
    for (size_t i = 0; i < n_sample; ++i) {
      const float* v = sample.data() + i * d;
      const float* c = leaf.data() + static_cast<size_t>(ids[i]) * d;
      float* r = residuals.data() + i * d;
      for (uint32_t j = 0; j < d; ++j) {
        r[j] = v[j] - c[j];
      }
    }
    qw->Train(residuals.data(), n_sample);
  }

  const auto valid = ReadValidity(vector_column, rows, ctx);
  uint64_t valid_count = 0;
  for (uint64_t r = 0; r < rows; ++r) {
    valid_count += valid[r] ? 1 : 0;
  }

  std::vector<uint32_t> doc_cluster;
  doc_cluster.reserve(valid_count);

  {
    std::vector<float> gather;
    gather.reserve(STANDARD_VECTOR_SIZE * d);
    size_t gathered = 0;
    const auto flush = [&]() {
      if (gathered == 0) {
        return;
      }
      auto ids = centroids.AssignCentroids({gather.data(), gathered * d}, d);
      SDB_ASSERT(ids.size() == gathered);
      doc_cluster.insert(doc_cluster.end(), ids.begin(), ids.end());
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
    SDB_ASSERT(doc_cluster.size() == valid_count);
  }

  result.cluster_offsets.assign(n_leaf + 1, 0);
  for (const uint32_t c : doc_cluster) {
    ++result.cluster_offsets[c + 1];
  }
  for (uint32_t c = 0; c < n_leaf; ++c) {
    result.cluster_offsets[c + 1] += result.cluster_offsets[c];
  }
  result.cluster_docs.resize(valid_count);
  {
    std::vector<uint64_t> cursor(result.cluster_offsets.begin(),
                                 result.cluster_offsets.begin() + n_leaf);
    size_t seen = 0;
    for (uint64_t r = 0; r < rows; ++r) {
      if (!valid[r]) {
        continue;
      }
      const uint32_t c = doc_cluster[seen++];
      result.cluster_docs[cursor[c]++] =
        static_cast<doc_id_t>(r + doc_limits::min());
    }
    SDB_ASSERT(seen == valid_count);
  }

  result.centroids = std::move(centroids);
  result.empty = false;
  return result;
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

    IRS_DOC_ITERATOR_DEFAULTS

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
  size_t first = _count;
  size_t last = _count;
  for (size_t c = 0; c < _count; ++c) {
    if (cluster_offsets[c + 1] == cluster_offsets[c]) {
      continue;
    }
    if (first == _count) {
      first = c;
    }
    last = c;
  }
  if (first != _count) {
    EncodeCentroidTerm(static_cast<uint32_t>(first), _min_buf.data());
    _min = {_min_buf.data(), _min_buf.size()};
    EncodeCentroidTerm(static_cast<uint32_t>(last), _max_buf.data());
    _max = {_max_buf.data(), _max_buf.size()};
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
  _qw->BeginCluster(n);
  constexpr size_t kBatch = STANDARD_VECTOR_SIZE;
  RangeScanCursor cursor{*_vectors->Child(), *_ctx};
  duckdb::Vector batch{duckdb::LogicalType::FLOAT,
                       static_cast<duckdb::idx_t>(kBatch) * _d};
  const float* vecs = duckdb::FlatVector::GetData<float>(batch);
  size_t filled = 0;
  size_t k = 0;
  while (k < n) {
    const size_t run = std::min(ConsecutiveRunLength(docs, k), kBatch - filled);
    const uint64_t r = static_cast<uint64_t>(docs[k]) - doc_limits::min();
    cursor.SeekTo(r * _d);
    cursor.Read(batch, static_cast<duckdb::idx_t>(run) * _d,
                static_cast<duckdb::idx_t>(filled) * _d);
    filled += run;
    k += run;
    if (filled == kBatch) {
      _qw->EncodeCluster(out, vecs, filled);
      filled = 0;
    }
  }
  if (filled != 0) {
    _qw->EncodeCluster(out, vecs, filled);
  }
  _qw->FinishCluster(out);
}

void IvfTermReader::Finish(IndexOutput& /*out*/) { SDB_ASSERT(_qw); }

void IvfWriter::Compute(const ColumnReader& col, ReadContext& ctx) {
  SDB_ASSERT(_idx != nullptr,
             "IvfWriter::Compute: SetIdxWriter must be called first");
  const auto d = static_cast<uint32_t>(col.ArraySize());
  const uint32_t pq_niter =
    _info.cluster_iters != 0 ? _info.cluster_iters : kDefaultClusterIters;
  auto qw =
    MakeQuantizerWriter(_info.quant.kind, d, _info.metric, _info.quant.pq_m,
                        pq_niter, _info.quant.nb_bits);

  IvfBuilder builder{_info};
  auto built = builder.Compute(col, ctx, qw.get());
  if (built.empty) {
    return;
  }
  _result = Result{.postings_id = _info.postings_id,
                   .qw = std::move(qw),
                   .data = std::move(built)};
  _built = true;
}

void IvfWriter::FlushTree() {
  if (!_built) {
    return;
  }
  auto& out = _idx->BlocksOut();
  const auto tree_span = _result.data.centroids.Serialize(out);
  const auto stats = _result.qw != nullptr ? _result.qw->StatsBytes()
                                           : std::span<const byte_type>{};
  const uint64_t stats_offset = out.Position();
  out.WriteU64(stats.size());
  if (!stats.empty()) {
    out.WriteData(stats.data(), stats.size());
  }
  const uint64_t stats_byte_size = out.Position() - stats_offset;
  _idx->AddIvf(_info.centroids_id,
               IvfCentroidMeta{.tree_offset = tree_span.offset,
                               .tree_byte_size = tree_span.byte_size,
                               .stats_offset = stats_offset,
                               .stats_byte_size = stats_byte_size});
}

const BasicTermReader* IvfWriter::ClusterReader(ReadContext& ctx,
                                                const ColReader& col_reader) {
  if (!_built) {
    return nullptr;
  }
  if (!_reader) {
    _reader = std::make_unique<IvfTermReader>(
      _result.postings_id, _result.data.cluster_docs,
      _result.data.cluster_offsets, _result.qw.get(),
      col_reader.Column(_result.postings_id), &ctx, _result.data.d,
      _result.data.centroids.LeafCentroids());
  }
  return _reader.get();
}

IvfWriter::IvfWriter(IvfInfo info) : _info{std::move(info)} {}

IvfWriter::~IvfWriter() = default;

}  // namespace irs
