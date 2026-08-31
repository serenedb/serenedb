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

#include "iresearch/formats/hnsw/hnsw_writer.hpp"

#include <algorithm>
#include <array>
#include <cstring>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/array_vector.hpp>

#include "basics/assert.h"
#include "basics/down_cast.h"
#include "basics/misc.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/merge.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/formats/hnsw/hnsw_reader.hpp"
#include "iresearch/formats/index/idx_reader.hpp"
#include "iresearch/formats/index/idx_writer.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/store/data_output.hpp"
#include "iresearch/utils/vector.hpp"

namespace irs {
namespace {

template<VectorMetric M>
struct HnswRawDist {
  const float* base;
  uint32_t d;
  const float* q = nullptr;

  void SetQuery(uint32_t id) noexcept { q = Row(id); }

  const float* Row(uint32_t id) const noexcept {
    return base + static_cast<size_t>(id) * d;
  }

  void BatchFrom(const float* query, std::span<const uint32_t> ids,
                 score_t* out) const noexcept {
    if constexpr (EffectiveQuantMetric(M) == VectorMetric::L2Sqr) {
      HnswBatchL2Sqr(query, base, d, ids, out);
    } else if constexpr (EffectiveQuantMetric(M) ==
                         VectorMetric::InnerProduct) {
      HnswBatchIp(query, base, d, ids, out);
    } else {
      for (size_t i = 0; i < ids.size(); ++i) {
        out[i] = ComputeDistance<EffectiveQuantMetric(M)>(
          query, Row(ids[i]), static_cast<uint16_t>(d));
      }
    }
  }

  score_t One(uint32_t id) const noexcept {
    score_t s{};
    BatchFrom(q, {&id, 1}, &s);
    return s;
  }

  void Batch(std::span<const uint32_t> ids, score_t* out,
             score_t /*threshold*/ = 0.f) const noexcept {
    BatchFrom(q, ids, out);
  }

  void Prefetch(uint32_t id) const noexcept {
    __builtin_prefetch(Row(id), 0, 3);
  }

  score_t Pair(uint32_t a, uint32_t b) const noexcept {
    score_t s{};
    BatchFrom(Row(a), {&b, 1}, &s);
    return s;
  }
};

// Largest merge source whose graph we can adopt wholesale, or nullptr. Only
// the donor needs a reusable graph -- every other source's rows are inserted
// as a delta, exactly as a fresh flush would insert them.
struct MergeDonor {
  const HnswIndex* index = nullptr;
  const SubReader* reader = nullptr;
  const DocumentMask* mask = nullptr;
  uint64_t out_base = 0;
  uint64_t alive = 0;
};

// qdrant's healing_threshold: past this share of deleted rows the donor's link
// structure has decayed enough that healing costs more than it saves.
inline constexpr double kHnswMaxMissingRatio = 0.3;

MergeDonor PickMergeDonor(std::span<const MergeSource> sources, field_id column,
                          uint32_t d, VectorMetric metric, uint32_t m) {
  MergeDonor best;
  uint64_t out_base = 0;
  for (const auto& src : sources) {
    const uint64_t base = out_base;
    out_base += src.alive_count;
    if (src.reader == nullptr || src.alive_count <= best.alive) {
      continue;
    }
    const auto* ann = src.reader->Ann(column);
    if (ann == nullptr || ann->Kind() != AnnKind::Hnsw || ann->Empty()) {
      continue;
    }
    const auto& hnsw = sdb::basics::downCast<const HnswIndex>(*ann);
    const auto& header = hnsw.Header();
    if (header.d != d || header.metric != metric || header.rows == 0) {
      continue;
    }
    const double missing = 1.0 - static_cast<double>(src.alive_count) /
                                   static_cast<double>(header.rows);
    if (missing > kHnswMaxMissingRatio) {
      continue;
    }
    best = MergeDonor{.index = &hnsw,
                      .reader = src.reader,
                      .mask = src.mask,
                      .out_base = base,
                      .alive = src.alive_count};
  }
  if (best.index != nullptr) {
    auto data = best.index->Load(*best.reader);
    if (!data || data->graph.Empty() || data->graph.M() != m) {
      return {};
    }
  }
  return best;
}

// Re-select a node's neighbours at `level` after some were dropped. Candidates
// are the surviving links plus the frontier found by walking the DONOR graph
// *through* the deleted nodes -- the survivors a deleted node used to bridge to
// are unreachable in the new graph, since it no longer contains that node.
template<typename Dist>
void HealNode(HnswGraph& graph, const HnswGraph& src_graph,
              std::span<const uint32_t> remap, Dist& dist, uint32_t src_node,
              uint32_t node, uint32_t level, HnswBuildScratch& scratch,
              std::vector<HnswCandidate>& pool, std::vector<uint32_t>& seen,
              std::vector<uint32_t>& stack) {
  auto links = graph.Neighbors(node, level);
  pool.clear();
  seen.clear();
  seen.push_back(node);
  const auto known = [&](uint32_t id) {
    return std::ranges::find(seen, id) != seen.end();
  };
  for (const auto id : links) {
    if (id == kHnswInvalidNode) {
      break;
    }
    if (!known(id)) {
      seen.push_back(id);
      pool.push_back({dist.Pair(node, id), id});
    }
  }

  stack.clear();
  std::vector<uint32_t> walked;
  for (const auto id : src_graph.Neighbors(src_node, level)) {
    if (id == kHnswInvalidNode) {
      break;
    }
    if (remap[id] == kHnswInvalidNode) {
      stack.push_back(id);
    }
  }
  while (!stack.empty()) {
    const auto cur = stack.back();
    stack.pop_back();
    if (std::ranges::find(walked, cur) != walked.end()) {
      continue;
    }
    walked.push_back(cur);
    for (const auto nb : src_graph.Neighbors(cur, level)) {
      if (nb == kHnswInvalidNode) {
        break;
      }
      const auto mapped = remap[nb];
      if (mapped == kHnswInvalidNode) {
        stack.push_back(nb);
      } else if (!known(mapped)) {
        seen.push_back(mapped);
        pool.push_back({dist.Pair(node, mapped), mapped});
      }
    }
  }

  if (pool.empty()) {
    return;
  }
  std::ranges::sort(pool, [](const HnswCandidate& l, const HnswCandidate& r) {
    return l.score > r.score;
  });
  HnswSelectNeighbors(dist, pool, static_cast<uint32_t>(links.size()),
                      scratch.selected);
  for (size_t i = 0; i < links.size(); ++i) {
    links[i] =
      i < scratch.selected.size() ? scratch.selected[i] : kHnswInvalidNode;
  }
  for (const auto peer : scratch.selected) {
    HnswLinkReverse(graph, dist, peer, node, level, scratch);
  }
}

// Seed `graph` from the donor's links (remapped to output ids, dropping links
// to deleted rows), heal the nodes that lost neighbours, then insert every
// remaining row. Returns false if the donor is unusable, leaving `graph` for
// the caller to build from scratch.
template<VectorMetric M>
bool BuildGraphFromMerge(HnswGraph& graph, const float* base, uint32_t d,
                         std::span<const uint8_t> valid, uint32_t m,
                         uint32_t ef_construction, uint64_t seed,
                         const MergeDonor& donor) {
  auto data = donor.index->Load(*donor.reader);
  if (!data) {
    return false;
  }
  const auto& src_graph = data->graph;
  const auto src_rows = src_graph.Size();
  const auto rows = valid.size();

  std::vector<uint32_t> remap(src_rows, kHnswInvalidNode);
  uint64_t rank = 0;
  for (size_t r = 0; r < src_rows; ++r) {
    const auto doc = static_cast<doc_id_t>(r) + doc_limits::min();
    if (donor.mask != nullptr && donor.mask->contains(doc)) {
      continue;
    }
    if (donor.out_base + rank >= rows) {
      return false;
    }
    remap[r] = static_cast<uint32_t>(donor.out_base + rank);
    ++rank;
  }
  if (rank != donor.alive) {
    return false;
  }

  graph.Reset(rows, m);
  uint64_t rng = seed;
  for (size_t r = 0; r < src_rows; ++r) {
    if (remap[r] != kHnswInvalidNode &&
        src_graph.LevelOf(static_cast<uint32_t>(r)) != 0) {
      graph.SetLevel(remap[r], src_graph.LevelOf(static_cast<uint32_t>(r)));
    }
  }
  const auto donor_end = donor.out_base + donor.alive;
  for (size_t i = 0; i < rows; ++i) {
    if (i >= donor.out_base && i < donor_end) {
      continue;
    }
    if (valid[i] != 0) {
      graph.SetLevel(static_cast<uint32_t>(i), HnswRandomLevel(rng, m));
    }
  }
  graph.AllocateLinks();

  std::vector<std::array<uint32_t, 3>> to_heal;
  uint32_t entry = kHnswInvalidNode;
  uint32_t entry_level = 0;
  for (size_t r = 0; r < src_rows; ++r) {
    const auto node = remap[r];
    if (node == kHnswInvalidNode) {
      continue;
    }
    const auto levels = src_graph.LevelOf(static_cast<uint32_t>(r));
    if (levels == 0) {
      continue;
    }
    if (levels > entry_level) {
      entry_level = levels;
      entry = node;
    }
    for (uint32_t level = 0; level < levels; ++level) {
      auto in = src_graph.Neighbors(static_cast<uint32_t>(r), level);
      auto out = graph.Neighbors(node, level);
      size_t k = 0;
      bool lost = false;
      for (const auto id : in) {
        if (id == kHnswInvalidNode) {
          break;
        }
        const auto mapped = remap[id];
        if (mapped == kHnswInvalidNode) {
          lost = true;
          continue;
        }
        if (k < out.size()) {
          out[k++] = mapped;
        }
      }
      for (size_t j = k; j < out.size(); ++j) {
        out[j] = kHnswInvalidNode;
      }
      if (lost) {
        to_heal.push_back({static_cast<uint32_t>(r), node, level});
      }
    }
  }
  if (entry == kHnswInvalidNode) {
    return false;
  }
  graph.SetEntryPoint(entry);

  HnswRawDist<M> dist{.base = base, .d = d};
  HnswBuildScratch scratch;
  scratch.search.visited.Reset(rows);
  std::vector<HnswCandidate> pool;
  std::vector<uint32_t> seen;
  std::vector<uint32_t> stack;
  for (const auto& [src_node, node, level] : to_heal) {
    dist.SetQuery(node);
    HealNode(graph, src_graph, remap, dist, src_node, node, level, scratch,
             pool, seen, stack);
  }

  for (size_t i = 0; i < rows; ++i) {
    if ((i >= donor.out_base && i < donor_end) || valid[i] == 0) {
      continue;
    }
    const auto node = static_cast<uint32_t>(i);
    dist.SetQuery(node);
    HnswInsert(graph, node, dist, ef_construction, scratch);
  }
  return true;
}

template<VectorMetric M>
void BuildGraph(HnswGraph& graph, const float* base, uint32_t d,
                std::span<const uint8_t> valid, uint32_t m,
                uint32_t ef_construction, uint64_t seed) {
  const auto rows = valid.size();
  graph.Reset(rows, m);

  uint64_t rng = seed;
  for (size_t i = 0; i < rows; ++i) {
    if (valid[i] != 0) {
      graph.SetLevel(static_cast<uint32_t>(i), HnswRandomLevel(rng, m));
    }
  }
  graph.AllocateLinks();

  HnswRawDist<M> dist{.base = base, .d = d};
  HnswBuildScratch scratch;
  scratch.search.visited.Reset(rows);

  for (size_t i = 0; i < rows; ++i) {
    if (valid[i] == 0) {
      continue;
    }
    const auto node = static_cast<uint32_t>(i);
    dist.SetQuery(node);
    HnswInsert(graph, node, dist, ef_construction, scratch);
  }
}

}  // namespace

HnswWriter::HnswWriter(AnnInfo info) : _info{std::move(info)} {}

HnswWriter::~HnswWriter() = default;

void HnswWriter::Compute(const ColumnReader& col, ReadContext& ctx) {
  _d = static_cast<uint32_t>(col.ArraySize());
  _rows = col.RowCount();
  if (_d == 0 || _rows == 0) {
    return;
  }

  _vectors.resize(static_cast<size_t>(_rows) * _d);
  std::vector<uint8_t> valid(_rows, 1);

  ColumnReader::VectorScratch scratch{col.Type()};
  auto scan = col.InitScan(ctx);
  for (uint64_t done = 0; done < _rows;) {
    const auto take = static_cast<duckdb::idx_t>(
      std::min<uint64_t>(STANDARD_VECTOR_SIZE, _rows - done));
    auto& batch = scratch.Reset();
    col.Scan(scan, batch, take);
    const auto& mask = duckdb::FlatVector::Validity(batch);
    const auto* src =
      duckdb::FlatVector::GetData<float>(duckdb::ArrayVector::GetChild(batch));
    std::memcpy(_vectors.data() + static_cast<size_t>(done) * _d, src,
                static_cast<size_t>(take) * _d * sizeof(float));
    for (duckdb::idx_t i = 0; i < take; ++i) {
      if (!mask.RowIsValid(i)) {
        valid[done + i] = 0;
      }
    }
    done += take;
  }

  if (_info.metric == VectorMetric::Cosine) {
    for (uint64_t i = 0; i < _rows; ++i) {
      float* v = _vectors.data() + static_cast<size_t>(i) * _d;
      vector::L2Space<float, float, float>::Normalize(
        reinterpret_cast<const byte_type*>(v), static_cast<uint16_t>(_d), v);
    }
  }

  const auto m = _info.m != 0 ? _info.m : kHnswDefaultM;
  const auto ef = _info.ef_construction != 0 ? _info.ef_construction
                                             : kHnswDefaultEfConstruction;
  const auto donor =
    PickMergeDonor(_merge_sources, _info.centroids_id, _d, _info.metric, m);
  ResolveEnum<VectorMetric>(_info.metric, [&]<VectorMetric M>() {
    if (donor.index != nullptr &&
        BuildGraphFromMerge<M>(_graph, _vectors.data(), _d, valid, m, ef,
                               kHnswBuildSeed, donor)) {
      return;
    }
    BuildGraph<M>(_graph, _vectors.data(), _d, valid, m, ef, kHnswBuildSeed);
  });

  if (_info.quant.kind == VectorQuantization::None) {
    return;
  }
  _qw = MakeQuantizerWriter(_info.quant.kind, _d,
                            EffectiveQuantMetric(_info.metric),
                            _info.quant.pq_m, 0, _info.quant.nb_bits,
                            /*row_major=*/true);
  SDB_ASSERT(_qw);
  _qw->Train(_vectors.data(), _rows);
  if (QuantizerNeedsCentroid(_info.quant.kind)) {
    _centroid.assign(_d, 0.f);
    for (size_t i = 0; i < _rows; ++i) {
      const float* row = _vectors.data() + i * size_t{_d};
      for (uint32_t j = 0; j < _d; ++j) {
        _centroid[j] += row[j];
      }
    }
    if (_rows != 0) {
      const float inv = 1.f / static_cast<float>(_rows);
      for (uint32_t j = 0; j < _d; ++j) {
        _centroid[j] *= inv;
      }
    }
    _qw->SetClusterCentroid(_centroid.data());
  }
  _record_size = _qw->BlockSetting().record_size;
}

void HnswWriter::Flush() {
  if (_graph.Empty()) {
    return;
  }
  SDB_ASSERT(_idx != nullptr);

  auto& out = _idx->BlocksOut();
  const uint64_t offset = out.Position();

  out.WriteU32(kHnswFormatVersion);
  out.WriteU32(_d);
  out.WriteU32(static_cast<uint32_t>(_info.metric));
  out.WriteU32(static_cast<uint32_t>(_info.quant.kind));
  out.WriteU32(_info.ef_construction);
  out.WriteU32(_record_size);
  out.WriteU64(_rows);
  _graph.Serialize(out);
  if (_qw) {
    _qw->Serialize(out);
    if (!_centroid.empty()) {
      out.WriteData(reinterpret_cast<const byte_type*>(_centroid.data()),
                    _centroid.size() * sizeof(float));
    }
    _qw->Encode(out, _vectors.data(), _rows);
    _qw->Finish(out);
  } else {
    out.WriteData(reinterpret_cast<const byte_type*>(_vectors.data()),
                  _vectors.size() * sizeof(float));
  }

  _idx->AddHnsw(
    _info.centroids_id,
    HnswMeta{.offset = offset, .byte_size = out.Position() - offset});

  _vectors.clear();
  _vectors.shrink_to_fit();
}

}  // namespace irs
