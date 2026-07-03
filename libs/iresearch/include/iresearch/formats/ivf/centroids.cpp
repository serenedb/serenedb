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

#include "iresearch/formats/ivf/centroids.hpp"

#include <algorithm>
#include <cstring>
#include <limits>
#include <numeric>
#include <utility>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

void TopK(std::vector<std::pair<float, uint32_t>>& scored, uint32_t k,
          bool nearest_is_largest, std::vector<uint32_t>& out) {
  const auto mid = scored.begin() + k;
  irs::ResolveBool(nearest_is_largest, [&]<bool NearestIsLarger>() {
    std::partial_sort(scored.begin(), mid, scored.end(),
                      [&](const auto& l, const auto& r) noexcept {
                        if constexpr (NearestIsLarger) {
                          return l.first > r.first;
                        } else {
                          return l.first < r.first;
                        }
                      });
  });
  SDB_ASSERT(out.empty());
  out.reserve(out.size() + k);
  for (auto it = scored.begin(); it != mid; ++it) {
    out.push_back(it->second);
  }
}

void SortProbedAscending(std::vector<uint32_t>& out_ids,
                         std::vector<float>* out_centroids, uint32_t d) {
  const size_t k = out_ids.size();
  if (k <= 1) {
    return;
  }
  std::vector<uint32_t> perm(k);
  std::iota(perm.begin(), perm.end(), 0u);
  std::sort(perm.begin(), perm.end(), [&](uint32_t a, uint32_t b) noexcept {
    return out_ids[a] < out_ids[b];
  });
  std::vector<uint32_t> ids(k);
  for (size_t i = 0; i < k; ++i) {
    ids[i] = out_ids[perm[i]];
  }
  out_ids = std::move(ids);
  if (out_centroids != nullptr) {
    std::vector<float> cens(k * d);
    for (size_t i = 0; i < k; ++i) {
      std::memcpy(cens.data() + i * d,
                  out_centroids->data() + static_cast<size_t>(perm[i]) * d,
                  static_cast<size_t>(d) * sizeof(float));
    }
    *out_centroids = std::move(cens);
  }
}

}  // namespace

void TwoLayerCentroids::WriteFooter(IndexOutput& out, VectorMetric metric,
                                    uint32_t d, uint32_t n_l1,
                                    std::span<const float> l1_centroids,
                                    std::span<const uint64_t> body_offsets,
                                    std::span<const byte_type> quant_stats) {
  out.WriteByte(static_cast<byte_type>(metric));
  out.WriteU32(d);
  out.WriteU32(n_l1);
  if (!l1_centroids.empty()) {
    out.WriteData(reinterpret_cast<const byte_type*>(l1_centroids.data()),
                  l1_centroids.size() * sizeof(float));
  }
  for (const uint64_t off : body_offsets) {
    out.WriteU64(off);
  }
  out.WriteU64(quant_stats.size());
  if (!quant_stats.empty()) {
    out.WriteData(quant_stats.data(), quant_stats.size());
  }
}

TwoLayerCentroids TwoLayerCentroids::Deserialize(IndexInput& in,
                                                 uint64_t byte_size) {
  TwoLayerCentroids out;
  out._metric = static_cast<VectorMetric>(in.ReadByte());
  out._d = static_cast<uint32_t>(in.ReadI32());
  out._n_l1 = static_cast<uint32_t>(in.ReadI32());

  const size_t l1_count = static_cast<size_t>(out._n_l1) * out._d;

  out._l1_centroids.resize(l1_count);
  if (l1_count != 0) {
    in.ReadData(reinterpret_cast<byte_type*>(out._l1_centroids.data()),
                l1_count * sizeof(float));
  }
  out._offsets.resize(out._n_l1);
  for (uint32_t i = 0; i < out._n_l1; ++i) {
    out._offsets[i] = static_cast<uint64_t>(in.ReadI64());
  }

  const uint64_t stats_len = static_cast<uint64_t>(in.ReadI64());

  SDB_ENSURE(
    TwoLayerCentroids::FooterSize(out._d, out._n_l1, stats_len) == byte_size,
    sdb::ERROR_SERVER_CORRUPTED_DATAFILE,
    "idx: two-layer centroid resident size mismatch (computed ",
    TwoLayerCentroids::FooterSize(out._d, out._n_l1, stats_len), ", expected ",
    byte_size, ")");

  out._quant_stats.resize(stats_len);
  if (stats_len != 0) {
    in.ReadData(out._quant_stats.data(), stats_len);
  }

  return out;
}

void TwoLayerCentroids::SearchL1(std::span<const float> query, uint32_t n1,
                                 std::vector<uint32_t>& out) const {
  if (_n_l1 == 0) {
    return;
  }
  n1 = std::min<uint32_t>(std::max<uint32_t>(n1, 1), _n_l1);

  const auto dist = ResolveVectorDistance(_metric);
  const bool nearest_is_largest = VectorMetricNearestIsLargest(_metric);
  const auto* q = reinterpret_cast<const byte_type*>(query.data());
  const auto d = static_cast<uint16_t>(_d);

  std::vector<std::pair<float, uint32_t>> scored;
  scored.reserve(_n_l1);
  for (uint32_t c = 0; c < _n_l1; ++c) {
    const auto* cv = reinterpret_cast<const byte_type*>(L1Centroid(c));
    scored.emplace_back(dist(q, cv, d), c);
  }

  TopK(scored, n1, nearest_is_largest, out);
}

void TwoLayerCentroids::ReadL2Body(IndexInput& in, uint32_t l1_id,
                                   L2BodyView& view) const {
  SDB_ASSERT(l1_id < _n_l1);
  in.Seek(_offsets[l1_id]);

  const uint32_t n_l2 = static_cast<uint32_t>(in.ReadI32());
  view.n_l2 = n_l2;
  if (n_l2 == 0) {
    return;
  }

  const size_t centroids_bytes = static_cast<size_t>(n_l2) * _d * sizeof(float);
  const size_t ids_bytes = static_cast<size_t>(n_l2) * sizeof(uint32_t);
  const size_t radii_bytes = static_cast<size_t>(n_l2) * sizeof(float);
  const size_t total = centroids_bytes + ids_bytes + radii_bytes;

  const byte_type* p = in.ReadStable(total);
  if (p == nullptr) {
    view.buf.resize(total);
    in.ReadData(view.buf.data(), total);
    p = view.buf.data();
  }
  view.l2_centroids = p;

  const byte_type* ids = p + centroids_bytes;

  view.fine_ids = std::span{reinterpret_cast<const uint32_t*>(ids), n_l2};
  view.radii = std::span{
    reinterpret_cast<const float*>(p + centroids_bytes + ids_bytes), n_l2};
}

void TwoLayerCentroids::ForEachFineCluster(IndexInput& in,
                                           const FineClusterVisitor& fn) const {
  const size_t stride = static_cast<size_t>(_d) * sizeof(float);
  L2BodyView body;
  for (uint32_t l1 = 0; l1 < _n_l1; ++l1) {
    ReadL2Body(in, l1, body);
    for (uint32_t s = 0; s < body.n_l2; ++s) {
      fn(body.fine_ids[s], body.l2_centroids + s * stride, body.radii[s]);
    }
  }
}

void TwoLayerCentroids::SearchL2(std::span<const float> query, uint32_t nprobe,
                                 std::span<uint32_t> l1_ids, IndexInput& in,
                                 std::vector<uint32_t>& out_ids,
                                 std::vector<float>* out_centroids) const {
  SDB_ASSERT(out_ids.empty());
  SDB_ASSERT(!out_centroids || out_centroids->empty());
  const auto dist = ResolveVectorDistance(_metric);
  const bool nearest_is_largest = VectorMetricNearestIsLargest(_metric);
  const auto* q = reinterpret_cast<const byte_type*>(query.data());
  const auto d = static_cast<uint16_t>(_d);
  const size_t stride = static_cast<size_t>(_d) * sizeof(float);

  absl::c_sort(l1_ids);

  std::vector<std::pair<float, uint32_t>> scored;
  std::vector<uint32_t> cand_fine_id;
  std::vector<uint32_t> cand_begin;
  cand_begin.reserve(l1_ids.size() + 1);
  L2BodyView body;
  for (const uint32_t l1 : l1_ids) {
    cand_begin.push_back(static_cast<uint32_t>(cand_fine_id.size()));
    ReadL2Body(in, l1, body);
    scored.reserve(scored.size() + body.n_l2);
    cand_fine_id.reserve(cand_fine_id.size() + body.n_l2);
    for (uint32_t s = 0; s < body.n_l2; ++s) {
      const byte_type* cv = body.l2_centroids + s * stride;
      const auto cand = static_cast<uint32_t>(cand_fine_id.size());
      scored.emplace_back(dist(q, cv, d), cand);
      cand_fine_id.push_back(body.fine_ids[s]);
    }
  }
  cand_begin.push_back(static_cast<uint32_t>(cand_fine_id.size()));
  if (scored.empty()) {
    return;
  }

  const uint32_t k =
    std::min<uint32_t>(nprobe, static_cast<uint32_t>(scored.size()));
  std::vector<uint32_t> cand_idx;
  TopK(scored, k, nearest_is_largest, cand_idx);

  out_ids.reserve(cand_idx.size());
  for (const uint32_t ci : cand_idx) {
    out_ids.push_back(cand_fine_id[ci]);
  }
  if (out_centroids == nullptr) {
    return;
  }

  constexpr uint32_t kUnselected = std::numeric_limits<uint32_t>::max();
  std::vector<uint32_t> rank_of(cand_fine_id.size(), kUnselected);
  for (uint32_t p = 0; p < cand_idx.size(); ++p) {
    rank_of[cand_idx[p]] = p;
  }
  out_centroids->resize(static_cast<size_t>(cand_idx.size()) * _d);
  for (size_t li = 0; li < l1_ids.size(); ++li) {
    const uint32_t begin = cand_begin[li];
    const uint32_t end = cand_begin[li + 1];
    bool selected = false;
    for (uint32_t ci = begin; ci < end && !selected; ++ci) {
      selected = rank_of[ci] != kUnselected;
    }
    if (!selected) {
      continue;
    }
    ReadL2Body(in, l1_ids[li], body);
    SDB_ASSERT(body.n_l2 == end - begin);
    for (uint32_t s = 0; s < body.n_l2; ++s) {
      const uint32_t p = rank_of[begin + s];
      if (p == kUnselected) {
        continue;
      }
      std::memcpy(out_centroids->data() + static_cast<size_t>(p) * _d,
                  body.l2_centroids + s * stride, stride);
    }
  }
}

void TwoLayerCentroids::SearchGlobal(std::span<const float> query,
                                     IndexInput& in, uint32_t n1,
                                     uint32_t nprobe,
                                     std::vector<uint32_t>& out_ids,
                                     std::vector<float>* out_centroids) const {
  out_ids.clear();
  if (out_centroids != nullptr) {
    out_centroids->clear();
  }
  if (_n_l1 == 0 || nprobe == 0) {
    return;
  }

  std::vector<uint32_t> l1_ids;
  SearchL1(query, n1, l1_ids);
  if (l1_ids.empty()) {
    return;
  }
  SearchL2(query, nprobe, l1_ids, in, out_ids, out_centroids);
  SortProbedAscending(out_ids, out_centroids, _d);
}

}  // namespace irs
