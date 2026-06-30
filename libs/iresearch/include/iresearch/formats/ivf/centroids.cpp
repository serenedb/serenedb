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
#include <utility>

#include "basics/errors.h"
#include "basics/exceptions.h"
#include "basics/misc.hpp"
#include "basics/resource_manager.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/store/data_output.hpp"

namespace irs {
namespace {

void TopK(std::vector<std::pair<float, uint32_t>>& scored, uint32_t k,
          bool nearest_is_largest, std::vector<uint32_t>& out) {
  const auto mid = scored.begin() + k;
  std::partial_sort(
    scored.begin(), mid, scored.end(),
    [nearest_is_largest](const auto& l, const auto& r) noexcept {
      return nearest_is_largest ? l.first > r.first : l.first < r.first;
    });
  out.reserve(out.size() + k);
  for (auto it = scored.begin(); it != mid; ++it) {
    out.push_back(it->second);
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

void TwoLayerCentroids::Release() noexcept {
  _l1_centroids.clear();
  _offsets.clear();
  _n_l1 = 0;
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
  view.fine_ids.clear();
  view.l2_centroids = nullptr;
  if (n_l2 == 0) {
    return;
  }

  const size_t centroids_bytes = static_cast<size_t>(n_l2) * _d * sizeof(float);
  const size_t ids_bytes = static_cast<size_t>(n_l2) * sizeof(uint32_t);
  const size_t total = centroids_bytes + ids_bytes;

  const byte_type* p = in.ReadStable(total);
  if (p == nullptr) {
    view.buf.resize(total);
    in.ReadData(view.buf.data(), total);
    p = view.buf.data();
  }
  view.l2_centroids = p;

  view.fine_ids.resize(n_l2);
  const byte_type* ids = p + centroids_bytes;
  for (uint32_t i = 0; i < n_l2; ++i) {
    uint32_t v;
    std::memcpy(&v, ids + static_cast<size_t>(i) * sizeof(uint32_t), sizeof(v));
    view.fine_ids[i] = v;
  }
}

void TwoLayerCentroids::SearchL2(std::span<const float> query,
                                 const L2BodyView& view, uint32_t n2,
                                 std::vector<uint32_t>& out) const {
  if (view.n_l2 == 0) {
    return;
  }
  n2 = std::min<uint32_t>(std::max<uint32_t>(n2, 1), view.n_l2);

  const auto dist = ResolveVectorDistance(_metric);
  const bool nearest_is_largest = VectorMetricNearestIsLargest(_metric);
  const auto* q = reinterpret_cast<const byte_type*>(query.data());
  const auto d = static_cast<uint16_t>(_d);
  const size_t stride = static_cast<size_t>(_d) * sizeof(float);

  std::vector<std::pair<float, uint32_t>> scored;
  scored.reserve(view.n_l2);
  for (uint32_t s = 0; s < view.n_l2; ++s) {
    const byte_type* cv = view.l2_centroids + s * stride;
    scored.emplace_back(dist(q, cv, d), view.fine_ids[s]);
  }

  TopK(scored, n2, nearest_is_largest, out);
}

}  // namespace irs
