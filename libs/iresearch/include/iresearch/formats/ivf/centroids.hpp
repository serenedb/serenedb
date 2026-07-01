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

#include <cstdint>
#include <span>
#include <vector>

#include "iresearch/index/column_info.hpp"
#include "iresearch/types.hpp"
#include "iresearch/utils/string.hpp"

namespace irs {

static constexpr uint64_t kHeaderSize = sizeof(uint8_t) + 2 * sizeof(uint32_t);

class IndexInput;
class IndexOutput;
struct IResourceManager;

struct L2BodyView {
  const byte_type* l2_centroids = nullptr;
  std::vector<uint32_t> fine_ids;
  bstring buf;
  uint32_t n_l2 = 0;
};

class TwoLayerCentroids {
 public:
  TwoLayerCentroids() = default;
  TwoLayerCentroids(const TwoLayerCentroids&) = delete;
  TwoLayerCentroids& operator=(const TwoLayerCentroids&) = delete;
  TwoLayerCentroids(TwoLayerCentroids&& rhs) noexcept = default;
  TwoLayerCentroids& operator=(TwoLayerCentroids&& rhs) noexcept = default;
  ~TwoLayerCentroids() = default;

  static TwoLayerCentroids Deserialize(IndexInput& in, uint64_t byte_size);

  void SearchL1(std::span<const float> query, uint32_t n1,
                std::vector<uint32_t>& out) const;

  void ReadL2Body(IndexInput& in, uint32_t l1_id, L2BodyView& view) const;

  void SearchL2(std::span<const float> query, const L2BodyView& view,
                uint32_t n2, std::vector<uint32_t>& out) const;

  void SearchGlobal(std::span<const float> query, IndexInput& in, uint32_t n1,
                    uint32_t nprobe, std::vector<uint32_t>& out_ids,
                    std::vector<float>* out_centroids) const;

  uint32_t Dimension() const noexcept { return _d; }
  uint32_t L1Count() const noexcept { return _n_l1; }
  VectorMetric Metric() const noexcept { return _metric; }
  bool Empty() const noexcept { return _n_l1 == 0; }

  std::span<const byte_type> QuantStats() const noexcept {
    return {_quant_stats.data(), _quant_stats.size()};
  }

  static void WriteFooter(IndexOutput& out, VectorMetric metric, uint32_t d,
                          uint32_t n_l1, std::span<const float> l1_centroids,
                          std::span<const uint64_t> body_offsets,
                          std::span<const byte_type> quant_stats);

  static inline constexpr uint64_t FooterSize(uint32_t d, uint32_t n_l1,
                                              uint64_t stats_len) noexcept {
    return kHeaderSize + static_cast<uint64_t>(n_l1) * d * sizeof(float) +
           static_cast<uint64_t>(n_l1) * sizeof(uint64_t) + sizeof(uint64_t) +
           stats_len;
  }

 private:
  const float* L1Centroid(uint32_t i) const noexcept {
    return _l1_centroids.data() + static_cast<size_t>(i) * _d;
  }

  VectorMetric _metric = VectorMetric::L2Sqr;
  uint32_t _d = 0;
  uint32_t _n_l1 = 0;
  std::vector<float> _l1_centroids;
  std::vector<uint64_t> _offsets;
  bstring _quant_stats;
};

}  // namespace irs
