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
#include <memory>
#include <span>
#include <vector>

#include "iresearch/formats/ivf/centroids.hpp"

namespace irs {

class IndexOutput;

// Centroid count below which the graph loses to a flat scan of the same
// centroids, so the writer emits a tree instead. Both Qdrant and Manticore gate
// this way; the value is a starting point pending the crossover sweep, and is
// deliberately conservative because our flat path also has Panorama pruning.
inline constexpr size_t kHnswMinCentroids = 8192;

class HnswGraph;

// Navigates the centroids through a USearch HNSW graph instead of a tree
// descent. Requires a single flat centroid layer, which IvfBuilder forces for
// this layout. Never rotated, so Panorama never applies.
class HnswCentroids final : public CentroidsIndex {
 public:
  HnswCentroids(IVFHeader&& head, std::vector<float>&& centroids,
                std::unique_ptr<HnswGraph>&& graph);
  ~HnswCentroids() final;

  static std::unique_ptr<HnswCentroids> Deserialize(IVFHeader&& head,
                                                    IndexInput& in);

  void Search(std::span<const float> query, IndexInput& in, uint32_t nprobe,
              std::vector<uint32_t>& out_ids, std::vector<float>* out_centroids,
              uint32_t max_search_fanout, bool prune = true,
              CentroidsSearchStats* out_stats = nullptr) const final;

  size_t Size() const noexcept { return _n; }

 private:
  std::vector<float> _centroids;
  size_t _n = 0;
  std::unique_ptr<HnswGraph> _graph;
};

// Builds the graph over `centroids` and appends it to `out`. Called right after
// CentroidsBuilder::Serialize, so the blob lands where HnswCentroids expects
// it: immediately after the flat centroid layer.
void WriteHnswGraph(IndexOutput& out, std::span<const float> centroids,
                    uint32_t d, VectorMetric metric, uint32_t m,
                    uint32_t ef_construction);

}  // namespace irs
