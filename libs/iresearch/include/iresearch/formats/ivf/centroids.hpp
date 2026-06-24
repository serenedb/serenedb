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

namespace irs {

class IndexInput;
class IndexOutput;

class FlatCentroids {
 public:
  FlatCentroids() = default;
  FlatCentroids(VectorMetric metric, uint32_t nlist, uint32_t d,
                std::vector<float> centroids);

  // Resolves up to `nprobe` nearest posting lists for `query` and appends their
  // ids to `out`. Uses the stored metric internally.
  void Search(std::span<const float> query, uint32_t nprobe,
              std::vector<uint32_t>& out) const;

  void Serialize(IndexOutput& out) const;
  static FlatCentroids Deserialize(IndexInput& in, uint64_t byte_size);

  uint32_t Count() const noexcept { return _nlist; }
  uint32_t Dimension() const noexcept { return _d; }
  VectorMetric Metric() const noexcept { return _metric; }
  bool Empty() const noexcept { return _nlist == 0; }

 private:
  const float* Centroid(uint32_t c) const noexcept {
    return _centroids.data() + static_cast<size_t>(c) * _d;
  }

  VectorMetric _metric = VectorMetric::L2Sqr;
  uint32_t _nlist = 0;
  uint32_t _d = 0;
  std::vector<float> _centroids;  // nlist x d row-major
};

}  // namespace irs
