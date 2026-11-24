////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "faiss/Clustering.h"
#include "faiss/Index.h"
#include "iresearch/formats/ivf/cuda/bridge.hpp"

namespace irs {

class IvfCudaIndex : public faiss::Index {
 public:
  explicit IvfCudaIndex(uint32_t dimensions)
    : Index{dimensions, faiss::METRIC_INNER_PRODUCT} {}

  virtual void TrainClustering(faiss::Clustering& clustering,
                               faiss::idx_t count, const float* vectors) = 0;
  virtual sdb_ivf_cuda_training_stats TrainingStats() const = 0;
};

std::unique_ptr<IvfCudaIndex> MakeIvfCudaIndex(uint32_t dimensions,
                                               size_t* gpu_batches);

class IvfCudaPca {
 public:
  IvfCudaPca(int32_t dimensions, int32_t capacity, const float* matrix);
  void Apply(int32_t count, const float* vectors, float* output) const;

 private:
  std::unique_ptr<sdb_ivf_cuda_pca, decltype(&sdb_ivf_cuda_pca_destroy)> _pca;
};

}  // namespace irs
