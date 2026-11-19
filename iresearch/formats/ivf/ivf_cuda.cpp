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

#include "iresearch/formats/ivf/ivf_cuda.hpp"

#include <absl/cleanup/cleanup.h>

#include "iresearch/formats/ivf/cuda/bridge.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

void CheckCuda(int status, const sdb_ivf_cuda_error& error) {
  if (!status) {
    THROW_SQL_ERROR(ERR_MSG("FAISS GPU IVF build failed: ", error.message));
  }
}

class FaissGpuIndex final : public IvfCudaIndex {
 public:
  FaissGpuIndex(uint32_t dimensions, size_t* gpu_batches)
    : IvfCudaIndex{dimensions},
      _index{nullptr, sdb_ivf_cuda_destroy},
      _gpu_batches{gpu_batches} {
    sdb_ivf_cuda_error error;
    _index.reset(sdb_ivf_cuda_create(d, &error));
    CheckCuda(static_cast<bool>(_index), error);
  }

  void TrainClustering(faiss::Clustering& clustering, faiss::idx_t count,
                       const float* vectors) final {
    reset();
    auto release_sample =
      absl::Cleanup{[&] { sdb_ivf_cuda_end_training(_index.get()); }};
    sdb_ivf_cuda_begin_training(_index.get(), count, vectors);
    clustering.train(count, vectors, *this);
  }

  void reset() final {
    sdb_ivf_cuda_error error;
    CheckCuda(sdb_ivf_cuda_reset(_index.get(), &error), error);
    ntotal = 0;
  }

  void add(faiss::idx_t count, const float* vectors) final {
    sdb_ivf_cuda_error error;
    CheckCuda(sdb_ivf_cuda_add(_index.get(), count, vectors, &error), error);
    ntotal += count;
  }

  void search(faiss::idx_t count, const float* vectors, faiss::idx_t k,
              float* distances, faiss::idx_t* labels,
              const faiss::SearchParameters* params) const final {
    SDB_ASSERT(!params);
    sdb_ivf_cuda_error error;
    CheckCuda(sdb_ivf_cuda_search(_index.get(), count, vectors, k, distances,
                                  labels, &error),
              error);
    if (_gpu_batches && count > 0) {
      ++*_gpu_batches;
    }
  }

 private:
  std::unique_ptr<sdb_ivf_cuda_index, decltype(&sdb_ivf_cuda_destroy)> _index;
  size_t* _gpu_batches;
};

}  // namespace

std::unique_ptr<IvfCudaIndex> MakeIvfCudaIndex(uint32_t dimensions,
                                               size_t* gpu_batches) {
  return std::make_unique<FaissGpuIndex>(dimensions, gpu_batches);
}

}  // namespace irs
