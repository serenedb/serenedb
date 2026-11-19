////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///     http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#include "bridge.hpp"

#include <cuda_runtime_api.h>

#include <algorithm>
#include <exception>
#include <memory>
#include <string_view>
#include <utility>

#include "faiss/gpu/GpuIndexFlat.h"
#include "faiss/gpu/GpuResources.h"
#include "faiss/gpu/StandardGpuResources.h"
#include "faiss/gpu/utils/DeviceUtils.h"
#include "faiss/impl/FaissAssert.h"

namespace {

std::shared_ptr<faiss::gpu::GpuResources> MakeResources() {
  constexpr size_t kMiB = 1024 * 1024;
  faiss::gpu::StandardGpuResources resources;
  resources.setTempMemory(64 * kMiB);
  resources.setPinnedMemory(16 * kMiB);
  return resources.getResources();
}

void SetError(sdb_ivf_cuda_error& error, std::string_view message) noexcept {
  const auto size = std::min(message.size(), sizeof(error.message) - 1);
  std::copy_n(message.data(), size, error.message);
  error.message[size] = '\0';
}

auto Call(sdb_ivf_cuda_error& error, auto operation) noexcept
  -> decltype(operation()) {
  try {
    return operation();
  } catch (const std::exception& exception) {
    SetError(error, exception.what());
  } catch (...) {
    SetError(error, "Unknown FAISS GPU error");
  }
  return {};
}

}  // namespace

struct sdb_ivf_cuda_index {
  explicit sdb_ivf_cuda_index(int32_t dimensions)
    : index{MakeResources(), dimensions} {}

  const float* SearchVectors(int64_t count, const float* vectors) {
    if (!training_host || vectors != training_host || count != training_count ||
        count <= 0) {
      return vectors;
    }
    if (!training_device.get()) {
      const auto resources = index.getResources();
      const auto device = index.getDevice();
      const auto row_bytes = static_cast<size_t>(index.d) * sizeof(float);
      const auto rows = static_cast<size_t>(count);
      const auto paging_size = index.getMinPagingSize();
      if (paging_size == 0 || rows > (paging_size - 1) / row_bytes) {
        training_host = nullptr;
        return vectors;
      }
      faiss::gpu::DeviceScope scope{device};
      const auto stream = resources->getDefaultStream(device);
      const auto bytes = rows * row_bytes;
      auto sample = resources->allocMemoryHandle(
        {faiss::gpu::AllocType::Other, device,
         faiss::gpu::MemorySpace::Temporary, stream, bytes});
      const auto status = cudaMemcpyAsync(sample.get(), vectors, bytes,
                                          cudaMemcpyHostToDevice, stream);
      FAISS_THROW_IF_NOT_FMT(status == cudaSuccess,
                             "Copying IVF training data to CUDA failed: %s",
                             cudaGetErrorString(status));
      training_device = std::move(sample);
    }
    return static_cast<const float*>(training_device.get());
  }

  faiss::gpu::GpuIndexFlatIP index;
  const float* training_host = nullptr;
  int64_t training_count = 0;
  faiss::gpu::GpuMemoryReservation training_device;
};

sdb_ivf_cuda_index* sdb_ivf_cuda_create(int32_t dimensions,
                                        sdb_ivf_cuda_error* error) {
  return Call(*error, [&] {
    return std::make_unique<sdb_ivf_cuda_index>(dimensions).release();
  });
}

void sdb_ivf_cuda_destroy(sdb_ivf_cuda_index* index) { delete index; }

void sdb_ivf_cuda_begin_training(sdb_ivf_cuda_index* index, int64_t count,
                                 const float* vectors) {
  index->training_host = vectors;
  index->training_count = count;
}

void sdb_ivf_cuda_end_training(sdb_ivf_cuda_index* index) {
  index->training_device.release();
  index->training_host = nullptr;
  index->training_count = 0;
}

int sdb_ivf_cuda_add(sdb_ivf_cuda_index* index, int64_t count,
                     const float* vectors, sdb_ivf_cuda_error* error) {
  return Call(*error, [&] {
    index->index.add(count, vectors);
    return 1;
  });
}

int sdb_ivf_cuda_reset(sdb_ivf_cuda_index* index, sdb_ivf_cuda_error* error) {
  return Call(*error, [&] {
    index->index.reset();
    return 1;
  });
}

int sdb_ivf_cuda_search(sdb_ivf_cuda_index* index, int64_t count,
                        const float* vectors, int64_t k, float* distances,
                        int64_t* labels, sdb_ivf_cuda_error* error) {
  return Call(*error, [&] {
    index->index.search(count, index->SearchVectors(count, vectors), k,
                        distances, labels);
    return 1;
  });
}
