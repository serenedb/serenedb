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

#include <cublas_v2.h>
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

constexpr size_t kMiB = 1024 * 1024;

std::shared_ptr<faiss::gpu::GpuResources> MakeResources(
  size_t temporary = 64 * kMiB, size_t pinned = 16 * kMiB) {
  faiss::gpu::StandardGpuResources resources;
  resources.setTempMemory(temporary);
  resources.setPinnedMemory(pinned);
  return resources.getResources();
}

void CheckRuntime(cudaError_t status, const char* operation) {
  FAISS_THROW_IF_NOT_FMT(status == cudaSuccess, "%s: %s", operation,
                         cudaGetErrorString(status));
}

void CheckBlas(cublasStatus_t status, const char* operation) {
  FAISS_THROW_IF_NOT_FMT(status == CUBLAS_STATUS_SUCCESS, "%s: %s", operation,
                         cublasGetStatusString(status));
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

struct sdb_ivf_cuda_pca {
  sdb_ivf_cuda_pca(int32_t dimensions, int32_t capacity, const float* rotation)
    : dimensions{dimensions},
      capacity{capacity},
      resources{MakeResources(0, 0)} {
    FAISS_THROW_IF_NOT(dimensions > 0 && capacity > 0 && rotation);
    faiss::gpu::DeviceScope scope{kDevice};
    stream = resources->getDefaultStream(kDevice);
    handle = resources->getBlasHandle(kDevice);
    CheckBlas(cublasSetStream(handle, stream), "Setting PCA stream");
    CheckBlas(cublasSetMathMode(handle, CUBLAS_PEDANTIC_MATH),
              "Setting PCA FP32 math mode");
    matrix = Allocate(dimensions);
    input = Allocate(capacity);
    output = Allocate(capacity);
    CheckRuntime(cudaMemcpyAsync(matrix.get(), rotation, Bytes(dimensions),
                                 cudaMemcpyHostToDevice, stream),
                 "Uploading PCA matrix");
    CheckRuntime(cudaStreamSynchronize(stream),
                 "Waiting for PCA matrix upload");
  }

  void Apply(int32_t count, const float* vectors, float* result) {
    FAISS_THROW_IF_NOT(count > 0 && count <= capacity && vectors && result);
    faiss::gpu::DeviceScope scope{kDevice};
    const auto bytes = Bytes(count);
    constexpr float one = 1.f;
    constexpr float zero = 0.f;
    CheckRuntime(cudaMemcpyAsync(input.get(), vectors, bytes,
                                 cudaMemcpyHostToDevice, stream),
                 "Uploading PCA vectors");
    try {
      CheckBlas(
        cublasSgemm(handle, CUBLAS_OP_T, CUBLAS_OP_N, dimensions, count,
                    dimensions, &one, static_cast<const float*>(matrix.get()),
                    dimensions, static_cast<const float*>(input.get()),
                    dimensions, &zero, static_cast<float*>(output.get()),
                    dimensions),
        "Applying PCA rotation");
      CheckRuntime(cudaMemcpyAsync(result, output.get(), bytes,
                                   cudaMemcpyDeviceToHost, stream),
                   "Downloading PCA vectors");
    } catch (...) {
      cudaStreamSynchronize(stream);
      throw;
    }
    CheckRuntime(cudaStreamSynchronize(stream), "Waiting for PCA vectors");
  }

 private:
  static constexpr int kDevice = 0;

  size_t Bytes(int32_t count) const {
    return static_cast<size_t>(count) * dimensions * sizeof(float);
  }

  faiss::gpu::GpuMemoryReservation Allocate(int32_t count) {
    return resources->allocMemoryHandle({faiss::gpu::AllocType::Other, kDevice,
                                         faiss::gpu::MemorySpace::Device,
                                         stream, Bytes(count)});
  }

  int32_t dimensions;
  int32_t capacity;
  std::shared_ptr<faiss::gpu::GpuResources> resources;
  cudaStream_t stream = nullptr;
  cublasHandle_t handle = nullptr;
  faiss::gpu::GpuMemoryReservation matrix;
  faiss::gpu::GpuMemoryReservation input;
  faiss::gpu::GpuMemoryReservation output;
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

sdb_ivf_cuda_pca* sdb_ivf_cuda_pca_create(int32_t dimensions, int32_t capacity,
                                          const float* matrix,
                                          sdb_ivf_cuda_error* error) {
  return Call(*error, [&] {
    return std::make_unique<sdb_ivf_cuda_pca>(dimensions, capacity, matrix)
      .release();
  });
}

void sdb_ivf_cuda_pca_destroy(sdb_ivf_cuda_pca* pca) { delete pca; }

int sdb_ivf_cuda_pca_apply(sdb_ivf_cuda_pca* pca, int32_t count,
                           const float* vectors, float* output,
                           sdb_ivf_cuda_error* error) {
  return Call(*error, [&] {
    pca->Apply(count, vectors, output);
    return 1;
  });
}
