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
#include <atomic>
#include <cstdlib>
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
constexpr size_t kLargeSampleCacheBytes = 3072 * kMiB;
std::atomic<size_t> large_sample_bytes{0};

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

class LargeTrainingSample {
 public:
  LargeTrainingSample() = default;
  LargeTrainingSample(const LargeTrainingSample&) = delete;
  LargeTrainingSample& operator=(const LargeTrainingSample&) = delete;
  ~LargeTrainingSample() { Release(); }

  bool Allocate(int device, size_t bytes) {
    FAISS_ASSERT(!_data);
    auto used = large_sample_bytes.load(std::memory_order_relaxed);
    do {
      if (bytes > kLargeSampleCacheBytes - used) {
        return false;
      }
    } while (!large_sample_bytes.compare_exchange_weak(
      used, used + bytes, std::memory_order_relaxed));

    void* data = nullptr;
    const auto status = cudaMalloc(&data, bytes);
    if (status != cudaSuccess) {
      large_sample_bytes.fetch_sub(bytes, std::memory_order_relaxed);
      if (status == cudaErrorMemoryAllocation) {
        cudaGetLastError();
        return false;
      }
      CheckRuntime(status, "Allocating the large IVF training sample");
    }
    _data = data;
    _device = device;
    _bytes = bytes;
    return true;
  }

  void Release() noexcept {
    if (_data) {
      faiss::gpu::DeviceScope scope{_device};
      const auto status = cudaFree(_data);
      FAISS_ASSERT_FMT(status == cudaSuccess,
                       "Releasing IVF training sample: %s",
                       cudaGetErrorString(status));
      large_sample_bytes.fetch_sub(_bytes, std::memory_order_relaxed);
      _data = nullptr;
    }
  }

  void* Get() const noexcept { return _data; }

 private:
  void* _data = nullptr;
  size_t _bytes = 0;
  int _device = 0;
};

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
    if (const auto* cached = static_cast<const float*>(large_sample.Get())) {
      return cached;
    }
    if (const auto* cached = static_cast<const float*>(training_device.get())) {
      return cached;
    }
    const auto bytes = static_cast<size_t>(count) * index.d * sizeof(float);
    if (training_paged) {
      training_stats.paged_sample_bytes += bytes;
      return vectors;
    }
    const auto resources = index.getResources();
    const auto device = index.getDevice();
    const bool large = bytes >= index.getMinPagingSize();
    static const bool cache_large =
      std::getenv("SDB_IVF_CUDA_LARGE_TRAIN_CACHE") != nullptr;
    faiss::gpu::DeviceScope scope{device};
    const auto stream = resources->getDefaultStream(device);
    void* destination;
    if (large) {
      training_stats.largest_sample_bytes =
        std::max<uint64_t>(training_stats.largest_sample_bytes, bytes);
      if (!cache_large || !large_sample.Allocate(device, bytes)) {
        training_stats.large_cache_fallbacks += cache_large ? 1 : 0;
        training_paged = true;
        training_stats.paged_sample_bytes += bytes;
        return vectors;
      }
      destination = large_sample.Get();
      training_stats.large_sample_bytes += bytes;
      ++training_stats.large_sample_uploads;
    } else {
      training_device = resources->allocMemoryHandle(
        {faiss::gpu::AllocType::Other, device,
         faiss::gpu::MemorySpace::Temporary, stream, bytes});
      destination = training_device.get();
      training_stats.small_sample_bytes += bytes;
    }
    CheckRuntime(cudaMemcpyAsync(destination, vectors, bytes,
                                 cudaMemcpyHostToDevice, stream),
                 "Uploading IVF training sample");
    return static_cast<const float*>(destination);
  }

  faiss::gpu::GpuIndexFlatIP index;
  const float* training_host = nullptr;
  int64_t training_count = 0;
  bool training_paged = false;
  faiss::gpu::GpuMemoryReservation training_device;
  LargeTrainingSample large_sample;
  sdb_ivf_cuda_training_stats training_stats{};
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
  index->large_sample.Release();
  index->training_host = nullptr;
  index->training_count = 0;
  index->training_paged = false;
}

sdb_ivf_cuda_training_stats sdb_ivf_cuda_get_training_stats(
  const sdb_ivf_cuda_index* index) {
  return index->training_stats;
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
