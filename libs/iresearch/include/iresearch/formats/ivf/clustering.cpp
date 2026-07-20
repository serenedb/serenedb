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

#include "iresearch/formats/ivf/clustering.hpp"

#include <faiss/Clustering.h>
#include <faiss/IndexFlat.h>
#include <faiss/SuperKMeans.h>
#include <faiss/VectorTransform.h>
#include <faiss/utils/distances.h>

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>

#include "basics/misc.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/types.hpp"

namespace irs {
namespace {

constexpr uint32_t kSuperKMeansMinD = 32;
constexpr uint32_t kSuperKMeansMinK = 512;
constexpr uint32_t kSuperKMeansSphericalMinK = 4096;

void ConfigureClusteringParams(faiss::ClusteringParameters& cp, uint32_t niter,
                               uint32_t nredo, uint32_t seed, size_t n,
                               uint32_t k) {
  cp.niter = static_cast<int>(niter);
  cp.nredo = static_cast<int>(nredo);
  cp.seed = static_cast<int>(seed);
  cp.min_points_per_centroid = 1;
  cp.max_points_per_centroid = std::max<int>(cp.max_points_per_centroid,
                                             static_cast<int>((n + k - 1) / k));
}

std::vector<float> RunSuperKMeans(const float* data, size_t n, uint32_t k,
                                  uint32_t d, uint32_t seed, uint32_t niter,
                                  uint32_t nredo,
                                  const float* rotation = nullptr) {
  faiss::SuperKMeansParameters cp;
  ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
  cp.rotation = rotation;
  faiss::SuperKMeans kmeans(static_cast<int>(d), static_cast<int>(k), cp);
  kmeans.train(static_cast<faiss::idx_t>(n), data);
  return std::move(kmeans.centroids);
}

std::vector<float> RunLloyd(const float* data, size_t n, uint32_t k, uint32_t d,
                            uint32_t seed, uint32_t niter, uint32_t nredo) {
  faiss::ClusteringParameters cp;
  ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
  faiss::Clustering clus(static_cast<int>(d), static_cast<int>(k), cp);
  faiss::IndexFlatL2 index(static_cast<int>(d));
  clus.train(static_cast<faiss::idx_t>(n), data, index);
  return std::move(clus.centroids);
}

}  // namespace

std::vector<float> MakeRotation(uint32_t d, uint32_t seed) {
  faiss::RandomRotationMatrix rotation(static_cast<int>(d),
                                       static_cast<int>(d));
  rotation.init(static_cast<int>(seed));
  return std::move(rotation.A);
}

void NormalizeRows(float* data, size_t n, uint32_t d) {
  for (size_t i = 0; i < n; ++i) {
    float* row = data + i * d;
    float sum = 0.f;
    for (uint32_t j = 0; j < d; ++j) {
      sum += row[j] * row[j];
    }
    if (sum == 0.f) {
      continue;
    }
    const float inv_norm = 1.f / std::sqrt(sum);
    for (uint32_t j = 0; j < d; ++j) {
      row[j] *= inv_norm;
    }
  }
}

std::vector<float> TrainCentroids(VectorMetric metric, const float* data,
                                  size_t n, uint32_t k, uint32_t d,
                                  uint32_t seed, uint32_t niter, uint32_t nredo,
                                  ClusteringAlgo algo, const float* rotation) {
  if (n == 0 || k == 0) {
    return {};
  }
  k = static_cast<uint32_t>(std::min<size_t>(k, n));

  if (VectorMetricIsAngular(metric)) {
    const bool use_skm =
      algo == ClusteringAlgo::FlatSuperKMeans ||
      (algo == ClusteringAlgo::Auto && metric == VectorMetric::Cosine &&
       d >= kSuperKMeansMinD && k >= kSuperKMeansSphericalMinK);
    if (use_skm) {
      auto centroids =
        RunSuperKMeans(data, n, k, d, seed, niter, nredo, rotation);
      NormalizeRows(centroids.data(), centroids.size() / d, d);
      return centroids;
    }
    faiss::ClusteringParameters cp;
    ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
    cp.spherical = true;
    faiss::Clustering clus(static_cast<int>(d), static_cast<int>(k), cp);
    faiss::IndexFlatIP index(static_cast<int>(d));
    clus.train(static_cast<faiss::idx_t>(n), data, index);
    return std::move(clus.centroids);
  }

  ClusteringAlgo eff = algo;
  if (eff == ClusteringAlgo::Auto) {
    eff = d >= kSuperKMeansMinD && k >= kSuperKMeansMinK
            ? ClusteringAlgo::FlatSuperKMeans
            : ClusteringAlgo::Lloyd;
  }
  if (eff == ClusteringAlgo::FlatSuperKMeans) {
    return RunSuperKMeans(data, n, k, d, seed, niter, nredo, rotation);
  }
  return RunLloyd(data, n, k, d, seed, niter, nredo);
}

namespace {

template<VectorMetric Metric>
uint32_t NearestCentroidT(const float* v, const float* centroids, uint32_t k,
                          uint32_t d) noexcept {
  const auto* q = reinterpret_cast<const byte_type*>(v);
  const auto dd = static_cast<uint16_t>(d);
  uint32_t best = 0;
  float best_score = -std::numeric_limits<float>::max();
  for (uint32_t s = 0; s < k; ++s) {
    const auto* cv = reinterpret_cast<const byte_type*>(
      centroids + static_cast<size_t>(s) * d);
    const float score = ComputeDistance<Metric>(q, cv, dd);
    if (score > best_score) {
      best_score = score;
      best = s;
    }
  }
  return best;
}

template<VectorMetric Metric>
void AssignNearestT(const float* data, size_t n, const float* centroids,
                    uint32_t k, uint32_t d, std::vector<uint32_t>& out) {
  const size_t base = out.size();
  out.resize(base + n);
  if (n == 0) {
    return;
  }
  if (Metric == VectorMetric::L1 || k == 0) {
    for (size_t i = 0; i < n; ++i) {
      out[base + i] = NearestCentroidT<Metric>(data + i * d, centroids, k, d);
    }
  } else {
    std::vector<int64_t> indexes(n);
    std::vector<float> distances(n);
    if constexpr (Metric == VectorMetric::InnerProduct ||
                  Metric == VectorMetric::Cosine) {
      faiss::knn_inner_product(data, centroids, d, n, k, 1, distances.data(),
                               indexes.data());
    } else {
      faiss::knn_L2sqr(data, centroids, d, n, k, 1, distances.data(),
                       indexes.data());
    }
    for (size_t i = 0; i < n; ++i) {
      out[base + i] = static_cast<uint32_t>(indexes[i]);
    }
  }
}

void AssignNearest(VectorMetric metric, const float* data, size_t n,
                   const float* centroids, uint32_t k, uint32_t d,
                   std::vector<uint32_t>& out) {
  ResolveEnum<VectorMetric>(metric, [&]<VectorMetric Metric>() {
    AssignNearestT<Metric>(data, n, centroids, k, d, out);
  });
}

}  // namespace

void AssignNearestGrouped(VectorMetric metric, std::span<const float> centroids,
                          size_t d, std::span<float> data,
                          std::span<size_t> ids, std::span<size_t> perm,
                          std::span<std::span<const float>> gathered) {
  const size_t n = data.size() / d;
  const size_t k = centroids.size() / d;
  SDB_ASSERT(ids.size() * d == data.size());
  SDB_ASSERT(perm.empty() || perm.size() == n);
  SDB_ASSERT(gathered.empty() || gathered.size() == n);
  if (n == 0 || k == 0) {
    std::fill(ids.begin(), ids.end(), 0);
    return;
  }
  std::vector<uint32_t> assign;
  AssignNearest(metric, data.data(), n, centroids.data(),
                static_cast<uint32_t>(k), static_cast<uint32_t>(d), assign);

  std::vector<size_t> cursor(k, 0);
  for (const uint32_t a : assign) {
    ++cursor[a];
  }
  for (size_t i = 0, start = 0; i < k; ++i) {
    const size_t count = cursor[i];
    cursor[i] = start;
    start += count;
  }

  std::vector<float> reordered(data.size());
  std::vector<size_t> reordered_perm(perm.empty() ? 0 : n);
  for (size_t i = 0; i < n; ++i) {
    const uint32_t bucket = assign[i];
    const size_t pos = cursor[bucket]++;
    std::memcpy(reordered.data() + pos * d, data.data() + i * d,
                d * sizeof(float));
    ids[pos] = bucket;
    if (!perm.empty()) {
      reordered_perm[pos] = perm[i];
    }
    if (!gathered.empty()) {
      gathered[pos] = centroids.subspan(static_cast<size_t>(bucket) * d, d);
    }
  }
  std::memcpy(data.data(), reordered.data(), data.size() * sizeof(float));
  if (!perm.empty()) {
    std::copy(reordered_perm.begin(), reordered_perm.end(), perm.begin());
  }
}

std::vector<bool> ReadValidity(const ColumnReader& vector_column, uint64_t rows,
                               ReadContext& ctx) {
  std::vector<bool> valid(rows, true);
  const ColumnReader* validity = vector_column.Validity();
  if (!validity) {
    return valid;
  }
  duckdb::Vector vbatch{duckdb::LogicalType{duckdb::LogicalTypeId::VALIDITY},
                        duckdb::idx_t{0}};
  vbatch.BufferMutable().GetValidityMask().Initialize(STANDARD_VECTOR_SIZE);
  auto vscan = validity->InitScan(ctx);
  for (uint64_t start = 0; start < rows; start += STANDARD_VECTOR_SIZE) {
    const auto take =
      std::min<duckdb::idx_t>(STANDARD_VECTOR_SIZE, rows - start);
    validity->Scan(vscan, vbatch, take);
    const auto& mask = vbatch.Buffer().GetValidityMask();
    for (uint64_t k = 0; k < take; ++k) {
      valid[start + k] = mask.RowIsValid(k);
    }
  }
  return valid;
}

}  // namespace irs
