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
#include <faiss/utils/utils.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <numeric>

#include "basics/misc.hpp"
#include "iresearch/formats/ivf/ivf_reader.hpp"
#include "iresearch/types.hpp"

// The vendored OpenBLAS exports sgemm_/sgemv_ only; faiss declares them the
// same way in its own translation units.
using FINTEGER = int;

extern "C" {
int sgemm_(const char* transa, const char* transb, FINTEGER* m, FINTEGER* n,
           FINTEGER* k, const float* alpha, const float* a, FINTEGER* lda,
           const float* b, FINTEGER* ldb, const float* beta, float* c,
           FINTEGER* ldc);

int sgemv_(const char* trans, FINTEGER* m, FINTEGER* n, const float* alpha,
           const float* a, FINTEGER* lda, const float* x, FINTEGER* incx,
           const float* beta, float* y, FINTEGER* incy);
}

namespace irs {
namespace {

constexpr uint32_t kSuperKMeansMinD = 32;
constexpr uint32_t kSuperKMeansMinK = 512;
constexpr uint32_t kSuperKMeansSphericalMinK = 4096;
constexpr uint32_t kPcaIters = 12;

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
                            uint32_t seed, uint32_t niter, uint32_t nredo,
                            bool spherical = false) {
  faiss::ClusteringParameters cp;
  ConfigureClusteringParams(cp, niter, nredo, seed, n, k);
  cp.spherical = spherical;
  faiss::Clustering clus(static_cast<int>(d), static_cast<int>(k), cp);
  if (spherical) {
    faiss::IndexFlatIP index(static_cast<int>(d));
    clus.train(static_cast<faiss::idx_t>(n), data, index);
  } else {
    faiss::IndexFlatL2 index(static_cast<int>(d));
    clus.train(static_cast<faiss::idx_t>(n), data, index);
  }
  return std::move(clus.centroids);
}

bool SuperKMeansGate(uint32_t d, uint32_t k, uint32_t min_k) {
  return d >= kSuperKMeansMinD && k >= min_k;
}

// Column-major `d x d` second moment of the row-major `n x d` sample. The
// second moment, not the covariance: the basis is applied as a pure rotation
// with no bias, so it has to be optimal for uncentered vectors.
void SecondMoment(const float* data, size_t n, uint32_t d, float* out) {
  FINTEGER dd = static_cast<FINTEGER>(d);
  FINTEGER nn = static_cast<FINTEGER>(n);
  const float one = 1.f;
  const float zero = 0.f;
  sgemm_("N", "T", &dd, &dd, &nn, &one, data, &dd, data, &dd, &zero, out, &dd);
}

// Column-major `c = a * b`, all `d x d`.
void MulSquare(const float* a, const float* b, float* c, uint32_t d) {
  FINTEGER dd = static_cast<FINTEGER>(d);
  const float one = 1.f;
  const float zero = 0.f;
  sgemm_("N", "N", &dd, &dd, &dd, &one, a, &dd, b, &dd, &zero, c, &dd);
}

}  // namespace

std::vector<float> MakeRotation(uint32_t d, uint32_t seed) {
  faiss::RandomRotationMatrix rotation(static_cast<int>(d),
                                       static_cast<int>(d));
  rotation.init(static_cast<int>(seed));
  return std::move(rotation.A);
}

std::vector<float> TrainPcaRotation(const float* data, size_t n, uint32_t d) {
  if (d == 0 || n < d) {
    return {};
  }

  std::vector<float> moment(size_t{d} * d);
  SecondMoment(data, n, d, moment.data());

  std::vector<float> basis(size_t{d} * d, 0.f);
  for (uint32_t i = 0; i < d; ++i) {
    basis[i + size_t{i} * d] = 1.f;
  }
  std::vector<float> next(size_t{d} * d);
  for (uint32_t iter = 0; iter < kPcaIters; ++iter) {
    MulSquare(moment.data(), basis.data(), next.data(), d);
    faiss::matrix_qr(static_cast<int>(d), static_cast<int>(d), next.data());
    basis.swap(next);
  }

  MulSquare(moment.data(), basis.data(), next.data(), d);
  std::vector<uint32_t> order(d);
  std::iota(order.begin(), order.end(), 0u);
  std::vector<float> eigen(d);
  for (uint32_t j = 0; j < d; ++j) {
    const float* q = basis.data() + size_t{j} * d;
    const float* aq = next.data() + size_t{j} * d;
    float sum = 0.f;
    for (uint32_t i = 0; i < d; ++i) {
      sum += q[i] * aq[i];
    }
    eigen[j] = sum;
  }
  std::sort(order.begin(), order.end(),
            [&](uint32_t l, uint32_t r) { return eigen[l] > eigen[r]; });

  std::vector<float> rotation(size_t{d} * d);
  for (uint32_t i = 0; i < d; ++i) {
    std::memcpy(rotation.data() + size_t{i} * d,
                basis.data() + size_t{order[i]} * d, size_t{d} * sizeof(float));
  }
  return rotation;
}

void ApplyRotation(const float* rotation, const float* in, float* out, size_t n,
                   uint32_t d) {
  FINTEGER dd = static_cast<FINTEGER>(d);
  const float one = 1.f;
  const float zero = 0.f;
  if (n == 1) {
    FINTEGER inc = 1;
    sgemv_("T", &dd, &dd, &one, rotation, &dd, in, &inc, &zero, out, &inc);
    return;
  }
  FINTEGER nn = static_cast<FINTEGER>(n);
  sgemm_("T", "N", &dd, &nn, &dd, &one, rotation, &dd, in, &dd, &zero, out,
         &dd);
}

void NormalizeRows(float* data, size_t n, uint32_t d) {
  for (size_t i = 0; i < n; ++i) {
    float* row = data + i * d;
    vector::L2Space<float, float, float>::Normalize(
      reinterpret_cast<const byte_type*>(row), static_cast<uint16_t>(d), row);
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
       SuperKMeansGate(d, k, kSuperKMeansSphericalMinK));
    if (use_skm) {
      auto centroids =
        RunSuperKMeans(data, n, k, d, seed, niter, nredo, rotation);
      NormalizeRows(centroids.data(), centroids.size() / d, d);
      return centroids;
    }
    return RunLloyd(data, n, k, d, seed, niter, nredo, /*spherical=*/true);
  }

  ClusteringAlgo eff = algo;
  if (eff == ClusteringAlgo::Auto) {
    eff = SuperKMeansGate(d, k, kSuperKMeansMinK)
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
  const auto dd = static_cast<uint16_t>(d);
  uint32_t best = 0;
  float best_score = -std::numeric_limits<float>::max();
  for (uint32_t s = 0; s < k; ++s) {
    const float score =
      ComputeDistance<Metric>(v, centroids + static_cast<size_t>(s) * d, dd);
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
  std::exclusive_scan(cursor.begin(), cursor.end(), cursor.begin(), size_t{0});

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

}  // namespace irs
