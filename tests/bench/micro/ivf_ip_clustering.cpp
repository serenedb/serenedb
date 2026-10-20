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

#include <absl/algorithm/container.h>
#include <absl/strings/str_format.h>
#include <faiss/Clustering.h>
#include <faiss/IndexFlat.h>
#include <faiss/utils/distances.h>

#include <cmath>
#include <cstdint>
#include <iresearch/formats/ivf/clustering.hpp>
#include <random>
#include <span>
#include <string_view>
#include <vector>

namespace irs {
namespace {

constexpr size_t kTopK = 10;

struct Data {
  std::string_view name;
  uint32_t d = 0;
  size_t n = 0;
  size_t nq = 0;
  std::vector<float> base;
  std::vector<float> queries;
};

void Fill(std::vector<float>& out, size_t n, uint32_t d, uint32_t latent,
          std::span<const float> centers, std::span<const float> basis,
          float spread, float norm_sigma, bool normalize, std::mt19937& rng) {
  const size_t blobs = centers.size() / latent;
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> z(latent);
  out.assign(n * d, 0.f);
  for (size_t i = 0; i < n; ++i) {
    const float* c = centers.data() + rng() % blobs * latent;
    for (uint32_t j = 0; j < latent; ++j) {
      z[j] = c[j] + spread * nd(rng);
    }
    float* row = out.data() + i * d;
    for (uint32_t j = 0; j < latent; ++j) {
      const float* b = basis.data() + size_t{j} * d;
      for (uint32_t t = 0; t < d; ++t) {
        row[t] += z[j] * b[t];
      }
    }
    for (uint32_t t = 0; t < d; ++t) {
      row[t] += 0.05f * nd(rng);
    }
    if (normalize) {
      NormalizeRows(row, 1, d);
    }
    if (norm_sigma > 0) {
      const float s = std::exp(norm_sigma * nd(rng));
      for (uint32_t t = 0; t < d; ++t) {
        row[t] *= s;
      }
    }
  }
}

Data Make(std::string_view name, uint32_t d, size_t n, size_t nq,
          uint32_t latent, size_t blobs, float spread, float norm_sigma,
          bool normalize) {
  std::mt19937 rng{7};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> centers(blobs * latent);
  for (auto& v : centers) {
    v = nd(rng);
  }
  const auto basis = MakeRotation(d, 11);
  Data data{.name = name, .d = d, .n = n, .nq = nq};
  Fill(data.base, n, d, latent, centers, basis, spread, norm_sigma, normalize,
       rng);
  Fill(data.queries, nq, d, latent, centers, basis, spread, 0.f, true, rng);
  return data;
}

std::vector<float> Train(const Data& data, size_t k, bool spherical) {
  faiss::ClusteringParameters cp;
  cp.niter = 15;
  cp.seed = 42;
  cp.spherical = spherical;
  cp.max_points_per_centroid = static_cast<int>(data.n);
  faiss::Clustering clus{static_cast<int>(data.d), static_cast<int>(k), cp};
  if (spherical) {
    faiss::IndexFlatIP index{static_cast<int>(data.d)};
    clus.train(static_cast<faiss::idx_t>(data.n), data.base.data(), index);
  } else {
    faiss::IndexFlatL2 index{static_cast<int>(data.d)};
    clus.train(static_cast<faiss::idx_t>(data.n), data.base.data(), index);
  }
  return std::move(clus.centroids);
}

std::vector<int64_t> Nearest(std::span<const float> x, size_t n,
                             std::span<const float> y, size_t m, uint32_t d,
                             size_t k) {
  std::vector<int64_t> ids(n * k);
  std::vector<float> dist(n * k);
  faiss::knn_inner_product(x.data(), y.data(), d, n, m, k, dist.data(),
                           ids.data());
  return ids;
}

void Report(const Data& data, size_t lists) {
  const auto truth =
    Nearest(data.queries, data.nq, data.base, data.n, data.d, kTopK);
  for (const bool spherical : {true, false}) {
    const auto centroids = Train(data, lists, spherical);
    const auto assign = Nearest(data.base, data.n, centroids, lists, data.d, 1);
    std::vector<size_t> sizes(lists);
    for (const auto a : assign) {
      ++sizes[a];
    }
    for (const size_t nprobe : {1, 2, 4, 8, 16, 32, 64}) {
      const auto probed =
        Nearest(data.queries, data.nq, centroids, lists, data.d, nprobe);
      size_t hits = 0;
      size_t scanned = 0;
      for (size_t q = 0; q < data.nq; ++q) {
        const auto lists_q = std::span{probed}.subspan(q * nprobe, nprobe);
        for (const auto l : lists_q) {
          scanned += sizes[l];
        }
        for (size_t t = 0; t < kTopK; ++t) {
          hits += absl::c_linear_search(lists_q, assign[truth[q * kTopK + t]]);
        }
      }
      absl::PrintF("%-12s %-9s nprobe %3u recall %.4f scanned %8.0f\n",
                   data.name, spherical ? "spherical" : "l2", nprobe,
                   static_cast<double>(hits) / (kTopK * data.nq),
                   static_cast<double>(scanned) / data.nq);
    }
  }
}

int Main(int /*argc*/, char** /*argv*/) {
  constexpr uint32_t d = 128;
  constexpr size_t n = 200000;
  constexpr size_t nq = 300;
  constexpr size_t lists = n / 256;
  Report(Make("unit", d, n, nq, 32, 300, 0.6f, 0.f, true), lists);
  Report(Make("mips-0.25", d, n, nq, 32, 300, 0.6f, 0.25f, true), lists);
  Report(Make("mips-0.5", d, n, nq, 32, 300, 0.6f, 0.5f, true), lists);
  Report(Make("raw", d, n, nq, 32, 300, 0.6f, 0.f, false), lists);
  return 0;
}

}  // namespace
}  // namespace irs

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &irs::Main);
