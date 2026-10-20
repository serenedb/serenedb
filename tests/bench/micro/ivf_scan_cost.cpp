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
#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iresearch/formats/ivf/centroids.hpp>
#include <iresearch/formats/ivf/quantizer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/resource_manager.hpp>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <string_view>
#include <vector>

namespace irs {
namespace {

constexpr size_t kRows = 262144;
constexpr size_t kPosting = 1024;
constexpr size_t kClusters = kRows / kPosting;
constexpr size_t kProbes = 32;
constexpr size_t kQueries = 32;
constexpr size_t kTopK = 10;
constexpr size_t kRouteSample = 8192;
constexpr size_t kRouteLeaf = 4;
constexpr size_t kPasses = 3;
constexpr uint32_t kTrainIters = 25;
constexpr double kOverscanSlope = 8;
constexpr double kModelRows = 250000;
constexpr VectorMetric kMetric = VectorMetric::L2Sqr;

struct Config {
  std::string_view name;
  AnnInfo::Quantizer quant;
  size_t posting = kPosting;
};

std::vector<Config> Configs(uint32_t d) {
  using enum VectorQuantization;
  return {
    {"none", {.kind = None}},
    {"none@128", {.kind = None}, 128},
    {"sq8@128", {.kind = SQ8}, 128},
    {"sq8", {.kind = SQ8}},
    {"sq4", {.kind = SQ4}},
    {"usq8", {.kind = USQ8}},
    {"usq4", {.kind = USQ4}},
    {"fp16", {.kind = FP16}},
    {"bf16", {.kind = BF16}},
    {"pq", {.kind = PQ, .pq_m = d / 2}},
    {"rabitq", {.kind = RaBitQ, .nb_bits = 1}},
    {"rabitq:4", {.kind = RaBitQ, .nb_bits = 4}},
    {"tq", {.kind = TQ, .nb_bits = 3}},
    {"tq:1.5", {.kind = TQ, .nb_bits = 1, .half_bit = true}},
    {"bq", {.kind = BQ, .nb_bits = 1, .query_bits = 1}},
    {"bq:q8", {.kind = BQ, .nb_bits = 1, .query_bits = 8}},
    {"bq:2", {.kind = BQ, .nb_bits = 2, .query_bits = 1}},
    {"osq", {.kind = OSQ, .nb_bits = 1, .query_bits = 4}},
    {"osq:2", {.kind = OSQ, .nb_bits = 2, .query_bits = 4}},
    {"osq:4", {.kind = OSQ, .nb_bits = 4, .query_bits = 4}},
    {"osq:7", {.kind = OSQ, .nb_bits = 7, .query_bits = 7}},
    {"osq:8", {.kind = OSQ, .nb_bits = 8, .query_bits = 8}},
  };
}

std::vector<float> MakeVectors(size_t rows, uint32_t d, uint32_t seed,
                               uint32_t latent) {
  constexpr size_t kBlobs = 1024;
  std::mt19937 rng{seed};
  std::uniform_real_distribution<float> unit{-1.f, 1.f};
  const uint32_t width = latent != 0 ? latent : d;
  std::vector<float> centers(kBlobs * width);
  for (auto& c : centers) {
    c = 4 * unit(rng);
  }
  std::vector<float> lift;
  if (latent != 0) {
    std::mt19937 lift_rng{1};
    lift.resize(size_t{latent} * d);
    for (auto& v : lift) {
      v = unit(lift_rng) / std::sqrt(static_cast<float>(latent));
    }
  }
  std::vector<float> z(width);
  std::vector<float> out(rows * d);
  for (size_t i = 0; i < rows; ++i) {
    const float* c = centers.data() + rng() % kBlobs * width;
    for (uint32_t j = 0; j < width; ++j) {
      z[j] = c[j] + unit(rng);
    }
    float* row = out.data() + i * d;
    if (latent == 0) {
      absl::c_copy(z, row);
      continue;
    }
    for (uint32_t t = 0; t < d; ++t) {
      row[t] = 0.05f * unit(rng);
    }
    for (uint32_t j = 0; j < latent; ++j) {
      const float* l = lift.data() + size_t{j} * d;
      for (uint32_t t = 0; t < d; ++t) {
        row[t] += z[j] * l[t];
      }
    }
  }
  return out;
}

std::vector<float> ChunkMeans(std::span<const float> data, uint32_t d) {
  std::vector<float> means(kClusters * d);
  for (size_t c = 0; c < kClusters; ++c) {
    float* m = means.data() + c * d;
    for (size_t i = 0; i < kPosting; ++i) {
      const float* v = data.data() + (c * kPosting + i) * d;
      for (uint32_t j = 0; j < d; ++j) {
        m[j] += v[j];
      }
    }
    for (uint32_t j = 0; j < d; ++j) {
      m[j] /= kPosting;
    }
  }
  return means;
}

template<typename Fn>
double NsPer(size_t items, Fn&& fn) {
  fn();
  const auto start = std::chrono::steady_clock::now();
  for (size_t pass = 0; pass < kPasses; ++pass) {
    fn();
  }
  const std::chrono::duration<double, std::nano> took =
    std::chrono::steady_clock::now() - start;
  return took.count() / static_cast<double>(kPasses * items);
}

struct Route {
  double plain = 0;
  double with_centroids = 0;
};

Route MeasureRoute(std::span<const float> data, uint32_t d) {
  std::vector<float> sample(data.begin(), data.begin() + kRouteSample * d);
  const auto queries = data.subspan(kRouteSample * d, kQueries * d);
  auto builder = CentroidsBuilder::CreateFromSample(
    std::move(sample), d, kMetric,
    {.posting_size = kRouteLeaf, .max_fanout = kRouteSample});
  MemoryFile file{IResourceManager::gNoop};
  CentroidsSpan span;
  {
    MemoryIndexOutput out{file};
    span = builder.Serialize(out);
    out.Flush();
  }
  MemoryIndexInput in{file};
  in.Seek(span.offset);
  const auto tree = CentroidsTree::Deserialize(in, span.byte_size);
  const size_t centroids = kQueries * tree.RootSize();
  std::vector<uint32_t> ids;
  ManagedVector<float> cents{{IResourceManager::gNoop}};
  const auto run = [&](bool want) {
    for (size_t q = 0; q < kQueries; ++q) {
      ids.clear();
      cents.clear();
      tree.Search(queries.subspan(q * d, d), in, kProbes, ids,
                  want ? &cents : nullptr);
      benchmark::DoNotOptimize(ids.data());
    }
  };
  return {.plain = NsPer(centroids, [&] { run(false); }),
          .with_centroids = NsPer(centroids, [&] { run(true); })};
}

struct Scan {
  uint32_t record_size = 0;
  uint32_t cost_bytes = 0;
  double ns = 0;
};

Scan MeasureScan(const Config& config, std::span<const float> data,
                 std::span<const float> means, std::span<const float> queries,
                 uint32_t d, size_t posting) {
  auto writer = MakeQuantizerWriter(config.quant, d, kMetric, kTrainIters);
  const bool needs_centroid = QuantizerNeedsCentroid(config.quant.kind);
  SDB_ASSERT(!needs_centroid || posting == kPosting);
  const size_t clusters = kRows / posting;
  const size_t probes_per_query = kProbes * kPosting / posting;
  if (const size_t train = writer->TrainSamples(kRows);
      train == QuantizerWriter::kTrainStreaming) {
    writer->Train(data.data(), kRows);
  } else if (train != 0) {
    writer->Train(data.data(), std::min(train, kRows));
  }
  MemoryFile file{IResourceManager::gNoop};
  {
    MemoryIndexOutput out{file};
    for (size_t c = 0; c < clusters; ++c) {
      if (needs_centroid) {
        writer->SetClusterCentroid(means.data() + c * d);
      }
      writer->Encode(out, data.data() + c * posting * d, posting);
      writer->EndList(out);
    }
    writer->Finish(out);
    out.Flush();
  }
  bstring codes;
  {
    BytesOutput out{codes};
    file >> out;
  }
  bstring framed;
  {
    BytesOutput out{framed};
    writer->Serialize(out);
  }
  const auto setting = writer->BlockSetting();
  const auto stats = MakeQuantizerStats(
    config.quant.kind, d,
    std::span<const byte_type>{framed}.subspan(sizeof(uint64_t)), kMetric,
    /*row_major=*/false, setting.group_size > 1 && !setting.padded);
  const size_t cluster_bytes = posting * setting.record_size;
  std::vector<std::shared_ptr<const QuantizerCodebook>> books;
  for (size_t q = 0; q < kQueries; ++q) {
    books.push_back(stats->MakeCodebook(queries.subspan(q * d, d)));
  }
  std::mt19937 rng{7};
  std::vector<uint32_t> probes(kQueries * probes_per_query);
  for (auto& p : probes) {
    p = static_cast<uint32_t>(rng() % clusters);
  }
  std::vector<score_t> scores(posting);
  std::vector<score_t> top;
  const auto run = [&] {
    for (size_t q = 0; q < kQueries; ++q) {
      top.clear();
      for (size_t p = 0; p < probes_per_query; ++p) {
        const size_t c = probes[q * probes_per_query + p];
        auto reader = MakeQuantizerReader(books[q]);
        reader->StartCluster(needs_centroid ? means.data() + c * d : nullptr);
        reader->ComputeBlock(std::span<const byte_type>{codes}.subspan(
                               c * cluster_bytes, cluster_bytes),
                             top.size() == kTopK
                               ? top.front()
                               : std::numeric_limits<score_t>::lowest(),
                             scores.data());
        for (const score_t s : scores) {
          if (top.size() < kTopK) {
            top.push_back(s);
            absl::c_push_heap(top, std::greater{});
          } else if (s > top.front()) {
            absl::c_pop_heap(top, std::greater{});
            top.back() = s;
            absl::c_push_heap(top, std::greater{});
          }
        }
      }
    }
  };
  return {.record_size = setting.record_size,
          .cost_bytes = writer->ScanCost(kRows).bytes,
          .ns = NsPer(kQueries * probes_per_query * posting, run)};
}

int Main(int argc, char** argv) {
  std::vector<uint32_t> dims;
  uint32_t latent = 0;
  for (int i = 1; i < argc; ++i) {
    const std::string_view arg = argv[i];
    uint32_t value = 0;
    if (arg.starts_with("latent=") &&
        absl::SimpleAtoi(arg.substr(7), &latent)) {
      continue;
    }
    if (!absl::SimpleAtoi(arg, &value) || value == 0) {
      absl::FPrintF(stderr, "usage: ivf_scan_cost [latent=N] [d...]\n");
      return 1;
    }
    dims.push_back(value);
  }
  if (dims.empty()) {
    dims = {128, 384, 768, 1536};
  }
  for (const uint32_t d : dims) {
    const auto data = MakeVectors(kRows, d, 42, latent);
    const auto queries = MakeVectors(kQueries, d, 43, latent);
    const auto means = ChunkMeans(data, d);
    const auto route = MeasureRoute(data, d);
    absl::PrintF(
      "d=%u rows=%u posting=%u probes=%u route %.2f ns/centroid, %.2f "
      "copying centroids\n",
      d, kRows, kPosting, kProbes, route.plain, route.with_centroids);
    absl::PrintF("%-10s %8s %10s %10s %10s %8s\n", "quant", "record", "scan_ns",
                 "measured_B", "model_B", "t@250k");
    for (const auto& config : Configs(d)) {
      const auto scan =
        MeasureScan(config, data, means, queries, d, config.posting);
      const double route_ns = QuantizerNeedsCentroid(config.quant.kind)
                                ? route.with_centroids
                                : route.plain;
      const double measured = 4.0 * d * scan.ns / route_ns;
      const double t =
        std::sqrt(kModelRows * route_ns / scan.ns / kOverscanSlope);
      absl::PrintF("%-10s %8u %10.3f %10.1f %10u %8.0f\n", config.name,
                   scan.record_size, scan.ns, measured, scan.cost_bytes, t);
    }
  }
  return 0;
}

}  // namespace
}  // namespace irs

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &irs::Main);
