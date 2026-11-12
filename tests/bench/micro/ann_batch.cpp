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

#include <benchmark/benchmark.h>
#include <faiss/utils/distances.h>

#include <core_functions/array_kernels.hpp>
#include <cstdint>
#include <iresearch/formats/ivf/quantizer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/bytes_output.hpp>
#include <iresearch/utils/resource_manager.hpp>
#include <limits>
#include <memory>
#include <random>
#include <span>
#include <vector>

extern "C" int sgemm_(const char* transa, const char* transb, const int* m,
                      const int* n, const int* k, const float* alpha,
                      const float* a, const int* lda, const float* b,
                      const int* ldb, const float* beta, float* c,
                      const int* ldc);

namespace {

constexpr size_t kTrain = 4096;
constexpr size_t kRows = 256;
constexpr uint32_t kSeed = 7;

std::vector<float> Gaussian(size_t n, uint32_t d, uint32_t seed) {
  std::mt19937 rng{seed};
  std::normal_distribution<float> nd{0.f, 1.f};
  std::vector<float> out(n * d);
  for (auto& x : out) {
    x = nd(rng);
  }
  return out;
}

struct Cluster {
  std::vector<std::unique_ptr<irs::QuantizerReader>> readers;
  std::vector<irs::QuantizerReader*> peers;
  irs::bstring block;
};

Cluster MakeCluster(const irs::AnnInfo::Quantizer& quant, uint32_t d,
                    size_t queries) {
  const auto metric = irs::VectorMetric::L2Sqr;
  auto writer = irs::MakeQuantizerWriter(quant, d, metric);
  const auto train = Gaussian(kTrain, d, kSeed);
  writer->Train(train.data(), kTrain);
  const std::vector<float> centroid(d, 0.f);
  writer->SetClusterCentroid(centroid.data());
  irs::MemoryFile file{irs::IResourceManager::gNoop};
  {
    irs::MemoryIndexOutput out{file};
    const auto rows = Gaussian(kRows, d, kSeed + 1);
    writer->Encode(out, rows.data(), kRows);
    writer->Finish(out);
    out.Flush();
  }
  Cluster c;
  c.block.resize(file.Length());
  irs::MemoryIndexInput in{file};
  in.ReadData(0, c.block.data(), c.block.size());
  irs::bstring framed;
  irs::BytesOutput stats_out{framed};
  writer->Serialize(stats_out);
  const auto stats = irs::MakeQuantizerStats(
    quant.kind, d,
    std::span<const irs::byte_type>{framed}.subspan(sizeof(uint64_t)), metric);
  const auto q = Gaussian(queries, d, kSeed + 2);
  for (size_t i = 0; i < queries; ++i) {
    auto& reader = c.readers.emplace_back(irs::MakeQuantizerReader(
      stats->MakeCodebook(std::span<const float>{q}.subspan(i * d, d))));
    reader->StartCluster(centroid.data());
    c.peers.push_back(reader.get());
  }
  return c;
}

void ScoreBlocks(benchmark::State& state, const irs::AnnInfo::Quantizer& quant,
                 bool joint) {
  const auto d = static_cast<uint32_t>(state.range(0));
  const auto queries = static_cast<size_t>(state.range(1));
  auto c = MakeCluster(quant, d, queries);
  const auto setting = c.readers.front()->BlockSetting();
  const std::span<const irs::byte_type> block{c.block};
  const size_t records = block.size() / setting.record_size;
  std::vector<irs::score_t> floors(queries,
                                   std::numeric_limits<irs::score_t>::lowest());
  std::vector<irs::score_t> out(queries * records);
  if (joint) {
    c.peers.front()->StartBlocks(c.peers);
  }
  for (auto _ : state) {
    if (joint) {
      c.peers.front()->ComputeBlocks(c.peers, block, floors, out.data());
    } else {
      for (size_t q = 0; q < queries; ++q) {
        c.peers[q]->ComputeBlock(block, floors[q], out.data() + q * records);
      }
    }
    benchmark::DoNotOptimize(out.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(queries * records));
}

using enum irs::VectorQuantization;

void BM_Sq8Single(benchmark::State& state) {
  ScoreBlocks(state, {.kind = SQ8}, false);
}
void BM_Sq8Joint(benchmark::State& state) {
  ScoreBlocks(state, {.kind = SQ8}, true);
}
void BM_PqSingle(benchmark::State& state) {
  ScoreBlocks(state, {.kind = PQ, .pq_m = 32}, false);
}
void BM_PqJoint(benchmark::State& state) {
  ScoreBlocks(state, {.kind = PQ, .pq_m = 32}, true);
}
void BM_RaBitQSingle(benchmark::State& state) {
  ScoreBlocks(state, {.kind = RaBitQ, .nb_bits = 1}, false);
}
void BM_RaBitQJoint(benchmark::State& state) {
  ScoreBlocks(state, {.kind = RaBitQ, .nb_bits = 1}, true);
}

void BlockArgs(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {128, 768, 1536}) {
    for (const int64_t queries : {1, 4, 8, 16}) {
      b->Args({d, queries});
    }
  }
}

BENCHMARK(BM_Sq8Single)->Apply(BlockArgs);
BENCHMARK(BM_Sq8Joint)->Apply(BlockArgs);
BENCHMARK(BM_PqSingle)->Apply(BlockArgs);
BENCHMARK(BM_PqJoint)->Apply(BlockArgs);
BENCHMARK(BM_RaBitQSingle)->Apply(BlockArgs);
BENCHMARK(BM_RaBitQJoint)->Apply(BlockArgs);

void Route(benchmark::State& state, bool gemm) {
  const auto queries = static_cast<int>(state.range(0));
  const auto centroids = static_cast<int>(state.range(1));
  const auto d = static_cast<int>(state.range(2));
  const auto q = Gaussian(queries, d, kSeed);
  const auto c = Gaussian(centroids, d, kSeed + 1);
  std::vector<float> qn(queries);
  std::vector<float> cn(centroids);
  for (int i = 0; i < queries; ++i) {
    qn[i] =
      duckdb::InnerProductOp::Operation(q.data() + i * d, q.data() + i * d, d);
  }
  for (int i = 0; i < centroids; ++i) {
    cn[i] =
      duckdb::InnerProductOp::Operation(c.data() + i * d, c.data() + i * d, d);
  }
  std::vector<float> out(size_t(queries) * centroids);
  const float one = 1.f;
  const float zero = 0.f;
  for (auto _ : state) {
    if (gemm) {
      sgemm_("T", "N", &centroids, &queries, &d, &one, c.data(), &d, q.data(),
             &d, &zero, out.data(), &centroids);
      for (int i = 0; i < queries; ++i) {
        for (int j = 0; j < centroids; ++j) {
          auto& v = out[size_t(i) * centroids + j];
          v = -(qn[i] + cn[j] - 2.f * v);
        }
      }
    } else {
      for (int i = 0; i < queries; ++i) {
        for (int j = 0; j < centroids; ++j) {
          out[size_t(i) * centroids + j] =
            -duckdb::DistanceSquaredOp::Operation(q.data() + i * d,
                                                  c.data() + j * d, d);
        }
      }
    }
    benchmark::DoNotOptimize(out.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(queries) *
                          centroids);
}

void BM_RoutePerPair(benchmark::State& state) { Route(state, false); }
void BM_RouteGemm(benchmark::State& state) { Route(state, true); }

void RouteArgs(benchmark::internal::Benchmark* b) {
  for (const int64_t d : {128, 768, 1536}) {
    for (const int64_t queries : {1, 4, 16, 85}) {
      for (const int64_t centroids : {64, 1024}) {
        b->Args({queries, centroids, d});
      }
    }
  }
}

BENCHMARK(BM_RoutePerPair)->Apply(RouteArgs);
BENCHMARK(BM_RouteGemm)->Apply(RouteArgs);

void OneToMany(benchmark::State& state, bool faiss) {
  const auto d = static_cast<size_t>(state.range(0));
  const auto q = Gaussian(1, d, kSeed);
  const auto rows = Gaussian(kRows, d, kSeed + 1);
  std::vector<float> out(kRows);
  for (auto _ : state) {
    if (faiss) {
      faiss::fvec_L2sqr_ny(out.data(), q.data(), rows.data(), d, kRows);
    } else {
      for (size_t i = 0; i < kRows; ++i) {
        out[i] = duckdb::DistanceSquaredOp::Operation(q.data(),
                                                      rows.data() + i * d, d);
      }
    }
    benchmark::DoNotOptimize(out.data());
    benchmark::ClobberMemory();
  }
  state.SetItemsProcessed(state.iterations() * static_cast<int64_t>(kRows));
}

void BM_RescoreDuckDB(benchmark::State& state) { OneToMany(state, false); }
void BM_RescoreFaiss(benchmark::State& state) { OneToMany(state, true); }

BENCHMARK(BM_RescoreDuckDB)->Arg(128)->Arg(768)->Arg(1536);
BENCHMARK(BM_RescoreFaiss)->Arg(128)->Arg(768)->Arg(1536);

}  // namespace
