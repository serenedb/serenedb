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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb.hpp>
#include <duckdb/common/vector/array_vector.hpp>
#include <iresearch/formats/ann_build_env.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <limits>
#include <memory>
#include <random>
#include <vector>
#include <yaclib/runtime/fair_thread_pool.hpp>

#include "formats/column/test_cs_helpers.hpp"
#include "search/ann_build_pool.h"

namespace {

constexpr irs::field_id kVec = 1;
constexpr size_t kRows = 100000;
constexpr uint32_t kClusters = 64;

std::atomic<uint32_t> gCap{1};

uint32_t CapAcquire(uint32_t want) noexcept {
  return std::clamp(want, 1U, gCap.load());
}

void CapRelease(uint32_t) noexcept {}

std::vector<float> MakeMixture(size_t rows, uint32_t d) {
  std::mt19937 rng{20261011};
  std::normal_distribution<float> noise{0.f, 1.f};
  std::uniform_real_distribution<float> center{-3.f, 3.f};
  std::vector<float> centers(size_t{kClusters} * d);
  for (auto& c : centers) {
    c = center(rng);
  }
  std::vector<float> out(rows * d);
  for (size_t i = 0; i < rows; ++i) {
    const float* c = centers.data() + (i % kClusters) * d;
    for (uint32_t j = 0; j < d; ++j) {
      out[i * d + j] = c[j] + noise(rng);
    }
  }
  return out;
}

irs::IndexWriterOptions Options(uint32_t d, irs::VectorQuantization quant,
                                const irs::AnnBuildEnv* env) {
  auto opts = irs::tests::DefaultWriterOptions();
  opts.ann_env = env;
  opts.segment_memory_max = std::numeric_limits<size_t>::max();
  opts.column_options = [d, quant](irs::field_id id) -> irs::ColumnOptions {
    irs::ColumnOptions col;
    if (id == kVec) {
      col.ann_info = irs::AnnInfo{
        .kind = irs::AnnKind::Hnsw,
        .centroids_id = kVec,
        .postings_id = kVec,
        .d = static_cast<int>(d),
        .metric = irs::VectorMetric::L2Sqr,
        .quant = {.kind = quant},
        .m = 16,
        .ef_construction = 100,
      };
    }
    return col;
  };
  return opts;
}

void BuildOnce(benchmark::State& state, const std::vector<float>& data,
               uint32_t d, irs::VectorQuantization quant,
               const irs::AnnBuildEnv* env) {
  const auto vtype = duckdb::LogicalType::ARRAY(duckdb::LogicalType::FLOAT, d);
  irs::MemoryDirectory dir;
  auto writer =
    irs::IndexWriter::Make(dir, irs::kOmCreate, Options(d, quant, env));
  {
    auto trx = writer->GetBatch();
    duckdb::Vector v{vtype, 1};
    for (size_t i = 0; i < kRows; ++i) {
      auto doc = trx.Insert();
      auto& cw = doc.GetColWriter()->OpenColumn(kVec, vtype);
      auto& child = duckdb::ArrayVector::GetChildMutable(v);
      std::copy_n(data.data() + i * d, d,
                  duckdb::FlatVector::GetDataMutable<float>(child));
      duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
      duckdb::FlatVector::ValidityMutable(child).SetAllValid(d);
      duckdb::FlatVector::SetSize(v, 1);
      cw.Append(static_cast<uint64_t>(doc.DocId()) - irs::doc_limits::min(), v,
                1);
    }
    trx.Commit();
  }
  state.ResumeTiming();
  writer->RefreshCommit();
  state.PauseTiming();
}

enum class Executor {
  Serial,
  Pool,
};

void BM_HnswBuild(benchmark::State& state) {
  const auto executor = static_cast<Executor>(state.range(0));
  const auto workers = static_cast<uint32_t>(state.range(1));
  const auto d = static_cast<uint32_t>(state.range(2));
  const auto quant = state.range(3) == 0 ? irs::VectorQuantization::None
                                         : irs::VectorQuantization::SQ8;
  const auto data = MakeMixture(kRows, d);
  gCap = workers;

  yaclib::IntrusivePtr<yaclib::FairThreadPool> pool;
  std::unique_ptr<sdb::search::AnnBuildPool> env;
  if (executor == Executor::Pool) {
    pool = yaclib::MakeFairThreadPool(workers - 1);
    env = std::make_unique<sdb::search::AnnBuildPool>(*pool, CapAcquire,
                                                      CapRelease);
  }

  for (auto _ : state) {
    state.PauseTiming();
    BuildOnce(state, data, d, quant, env.get());
    state.ResumeTiming();
  }
  state.counters["rows"] = static_cast<double>(kRows);
  if (pool) {
    pool->SoftStop();
    pool->Wait();
  }
}

void Args(benchmark::internal::Benchmark* b) {
  for (const int64_t quant : {0, 1}) {
    for (const int64_t d : {128}) {
      b->Args({static_cast<int64_t>(Executor::Serial), 1, d, quant});
      for (const int64_t workers : {2, 4, 8, 16}) {
        b->Args({static_cast<int64_t>(Executor::Pool), workers, d, quant});
      }
    }
  }
}

BENCHMARK(BM_HnswBuild)
  ->Apply(Args)
  ->ArgNames({"executor", "workers", "d", "sq8"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime()
  ->Iterations(3);

int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
