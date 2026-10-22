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

#include <cstring>
#include <duckdb/main/database.hpp>
#include <duckdb/storage/block_cache/block_cache.hpp>
#include <iresearch/utils/duckdb_engine.hpp>

namespace {

constexpr duckdb::idx_t kBlocks = 64;
constexpr duckdb::idx_t kRangeBlocks = 4;

duckdb::shared_ptr<duckdb::BlockCacheFile> gFile;

duckdb::BlockCache& Cache() {
  return duckdb::BlockCache::Get(irs::DuckDBEngine::Instance().instance());
}

void Fill() {
  auto& cache = Cache();
  gFile = cache.CreateUnbackedFile("memory://bench/block_cache_pins");
  for (duckdb::idx_t block = 0; block != kBlocks; ++block) {
    auto pin = cache.CreateBlock(*gFile, block);
    std::memset(pin.GetDataMutable(), static_cast<int>(block),
                duckdb::BlockCache::BLOCK_SIZE);
  }
  cache.SetUnbackedSize(*gFile, kBlocks * duckdb::BlockCache::BLOCK_SIZE);
}

void BmPinSameBlock(benchmark::State& state) {
  auto& cache = Cache();
  for (auto _ : state) {
    auto pin = cache.Pin(gFile, 0);
    benchmark::DoNotOptimize(pin.Ptr());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BmPinOwnBlock(benchmark::State& state) {
  auto& cache = Cache();
  const auto block = static_cast<duckdb::idx_t>(state.thread_index()) % kBlocks;
  for (auto _ : state) {
    auto pin = cache.Pin(gFile, block);
    benchmark::DoNotOptimize(pin.Ptr());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

void BmPinRange(benchmark::State& state) {
  auto& cache = Cache();
  const auto first = static_cast<duckdb::idx_t>(state.thread_index()) *
                     kRangeBlocks % (kBlocks - kRangeBlocks + 1);
  for (auto _ : state) {
    auto pins = cache.PinRange(gFile, first * duckdb::BlockCache::BLOCK_SIZE,
                               kRangeBlocks * duckdb::BlockCache::BLOCK_SIZE);
    benchmark::DoNotOptimize(pins.data());
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

BENCHMARK(BmPinSameBlock)->ThreadRange(1, 64)->UseRealTime();
BENCHMARK(BmPinOwnBlock)->ThreadRange(1, 64)->UseRealTime();
BENCHMARK(BmPinRange)->ThreadRange(1, 64)->UseRealTime();

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  Fill();
  benchmark::Initialize(&argc, argv);
  const bool unrecognized = benchmark::ReportUnrecognizedArguments(argc, argv);
  if (!unrecognized) {
    benchmark::RunSpecifiedBenchmarks();
  }
  benchmark::Shutdown();
  gFile.reset();
  irs::DuckDBEngine::Instance().Shutdown();
  return unrecognized ? 1 : 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
