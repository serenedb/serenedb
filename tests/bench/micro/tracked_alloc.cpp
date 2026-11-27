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

#include <cstdint>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/tracked.hpp>
#include <memory>
#include <vector>

namespace {

struct Plain {
  uint64_t words[8];
};

void NewDelete(benchmark::State& state) {
  for (auto _ : state) {
    auto object = std::make_unique<Plain>();
    benchmark::DoNotOptimize(object.get());
  }
}

void NewDeleteTracked(benchmark::State& state) {
  irs::LocalBudget budget{irs::gRootBudget};
  const irs::TrackedAllocator<> alloc{budget};
  for (auto _ : state) {
    auto object = irs::MakeTrackedUnique<Plain>(alloc);
    benchmark::DoNotOptimize(object.get());
  }
}

template<typename Vector>
void Fill(Vector& values, uint32_t n) {
  for (uint32_t i = 0; i < n; ++i) {
    values.push_back(i);
  }
  benchmark::DoNotOptimize(values.data());
}

void Growth(benchmark::State& state) {
  const auto n = static_cast<uint32_t>(state.range(0));
  for (auto _ : state) {
    std::vector<uint32_t> values;
    Fill(values, n);
  }
}

void GrowthTracked(benchmark::State& state) {
  const auto n = static_cast<uint32_t>(state.range(0));
  irs::LocalBudget budget{irs::gRootBudget};
  for (auto _ : state) {
    irs::TrackedVector<uint32_t> values{irs::TrackedAllocator<>{budget}};
    Fill(values, n);
  }
}

BENCHMARK(NewDelete)->ThreadRange(1, 32);
BENCHMARK(NewDeleteTracked)->ThreadRange(1, 32);
BENCHMARK(Growth)->Arg(16)->Arg(1024)->Arg(1 << 20);
BENCHMARK(GrowthTracked)->Arg(16)->Arg(1024)->Arg(1 << 20);

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
