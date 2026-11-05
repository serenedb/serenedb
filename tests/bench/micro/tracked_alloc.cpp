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

struct Charged : irs::Tracked {
  uint64_t words[8];
};

template<typename T>
void NewDelete(benchmark::State& state) {
  for (auto _ : state) {
    auto object = std::make_unique<T>();
    benchmark::DoNotOptimize(object.get());
  }
}

template<typename Vector>
void Growth(benchmark::State& state) {
  const auto n = static_cast<uint32_t>(state.range(0));
  for (auto _ : state) {
    Vector values;
    for (uint32_t i = 0; i < n; ++i) {
      values.push_back(i);
    }
    benchmark::DoNotOptimize(values.data());
  }
}

BENCHMARK(NewDelete<Plain>)->ThreadRange(1, 32);
BENCHMARK(NewDelete<Charged>)->ThreadRange(1, 32);
BENCHMARK(Growth<std::vector<uint32_t>>)->Arg(16)->Arg(1024)->Arg(1 << 20);
BENCHMARK(Growth<irs::TrackedVector<uint32_t>>)
  ->Arg(16)
  ->Arg(1024)
  ->Arg(1 << 20);

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
