////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2022 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <absl/container/flat_hash_map.h>
#include <absl/container/node_hash_map.h>
#include <benchmark/benchmark.h>

namespace {

template<typename T>
void Fill(T& map, size_t size, int seed) {
  ::srand(seed);
  while (size) {
    const auto kk = rand();
    map.emplace(kk, kk);
    --size;
  }
}

void BmHashStd(benchmark::State& state) {
  static constexpr int kSeed = 41;

  std::unordered_map<uint32_t, uint64_t> map;
  Fill(map, state.range(0), kSeed);

  for (auto _ : state) {
    for (int i = 0; i < 100; ++i) {
      srand(kSeed);
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto it = map.find(rand());
        if (it == map.end()) {
          std::abort();
        }
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

void BmHashAbslFlat(benchmark::State& state) {
  static constexpr int kSeed = 41;

  absl::flat_hash_map<uint32_t, uint64_t> map;
  Fill(map, state.range(0), kSeed);

  for (auto _ : state) {
    for (int i = 0; i < 100; ++i) {
      srand(kSeed);
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto it = map.find(rand());
        if (it == map.end()) {
          std::abort();
        }
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

void BmHashAbslNode(benchmark::State& state) {
  static constexpr int kSeed = 41;

  absl::node_hash_map<uint32_t, uint64_t> map;
  Fill(map, state.range(0), kSeed);

  for (auto _ : state) {
    for (int i = 0; i < 100; ++i) {
      srand(kSeed);
      for (int64_t i = 0; i < state.range(0); ++i) {
        auto it = map.find(rand());
        if (it == map.end()) {
          std::abort();
        }
        benchmark::DoNotOptimize(it);
      }
    }
  }
}

BENCHMARK(BmHashAbslFlat)->RangeMultiplier(2)->Range(0, 100);
BENCHMARK(BmHashAbslNode)->RangeMultiplier(2)->Range(0, 100);
BENCHMARK(BmHashStd)->RangeMultiplier(2)->Range(0, 100);

}  // namespace

BENCHMARK_MAIN();
