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

#include <absl/container/flat_hash_set.h>
#include <absl/container/linked_hash_map.h>
#include <benchmark/benchmark.h>

#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/insertion_order_preserving_map.hpp"

namespace {

using CIHash = duckdb::CaseInsensitiveStringHashFunction;
using CIEq = duckdb::CaseInsensitiveStringEquality;
using Value = std::string;

struct InsertionOrder {
  using Map = duckdb::InsertionOrderPreservingMap<Value>;

  static void Put(Map& map, std::string_view key, std::string_view value) {
    map.try_emplace(key, value);
  }
};

struct Linked {
  using Map = absl::linked_hash_map<std::string, Value, CIHash, CIEq>;

  static void Put(Map& map, std::string_view key, std::string_view value) {
    map.try_emplace(key, value);
  }
};

struct Data {
  std::vector<std::string> keys;
  std::vector<std::string> probes;
  std::vector<std::string> misses;
  std::vector<std::string> values;
};

std::string RandomKey(std::mt19937& rng) {
  static constexpr std::string_view kChars =
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";
  std::uniform_int_distribution<size_t> length{4, 24};
  std::uniform_int_distribution<size_t> pick{0, kChars.size() - 1};
  std::string key(length(rng), ' ');
  for (auto& c : key) {
    c = kChars[pick(rng)];
  }
  return key;
}

std::string FlipCase(std::string key) {
  for (auto& c : key) {
    if (c >= 'a' && c <= 'z') {
      c = static_cast<char>(c - 'a' + 'A');
    } else if (c >= 'A' && c <= 'Z') {
      c = static_cast<char>(c - 'A' + 'a');
    }
  }
  return key;
}

Data MakeData(size_t size) {
  std::mt19937 rng{42};
  Data data;
  absl::flat_hash_set<std::string, CIHash, CIEq> seen;
  while (data.keys.size() < size) {
    auto key = RandomKey(rng);
    if (seen.insert(key).second) {
      data.probes.push_back(FlipCase(key));
      data.values.push_back("value_" + std::to_string(data.keys.size()));
      data.keys.push_back(std::move(key));
    }
  }
  while (data.misses.size() < size) {
    auto key = RandomKey(rng);
    if (seen.insert(key).second) {
      data.misses.push_back(std::move(key));
    }
  }
  return data;
}

template<typename Ops>
typename Ops::Map Build(const Data& data) {
  typename Ops::Map map;
  for (size_t i = 0; i < data.keys.size(); ++i) {
    Ops::Put(map, data.keys[i], data.values[i]);
  }
  return map;
}

template<typename Ops>
void BmBuild(benchmark::State& state) {
  const auto data = MakeData(state.range(0));
  for (auto _ : state) {
    typename Ops::Map map;
    for (size_t i = 0; i < data.keys.size(); ++i) {
      Ops::Put(map, data.keys[i], data.values[i]);
    }
    benchmark::DoNotOptimize(map);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

template<typename Ops>
void BmFindHit(benchmark::State& state) {
  const auto data = MakeData(state.range(0));
  const auto map = Build<Ops>(data);
  for (auto _ : state) {
    for (const auto& probe : data.probes) {
      auto it = map.find(std::string_view{probe});
      if (it == map.end()) {
        state.SkipWithError("missing key");
        return;
      }
      benchmark::DoNotOptimize(it);
    }
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

template<typename Ops>
void BmFindMiss(benchmark::State& state) {
  const auto data = MakeData(state.range(0));
  const auto map = Build<Ops>(data);
  for (auto _ : state) {
    for (const auto& miss : data.misses) {
      auto it = map.find(std::string_view{miss});
      if (it != map.end()) {
        state.SkipWithError("unexpected key");
        return;
      }
      benchmark::DoNotOptimize(it);
    }
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

template<typename Ops>
void BmIterate(benchmark::State& state) {
  const auto data = MakeData(state.range(0));
  const auto map = Build<Ops>(data);
  for (auto _ : state) {
    size_t total = 0;
    for (const auto& [key, value] : map) {
      total += key.size() + value.size();
    }
    benchmark::DoNotOptimize(total);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

template<typename Ops>
void BmCopy(benchmark::State& state) {
  const auto data = MakeData(state.range(0));
  const auto map = Build<Ops>(data);
  for (auto _ : state) {
    typename Ops::Map copy{map};
    benchmark::DoNotOptimize(copy);
  }
  state.SetItemsProcessed(state.iterations() * state.range(0));
}

#define SDB_IOPM_BENCH(NAME)               \
  BENCHMARK_TEMPLATE(NAME, InsertionOrder) \
    ->RangeMultiplier(4)                   \
    ->Range(1, 1024);                      \
  BENCHMARK_TEMPLATE(NAME, Linked)->RangeMultiplier(4)->Range(1, 1024)

SDB_IOPM_BENCH(BmBuild);
SDB_IOPM_BENCH(BmFindHit);
SDB_IOPM_BENCH(BmFindMiss);
SDB_IOPM_BENCH(BmIterate);
SDB_IOPM_BENCH(BmCopy);

}  // namespace
