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
#include <iresearch/formats/column/column_reader.hpp>
#include <numeric>
#include <vector>

namespace {

struct PlainRows {
  const uint64_t* data;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return data[i]; }
};

struct ContiguousRows {
  using contiguous_range_tag = void;
  const uint64_t* data;
  size_t n;
  size_t size() const noexcept { return n; }
  uint64_t operator[](size_t i) const noexcept { return data[i]; }
};

template<typename Rows>
void Consume(benchmark::State& state) {
  std::vector<uint64_t> ids(static_cast<size_t>(state.range(0)));
  std::iota(ids.begin(), ids.end(), uint64_t{0});
  benchmark::DoNotOptimize(ids.data());
  for (auto _ : state) {
    const Rows rows{ids.data(), ids.size()};
    size_t i = 0;
    size_t total = 0;
    while (i < rows.size()) {
      const size_t run = irs::ConsecutiveRunLength(rows, i);
      total += run;
      i += run;
    }
    benchmark::DoNotOptimize(total);
  }
}

void BmRunLengthContiguous(benchmark::State& state) {
  Consume<ContiguousRows>(state);
}
BENCHMARK(BmRunLengthContiguous)->Range(64, 1 << 16);

void BmRunLengthGeneric(benchmark::State& state) { Consume<PlainRows>(state); }
BENCHMARK(BmRunLengthGeneric)->Range(64, 1 << 16);

}  // namespace

BENCHMARK_MAIN();
