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

#include "server/utils/byte_pool.h"

#include <cstddef>

#include "benchmark/benchmark.h"

namespace {

using BytePool = sdb::utils::BytePool<struct BenchTag>;

void BM_AllocateFree(benchmark::State& state) {
  const auto size = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    auto* bytes = BytePool::Allocate(size);
    benchmark::DoNotOptimize(bytes);
    BytePool::Free(bytes);
  }
}
BENCHMARK(BM_AllocateFree)
  ->Arg(16 << 10)
  ->Arg(1 << 20)
  ->ThreadRange(1, 16)
  ->UseRealTime();

}  // namespace
