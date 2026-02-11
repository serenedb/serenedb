////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// @author Valerii Mironov
////////////////////////////////////////////////////////////////////////////////

#include <absl/crc/crc32c.h>
#include <benchmark/benchmark.h>

#include <basics/crc.hpp>
#include <string>

#ifdef BENCH_FOLLY
#include <folly/hash/Checksum.h>
#endif

#ifdef BENCH_ROCKSDB
#include <util/crc32c.h>
#endif

namespace {

std::string TestString(size_t len) {
  std::string result;
  result.reserve(len);
  for (size_t i = 0; i < len; ++i) {
    result.push_back(static_cast<char>(i % 256));
  }
  return result;
}

void BmCalculateAbseil(benchmark::State& state) {
  int len = state.range(0);
  std::string data = TestString(len);
  for (auto s : state) {
    benchmark::DoNotOptimize(data);
    auto crc = absl::ExtendCrc32c(absl::crc32c_t{2}, data, 0);
    benchmark::DoNotOptimize(crc);
  }
}

#ifdef BENCH_FOLLY
void BmCalculateFolly(benchmark::State& state) {
  int len = state.range(0);
  std::string data = TestString(len);
  for (auto s : state) {
    benchmark::DoNotOptimize(data);
    auto crc = folly::crc32c(reinterpret_cast<const uint8_t*>(data.data()),
                             data.size(), 2);
    benchmark::DoNotOptimize(crc);
  }
}
#endif

#ifdef BENCH_ROCKSDB
void BmCalculateRocksDB(benchmark::State& state) {
  int len = state.range(0);
  std::string data = TestString(len);
  for (auto s : state) {
    benchmark::DoNotOptimize(data);
    auto crc = rocksdb::crc32c::Extend(2, data.data(), data.size());
    benchmark::DoNotOptimize(crc);
  }
}
#endif

// clang-format off
#define ARGS \
   Arg(0)->Arg(1)->Arg(100)->Arg(2048)->Arg(10000)->Arg(500000)->Arg(100 * 1000 * 1000) /* absl bench */ \
 ->Arg(512)->Arg(1024)->Arg(2048)->Arg(4096)->Arg(8192)->Arg(16384)->Arg(32768)->Arg(65536)->Arg(131072)->Arg(262144)->Arg(524288) /* folly bench */ \
 ->Arg(7)->Arg(8)->Arg(9)                            /* folly first branch */ \
 ->Arg(63)->Arg(64)->Arg(65)                         /* abseil first branch */ \
 ->Arg(215)->Arg(216)->Arg(217)                      /* folly second branch */ \
 ->Arg(2047)->Arg(2048)->Arg(2049)                   /* abseil second branch */ \
 ->Arg(4095)->Arg(4096)->Arg(4097)                   /* folly third branch */

#define ARGS2 \
      Arg((1 << 0) - 1)->Arg(1 << 0)->Arg((1 << 0) + 1) \
    ->Arg((1 << 2) - 1)->Arg(1 << 2)->Arg((1 << 2) + 1) \
    ->Arg((1 << 3) - 1)->Arg(1 << 3)->Arg((1 << 3) + 1) \
    ->Arg((1 << 4) - 1)->Arg(1 << 4)->Arg((1 << 4) + 1) \
    ->Arg((1 << 5) - 1)->Arg(1 << 5)->Arg((1 << 5) + 1) \
    ->Arg((1 << 6) - 1)->Arg(1 << 6)->Arg((1 << 6) + 1) \
    ->Arg((1 << 7) - 1)->Arg(1 << 7)->Arg((1 << 7) + 1) \
    ->Arg((1 << 8) - 1)->Arg(1 << 8)->Arg((1 << 8) + 1) \
    ->Arg((1 << 9) - 1)->Arg(1 << 9)->Arg((1 << 9) + 1) \
    ->Arg((1 << 10) - 1)->Arg(1 << 10)->Arg((1 << 10) + 1) \
    ->Arg((1 << 11) - 1)->Arg(1 << 11)->Arg((1 << 11) + 1) \
    ->Arg((1 << 12) - 1)->Arg(1 << 12)->Arg((1 << 12) + 1) \
    ->Arg((1 << 13) - 1)->Arg(1 << 13)->Arg((1 << 13) + 1) \
    ->Arg((1 << 14) - 1)->Arg(1 << 14)->Arg((1 << 14) + 1)

// clang-format on

BENCHMARK(BmCalculateAbseil)->ARGS2;

#ifdef BENCH_FOLLY
BENCHMARK(BmCalculateFolly)->ARGS2;
#endif

#ifdef BENCH_ROCKSDB
BENCHMARK(BmCalculateRocksDB)->ARGS2;
#endif

}  // namespace

BENCHMARK_MAIN();
