/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <absl/flags/parse.h>
#include <folly/Benchmark.h>
#include <folly/lang/Keep.h>
#include <folly/synchronization/CallOnce.h>
#include <glog/logging.h>

#include <deque>
#include <mutex>
#include <thread>

DECLARE_int32(bm_min_iters);
DECLARE_int64(bm_max_iters);

DEFINE_int32(threads,
             std::max<int32_t>(std::thread::hardware_concurrency() / 2, 1),
             "benchmark concurrency");

template<typename OnceFlag>
void BmImpl(size_t iters) {
  OnceFlag flag;
  std::atomic_int out = 0;
  {
    std::vector<std::jthread> threads;
    threads.reserve(FLAGS_threads);
    for (size_t i = 0; i < FLAGS_threads; ++i) {
      threads.emplace_back([&] {
        for (size_t j = 0u; j < iters; ++j) {
          call_once(flag, [&] { out.fetch_add(1, std::memory_order_relaxed); });
        }
      });
    }
  }
  CHECK_EQ(out, 1);
}

BENCHMARK(AbseilCallOnceBench, iters) { BmImpl<absl::once_flag>(iters); }

BENCHMARK(FollyCallOnceBench, iters) { BmImpl<folly::old::once_flag>(iters); }

BENCHMARK(FollyCompactCallOnceBench, iters) {
  BmImpl<folly::old::compact_once_flag>(iters);
}

BENCHMARK(StdCallOnceBench, iters) { BmImpl<std::once_flag>(iters); }

template<typename OnceFlag>
void Print(std::string_view name) {
  std::cout << name << ": sizeof is " << sizeof(OnceFlag) << ", alignof is "
            << alignof(OnceFlag) << std::endl;
}

int main(int argc, char** argv) {
  Print<absl::once_flag>("absl");
  Print<folly::old::once_flag>("folly");
  Print<folly::old::compact_once_flag>("folly::compact");
  Print<std::once_flag>("std");
  absl::SetFlag<int32_t>(&FLAGS_bm_min_iters, 10'000'000);
  absl::SetFlag<int64_t>(&FLAGS_bm_max_iters, 10'000'001);
  absl::ParseCommandLine(argc, argv);
  folly::runBenchmarks();
  return 0;
}
