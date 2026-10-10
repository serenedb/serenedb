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

#pragma once

#include <benchmark/benchmark.h>

#include <memory>
#include <string_view>

namespace sdb::bench {

bool AddMain(std::string_view module, int (*main)(int argc, char** argv));
bool AddMain(std::string_view module,
             int (*main)(int argc, const char* argv[]));
bool AddMain(std::string_view module, int (*main)());

}  // namespace sdb::bench
namespace benchmark::internal {

::benchmark::Benchmark* DeferBenchmark(
  std::string_view module, std::unique_ptr<::benchmark::Benchmark> benchmark);

}  // namespace benchmark::internal

#define RegisterBenchmarkInternal(...) \
  DeferBenchmark(SDB_BENCH_MODULE, __VA_ARGS__)
