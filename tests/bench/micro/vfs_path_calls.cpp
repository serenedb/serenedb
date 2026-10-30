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

#include <duckdb/common/virtual_file_system.hpp>
#include <string>

namespace {

duckdb::VirtualFileSystem& Vfs() {
  static duckdb::VirtualFileSystem vfs;
  return vfs;
}

void BmIsDisabledForPath(benchmark::State& state) {
  auto& vfs = Vfs();
  const std::string path = "/tmp/vfs_path_calls";
  for (auto _ : state) {
    benchmark::DoNotOptimize(vfs.IsDisabledForPath(path));
  }
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations()));
}

BENCHMARK(BmIsDisabledForPath)->ThreadRange(1, 32)->UseRealTime();

}  // namespace
