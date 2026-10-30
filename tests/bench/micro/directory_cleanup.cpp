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

#include <absl/strings/str_cat.h>
#include <absl/synchronization/notification.h>
#include <absl/time/clock.h>
#include <absl/time/time.h>
#include <benchmark/benchmark.h>

#include <chrono>
#include <duckdb/common/local_file_system.hpp>
#include <filesystem>
#include <iresearch/store/directory.hpp>
#include <iresearch/store/index_files.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <thread>

namespace {

constexpr int kUnused = 16;
constexpr auto kRemoveLatency = std::chrono::milliseconds{30};

class SlowRemoveFileSystem final : public duckdb::LocalFileSystem {
 public:
  bool TryRemoveFile(const std::string& filename,
                     duckdb::optional_ptr<duckdb::FileOpener> opener) final {
    absl::SleepFor(absl::FromChrono(kRemoveLatency));
    return LocalFileSystem::TryRemoveFile(filename, opener);
  }
};

void BmCreateWhileCleaning(benchmark::State& state) {
  SlowRemoveFileSystem fs;
  const auto root =
    std::filesystem::temp_directory_path() / "sdb_bench_directory_cleanup";
  std::filesystem::remove_all(root);
  std::filesystem::create_directories(root);
  const irs::Directory dir{irs::DuckDBEngine::Instance().instance(), fs,
                           root.string()};
  irs::RefTracker tracker;
  const auto tracked = dir.Observed(tracker);
  uint64_t round = 0;
  for (auto _ : state) {
    for (int i = 0; i != kUnused; ++i) {
      const auto name = absl::StrCat("unused_", round, "_", i);
      dir.Create(name)->Close();
      dir.Refs().add(name);
    }
    absl::Notification started;
    absl::Notification cleaned;
    const std::jthread cleaner{[&] {
      started.Notify();
      irs::RemoveUnreferenced(dir);
      cleaned.Notify();
    }};
    started.WaitForNotification();
    absl::SleepFor(absl::FromChrono(kRemoveLatency / 3));
    const auto began = std::chrono::steady_clock::now();
    tracked.Create(absl::StrCat("new_", round))->Close();
    state.SetIterationTime(
      std::chrono::duration<double>(std::chrono::steady_clock::now() - began)
        .count());
    cleaned.WaitForNotification();
    tracker.Clear();
    ++round;
  }
  std::filesystem::remove_all(root);
}

BENCHMARK(BmCreateWhileCleaning)->UseManualTime()->Iterations(20);

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  const bool unrecognized = benchmark::ReportUnrecognizedArguments(argc, argv);
  if (!unrecognized) {
    benchmark::RunSpecifiedBenchmarks();
  }
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return unrecognized ? 1 : 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
