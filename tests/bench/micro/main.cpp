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

#include <absl/container/btree_map.h>
#include <absl/strings/str_format.h>

#include <cstdio>
#include <functional>
#include <string>
#include <vector>

#include "bench_module.h"

namespace sdb::bench {
namespace {

struct Module {
  std::vector<std::unique_ptr<::benchmark::Benchmark>> benchmarks;
  std::function<int(int, char**)> main;
};

using Modules = absl::btree_map<std::string, Module, std::less<>>;

Modules& GetModules() {
  static Modules modules;
  return modules;
}

Module& GetModule(std::string_view name) { return GetModules()[name]; }

int RunBenchmarks(int argc, char** argv) {
  ::benchmark::Initialize(&argc, argv);
  if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  ::benchmark::RunSpecifiedBenchmarks();
  ::benchmark::Shutdown();
  return 0;
}

int Run(Module& module, int argc, char** argv) {
  for (auto& benchmark : module.benchmarks) {
    (::benchmark::internal::RegisterBenchmarkInternal)(std::move(benchmark));
  }
  module.benchmarks.clear();
  return module.main ? module.main(argc, argv) : RunBenchmarks(argc, argv);
}

}  // namespace

bool AddMain(std::string_view module, int (*main)(int argc, char** argv)) {
  GetModule(module).main = main;
  return true;
}

bool AddMain(std::string_view module,
             int (*main)(int argc, const char* argv[])) {
  GetModule(module).main = [main](int argc, char** argv) {
    return main(argc, const_cast<const char**>(argv));
  };
  return true;
}

bool AddMain(std::string_view module, int (*main)()) {
  GetModule(module).main = [main](int, char**) { return main(); };
  return true;
}

}  // namespace sdb::bench
namespace benchmark::internal {

::benchmark::Benchmark* DeferBenchmark(
  std::string_view module, std::unique_ptr<::benchmark::Benchmark> benchmark) {
  auto* registered = benchmark.get();
  sdb::bench::GetModule(module).benchmarks.push_back(std::move(benchmark));
  return registered;
}

}  // namespace benchmark::internal

int main(int argc, char** argv) {
  ::benchmark::MaybeReenterWithoutASLR(argc, argv);
  auto& modules = sdb::bench::GetModules();
  std::string_view self{argv[0]};
  self.remove_prefix(self.rfind('/') + 1);
  if (auto it = modules.find(self); it != modules.end()) {
    return sdb::bench::Run(it->second, argc, argv);
  }
  if (argc > 1) {
    if (auto it = modules.find(std::string_view{argv[1]});
        it != modules.end()) {
      argv[1] = argv[0];
      return sdb::bench::Run(it->second, argc - 1, argv + 1);
    }
  }
  absl::FPrintF(stderr, "usage: %s <bench> [args...]\nbenches:", self);
  for (const auto& [name, module] : modules) {
    absl::FPrintF(stderr, " %s", name);
  }
  std::fputc('\n', stderr);
  return 1;
}
