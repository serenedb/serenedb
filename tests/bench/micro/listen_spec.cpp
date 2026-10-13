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

#include "network/listen_spec.h"

#include <string>
#include <vector>

#include "benchmark/benchmark.h"

namespace {

void Parse(benchmark::State& state, std::string url) {
  const std::vector<std::string> urls{std::move(url)};
  for (auto _ : state) {
    benchmark::DoNotOptimize(sdb::network::ParseListenSpecs(urls));
  }
}

BENCHMARK_CAPTURE(Parse, pg_unix,
                  "postgres:///tmp/sdbsock?port=5499&mode=0660");
BENCHMARK_CAPTURE(Parse, pg_tcp, "postgres://127.0.0.1:7890");
BENCHMARK_CAPTURE(Parse, http_tcp,
                  "http://127.0.0.1:9200?api=es&api=otel&db=telemetry");

}  // namespace
