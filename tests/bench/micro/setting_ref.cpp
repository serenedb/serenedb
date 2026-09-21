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

#include <duckdb.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <string>
#include <string_view>

#include "query/config.h"

namespace {

constexpr std::string_view kName = "sdb_levenshtein_max_terms";

duckdb::ClientContext& Context() {
  static auto* conn =
    new duckdb::Connection{irs::DuckDBEngine::Instance().instance()};
  return *conn->context;
}

void SettingRefCached(benchmark::State& state) {
  static constinit sdb::SettingRef gSetting{kName};
  auto& context = Context();
  for (auto _ : state) {
    benchmark::DoNotOptimize(gSetting.Int(context));
  }
}

void IndexLookupEveryRead(benchmark::State& state) {
  auto& context = Context();
  auto& config = duckdb::DBConfig::GetConfig(context);
  const duckdb::String name{kName.data(), static_cast<uint32_t>(kName.size())};
  for (auto _ : state) {
    duckdb::optional_ptr<const duckdb::ConfigurationOption> option;
    const auto index = config.TryGetSettingIndex(name, option);
    duckdb::Value value;
    if (!context.config.user_settings.TryGetSetting(config.user_settings,
                                                    index.GetIndex(), value)) {
      context.TryGetCurrentSetting(std::string{kName}, value);
    }
    benchmark::DoNotOptimize(value.GetValue<uint32_t>());
  }
}

void TryGetCurrentSetting(benchmark::State& state) {
  auto& context = Context();
  const std::string name{kName};
  for (auto _ : state) {
    duckdb::Value value;
    context.TryGetCurrentSetting(name, value);
    benchmark::DoNotOptimize(value.GetValue<uint32_t>());
  }
}

BENCHMARK(SettingRefCached);
BENCHMARK(IndexLookupEveryRead);
BENCHMARK(TryGetCurrentSetting);

}  // namespace

int main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize(
    &sdb::connector::RegisterConfigVariables);
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}
