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

#include <array>
#include <string>
#include <string_view>

#include "benchmark/benchmark.h"
#include "network/http/router.h"
#include "network/http/routes.h"

namespace {

using sdb::network::HttpApi;
using sdb::network::HttpMethod;
using sdb::network::HttpRequest;
using sdb::network::HttpRouter;

HttpRouter& Router() {
  static HttpRouter router = [] {
    HttpRouter built;
    constexpr std::array kApis{HttpApi::Es, HttpApi::Mcp, HttpApi::Otel,
                               HttpApi::Test};
    sdb::network::http::AddRoutes(built, kApis);
    return built;
  }();
  return router;
}

void Match(benchmark::State& state, HttpMethod method,
           std::string_view target) {
  auto& router = Router();
  HttpRequest request;
  request.method = method;
  request.target = std::string{target};
  for (auto _ : state) {
    benchmark::DoNotOptimize(router.Match(request));
  }
}

BENCHMARK_CAPTURE(Match, literal_bulk, HttpMethod::Post, "/_bulk");
BENCHMARK_CAPTURE(Match, literal_otel_logs, HttpMethod::Post, "/v1/logs");
BENCHMARK_CAPTURE(Match, index_search, HttpMethod::Post, "/logs-2026/_search");
BENCHMARK_CAPTURE(Match, index_doc_with_query, HttpMethod::Put,
                  "/logs-2026/_doc/abc?refresh=true");
BENCHMARK_CAPTURE(Match, miss, HttpMethod::Get, "/no/such/route/here");

}  // namespace

BENCHMARK_MAIN();
