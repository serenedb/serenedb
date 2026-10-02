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

#include <string_view>

#include "benchmark/benchmark.h"
#include "network/http/compression.h"
#include "network/http/response_writer.h"

namespace {

using sdb::network::http::HttpResponseWriter;
using sdb::network::http::HttpStatus;

class NullSink final : public sdb::network::http::ResponseSink {
 public:
  yaclib::Task<> Drain() override { co_return {}; }
  bool Broken() const noexcept override { return false; }
};

void BM_Negotiate(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(sdb::network::http::NegotiateContentCoding(
      "gzip, deflate, br;q=0.9, zstd"));
  }
}
BENCHMARK(BM_Negotiate);

void BM_ParseContentEncoding(benchmark::State& state) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(sdb::network::http::ParseContentEncoding("gzip"));
  }
}
BENCHMARK(BM_ParseContentEncoding);

void BM_SmallJsonResponse(benchmark::State& state) {
  sdb::message::Buffer send{16 * 1024, 1 << 20};
  NullSink sink;
  for (auto _ : state) {
    HttpResponseWriter writer{send, sink, true, false};
    writer.SetExtraHeaders("X-Elastic-Product: Elasticsearch\r\n");
    writer.Json(HttpStatus::Ok, R"({"acknowledged":true})");
    send.Clear();
  }
}
BENCHMARK(BM_SmallJsonResponse);

}  // namespace

BENCHMARK_MAIN();
