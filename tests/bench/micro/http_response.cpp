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

#include <string>
#include <string_view>

#include "benchmark/benchmark.h"
#include "network/http/compression.h"
#include "network/http/response_writer.h"

namespace {

using sdb::network::http::HttpResponseWriter;
using sdb::network::http::HttpStatus;

constexpr size_t kPiece = 16 * 1024;

std::string JsonDocs(size_t bytes) {
  std::string out;
  for (size_t i = 0; out.size() < bytes; ++i) {
    absl::StrAppend(&out, R"({"@timestamp":"2026-09-30T12:)", i % 60,
                    R"(:00Z","clientip":"10.0.)", i % 256, ".", (i * 7) % 256,
                    R"(","request":"GET /images/)", (i * 7919) % 9973,
                    R"(.gif HTTP/1.1","status":)", i % 3 ? 200 : 404,
                    R"(,"size":)", (i * 104729) % 65536, "}\n");
  }
  out.resize(bytes);
  return out;
}

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

void BM_StreamedResponse(benchmark::State& state, std::string_view token) {
  const auto* coding = sdb::network::http::FindContentCoding(token);
  const auto size = static_cast<size_t>(state.range(0));
  const auto body = JsonDocs(size);
  const std::string_view view{body};
  sdb::message::Buffer send{1024, 64 * 1024};
  NullSink sink;
  for (auto _ : state) {
    HttpResponseWriter writer{send, sink, true, false};
    writer.SetContentCoding(*coding);
    writer.WriteHeadChunked(HttpStatus::Ok, "application/json");
    for (size_t off = 0; off < view.size(); off += kPiece) {
      writer.Write(view.substr(off, kPiece));
    }
    writer.Finish();
    send.Clear();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * size));
}
BENCHMARK_CAPTURE(BM_StreamedResponse, gzip, "gzip")
  ->Arg(64 << 10)
  ->Arg(1 << 20);
BENCHMARK_CAPTURE(BM_StreamedResponse, deflate, "deflate")
  ->Arg(64 << 10)
  ->Arg(1 << 20);
BENCHMARK_CAPTURE(BM_StreamedResponse, zstd, "zstd")
  ->Arg(64 << 10)
  ->Arg(1 << 20);
BENCHMARK_CAPTURE(BM_StreamedResponse, br, "br")->Arg(64 << 10)->Arg(1 << 20);
BENCHMARK_CAPTURE(BM_StreamedResponse, lz4, "lz4")->Arg(64 << 10)->Arg(1 << 20);
BENCHMARK_CAPTURE(BM_StreamedResponse, zxc, "zxc")->Arg(64 << 10)->Arg(1 << 20);
BENCHMARK_CAPTURE(BM_StreamedResponse, snappy, "snappy")
  ->Arg(64 << 10)
  ->Arg(1 << 20);

}  // namespace
