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
#include <vector>

#include "benchmark/benchmark.h"
#include "network/http/compression.h"
#include "server/utils/message_buffer.h"

namespace {

using sdb::network::http::ContentCoding;
using sdb::network::http::ContentDecoder;
using sdb::network::http::ContentEncoder;
using sdb::network::http::FindContentCoding;
using sdb::network::http::kNoLevel;

inline constexpr size_t kChunk = 64 << 10;

constexpr std::string_view kCodings[] = {"gzip", "deflate", "zstd",  "br",
                                         "lz4",  "zxc",     "snappy"};

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

std::string Encode(ContentCoding coding, std::string_view body) {
  sdb::message::Buffer buffer{kChunk, kChunk};
  sdb::message::Writer writer{buffer};
  ContentEncoder{coding, kNoLevel}.Encode(body, true, writer);
  writer.Commit(false);
  std::string out;
  for (const auto chunk : buffer.Written()) {
    out.append(static_cast<const char*>(chunk.data()), chunk.size());
  }
  return out;
}

template<bool Fixed>
void BM_Compress(benchmark::State& state, ContentCoding coding, size_t size) {
  const auto body = JsonDocs(size);
  sdb::message::Buffer buffer{kChunk, kChunk};
  size_t compressed = 0;
  for (auto _ : state) {
    sdb::message::Writer writer{buffer};
    if constexpr (Fixed) {
      ContentEncoder{coding, kNoLevel}.EncodeAll(body, writer);
    } else {
      ContentEncoder{coding, kNoLevel}.Encode(body, true, writer);
    }
    compressed = writer.Commit(false);
    benchmark::DoNotOptimize(compressed);
    buffer.Clear();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * size));
  state.counters["ratio"] = static_cast<double>(size) / compressed;
}

void BM_Decode(benchmark::State& state, ContentCoding coding, size_t size) {
  const auto body = JsonDocs(size);
  const auto compressed = Encode(coding, body);
  for (auto _ : state) {
    size_t produced = 0;
    ContentDecoder{coding}.Decode(compressed, true, [&](std::string_view part) {
      produced += part.size();
    });
    benchmark::DoNotOptimize(produced);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * size));
}

}  // namespace

static int Main(int argc, char** argv) {
  for (const size_t size :
       {size_t{2} << 10, size_t{64} << 10, size_t{4} << 20}) {
    for (const auto token : kCodings) {
      const auto coding = FindContentCoding(token);
      const auto suffix = absl::StrCat("/", token, "/", size >> 10, "KiB");
      const int threads = size <= (size_t{2} << 10) ? 8 : 1;
      benchmark::RegisterBenchmark(absl::StrCat("encode", suffix),
                                   BM_Compress<false>, coding, size)
        ->ThreadRange(1, threads)
        ->UseRealTime();
      benchmark::RegisterBenchmark(absl::StrCat("fixed", suffix),
                                   BM_Compress<true>, coding, size)
        ->ThreadRange(1, threads)
        ->UseRealTime();
      benchmark::RegisterBenchmark(absl::StrCat("decode", suffix), BM_Decode,
                                   coding, size)
        ->ThreadRange(1, threads)
        ->UseRealTime();
    }
  }
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
