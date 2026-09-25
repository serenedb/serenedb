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

#include <lz4frame.h>
#include <zlib.h>
#include <zstd.h>
#include <zxc.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <iresearch/utils/serializer.hpp>
#include <map>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "benchmark/benchmark.h"
#include "network/http/compression.h"
#include "network/http/mcp/tools.h"
#include "server/utils/simdjson_sink.h"

// What the HTTP layer would send for MCP responses under each content-coding:
// tools/list, and read_doc for a median, the largest, and every docs page.
// Compression runs through the ContentCoding registry (the server path);
// decompression is the client side, straight from each library.

namespace {

using sdb::network::http::ContentCoding;
using sdb::network::http::FindContentCoding;

constexpr std::string_view kCodings[] = {"gzip", "zstd", "lz4", "zxc"};

struct TextContent {
  std::string type{"text"};
  std::string text;
};

struct ToolCallResult {
  std::vector<TextContent> content;
  bool isError = false;  // NOLINT(readability-identifier-naming)
};

template<typename R>
struct ResultResponse {
  std::string jsonrpc{"2.0"};
  int64_t id = 1;
  R result;
};

template<typename T>
std::string ToJson(const T& value) {
  simdjson::builder::string_builder sb;
  sdb::utils::JsonSink sink{sb};
  irs::utils::WriteObject(sink, value);
  return std::string{sb.view().value()};
}

std::string ReadDocResponse(std::string text) {
  return ToJson(ResultResponse<ToolCallResult>{
    .result = {.content = {{.text = std::move(text)}}}});
}

std::vector<std::string> DocPages() {
  std::vector<std::string> pages;
  for (const auto& entry :
       std::filesystem::recursive_directory_iterator(SDB_DOCS_DIR)) {
    const auto ext = entry.path().extension();
    if (!entry.is_regular_file() || (ext != ".md" && ext != ".mdx")) {
      continue;
    }
    std::ifstream file{entry.path()};
    std::stringstream text;
    text << file.rdbuf();
    pages.push_back(text.str());
  }
  std::sort(pages.begin(), pages.end(),
            [](const auto& a, const auto& b) { return a.size() < b.size(); });
  return pages;
}

struct Payload {
  std::string name;
  std::string body;
};

const std::vector<Payload>& Payloads() {
  static const std::vector<Payload> payloads = [] {
    const auto pages = DocPages();
    std::string all;
    for (const auto& page : pages) {
      all += page;
      all += "\n\n";
    }
    return std::vector<Payload>{
      {"tools_list", ToJson(ResultResponse<sdb::network::http::mcp::ToolsList>{
                       .result = sdb::network::http::mcp::Tools()})},
      {"read_doc_median", ReadDocResponse(pages[pages.size() / 2])},
      {"read_doc_largest", ReadDocResponse(pages.back())},
      {"read_doc_all_pages", ReadDocResponse(all)},
    };
  }();
  return payloads;
}

std::string Compress(const ContentCoding& coding, std::string_view body) {
  std::string out;
  coding.make()->Encode(body, true,
                        [&](std::string_view piece) { out.append(piece); });
  return out;
}

std::string Decompress(std::string_view token, std::string_view in,
                       size_t original_size) {
  std::string out;
  if (token == "gzip") {
    z_stream stream{};
    inflateInit2(&stream, 15 + 16);
    stream.next_in =
      const_cast<Bytef*>(reinterpret_cast<const Bytef*>(in.data()));
    stream.avail_in = static_cast<uInt>(in.size());
    out.resize(original_size);
    stream.next_out = reinterpret_cast<Bytef*>(out.data());
    stream.avail_out = static_cast<uInt>(out.size());
    inflate(&stream, Z_FINISH);
    inflateEnd(&stream);
  } else if (token == "zstd") {
    out.resize(original_size);
    ZSTD_decompress(out.data(), out.size(), in.data(), in.size());
  } else if (token == "lz4") {
    LZ4F_dctx* dctx = nullptr;
    LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
    out.resize(original_size);
    size_t dst = out.size();
    size_t src = in.size();
    LZ4F_decompress(dctx, out.data(), &dst, in.data(), &src, nullptr);
    LZ4F_freeDecompressionContext(dctx);
  } else if (token == "zxc") {
    out.resize(original_size);
    zxc_decompress(in.data(), in.size(), out.data(), out.size(), nullptr);
  }
  return out;
}

void BM_Compress(benchmark::State& state, const Payload& payload,
                 const ContentCoding& coding) {
  size_t compressed_size = 0;
  for (auto _ : state) {
    const auto out = Compress(coding, payload.body);
    compressed_size = out.size();
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(
    static_cast<int64_t>(state.iterations() * payload.body.size()));
  state.counters["ratio"] =
    static_cast<double>(payload.body.size()) / compressed_size;
  state.counters["compressed_bytes"] = static_cast<double>(compressed_size);
}

void BM_Decompress(benchmark::State& state, const Payload& payload,
                   const ContentCoding& coding) {
  const auto compressed = Compress(coding, payload.body);
  for (auto _ : state) {
    const auto out = Decompress(coding.token, compressed, payload.body.size());
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(
    static_cast<int64_t>(state.iterations() * payload.body.size()));
}

struct Row {
  double compress_mbps = 0;
  double decompress_mbps = 0;
  double ratio = 0;
  double compressed_bytes = 0;
};

// Prints, per payload, the codings ranked by compression throughput.
class RankingReporter final : public benchmark::ConsoleReporter {
 public:
  bool ReportContext(const Context& context) override {
    return ConsoleReporter::ReportContext(context);
  }

  void ReportRuns(const std::vector<Run>& reports) override {
    ConsoleReporter::ReportRuns(reports);
    for (const auto& run : reports) {
      // Aggregates (--benchmark_repetitions) repeat a run under a suffixed
      // name; the table ranks the real runs only.
      if (run.run_type == Run::RT_Aggregate) {
        continue;
      }
      // name: "<op>/<payload>/<coding>"
      const auto name = run.benchmark_name();
      const size_t first = name.find('/');
      const size_t last = name.rfind('/');
      const auto op = name.substr(0, first);
      const auto payload = name.substr(first + 1, last - first - 1);
      const auto coding = name.substr(last + 1);
      auto& row = _rows[payload][coding];
      const double mbps =
        run.counters.at("bytes_per_second") / (1024.0 * 1024.0);
      if (op == "compress") {
        row.compress_mbps = mbps;
        row.ratio = run.counters.at("ratio");
        row.compressed_bytes = run.counters.at("compressed_bytes");
      } else {
        row.decompress_mbps = mbps;
      }
    }
  }

  void Finalize() override {
    ConsoleReporter::Finalize();
    auto& out = GetOutputStream();
    for (const auto& payload : Payloads()) {
      const auto& rows = _rows[payload.name];
      if (rows.empty()) {  // filtered out
        continue;
      }
      std::vector<std::pair<std::string, Row>> ranked{rows.begin(), rows.end()};
      std::sort(ranked.begin(), ranked.end(), [](const auto& a, const auto& b) {
        return a.second.compress_mbps > b.second.compress_mbps;
      });
      out << "\n"
          << payload.name << " (" << payload.body.size()
          << " bytes), fastest compression first\n";
      out << "  rank  coding  compress MiB/s  decompress MiB/s   ratio  "
             "compressed\n";
      int rank = 0;
      for (const auto& [coding, row] : ranked) {
        ++rank;
        char line[160];
        std::snprintf(line, sizeof line,
                      "  %4d  %-6s  %14.1f  %16.1f  %6.2fx  %10.0f\n", rank,
                      coding.c_str(), row.compress_mbps, row.decompress_mbps,
                      row.ratio, row.compressed_bytes);
        out << line;
      }
      const auto best = [&](auto field) {
        return std::max_element(ranked.begin(), ranked.end(),
                                [&](const auto& a, const auto& b) {
                                  return field(a.second) < field(b.second);
                                })
          ->first;
      };
      out << "  fastest compress: "
          << best([](const Row& r) { return r.compress_mbps; })
          << ", fastest decompress: "
          << best([](const Row& r) { return r.decompress_mbps; })
          << ", best ratio: " << best([](const Row& r) { return r.ratio; })
          << "\n";
    }
    out.flush();
  }

 private:
  std::map<std::string, std::map<std::string, Row>> _rows;
};

}  // namespace

int main(int argc, char** argv) {
  for (const auto& payload : Payloads()) {
    for (const auto token : kCodings) {
      const auto* coding = FindContentCoding(token);
      benchmark::RegisterBenchmark(
        std::string{"compress/"} + payload.name + "/" + std::string{token},
        BM_Compress, payload, *coding);
      benchmark::RegisterBenchmark(
        std::string{"decompress/"} + payload.name + "/" + std::string{token},
        BM_Decompress, payload, *coding);
    }
  }
  benchmark::Initialize(&argc, argv);
  RankingReporter reporter;
  benchmark::RunSpecifiedBenchmarks(&reporter);
  benchmark::Shutdown();
  return 0;
}
