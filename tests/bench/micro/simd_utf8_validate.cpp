////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2025 SereneDB GmbH, Berlin, Germany
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

// Microbench: UTF-8 validation (bool). Decision 3 from the SIMD string-encoding
// task -- simdutf::validate_utf8 vs simdjson::validate_utf8, with DuckDB's
// current utf8proc Utf8Proc::Analyze as the baseline being replaced.

#include <benchmark/benchmark.h>
#include <simdjson.h>
#include <simdutf.h>

#include <string>
#include <vector>

#include "utf8proc_wrapper.hpp"

namespace {

// Append whole fragments until at least n bytes, so we never cut a codepoint
// mid-sequence (which would turn valid input into invalid input). Multibyte
// content is spelled with explicit \x escapes to keep the source ASCII-clean.
std::string MakeAscii(size_t n) {
  static const std::string words =
    "The Quick Brown Fox Jumps Over The Lazy Dog. ";
  std::string s;
  while (s.size() < n) {
    s += words;
  }
  return s;
}

std::string MakeMixedUtf8(size_t n) {
  // 1-byte ASCII, 2-byte (e-acute, i-diaeresis), 3-byte (CJK), 4-byte (emoji).
  static const std::string frag =
    "Caf\xC3\xA9 \xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E na\xC3\xAFve "
    "\xF0\x9F\x98\x80 ";
  std::string s;
  while (s.size() < n) {
    s += frag;
  }
  return s;
}

bool Utf8ProcValid(const char* p, size_t n) {
  return duckdb::Utf8Proc::Analyze(p, n) != duckdb::UnicodeType::INVALID;
}

struct Input {
  std::string name;
  std::string data;
};

std::vector<Input>& Inputs() {
  static std::vector<Input> inputs = [] {
    std::vector<Input> v;
    v.push_back({"ascii_title_40B", MakeAscii(40)});
    v.push_back({"mixed_utf8_60B", MakeMixedUtf8(60)});
    v.push_back({"ascii_1KB", MakeAscii(1024)});
    v.push_back({"mixed_utf8_1KB", MakeMixedUtf8(1024)});
    v.push_back({"ascii_1MB", MakeAscii(1u << 20)});
    v.push_back({"mixed_utf8_1MB", MakeMixedUtf8(1u << 20)});
    return v;
  }();
  return inputs;
}

template<bool (*Fn)(const char*, size_t)>
void Run(benchmark::State& state, const std::string* data) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(Fn(data->data(), data->size()));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data->size()));
}

void Register() {
  for (auto& in : Inputs()) {
    const std::string* d = &in.data;
    benchmark::RegisterBenchmark("simdutf/" + in.name,
                                 Run<simdutf::validate_utf8>, d);
    benchmark::RegisterBenchmark("simdjson/" + in.name,
                                 Run<simdjson::validate_utf8>, d);
    benchmark::RegisterBenchmark("utf8proc/" + in.name, Run<Utf8ProcValid>, d);
  }
}

}  // namespace

int main(int argc, char** argv) {
  Register();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
