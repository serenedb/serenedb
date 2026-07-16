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

// Microbench: JSON validation. Decision 2 from the SIMD string-encoding task --
// simdjson (full DOM parse) vs DuckDB's StringUtil::ValidateJSON (yyjson with
// ALLOW_INF_AND_NAN | ALLOW_TRAILING_COMMAS | BIGNUM_AS_RAW). Also prints a
// semantic-agreement table for the validity-edge cases the task calls out.

#include <benchmark/benchmark.h>
#include <simdjson.h>

#include <cstdio>
#include <string>
#include <vector>

#include "duckdb/common/string_util.hpp"

namespace {

bool YyjsonValid(const std::string& s) {
  return duckdb::StringUtil::ValidateJSON(s.data(), s.size()).empty();
}

bool SimdjsonValid(simdjson::dom::parser& parser, const std::string& s) {
  return !parser.parse(s.data(), s.size()).error();
}

std::string SmallDoc() {
  return R"({"k1":"v1","k2":42,"k3":[1,2,3],"nested":{"a":true,"b":null},)"
         R"("f":3.14159,"s":"a longer string value here"})";
}

std::string LargeDoc(size_t count) {
  std::string s = "[";
  for (size_t i = 0; i < count; i++) {
    if (i) {
      s += ",";
    }
    s += SmallDoc();
  }
  s += "]";
  return s;
}

struct Input {
  std::string name;
  std::string data;
};

std::vector<Input>& Inputs() {
  static std::vector<Input> inputs = [] {
    std::vector<Input> v;
    v.push_back({"small_120B", SmallDoc()});
    v.push_back({"large_1k_objs", LargeDoc(1000)});
    return v;
  }();
  return inputs;
}

void BenchYyjson(benchmark::State& state, const std::string* d) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(YyjsonValid(*d));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(d->size()));
}

void BenchSimdjson(benchmark::State& state, const std::string* d) {
  simdjson::dom::parser parser;
  for (auto _ : state) {
    benchmark::DoNotOptimize(SimdjsonValid(parser, *d));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(d->size()));
}

void Register() {
  for (auto& in : Inputs()) {
    const std::string* d = &in.data;
    benchmark::RegisterBenchmark("yyjson/" + in.name, BenchYyjson, d);
    benchmark::RegisterBenchmark("simdjson/" + in.name, BenchSimdjson, d);
  }
}

void PrintSemanticTable() {
  struct Edge {
    const char* label;
    std::string json;
  };
  std::vector<Edge> edges = {
    {"plain_object", R"({"a":1})"},
    {"infinity", R"({"a":Infinity})"},
    {"nan", R"({"a":NaN})"},
    {"trailing_comma_arr", R"([1,2,3,])"},
    {"trailing_comma_obj", R"({"a":1,})"},
    {"trailing_content", R"({"a":1} garbage)"},
    {"duplicate_keys", R"({"a":1,"a":2})"},
    {"leading_zero", R"({"a":01})"},
    {"plus_number", R"({"a":+1})"},
    {"bare_word", "hello"},
    {"lone_surrogate_esc", R"({"a":"\uD800"})"},
    {"deep_nest_200",
     [] {
       std::string s;
       for (int i = 0; i < 200; i++) {
         s += "[";
       }
       for (int i = 0; i < 200; i++) {
         s += "]";
       }
       return s;
     }()},
    {"bignum", R"({"a":123456789012345678901234567890})"},
    {"empty", ""},
    {"just_number", "42"},
  };
  simdjson::dom::parser parser;
  std::printf("\n=== JSON validity: yyjson(ValidateJSON) vs simdjson ===\n");
  std::printf("%-22s %-8s %-8s %s\n", "case", "yyjson", "simdjson", "agree?");
  for (auto& e : edges) {
    bool y = YyjsonValid(e.json);
    bool s = SimdjsonValid(parser, e.json);
    std::printf("%-22s %-8s %-8s %s\n", e.label, y ? "valid" : "INVALID",
                s ? "valid" : "INVALID", (y == s) ? "yes" : "*** NO ***");
  }
  std::printf("\n");
}

}  // namespace

int main(int argc, char** argv) {
  PrintSemanticTable();
  Register();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
