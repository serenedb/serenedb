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

// Microbench: base64 encode/decode. Decision 1 from the SIMD string-encoding
// task -- absl::Base64Escape/Base64Unescape vs simdutf::binary_to_base64 /
// base64_to_binary_safe, with DuckDB's current scalar Blob::ToBase64/FromBase64
// as the baseline being replaced.

#include <absl/strings/escaping.h>
#include <benchmark/benchmark.h>
#include <simdutf.h>

#include <cstdint>
#include <random>
#include <string>
#include <vector>

#include "duckdb/common/types/blob.hpp"
#include "duckdb/common/types/string_type.hpp"

namespace {

std::string MakeRandomBytes(size_t n) {
  std::mt19937 rng(12345);
  std::uniform_int_distribution<int> dist(0, 255);
  std::string s(n, '\0');
  for (size_t i = 0; i < n; i++) {
    s[i] = static_cast<char>(dist(rng));
  }
  return s;
}

duckdb::string_t ST(const std::string& s) {
  return duckdb::string_t(s.data(), static_cast<uint32_t>(s.size()));
}

struct Case {
  std::string name;
  std::string raw;  // binary payload
  std::string b64;  // standard base64 (with padding) of `raw`
};

std::vector<Case>& Cases() {
  static std::vector<Case> cases = [] {
    std::vector<Case> v;
    for (size_t n : {32u, 256u, 1024u, 64u * 1024u, 1024u * 1024u}) {
      Case c;
      c.name = std::to_string(n) + "B";
      c.raw = MakeRandomBytes(n);
      c.b64 = absl::Base64Escape(c.raw);
      v.push_back(std::move(c));
    }
    return v;
  }();
  return cases;
}

// ---- encode ----
void EncodeDuckDB(benchmark::State& state, const Case* c) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(duckdb::Blob::ToBase64(ST(c->raw)));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(c->raw.size()));
}

void EncodeAbseil(benchmark::State& state, const Case* c) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(absl::Base64Escape(c->raw));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(c->raw.size()));
}

void EncodeSimdutf(benchmark::State& state, const Case* c) {
  std::string out;
  for (auto _ : state) {
    out.resize(simdutf::base64_length_from_binary(c->raw.size()));
    size_t written =
      simdutf::binary_to_base64(c->raw.data(), c->raw.size(), out.data());
    benchmark::DoNotOptimize(out.data());
    benchmark::DoNotOptimize(written);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(c->raw.size()));
}

// ---- decode ----
void DecodeDuckDB(benchmark::State& state, const Case* c) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(duckdb::Blob::FromBase64(ST(c->b64)));
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(c->b64.size()));
}

void DecodeAbseil(benchmark::State& state, const Case* c) {
  std::string out;
  for (auto _ : state) {
    bool ok = absl::Base64Unescape(c->b64, &out);
    benchmark::DoNotOptimize(ok);
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(c->b64.size()));
}

void DecodeSimdutf(benchmark::State& state, const Case* c) {
  std::string out;
  for (auto _ : state) {
    out.resize(
      simdutf::maximal_binary_length_from_base64(c->b64.data(), c->b64.size()));
    size_t outlen = out.size();
    auto r = simdutf::base64_to_binary_safe(
      c->b64.data(), c->b64.size(), out.data(), outlen, simdutf::base64_default,
      simdutf::last_chunk_handling_options::strict);
    benchmark::DoNotOptimize(r.error);
    benchmark::DoNotOptimize(out.data());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(c->b64.size()));
}

void Register() {
  for (auto& c : Cases()) {
    const Case* p = &c;
    benchmark::RegisterBenchmark("encode/duckdb/" + c.name, EncodeDuckDB, p);
    benchmark::RegisterBenchmark("encode/abseil/" + c.name, EncodeAbseil, p);
    benchmark::RegisterBenchmark("encode/simdutf/" + c.name, EncodeSimdutf, p);
    benchmark::RegisterBenchmark("decode/duckdb/" + c.name, DecodeDuckDB, p);
    benchmark::RegisterBenchmark("decode/abseil/" + c.name, DecodeAbseil, p);
    benchmark::RegisterBenchmark("decode/simdutf/" + c.name, DecodeSimdutf, p);
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
