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

// Cost of an ASCII verdict over a whole string vector, without any tokenizer
// work: prices the dedicated pre-scan pass a batch-level known-ascii hint
// would add in front of a fill. `all_ascii` arms early-exit on the first
// non-ASCII value; `probe_every` arms probe every value regardless (the work
// per-value probing performs inside a fill on mixed data).
//
//   taskset -c N ./serenedb-bench-micro-ascii_probe --benchmark_min_time=0.3s \
//     --benchmark_repetitions=12 --benchmark_report_aggregates_only=true

#include <benchmark/benchmark.h>
#include <simdutf.h>

#include <cstdint>
#include <duckdb/common/types/string_type.hpp>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "iresearch/analysis/text/classify/block_masks.hpp"

namespace {

constexpr size_t kValues = 2048;

struct Corpus {
  std::vector<std::string> storage;
  std::vector<duckdb::string_t> values;
  size_t total_bytes = 0;
};

Corpus MakeCorpus(size_t len, ptrdiff_t unicode_at) {
  Corpus c;
  c.storage.reserve(kValues);
  std::mt19937_64 rng{42};
  for (size_t i = 0; i < kValues; ++i) {
    std::string s(len, 'a');
    for (auto& ch : s) {
      ch = static_cast<char>('a' + rng() % 26);
    }
    if (std::cmp_equal(i, unicode_at) && len != 0) {
      s[len / 2] = '\xc3';
    }
    c.total_bytes += s.size();
    c.storage.push_back(std::move(s));
  }
  c.values.reserve(kValues);
  for (const auto& s : c.storage) {
    c.values.emplace_back(s.data(), static_cast<uint32_t>(s.size()));
  }
  return c;
}

IRS_FORCE_INLINE bool IsAsciiValue(const duckdb::string_t& v) noexcept {
  const auto size = static_cast<uint32_t>(v.GetSize());
  const char* data = v.GetData();
  return size <= 16 ? irs::analysis::classify::IsAsciiShort(data, size)
                    : simdutf::validate_ascii(data, size);
}

bool AllAscii(std::span<const duckdb::string_t> values) noexcept {
  for (const auto& v : values) {
    if (!IsAsciiValue(v)) {
      return false;
    }
  }
  return true;
}

size_t ProbeEvery(std::span<const duckdb::string_t> values) noexcept {
  size_t ascii = 0;
  for (const auto& v : values) {
    ascii += IsAsciiValue(v);
  }
  return ascii;
}

void Report(benchmark::State& state, const Corpus& c) {
  state.SetItemsProcessed(static_cast<int64_t>(state.iterations() * kValues));
  state.SetBytesProcessed(
    static_cast<int64_t>(state.iterations() * c.total_bytes));
}

void BM_AllAscii(benchmark::State& state, const Corpus& c) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(AllAscii(c.values));
  }
  Report(state, c);
}

void BM_ProbeEvery(benchmark::State& state, const Corpus& c) {
  for (auto _ : state) {
    benchmark::DoNotOptimize(ProbeEvery(c.values));
  }
  Report(state, c);
}

const Corpus& Inline8() {
  static const Corpus kCorpus = MakeCorpus(8, -1);
  return kCorpus;
}
const Corpus& Inline12() {
  static const Corpus kCorpus = MakeCorpus(12, -1);
  return kCorpus;
}
const Corpus& Short16() {
  static const Corpus kCorpus = MakeCorpus(16, -1);
  return kCorpus;
}
const Corpus& Long64() {
  static const Corpus kCorpus = MakeCorpus(64, -1);
  return kCorpus;
}
const Corpus& Long256() {
  static const Corpus kCorpus = MakeCorpus(256, -1);
  return kCorpus;
}
const Corpus& Long1k() {
  static const Corpus kCorpus = MakeCorpus(1024, -1);
  return kCorpus;
}
const Corpus& MixedHalf64() {
  static const Corpus kCorpus = MakeCorpus(64, kValues / 2);
  return kCorpus;
}
const Corpus& UnicodeFirst64() {
  static const Corpus kCorpus = MakeCorpus(64, 0);
  return kCorpus;
}

#define ASCII_PROBE_BENCH(name, corpus)            \
  BENCHMARK_CAPTURE(BM_AllAscii, name, corpus())   \
    ->Unit(benchmark::kMicrosecond);               \
  BENCHMARK_CAPTURE(BM_ProbeEvery, name, corpus()) \
    ->Unit(benchmark::kMicrosecond)

ASCII_PROBE_BENCH(inline8, Inline8);
ASCII_PROBE_BENCH(inline12, Inline12);
ASCII_PROBE_BENCH(short16, Short16);
ASCII_PROBE_BENCH(long64, Long64);
ASCII_PROBE_BENCH(long256, Long256);
ASCII_PROBE_BENCH(long1k, Long1k);
ASCII_PROBE_BENCH(mixed_half64, MixedHalf64);
ASCII_PROBE_BENCH(unicode_first64, UnicodeFirst64);

}  // namespace

BENCHMARK_MAIN();
