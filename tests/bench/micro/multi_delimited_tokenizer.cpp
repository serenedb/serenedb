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

// Microbenchmark: splitting text on a set of delimiters.
//
// `MultiDelimitedTokenizer::Make` picks one of four implementations, and they
// have nothing in common but the interface, so each is priced separately:
//
//   Single      -- one multi-byte delimiter, std::search with a searcher
//   SingleChar  -- one single-byte delimiter, memchr-shaped
//   Chars       -- several single-byte delimiters, a 256-entry lookup per byte
//   Generic     -- anything else, the Aho-Corasick DFA
//
// `*_Scan` is throughput over a fixed buffer with everything already built.
// `*_Build` is construction alone, which for Generic is where the cost used to
// be quadratic in the delimiter set.
//
// The Generic table is dense: `states x 256 x 4` bytes. `GenericScan/<n>` walks
// n delimiters of 4 bytes each, so the table grows with n and the point of the
// sweep is to find where it stops fitting.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <iresearch/analysis/multi_delimited_tokenizer.hpp>
#include <iresearch/analysis/token_batch.hpp>
#include <iresearch/utils/string.hpp>
#include <string>
#include <vector>

namespace {

using Tokenizer = irs::analysis::MultiDelimitedTokenizer;

constexpr size_t kTextBytes = 1u << 20;

irs::bstring Bytes(std::string_view s) {
  return irs::bstring{irs::ViewCast<irs::byte_type>(s)};
}

// Text over 'a'..'z' with a delimiter planted every `period` bytes, so the
// number of tokens is the same whatever the delimiter set is and the scan cost
// is comparable across them.
std::string MakeText(std::string_view delimiter, size_t period) {
  std::string text;
  text.reserve(kTextBytes + delimiter.size());
  uint32_t x = 12345;
  while (text.size() < kTextBytes) {
    for (size_t i = 0; i != period; ++i) {
      x = x * 1664525u + 1013904223u;
      text += static_cast<char>('a' + (x >> 24) % 26);
    }
    text += delimiter;
  }
  return text;
}

// n distinct 4-byte delimiters, prefix-free, over a byte range the text never
// produces, so only the planted one ever matches and the sweep prices the table
// rather than the match rate.
std::vector<irs::bstring> MakeDelimiters(size_t n) {
  std::vector<irs::bstring> out;
  out.reserve(n);
  for (size_t i = 0; i != n; ++i) {
    std::string d = "AB";
    d += static_cast<char>('A' + i / 26);
    d += static_cast<char>('A' + i % 26);
    out.push_back(Bytes(d));
  }
  return out;
}

struct CountingSink final : irs::TokenConsumer {
  void Consume(irs::TokenBatch& batch, irs::DocRuns /*runs*/) final {
    n += batch.count;
  }

  size_t n = 0;
};

size_t Drain(irs::analysis::Tokenizer& stream, std::string_view text) {
  static thread_local irs::TokenSink sink;
  CountingSink counter;
  sink.Bind(counter, nullptr);
  const duckdb::string_t value{text.data(), static_cast<uint32_t>(text.size())};
  if (!stream.Fill(value, irs::doc_limits::min(), sink,
                   {irs::TokenLayout::Terms})) {
    sink.Discard();
    return 0;
  }
  sink.Finish();
  return counter.n;
}

void ReportTokens(benchmark::State& state, size_t tokens, size_t bytes) {
  state.counters["tokens"] = benchmark::Counter(
    static_cast<double>(tokens) / static_cast<double>(state.iterations()));
  state.SetBytesProcessed(static_cast<int64_t>(bytes) *
                          static_cast<int64_t>(state.iterations()));
}

// -- scan ---------------------------------------------------------------

void BmSingleCharScan(benchmark::State& state) {
  const auto text = MakeText(",", static_cast<size_t>(state.range(0)));
  auto stream = Tokenizer::Make({.delimiters = {Bytes(",")}});

  size_t tokens = 0;
  for (auto _ : state) {
    tokens += Drain(*stream, text);
  }
  ReportTokens(state, tokens, text.size());
}

void BmCharsScan(benchmark::State& state) {
  const auto text = MakeText(",", static_cast<size_t>(state.range(0)));
  auto stream = Tokenizer::Make(
    {.delimiters = {Bytes(","), Bytes(";"), Bytes("|"), Bytes("\t")}});

  size_t tokens = 0;
  for (auto _ : state) {
    tokens += Drain(*stream, text);
  }
  ReportTokens(state, tokens, text.size());
}

void BmSingleScan(benchmark::State& state) {
  const auto text = MakeText("::", static_cast<size_t>(state.range(0)));
  auto stream = Tokenizer::Make({.delimiters = {Bytes("::")}});

  size_t tokens = 0;
  for (auto _ : state) {
    tokens += Drain(*stream, text);
  }
  ReportTokens(state, tokens, text.size());
}

// The delimiter set size is the argument: the planted delimiter is always the
// first one, so only the table size varies.
void BmGenericScan(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  auto delimiters = MakeDelimiters(n);
  const auto text = MakeText("ABAA", 32);
  auto stream = Tokenizer::Make({.delimiters = std::move(delimiters)});

  size_t tokens = 0;
  for (auto _ : state) {
    tokens += Drain(*stream, text);
  }
  ReportTokens(state, tokens, text.size());
}

// Delimiters that share prefixes, so the trie is deep and error transitions
// actually go somewhere other than the root.
void BmGenericSharedPrefixScan(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  std::vector<irs::bstring> delimiters;
  delimiters.reserve(n);
  std::string d = "AAAA";
  for (size_t i = 0; i != n; ++i) {
    d.back() = static_cast<char>('A' + i % 26);
    d[2] = static_cast<char>('A' + i / 26);
    delimiters.push_back(Bytes(d));
  }
  const auto text = MakeText("AAAA", 32);
  auto stream = Tokenizer::Make({.delimiters = std::move(delimiters)});

  size_t tokens = 0;
  for (auto _ : state) {
    tokens += Drain(*stream, text);
  }
  ReportTokens(state, tokens, text.size());
}

// -- build --------------------------------------------------------------

void BmGenericBuild(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    auto delimiters = MakeDelimiters(n);
    auto stream = Tokenizer::Make({.delimiters = std::move(delimiters)});
    benchmark::DoNotOptimize(stream.get());
  }
}

void BmGenericSharedPrefixBuild(benchmark::State& state) {
  const auto n = static_cast<size_t>(state.range(0));
  for (auto _ : state) {
    std::vector<irs::bstring> delimiters;
    delimiters.reserve(n);
    std::string d = "AAAA";
    for (size_t i = 0; i != n; ++i) {
      d.back() = static_cast<char>('A' + i % 26);
      d[2] = static_cast<char>('A' + i / 26);
      delimiters.push_back(Bytes(d));
    }
    auto stream = Tokenizer::Make({.delimiters = std::move(delimiters)});
    benchmark::DoNotOptimize(stream.get());
  }
}

void TokenPeriods(benchmark::internal::Benchmark* b) {
  b->Arg(8)->Arg(64)->Arg(1024)->Unit(benchmark::kMicrosecond);
}

void SetSizes(benchmark::internal::Benchmark* b) {
  b->Arg(2)->Arg(8)->Arg(64)->Arg(256)->Unit(benchmark::kMicrosecond);
}

BENCHMARK(BmSingleCharScan)->Apply(TokenPeriods);
BENCHMARK(BmCharsScan)->Apply(TokenPeriods);
BENCHMARK(BmSingleScan)->Apply(TokenPeriods);
BENCHMARK(BmGenericScan)->Apply(SetSizes);
BENCHMARK(BmGenericSharedPrefixScan)->Apply(SetSizes);
BENCHMARK(BmGenericBuild)->Apply(SetSizes);
BENCHMARK(BmGenericSharedPrefixBuild)->Apply(SetSizes);

}  // namespace

BENCHMARK_MAIN();
