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

#include <benchmark/benchmark.h>

#include <iresearch/analysis/path_hierarchy_tokenizer.hpp>

#include "bench_token_sink.hpp"

namespace {

using Options = irs::analysis::PathHierarchyTokenizer::Options;

void InitOnce() {
  // No-op: the legacy registry init no longer exists. The bench-style
  // helpers below construct analyzers directly via Make(Options).
}

std::string GeneratePath(size_t segments, std::string_view delimiter) {
  std::string path;
  path.reserve(segments * (delimiter.size() + 6));

  for (size_t i = 0; i < segments; ++i) {
    path += delimiter;
    path += "seg";
    path += std::to_string(i);
  }

  path += delimiter;
  return path;
}

std::string GenerateNoDelimiterPath(size_t size) {
  return std::string(size, 'a');
}

// fastest path: single-char delimiter + no replacement
static void BmForwardSingleCharZeroCopy(benchmark::State& state) {
  InitOnce();

  const std::string input = GeneratePath(state.range(0), "/");

  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = false;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);

    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

// multi-char delimiter, still zero-copy
static void BmForwardMultiCharZeroCopy(benchmark::State& state) {
  InitOnce();

  const std::string input = GeneratePath(state.range(0), "::");

  Options options;
  options.delimiter = "::";
  options.replacement = "::";
  options.reverse = false;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);

    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

// buffered path (delimiter != replacement)
static void BmForwardBuffered(benchmark::State& state) {
  InitOnce();

  const std::string input = GeneratePath(state.range(0), "/");

  Options options;
  options.delimiter = "/";
  options.replacement = "//";
  options.reverse = false;
  options.buffer_size = 4096;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);

    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

// reverse benchmarks
static void BmReverseSingleCharZeroCopy(benchmark::State& state) {
  InitOnce();

  const std::string input = GeneratePath(state.range(0), "/");

  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);

    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

static void BmReverseMultiCharZeroCopy(benchmark::State& state) {
  InitOnce();

  const std::string input = GeneratePath(state.range(0), "::");

  Options options;
  options.delimiter = "::";
  options.replacement = "::";
  options.reverse = true;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);

    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

static void BmReverseBuffered(benchmark::State& state) {
  InitOnce();

  const std::string input = GeneratePath(state.range(0), "/");

  Options options;
  options.delimiter = "/";
  options.replacement = "->";
  options.reverse = true;
  options.buffer_size = 4096;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);

    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

// skip benchmarks
static void BmForwardWithSkip(benchmark::State& state) {
  InitOnce();

  const size_t segments = state.range(0);
  const size_t skip = state.range(1);

  const std::string input = GeneratePath(segments, "/");

  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = false;
  options.skip = skip;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

static void BmReverseWithSkip(benchmark::State& state) {
  InitOnce();

  const size_t segments = state.range(0);
  const size_t skip = state.range(1);

  const std::string input = GeneratePath(segments, "/");

  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;
  options.skip = skip;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

// no delimiters
static void BmForwardNoDelimiters(benchmark::State& state) {
  InitOnce();

  const std::string input = GenerateNoDelimiterPath(state.range(0));

  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = false;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

static void BmReverseNoDelimiters(benchmark::State& state) {
  InitOnce();

  const std::string input = GenerateNoDelimiterPath(state.range(0));

  Options options;
  options.delimiter = "/";
  options.replacement = "/";
  options.reverse = true;

  auto tokenizer =
    irs::analysis::PathHierarchyTokenizer::Make(Options{options});

  bench::DrainSink sink;

  for (auto _ : state) {
    tokenizer->Fill(input, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }

  state.SetBytesProcessed(state.iterations() * input.size());
}

}  // namespace

BENCHMARK(BmForwardSingleCharZeroCopy)->Range(8, 256);
BENCHMARK(BmForwardMultiCharZeroCopy)->Range(8, 256);
BENCHMARK(BmForwardBuffered)->Range(8, 256);

BENCHMARK(BmReverseSingleCharZeroCopy)->Range(8, 256);
BENCHMARK(BmReverseMultiCharZeroCopy)->Range(8, 256);
BENCHMARK(BmReverseBuffered)->Range(8, 256);

BENCHMARK(BmForwardWithSkip)->ArgsProduct({{64}, {0, 4, 8, 16}});
BENCHMARK(BmReverseWithSkip)->ArgsProduct({{64}, {0, 4, 8, 16}});

BENCHMARK(BmForwardNoDelimiters)->Range(64, 4096);
BENCHMARK(BmReverseNoDelimiters)->Range(64, 4096);

BENCHMARK_MAIN();
