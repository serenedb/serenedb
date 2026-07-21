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

#include <absl/random/random.h>
#include <absl/strings/ascii.h>
#include <benchmark/benchmark.h>

#include <cstdint>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha.hpp>
#include <string>

#include "bench_token_sink.hpp"

namespace {

using namespace irs::analysis;

constexpr size_t kSize = 1u << 20;
constexpr char kAlnum[] =
  "0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";

std::string MakeMixedCorpus() {
  absl::BitGen bitgen;
  static constexpr char kChars[] =
    "0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM   ,.!-_";
  constexpr auto kN = sizeof(kChars) - 1;
  std::string data;
  data.reserve(kSize);
  for (size_t i = 0; i < kSize; ++i) {
    data += kChars[absl::Uniform(bitgen, 0u, static_cast<uint32_t>(kN))];
  }
  return data;
}

std::string MakeLongTokenCorpus() {
  absl::BitGen bitgen;
  constexpr auto kN = sizeof(kAlnum) - 1;
  constexpr size_t kRun = 64;
  std::string data;
  data.reserve(kSize);
  size_t run = 0;
  while (data.size() < kSize) {
    if (run == kRun) {
      data += ' ';
      run = 0;
      continue;
    }
    data += kAlnum[absl::Uniform(bitgen, 0u, static_cast<uint32_t>(kN))];
    ++run;
  }
  return data;
}

void SetBytes(benchmark::State& state, const std::string& data) {
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data.size()));
}

void RunFunction(benchmark::State& state, const std::string& data) {
  const bool to_lower = state.range(0) != 0;
  std::string lowered;
  for (auto _ : state) {
    SplitByNonAlpha(data, [&](std::string_view token) {
      if (to_lower) {
        lowered.resize(token.size());
        absl::ascii_internal::AsciiStrToLower(lowered.data(), token.data(),
                                              token.size());
        benchmark::DoNotOptimize(lowered);
      } else {
        benchmark::DoNotOptimize(token);
      }
    });
  }
  SetBytes(state, data);
}

void RunPattern(benchmark::State& state, const std::string& data) {
  PatternTokenizer::Options opts;
  opts.pattern = "[^A-Za-z0-9]+";
  opts.group = -1;
  auto stream = PatternTokenizer::Make(std::move(opts));
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(data, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }
  SetBytes(state, data);
}

void RunSegmentation(benchmark::State& state, const std::string& data) {
  SegmentationTokenizer::Options opts;
  opts.separate = SegmentationTokenizer::Options::Separate::Word;
  opts.accept = SegmentationTokenizer::Options::Accept::AlphaNumeric;
  opts.convert = SegmentationTokenizer::Options::Convert::Lower;
  auto stream = SegmentationTokenizer::Make(std::move(opts));
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(data, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }
  SetBytes(state, data);
}

std::string MakeSmallInput(size_t n) {
  absl::BitGen bitgen;
  static constexpr char kChars[] =
    "0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM   ,.!-_";
  constexpr auto kN = sizeof(kChars) - 1;
  std::string data;
  data.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    data += kChars[absl::Uniform(bitgen, 0u, static_cast<uint32_t>(kN))];
  }
  return data;
}

void BmSmallInput(benchmark::State& state) {
  const std::string data = MakeSmallInput(static_cast<size_t>(state.range(0)));
  for (auto _ : state) {
    SplitByNonAlpha(
      data, [](std::string_view token) { benchmark::DoNotOptimize(token); });
  }
  SetBytes(state, data);
}

class MixedCorpus : public benchmark::Fixture {
 public:
  std::string data = MakeMixedCorpus();
};

class LongTokenCorpus : public benchmark::Fixture {
 public:
  std::string data = MakeLongTokenCorpus();
};

}  // namespace

BENCHMARK_DEFINE_F(MixedCorpus, BmFunction)(benchmark::State& state) {
  RunFunction(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmPattern)(benchmark::State& state) {
  RunPattern(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmSegmentation)(benchmark::State& state) {
  RunSegmentation(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmFunction)(benchmark::State& state) {
  RunFunction(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmPattern)(benchmark::State& state) {
  RunPattern(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmSegmentation)(benchmark::State& state) {
  RunSegmentation(state, data);
}

BENCHMARK_REGISTER_F(MixedCorpus, BmFunction)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(MixedCorpus, BmPattern)->Arg(0);
BENCHMARK_REGISTER_F(MixedCorpus, BmSegmentation)->Arg(1);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmFunction)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmPattern)->Arg(0);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmSegmentation)->Arg(1);

BENCHMARK(BmSmallInput)->Arg(8)->Arg(16)->Arg(24)->Arg(31)->Arg(48)->Arg(64);

BENCHMARK_MAIN();
