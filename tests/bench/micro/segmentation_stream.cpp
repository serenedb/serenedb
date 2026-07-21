////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2021 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#include <absl/random/random.h>
#include <benchmark/benchmark.h>

#include <iresearch/analysis/segmentation_tokenizer.hpp>

#include "bench_token_sink.hpp"

namespace {

using namespace irs::analysis;

void BmSegmentationAnalyzer(benchmark::State& state) {
  SegmentationTokenizer::Options opts;
  opts.separate = SegmentationTokenizer::Options::Separate::Word;
  opts.accept = SegmentationTokenizer::Options::Accept::AlphaNumeric;
  opts.convert = SegmentationTokenizer::Options::Convert::Lower;

  auto stream = SegmentationTokenizer::Make(std::move(opts));

  const std::string_view str = "QUICK BROWN FOX JUMPS OVER THE LAZY DOG";
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(str, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }
}

class AsciiOptimizationFixture : public benchmark::Fixture {
 public:
  std::string data;
  static constexpr char kAlphabet[64] =
    " 0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
  static constexpr auto kMaxWordSize =
    SegmentationTokenizer::kMaxStringSizeToOptimizeAccept * 2;
  static constexpr auto kSize = kMaxWordSize * 1000;
  AsciiOptimizationFixture() {
    absl::BitGen bitgen;
    data.reserve(kSize);
    size_t current_word_size = 0;
    for (size_t i = 0; i < kSize; ++i) {
      if (current_word_size == kMaxWordSize) {
        data += ' ';
        current_word_size = 0;
        continue;
      }
      data += kAlphabet[absl::Uniform(bitgen, 0u, 63u)];
      current_word_size = data.back() == ' ' ? 0 : current_word_size + 1;
    }
  }
};

BENCHMARK_DEFINE_F(AsciiOptimizationFixture,
                   BM_ascii_optimization)(benchmark::State& state) {
  // NOTE: use_ascii_optimization is no longer a persistable Options field
  // (it's hardcoded to true inside SegmentationTokenizer::Make). The
  // benchmark dimension is therefore unused; state.range(0) ignored.
  SegmentationTokenizer::Options opts;
  opts.convert =
    static_cast<SegmentationTokenizer::Options::Convert>(state.range(1));
  opts.accept =
    static_cast<SegmentationTokenizer::Options::Accept>(state.range(2));
  opts.separate =
    static_cast<SegmentationTokenizer::Options::Separate>(state.range(3));

  auto stream = SegmentationTokenizer::Make(std::move(opts));

  bench::DrainSink sink;

  for (auto _ : state) {
    stream->Fill(data, sink);
    benchmark::DoNotOptimize(sink.Consume());
  }
}

}  // namespace

BENCHMARK(BmSegmentationAnalyzer);

BENCHMARK_REGISTER_F(AsciiOptimizationFixture, BM_ascii_optimization)
  ->ArgsProduct({
    /* use_ascii_optimization */ {0, 1},
    /* convert */ {0, 1, 2},
    /* accept */ {0, 1, 2, 3},
    /* separate */ {0, 1},
  });

BENCHMARK_MAIN();
