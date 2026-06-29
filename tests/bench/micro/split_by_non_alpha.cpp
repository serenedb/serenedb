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

// Compares ways of splitting text on non-alphanumeric runs:
//   * SplitByNonAlphaTokenizer  -- the iresearch streaming analyzer
//   * SplitByNonAlpha           -- the plain free function
//   * PatternTokenizer          -- RE2 split on `[^A-Za-z0-9]+`; this is the
//                                  same engine DuckDB's regexp_split_to_array /
//                                  string_split_regex use, so it answers "is the
//                                  built-in regex fast enough to reuse instead?"
//   * SegmentationTokenizer     -- ICU/UAX#29 word-break analyzer (alphanumeric,
//                                  lowercase) -- the closest existing analyzer
// `to_lower` is varied via Arg(0): 0 = keep case, 1 = ASCII-lowercase.
// PatternTokenizer and Segmentation are run in their natural configs (pattern
// keeps case; segmentation always lowercases).

#include <absl/random/random.h>
#include <benchmark/benchmark.h>

#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha.hpp>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <string>

namespace {

using namespace irs::analysis;

class CorpusFixture : public benchmark::Fixture {
 public:
  std::string data;
  // Mixed-case alphanumerics plus a few separator characters so runs vary in
  // length the way real text does.
  static constexpr char kAlphabet[] =
    "0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM   ,.!-_";
  static constexpr size_t kSize = 1u << 20;  // 1 MiB

  CorpusFixture() {
    absl::BitGen bitgen;
    data.reserve(kSize);
    constexpr auto kN = sizeof(kAlphabet) - 1;
    for (size_t i = 0; i < kSize; ++i) {
      data += kAlphabet[absl::Uniform(bitgen, 0u, static_cast<uint32_t>(kN))];
    }
  }

  void SetBytes(benchmark::State& state) const {
    state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                            static_cast<int64_t>(data.size()));
  }
};

BENCHMARK_DEFINE_F(CorpusFixture, BmTokenizer)(benchmark::State& state) {
  SplitByNonAlphaTokenizer::Options opts;
  opts.to_lower = state.range(0) != 0;
  auto stream = SplitByNonAlphaTokenizer::Make(std::move(opts));

  for (auto _ : state) {
    stream->reset(data);
    while (bool has_next = stream->next()) {
      benchmark::DoNotOptimize(has_next);
    }
  }
  SetBytes(state);
}

BENCHMARK_DEFINE_F(CorpusFixture, BmFunction)(benchmark::State& state) {
  const bool to_lower = state.range(0) != 0;
  std::string buf;

  for (auto _ : state) {
    SplitByNonAlpha(data, to_lower, buf, [](std::string_view token) {
      benchmark::DoNotOptimize(token);
    });
  }
  SetBytes(state);
}

BENCHMARK_DEFINE_F(CorpusFixture, BmPattern)(benchmark::State& state) {
  // RE2 split on non-alphanumeric runs -- equivalent to what DuckDB's
  // regexp_split_to_array('[^A-Za-z0-9]+') does. Keeps case.
  PatternTokenizer::Options opts;
  opts.pattern = "[^A-Za-z0-9]+";
  opts.group = -1;  // split mode: emit text between matches
  auto stream = PatternTokenizer::Make(std::move(opts));

  for (auto _ : state) {
    stream->reset(data);
    while (bool has_next = stream->next()) {
      benchmark::DoNotOptimize(has_next);
    }
  }
  SetBytes(state);
}

BENCHMARK_DEFINE_F(CorpusFixture, BmSegmentation)(benchmark::State& state) {
  SegmentationTokenizer::Options opts;
  opts.separate = SegmentationTokenizer::Options::Separate::Word;
  opts.accept = SegmentationTokenizer::Options::Accept::AlphaNumeric;
  opts.convert = SegmentationTokenizer::Options::Convert::Lower;
  auto stream = SegmentationTokenizer::Make(std::move(opts));

  for (auto _ : state) {
    stream->reset(data);
    while (bool has_next = stream->next()) {
      benchmark::DoNotOptimize(has_next);
    }
  }
  SetBytes(state);
}

}  // namespace

// Arg(0): to_lower (0 = keep case, 1 = ASCII lowercase)
BENCHMARK_REGISTER_F(CorpusFixture, BmTokenizer)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(CorpusFixture, BmFunction)->Arg(0)->Arg(1);
// Pattern keeps case; compare against the Arg(0) (keep-case) variants above.
BENCHMARK_REGISTER_F(CorpusFixture, BmPattern)->Arg(0);
// Segmentation always lowercases in the comparable configuration.
BENCHMARK_REGISTER_F(CorpusFixture, BmSegmentation)->Arg(1);

BENCHMARK_MAIN();
