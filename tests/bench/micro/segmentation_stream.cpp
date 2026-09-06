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

#include <benchmark/benchmark.h>

#include <iresearch/analysis/segmentation_tokenizer.hpp>
#include <iresearch/analysis/text/words/ascii.hpp>
#include <iresearch/analysis/text/words/unicode.hpp>
#include <random>

#include "bench_token_sink.h"

namespace {

using namespace irs::analysis;
using namespace irs::analysis::words;

constexpr size_t kCorpusBytes = 1u << 20;

constexpr std::string_view kEnglishVocab[] = {
  "the",      "quick", "brown",        "fox",
  "jumps",    "over",  "lazy",         "dog",
  "a",        "of",    "and",          "don't",
  "e.g.",     "3.14",  "2026",         "word",
  "database", "utf8",  "segmentation", "internationalization"};

constexpr std::string_view kPunct[] = {" ", " ",  " ",  " ", " ",
                                       " ", ". ", ", ", "\n"};

std::string MakeEnglishAscii() {
  std::mt19937_64 rng{7};
  std::string data;
  data.reserve(kCorpusBytes + 64);
  while (data.size() < kCorpusBytes) {
    data += kEnglishVocab[rng() % std::size(kEnglishVocab)];
    data += kPunct[rng() % std::size(kPunct)];
  }
  data.resize(kCorpusBytes);
  return data;
}

std::string MakeMixedAccent() {
  const char* accents[] = {"\xC3\xA9", "\xC3\xBC", "\xC3\xB1", "\xC3\xA0",
                           "\xC3\xB6"};
  std::mt19937_64 rng{11};
  std::string data;
  data.reserve(kCorpusBytes + 64);
  while (data.size() < kCorpusBytes) {
    std::string word{kEnglishVocab[rng() % std::size(kEnglishVocab)]};
    if (word.size() >= 2 && rng() % 32 == 0) {
      word.insert(1 + rng() % (word.size() - 1),
                  accents[rng() % std::size(accents)]);
    }
    data += word;
    data += kPunct[rng() % std::size(kPunct)];
  }
  data.resize(kCorpusBytes);
  return data;
}

std::string MakeMultilingual() {
  const char* cyrillic[] = {"\xD0\xBC\xD0\xBE\xD1\x81\xD0\xBA\xD0\xB2\xD0\xB0",
                            "\xD0\xBF\xD1\x80\xD0\xB8\xD0\xB2\xD0\xB5\xD1\x82",
                            "\xD1\x81\xD0\xBB\xD0\xBE\xD0\xB2\xD0\xBE"};
  const char* greek[] = {
    "\xCE\xBB\xCF\x8C\xCE\xB3\xCE\xBF\xCF\x82",
    "\xCF\x89\xCE\xBC\xCE\xAD\xCE\xB3\xCE\xB1",
    "\xCE\xB1\xCF\x81\xCE\xB9\xCE\xB8\xCE\xBC\xCF\x8C\xCF\x82"};
  const char* cjk[] = {"\xE4\xBB\x8A", "\xE5\xA4\xA9", "\xE4\xB8\x8B",
                       "\xE5\x8D\x88", "\xE7\x9A\x84", "\xE5\xA4\xAA",
                       "\xE9\x98\xB3", "\xE5\xBE\x88", "\xE6\xB8\xA9",
                       "\xE6\x9A\x96"};
  std::mt19937_64 rng{13};
  std::string data;
  data.reserve(kCorpusBytes + 64);
  while (data.size() < kCorpusBytes) {
    switch (rng() % 5) {
      case 0:
        data += kEnglishVocab[rng() % std::size(kEnglishVocab)];
        break;
      case 1:
        data += cyrillic[rng() % std::size(cyrillic)];
        break;
      case 2:
        data += greek[rng() % std::size(greek)];
        break;
      case 3: {
        const size_t glyphs = 2 + rng() % 4;
        for (size_t g = 0; g < glyphs; ++g) {
          data += cjk[rng() % std::size(cjk)];
        }
        break;
      }
      case 4:
        data += std::to_string(rng() % 100000);
        break;
    }
    data += kPunct[rng() % std::size(kPunct)];
  }
  data.resize(kCorpusBytes);
  return data;
}

std::string MakeLongWordsAscii() {
  constexpr size_t kMaxWordSize = 128;
  constexpr size_t kSize = kMaxWordSize * 1000;
  constexpr char kAlphabet[64] =
    " 0123456789qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM";
  std::mt19937_64 rng{17};
  std::string data;
  data.reserve(kSize);
  size_t current_word_size = 0;
  for (size_t i = 0; i < kSize; ++i) {
    if (current_word_size == kMaxWordSize) {
      data += ' ';
      current_word_size = 0;
      continue;
    }
    data += kAlphabet[rng() % 63u];
    current_word_size = data.back() == ' ' ? 0 : current_word_size + 1;
  }
  return data;
}

SegmentationTokenizer::Options MakeOpts(
  SegmentationTokenizer::Options::Convert convert,
  SegmentationTokenizer::Options::Accept accept,
  SegmentationTokenizer::Options::Separate separate) {
  SegmentationTokenizer::Options opts;
  opts.convert = convert;
  opts.accept = accept;
  opts.separate = separate;
  return opts;
}

SegmentationTokenizer::Options DefaultOpts() {
  return MakeOpts(SegmentationTokenizer::Options::Convert::Lower,
                  SegmentationTokenizer::Options::Accept::AlphaNumeric,
                  SegmentationTokenizer::Options::Separate::Word);
}

void RunCorpus(benchmark::State& state, const std::string& data,
               SegmentationTokenizer::Options opts) {
  auto stream = SegmentationTokenizer::Make(std::move(opts));
  bench::DrainSink sink;
  const duckdb::string_t value{data.data(), static_cast<uint32_t>(data.size())};
  for (auto _ : state) {
    stream->Fill(value, sink.writer, {sink.layout});
    benchmark::DoNotOptimize(sink.Consume());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data.size()));
}

class EnglishAscii : public benchmark::Fixture {
 public:
  std::string data = MakeEnglishAscii();
};

class MixedAccent : public benchmark::Fixture {
 public:
  std::string data = MakeMixedAccent();
};

class Multilingual : public benchmark::Fixture {
 public:
  std::string data = MakeMultilingual();
};

class LongWordsAscii : public benchmark::Fixture {
 public:
  std::string data = MakeLongWordsAscii();
};

BENCHMARK_DEFINE_F(EnglishAscii, BmSegmentation)(benchmark::State& state) {
  RunCorpus(state, data, DefaultOpts());
}

BENCHMARK_DEFINE_F(MixedAccent, BmSegmentation)(benchmark::State& state) {
  RunCorpus(state, data, DefaultOpts());
}

BENCHMARK_DEFINE_F(Multilingual, BmSegmentation)(benchmark::State& state) {
  RunCorpus(state, data, DefaultOpts());
}

BENCHMARK_DEFINE_F(LongWordsAscii, BmSegmentation)(benchmark::State& state) {
  RunCorpus(state, data, DefaultOpts());
}

BENCHMARK_DEFINE_F(EnglishAscii, BmScanOnly)(benchmark::State& state) {
  const duckdb::string_t value{data.data(), static_cast<uint32_t>(data.size())};
  for (auto _ : state) {
    uint64_t acc = 0;
    ScanAscii(value, [&](const AsciiSegment& seg) {
      acc += seg.end + static_cast<uint64_t>(seg.has_alpha);
    });
    benchmark::DoNotOptimize(acc);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data.size()));
}

BENCHMARK_DEFINE_F(EnglishAscii, BmScanWordRuns)(benchmark::State& state) {
  const duckdb::string_t value{data.data(), static_cast<uint32_t>(data.size())};
  for (auto _ : state) {
    uint64_t acc = 0;
    ScanAsciiRuns(value, [&](const AsciiSegment& seg) {
      acc += seg.end + static_cast<uint64_t>(seg.has_alpha);
    });
    benchmark::DoNotOptimize(acc);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data.size()));
}

void RunScanUnicode(benchmark::State& state, const std::string& data) {
  const duckdb::string_t value{data.data(), static_cast<uint32_t>(data.size())};
  for (auto _ : state) {
    uint64_t acc = 0;
    ScanUnicode(value, [&](const UnicodeSegment& seg) {
      acc += seg.end + static_cast<uint64_t>(seg.has_ascii_alpha);
    });
    benchmark::DoNotOptimize(acc);
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data.size()));
}

BENCHMARK_DEFINE_F(EnglishAscii, BmScanUnicode)(benchmark::State& state) {
  RunScanUnicode(state, data);
}

BENCHMARK_DEFINE_F(MixedAccent, BmScanUnicode)(benchmark::State& state) {
  RunScanUnicode(state, data);
}

BENCHMARK_DEFINE_F(Multilingual, BmScanUnicode)(benchmark::State& state) {
  RunScanUnicode(state, data);
}

BENCHMARK_DEFINE_F(EnglishAscii, BmSegmentationSweep)
(benchmark::State& state) {
  RunCorpus(
    state, data,
    MakeOpts(
      static_cast<SegmentationTokenizer::Options::Convert>(state.range(0)),
      static_cast<SegmentationTokenizer::Options::Accept>(state.range(1)),
      static_cast<SegmentationTokenizer::Options::Separate>(state.range(2))));
}

void BmSegmentationShortValues(benchmark::State& state) {
  const size_t n = static_cast<size_t>(state.range(0));
  const std::string base = MakeEnglishAscii();
  constexpr size_t kValues = 1024;
  std::vector<duckdb::string_t> values;
  values.reserve(kValues);
  for (size_t i = 0; i < kValues; ++i) {
    values.emplace_back(base.data() + i * 37 % (base.size() - n),
                        static_cast<uint32_t>(n));
  }
  auto stream = SegmentationTokenizer::Make(DefaultOpts());
  bench::DrainSink sink;
  size_t i = 0;
  for (auto _ : state) {
    stream->Fill(values[i++ & (kValues - 1)], sink.writer, {sink.layout});
    benchmark::DoNotOptimize(sink.Consume());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(n));
}

void BmSegmentationAnalyzer(benchmark::State& state) {
  auto stream = SegmentationTokenizer::Make(DefaultOpts());
  const duckdb::string_t str{"QUICK BROWN FOX JUMPS OVER THE LAZY DOG"};
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(str, sink.writer, {sink.layout});
    benchmark::DoNotOptimize(sink.Consume());
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(str.GetSize()));
}

}  // namespace

BENCHMARK(BmSegmentationAnalyzer);

BENCHMARK_REGISTER_F(EnglishAscii, BmSegmentation);
BENCHMARK_REGISTER_F(EnglishAscii, BmScanOnly);
BENCHMARK_REGISTER_F(EnglishAscii, BmScanWordRuns);
BENCHMARK_REGISTER_F(EnglishAscii, BmScanUnicode);
BENCHMARK_REGISTER_F(MixedAccent, BmScanUnicode);
BENCHMARK_REGISTER_F(Multilingual, BmScanUnicode);
BENCHMARK_REGISTER_F(MixedAccent, BmSegmentation);
BENCHMARK_REGISTER_F(Multilingual, BmSegmentation);
BENCHMARK_REGISTER_F(LongWordsAscii, BmSegmentation);

BENCHMARK_REGISTER_F(EnglishAscii, BmSegmentationSweep)
  ->ArgsProduct({
    /* convert */ {0, 1},
    /* accept */ {0, 2},
    /* separate */ {0, 1},
  });

BENCHMARK(BmSegmentationShortValues)
  ->Arg(8)
  ->Arg(16)
  ->Arg(24)
  ->Arg(31)
  ->Arg(64);

BENCHMARK_MAIN();
