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
#include <simdutf.h>

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <iresearch/analysis/pattern_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <iresearch/analysis/text/case/case.hpp>
#include <iresearch/analysis/text/classify/block_masks.hpp>
#include <iresearch/analysis/text/segment/fill.hpp>
#include <iresearch/analysis/text/words/masks.hpp>
#include <iresearch/analysis/text/words/split_by_non_alpha.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/utils/utf8_character_utils.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <string>
#include <vector>

#include "bench_token_sink.h"

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

std::string MakeUtf8Corpus() {
  absl::BitGen bitgen;
  static constexpr std::string_view kUnits[] = {"a",
                                                "b",
                                                "Z",
                                                "7",
                                                "q",
                                                "\xC3\xA9",
                                                "\xC3\xBC",
                                                "\xC3\x9F",
                                                "\xE6\x97\xA5",
                                                "\xE6\x9C\xAC",
                                                " ",
                                                " ",
                                                ",",
                                                ".",
                                                "\xE2\x80\x94"};
  std::string data;
  data.reserve(kSize + 3);
  while (data.size() < kSize) {
    data += kUnits[absl::Uniform(bitgen, size_t{0}, std::size(kUnits))];
  }
  return data;
}

std::vector<std::string> LatinLetters() {
  std::vector<std::string> letters;
  for (char c = 'a'; c <= 'z'; ++c) {
    letters.emplace_back(1, c);
  }
  letters.emplace_back("E");
  letters.emplace_back("T");
  return letters;
}

std::vector<std::string> CyrillicLetters() {
  std::vector<std::string> letters;
  for (uint32_t cp = 0x430; cp <= 0x44F; ++cp) {
    std::string letter(2, '\0');
    irs::utf8_utils::FromChar32(
      cp, reinterpret_cast<irs::byte_type*>(letter.data()));
    letters.push_back(std::move(letter));
  }
  return letters;
}

std::string MakeWordsCorpus(const std::vector<std::string>& letters) {
  absl::BitGen bitgen;
  static constexpr std::string_view kSeparators[] = {
    " ", " ", " ", " ", " ", " ", " ", ", ", ". ", "-", "'", "_"};
  std::string data;
  data.reserve(kSize + 64);
  while (data.size() < kSize) {
    const auto len =
      absl::Uniform(absl::IntervalClosed, bitgen, size_t{1}, size_t{10});
    for (size_t i = 0; i < len; ++i) {
      data += letters[absl::Uniform(bitgen, size_t{0}, letters.size())];
    }
    data +=
      kSeparators[absl::Uniform(bitgen, size_t{0}, std::size(kSeparators))];
  }
  return data;
}

class AlnumCodepoints {
 public:
  AlnumCodepoints() : _bits((kCodepoints + 63) / 64) {
    for (uint32_t c = 0; c < kCodepoints; ++c) {
      const char category = irs::utf8_utils::CharPrimaryCategory(c);
      if (category == 'L' || category == 'N') {
        _bits[c >> 6] |= uint64_t{1} << (c & 63);
      }
    }
  }

  bool Contains(uint32_t c) const noexcept {
    return c < kCodepoints && ((_bits[c >> 6] >> (c & 63)) & 1) != 0;
  }

 private:
  static constexpr uint32_t kCodepoints = 0x110000;

  std::vector<uint64_t> _bits;
};

const AlnumCodepoints& UnicodeAlnum() {
  static const AlnumCodepoints kTable;
  return kTable;
}

template<typename Emit>
void SplitByNonAlnumUnicode(std::string_view value, Emit&& emit) {
  constexpr size_t kBlock = classify::kClassifyBlock;
  const auto* bytes = reinterpret_cast<const irs::byte_type*>(value.data());
  const size_t size = value.size();
  const auto& alnum = UnicodeAlnum();
  uint32_t carry = 0;
  bool open = false;
  size_t begin = 0;
  for (size_t base = 0; base < size; base += kBlock) {
    const size_t n = std::min(kBlock, size - base);
    const auto block = n == kBlock ? classify::Load(bytes + base)
                                   : classify::LoadPadded(bytes + base, n);
    uint32_t mask = words::ClassifyAlnum(block);
    const uint32_t high = classify::MoveMask(block >= uint8_t{0x80});
    if (high != 0) {
      mask |= carry;
      carry = 0;
      uint32_t leads =
        high & ~classify::MoveMask((block & uint8_t{0xC0}) == uint8_t{0x80});
      while (leads != 0) {
        const auto at = static_cast<uint32_t>(std::countr_zero(leads));
        leads &= leads - 1;
        const auto* it = bytes + base + at;
        const uint32_t cp = irs::utf8_utils::ToChar32(it, bytes + size);
        if (alnum.Contains(cp)) {
          const auto len = static_cast<uint32_t>(it - (bytes + base + at));
          const uint64_t bits = ((uint64_t{1} << len) - 1) << at;
          mask |= static_cast<uint32_t>(bits);
          carry = static_cast<uint32_t>(bits >> kBlock);
        }
      }
    }
    const uint32_t before = (mask << 1) | uint32_t{open};
    uint32_t starts = mask & ~before;
    uint32_t ends = ~mask & before;
    if (open && ends != 0) {
      emit(begin, base + std::countr_zero(ends));
      ends &= ends - 1;
      open = false;
    }
    while (ends != 0) {
      emit(base + std::countr_zero(starts), base + std::countr_zero(ends));
      starts &= starts - 1;
      ends &= ends - 1;
    }
    if (starts != 0) {
      begin = base + std::countr_zero(starts);
      open = true;
    }
  }
  if (open) {
    emit(begin, size);
  }
}

void SetBytes(benchmark::State& state, const std::string& data) {
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(data.size()));
}

void SetTokens(benchmark::State& state, size_t tokens) {
  state.counters["tokens"] = benchmark::Counter(
    static_cast<double>(tokens), benchmark::Counter::kAvgIterations);
}

enum class Kernel : uint8_t {
  Ascii,
  KeepNonAscii,
  Unicode,
  Text,
};

template<Kernel K>
void RunKernel(benchmark::State& state, const std::string& data) {
  const duckdb::string_t value{data.data(), static_cast<uint32_t>(data.size())};
  size_t tokens = 0;
  const auto on_run = [&](size_t begin, size_t end) {
    const std::string_view token{data.data() + begin, end - begin};
    benchmark::DoNotOptimize(token);
    ++tokens;
  };
  if constexpr (K == Kernel::Unicode) {
    benchmark::DoNotOptimize(&UnicodeAlnum());
  }
  for (auto _ : state) {
    if constexpr (K == Kernel::Ascii) {
      words::SplitByNonAlpha(value, on_run);
    } else if constexpr (K == Kernel::KeepNonAscii) {
      words::SplitByNonAlpha<true>(value, on_run);
    } else if constexpr (K == Kernel::Unicode) {
      SplitByNonAlnumUnicode(data, on_run);
    } else {
      const auto on_segment = [&](const words::Segment& seg) {
        if (segment::AcceptSegment<segment::Accept::AlphaNumeric>(data.data(),
                                                                  seg)) {
          on_run(seg.begin, seg.end);
        }
      };
      if (simdutf::validate_ascii(data.data(), data.size())) {
        words::ScanAsciiRuns(value, on_segment);
      } else {
        words::ScanUnicode(value, on_segment);
      }
    }
  }
  SetBytes(state, data);
  SetTokens(state, tokens);
}

void RunFunction(benchmark::State& state, const std::string& data) {
  const bool to_lower = state.range(0) != 0;
  std::string lowered;
  for (auto _ : state) {
    words::SplitByNonAlpha(data, [&](size_t begin, size_t end) {
      const std::string_view token{data.data() + begin, end - begin};
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

using SplitChars = SplitByNonAlphaTokenizer::Options::Chars;

void RunSplit(benchmark::State& state, const std::string& data,
              SplitChars chars = SplitChars::Ascii) {
  SplitByNonAlphaTokenizer::Options opts;
  opts.case_convert = state.range(0) != 0 ? irs::Case::Lower : irs::Case::None;
  opts.chars = chars;
  auto stream = SplitByNonAlphaTokenizer::Make(opts);
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(data, sink.writer, {sink.layout});
    benchmark::DoNotOptimize(sink.Consume());
  }
  SetBytes(state, data);
}

enum class Fold : uint8_t {
  None,
  PerTokenAbsl,
  PerTokenExact,
  PerValueAbsl,
};

template<Fold F>
void RunEmit(benchmark::State& state, const std::string& data) {
  std::string out(data.size(), '\0');
  std::string value(data.size(), '\0');
  for (auto _ : state) {
    const char* src = data.data();
    if constexpr (F == Fold::PerValueAbsl) {
      absl::ascii_internal::AsciiStrToLower(value.data(), data.data(),
                                            data.size());
      src = value.data();
    }
    size_t at = 0;
    words::SplitByNonAlpha(
      duckdb::string_t{src, static_cast<uint32_t>(data.size())},
      [&](size_t begin, size_t end) {
        const size_t n = end - begin;
        char* dst = out.data() + at;
        if constexpr (F == Fold::PerTokenAbsl) {
          absl::ascii_internal::AsciiStrToLower(dst, src + begin, n);
        } else if constexpr (F == Fold::PerTokenExact) {
          irs::analysis::casing::CaseConvertAsciiExact<true>(dst, src + begin,
                                                             n);
        } else {
          std::memcpy(dst, src + begin, n);
        }
        at += n;
      });
    benchmark::DoNotOptimize(out.data());
    benchmark::ClobberMemory();
  }
  SetBytes(state, data);
}

void RunPatternWith(benchmark::State& state, const std::string& data,
                    std::string_view pattern) {
  PatternTokenizer::Options opts;
  opts.pattern = std::string{pattern};
  opts.group = -1;
  auto stream = PatternTokenizer::Make(std::move(opts));
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(data, sink.writer, {sink.layout});
    benchmark::DoNotOptimize(sink.Consume());
  }
  SetBytes(state, data);
}

void RunPattern(benchmark::State& state, const std::string& data) {
  RunPatternWith(state, data, "[^A-Za-z0-9]+");
}

void RunText(benchmark::State& state, const std::string& data) {
  TextTokenizer::Options opts;
  opts.separate = TextTokenizer::Options::Separate::Word;
  opts.accept = TextTokenizer::Options::Accept::AlphaNumeric;
  opts.convert = state.range(0) != 0 ? irs::Case::Lower : irs::Case::None;
  auto stream = TextTokenizer::Make(std::move(opts));
  bench::DrainSink sink;
  for (auto _ : state) {
    stream->Fill(data, sink.writer, {sink.layout});
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
    words::SplitByNonAlpha(data, [&](size_t begin, size_t end) {
      const std::string_view token{data.data() + begin, end - begin};
      benchmark::DoNotOptimize(token);
    });
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

class Utf8Corpus : public benchmark::Fixture {
 public:
  std::string data = MakeUtf8Corpus();
};

class ProseCorpus : public benchmark::Fixture {
 public:
  std::string data = MakeWordsCorpus(LatinLetters());
};

class CyrillicCorpus : public benchmark::Fixture {
 public:
  std::string data = MakeWordsCorpus(CyrillicLetters());
};

}  // namespace

BENCHMARK_DEFINE_F(MixedCorpus, BmFunction)(benchmark::State& state) {
  RunFunction(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmSplit)(benchmark::State& state) {
  RunSplit(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmPattern)(benchmark::State& state) {
  RunPattern(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmPatternLiterals)(benchmark::State& state) {
  RunPatternWith(state, data, ", |! |-_");
}
BENCHMARK_DEFINE_F(MixedCorpus, BmPatternLiteralsRegex)
(benchmark::State& state) { RunPatternWith(state, data, "(?:, |! |-_){1}"); }
BENCHMARK_DEFINE_F(MixedCorpus, BmPatternRunes)(benchmark::State& state) {
  RunPatternWith(state, data, "[,;§]");
}
BENCHMARK_DEFINE_F(MixedCorpus, BmPatternRunesRegex)(benchmark::State& state) {
  RunPatternWith(state, data, "(?:[,;§]){1}");
}
BENCHMARK_DEFINE_F(MixedCorpus, BmText)(benchmark::State& state) {
  RunText(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmFunction)(benchmark::State& state) {
  RunFunction(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmSplit)(benchmark::State& state) {
  RunSplit(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmPattern)(benchmark::State& state) {
  RunPattern(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmText)(benchmark::State& state) {
  RunText(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmSplitAsciiBytes)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::AsciiBytes);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmSplitAlnum)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::Alnum);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmSplit)(benchmark::State& state) {
  RunSplit(state, data);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmSplitAsciiBytes)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::AsciiBytes);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmSplitAlnum)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::Alnum);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmText)(benchmark::State& state) {
  RunText(state, data);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmSplit)(benchmark::State& state) {
  RunSplit(state, data);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmSplitAsciiBytes)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::AsciiBytes);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmSplitAlnum)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::Alnum);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmText)(benchmark::State& state) {
  RunText(state, data);
}
BENCHMARK_DEFINE_F(CyrillicCorpus, BmSplit)(benchmark::State& state) {
  RunSplit(state, data);
}
BENCHMARK_DEFINE_F(CyrillicCorpus, BmSplitAsciiBytes)
(benchmark::State& state) { RunSplit(state, data, SplitChars::AsciiBytes); }
BENCHMARK_DEFINE_F(CyrillicCorpus, BmSplitAlnum)(benchmark::State& state) {
  RunSplit(state, data, SplitChars::Alnum);
}
BENCHMARK_DEFINE_F(CyrillicCorpus, BmText)(benchmark::State& state) {
  RunText(state, data);
}

BENCHMARK_DEFINE_F(MixedCorpus, BmKernelAscii)(benchmark::State& state) {
  RunKernel<Kernel::Ascii>(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmKernelKeepNonAscii)
(benchmark::State& state) { RunKernel<Kernel::KeepNonAscii>(state, data); }
BENCHMARK_DEFINE_F(MixedCorpus, BmKernelUnicode)(benchmark::State& state) {
  RunKernel<Kernel::Unicode>(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmKernelText)(benchmark::State& state) {
  RunKernel<Kernel::Text>(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmKernelAscii)(benchmark::State& state) {
  RunKernel<Kernel::Ascii>(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmKernelKeepNonAscii)
(benchmark::State& state) { RunKernel<Kernel::KeepNonAscii>(state, data); }
BENCHMARK_DEFINE_F(LongTokenCorpus, BmKernelUnicode)(benchmark::State& state) {
  RunKernel<Kernel::Unicode>(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmKernelText)(benchmark::State& state) {
  RunKernel<Kernel::Text>(state, data);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmKernelAscii)(benchmark::State& state) {
  RunKernel<Kernel::Ascii>(state, data);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmKernelKeepNonAscii)
(benchmark::State& state) { RunKernel<Kernel::KeepNonAscii>(state, data); }
BENCHMARK_DEFINE_F(Utf8Corpus, BmKernelUnicode)(benchmark::State& state) {
  RunKernel<Kernel::Unicode>(state, data);
}
BENCHMARK_DEFINE_F(Utf8Corpus, BmKernelText)(benchmark::State& state) {
  RunKernel<Kernel::Text>(state, data);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmKernelAscii)(benchmark::State& state) {
  RunKernel<Kernel::Ascii>(state, data);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmKernelKeepNonAscii)
(benchmark::State& state) { RunKernel<Kernel::KeepNonAscii>(state, data); }
BENCHMARK_DEFINE_F(ProseCorpus, BmKernelUnicode)(benchmark::State& state) {
  RunKernel<Kernel::Unicode>(state, data);
}
BENCHMARK_DEFINE_F(ProseCorpus, BmKernelText)(benchmark::State& state) {
  RunKernel<Kernel::Text>(state, data);
}
BENCHMARK_DEFINE_F(CyrillicCorpus, BmKernelAscii)(benchmark::State& state) {
  RunKernel<Kernel::Ascii>(state, data);
}
BENCHMARK_DEFINE_F(CyrillicCorpus, BmKernelKeepNonAscii)
(benchmark::State& state) { RunKernel<Kernel::KeepNonAscii>(state, data); }
BENCHMARK_DEFINE_F(CyrillicCorpus, BmKernelUnicode)(benchmark::State& state) {
  RunKernel<Kernel::Unicode>(state, data);
}
BENCHMARK_DEFINE_F(CyrillicCorpus, BmKernelText)(benchmark::State& state) {
  RunKernel<Kernel::Text>(state, data);
}

BENCHMARK_DEFINE_F(MixedCorpus, BmEmitCopy)(benchmark::State& state) {
  RunEmit<Fold::None>(state, data);
}
BENCHMARK_DEFINE_F(MixedCorpus, BmEmitFoldPerTokenAbsl)
(benchmark::State& state) { RunEmit<Fold::PerTokenAbsl>(state, data); }
BENCHMARK_DEFINE_F(MixedCorpus, BmEmitFoldPerTokenExact)
(benchmark::State& state) { RunEmit<Fold::PerTokenExact>(state, data); }
BENCHMARK_DEFINE_F(MixedCorpus, BmEmitFoldPerValueAbsl)
(benchmark::State& state) { RunEmit<Fold::PerValueAbsl>(state, data); }
BENCHMARK_DEFINE_F(LongTokenCorpus, BmEmitCopy)(benchmark::State& state) {
  RunEmit<Fold::None>(state, data);
}
BENCHMARK_DEFINE_F(LongTokenCorpus, BmEmitFoldPerTokenAbsl)
(benchmark::State& state) { RunEmit<Fold::PerTokenAbsl>(state, data); }
BENCHMARK_DEFINE_F(LongTokenCorpus, BmEmitFoldPerTokenExact)
(benchmark::State& state) { RunEmit<Fold::PerTokenExact>(state, data); }
BENCHMARK_DEFINE_F(LongTokenCorpus, BmEmitFoldPerValueAbsl)
(benchmark::State& state) { RunEmit<Fold::PerValueAbsl>(state, data); }

BENCHMARK_REGISTER_F(MixedCorpus, BmPatternLiterals);
BENCHMARK_REGISTER_F(MixedCorpus, BmPatternLiteralsRegex);
BENCHMARK_REGISTER_F(MixedCorpus, BmPatternRunes);
BENCHMARK_REGISTER_F(MixedCorpus, BmPatternRunesRegex);
BENCHMARK_REGISTER_F(MixedCorpus, BmEmitCopy);
BENCHMARK_REGISTER_F(MixedCorpus, BmEmitFoldPerTokenAbsl);
BENCHMARK_REGISTER_F(MixedCorpus, BmEmitFoldPerTokenExact);
BENCHMARK_REGISTER_F(MixedCorpus, BmEmitFoldPerValueAbsl);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmEmitCopy);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmEmitFoldPerTokenAbsl);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmEmitFoldPerTokenExact);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmEmitFoldPerValueAbsl);

BENCHMARK_REGISTER_F(MixedCorpus, BmFunction)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(MixedCorpus, BmSplit)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(MixedCorpus, BmPattern)->Arg(0);
BENCHMARK_REGISTER_F(MixedCorpus, BmText)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmFunction)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmSplit)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmPattern)->Arg(0);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmText)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(MixedCorpus, BmSplitAsciiBytes)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(MixedCorpus, BmSplitAlnum)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(Utf8Corpus, BmSplit)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(Utf8Corpus, BmSplitAsciiBytes)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(Utf8Corpus, BmSplitAlnum)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(Utf8Corpus, BmText)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(ProseCorpus, BmSplit)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(ProseCorpus, BmSplitAsciiBytes)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(ProseCorpus, BmSplitAlnum)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(ProseCorpus, BmText)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmSplit)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmSplitAsciiBytes)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmSplitAlnum)->Arg(0)->Arg(1);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmText)->Arg(0)->Arg(1);

BENCHMARK_REGISTER_F(MixedCorpus, BmKernelAscii);
BENCHMARK_REGISTER_F(MixedCorpus, BmKernelKeepNonAscii);
BENCHMARK_REGISTER_F(MixedCorpus, BmKernelUnicode);
BENCHMARK_REGISTER_F(MixedCorpus, BmKernelText);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmKernelAscii);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmKernelKeepNonAscii);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmKernelUnicode);
BENCHMARK_REGISTER_F(LongTokenCorpus, BmKernelText);
BENCHMARK_REGISTER_F(Utf8Corpus, BmKernelAscii);
BENCHMARK_REGISTER_F(Utf8Corpus, BmKernelKeepNonAscii);
BENCHMARK_REGISTER_F(Utf8Corpus, BmKernelUnicode);
BENCHMARK_REGISTER_F(Utf8Corpus, BmKernelText);
BENCHMARK_REGISTER_F(ProseCorpus, BmKernelAscii);
BENCHMARK_REGISTER_F(ProseCorpus, BmKernelKeepNonAscii);
BENCHMARK_REGISTER_F(ProseCorpus, BmKernelUnicode);
BENCHMARK_REGISTER_F(ProseCorpus, BmKernelText);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmKernelAscii);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmKernelKeepNonAscii);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmKernelUnicode);
BENCHMARK_REGISTER_F(CyrillicCorpus, BmKernelText);

BENCHMARK(BmSmallInput)->Arg(8)->Arg(16)->Arg(24)->Arg(31)->Arg(48)->Arg(64);

BENCHMARK_MAIN();
