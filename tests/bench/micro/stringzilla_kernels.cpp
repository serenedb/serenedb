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

#include <benchmark/benchmark.h>
#include <stringzilla/utf8_norm/serial.h>
#if defined(__x86_64__)
#include <stringzilla/utf8_norm/icelake.h>
#include <stringzilla/utf8_norm/skylake.h>
#endif

#include <algorithm>
#include <cstdint>
#include <initializer_list>
#include <iresearch/analysis/text/classify/block_masks.hpp>
#include <iresearch/analysis/text/normalize/normalize.hpp>
#include <iresearch/analysis/text/sz/stringzilla.hpp>
#include <iresearch/analysis/text/words/split_by_non_alpha.hpp>
#include <iresearch/utils/utf8_utils.hpp>
#include <random>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace {

using namespace irs::analysis;

constexpr size_t kCorpusBytes = 1u << 20;
constexpr size_t kDocumentBytes = 2048;
constexpr sz_normal_form_t kForm = sz_normal_form_nfc_k;

using Values = std::vector<std::string>;

std::string Encode(uint32_t cp) {
  std::string out(irs::utf8_utils::kMaxCharSize, '\0');
  out.resize(irs::utf8_utils::FromChar32(
    cp, reinterpret_cast<irs::byte_type*>(out.data())));
  return out;
}

std::vector<std::string> Letters(
  std::initializer_list<std::pair<uint32_t, uint32_t>> ranges,
  std::initializer_list<std::string_view> extra = {}) {
  std::vector<std::string> out;
  for (const auto [lo, hi] : ranges) {
    for (uint32_t cp = lo; cp <= hi; ++cp) {
      out.push_back(Encode(cp));
    }
  }
  for (const auto piece : extra) {
    out.emplace_back(piece);
  }
  return out;
}

struct Script {
  std::string_view name;
  std::vector<std::string> letters;
  std::vector<std::string> separators;
};

std::vector<Script> MakeScripts() {
  return {
    {"ascii",
     Letters({{'a', 'z'}, {'a', 'z'}, {'A', 'Z'}}),
     {" ", " ", " ", " ", " ", ", ", ". ", "\n"}},
    {"latin",
     Letters({{'a', 'z'}, {'a', 'z'}, {0xC0, 0xD6}, {0xD8, 0xF6}, {0xF8, 0xFF}},
             {"e\xCC\x81", "u\xCC\x88", "a\xCC\x8A"}),
     {" ", " ", " ", " ", " ", ", ", ". ", "\n", "\xC2\xA0"}},
    {"cyrillic",
     Letters({{0x410, 0x44F}}),
     {" ", " ", " ", " ", " ", ", ", ". ", "\n", "\xC2\xA0"}},
    {"cjk",
     Letters({{0x4E00, 0x4FFF}, {0x3041, 0x3096}}),
     {"\xE3\x80\x81", "\xE3\x80\x82", "\xE3\x80\x80", "\n"}},
    {"emoji",
     Letters(
       {{'a', 'z'}},
       {"\xF0\x9F\x91\x8D\xF0\x9F\x8F\xBD", "\xF0\x9F\x87\xA9\xF0\x9F\x87\xAA",
        "\xF0\x9F\x91\xA8\xE2\x80\x8D\xF0\x9F\x91\xA9\xE2\x80\x8D\xF0\x9F"
        "\x91\xA7",
        "e\xCC\x81"}),
     {" ", " ", " ", ", ", ". ", "\n"}},
  };
}

enum class Shape : uint8_t {
  Tokens,
  Phrases,
  Documents,
};

constexpr std::string_view ShapeName(Shape shape) noexcept {
  switch (shape) {
    case Shape::Tokens:
      return "tokens";
    case Shape::Phrases:
      return "phrases";
    case Shape::Documents:
      return "documents";
  }
  return {};
}

struct Corpus {
  std::string_view name;
  Values tokens;
  Values phrases;
  Values documents;

  const Values& Get(Shape shape) const noexcept {
    switch (shape) {
      case Shape::Tokens:
        return tokens;
      case Shape::Phrases:
        return phrases;
      case Shape::Documents:
        return documents;
    }
    return tokens;
  }
};

Corpus MakeCorpus(const Script& script) {
  std::mt19937_64 gen{0x5eed};
  Corpus corpus{.name = script.name};
  std::string phrase;
  std::string document;
  size_t phrase_words = 1 + gen() % 6;
  size_t total = 0;
  while (total < kCorpusBytes) {
    std::string token;
    const size_t len = 1 + gen() % 10;
    for (size_t i = 0; i < len; ++i) {
      token += script.letters[gen() % script.letters.size()];
    }
    const auto& separator = script.separators[gen() % script.separators.size()];
    phrase += token;
    document += token;
    corpus.tokens.push_back(std::move(token));
    if (--phrase_words == 0) {
      corpus.phrases.push_back(std::move(phrase));
      phrase.clear();
      phrase_words = 1 + gen() % 6;
    } else {
      phrase += separator;
    }
    document += separator;
    if (document.size() >= kDocumentBytes) {
      total += document.size();
      corpus.documents.push_back(std::move(document));
      document.clear();
    }
  }
  return corpus;
}

const std::vector<Corpus>& Corpora() {
  static const std::vector<Corpus> kCorpora = [] {
    std::vector<Corpus> out;
    for (const auto& script : MakeScripts()) {
      out.push_back(MakeCorpus(script));
    }
    return out;
  }();
  return kCorpora;
}

size_t MaxSize(const Values& values) noexcept {
  size_t out = 0;
  for (const auto& v : values) {
    out = std::max(out, v.size());
  }
  return out;
}

void SetBytes(benchmark::State& state, const Values& values) {
  size_t bytes = 0;
  for (const auto& v : values) {
    bytes += v.size();
  }
  state.SetBytesProcessed(static_cast<int64_t>(state.iterations()) *
                          static_cast<int64_t>(bytes));
}

using Segmenter = sz_size_t (*)(sz_cptr_t, sz_size_t, sz_size_t*, sz_size_t*,
                                sz_size_t, sz_size_t*);
using Folder = sz_size_t (*)(sz_cptr_t, sz_size_t, sz_ptr_t);
using Normalizer = sz_size_t (*)(sz_cptr_t, sz_size_t, sz_normal_form_t,
                                 sz_ptr_t);
using QuickCheck = sz_cptr_t (*)(sz_cptr_t, sz_size_t, sz_normal_form_t);

template<Segmenter Fn>
size_t CountSegments(std::string_view v) {
  constexpr size_t kBatch = 64;
  sz_size_t starts[kBatch];
  sz_size_t lengths[kBatch];
  size_t count = 0;
  size_t offset = 0;
  while (offset < v.size()) {
    sz_size_t consumed = 0;
    count += Fn(v.data() + offset, v.size() - offset, starts, lengths, kBatch,
                &consumed);
    if (consumed == 0) {
      break;
    }
    offset += consumed;
  }
  return count;
}

template<Segmenter Fn>
void BmSegments(benchmark::State& state, const Values& values) {
  size_t segments = 0;
  for (auto _ : state) {
    for (const auto& v : values) {
      segments += CountSegments<Fn>(v);
    }
  }
  benchmark::DoNotOptimize(segments);
  SetBytes(state, values);
}

void BmNonSpaceRuns(benchmark::State& state, const Values& values) {
  size_t runs = 0;
  for (auto _ : state) {
    for (const auto& v : values) {
      words::SplitByNonSpace(
        duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())},
        [&](size_t, size_t) { ++runs; });
    }
  }
  benchmark::DoNotOptimize(runs);
  SetBytes(state, values);
}

void BmAlnumRuns(benchmark::State& state, const Values& values) {
  size_t runs = 0;
  for (auto _ : state) {
    for (const auto& v : values) {
      words::SplitByNonAlnum<false>(
        duckdb::string_t{v.data(), static_cast<uint32_t>(v.size())},
        [&](size_t, size_t) { ++runs; });
    }
  }
  benchmark::DoNotOptimize(runs);
  SetBytes(state, values);
}

template<Folder Fn>
void BmFold(benchmark::State& state, const Values& values) {
  std::string out(MaxSize(values) * sz::kFoldGrowth + 64, '\0');
  for (auto _ : state) {
    for (const auto& v : values) {
      benchmark::DoNotOptimize(Fn(v.data(), v.size(), out.data()));
    }
  }
  SetBytes(state, values);
}

template<Normalizer Fn>
void BmNorm(benchmark::State& state, const Values& values) {
  std::string out(normalize::Bound<kForm>(MaxSize(values)), '\0');
  for (auto _ : state) {
    for (const auto& v : values) {
      benchmark::DoNotOptimize(Fn(v.data(), v.size(), kForm, out.data()));
    }
  }
  SetBytes(state, values);
}

template<QuickCheck Fn>
void BmQuickCheck(benchmark::State& state, const Values& values) {
  size_t hits = 0;
  for (auto _ : state) {
    for (const auto& v : values) {
      hits += Fn(v.data(), v.size(), kForm) != nullptr;
    }
  }
  benchmark::DoNotOptimize(hits);
  SetBytes(state, values);
}

void BmQuickCheckOurs(benchmark::State& state, const Values& values) {
  size_t hits = 0;
  for (auto _ : state) {
    for (const auto& v : values) {
      hits += normalize::Denormalized<kForm>(v.data(), v.size());
    }
  }
  benchmark::DoNotOptimize(hits);
  SetBytes(state, values);
}

enum class Isa : uint8_t {
  Any,
  Skylake,
  Icelake,
};

bool Supported(Isa isa) noexcept {
#if defined(__x86_64__)
  if (isa == Isa::Icelake) {
    return sz::HasAvx512();
  }
  if (isa == Isa::Skylake) {
    static const bool kHas =
      __builtin_cpu_supports("avx512f") && __builtin_cpu_supports("avx512vl") &&
      __builtin_cpu_supports("avx512bw") && __builtin_cpu_supports("bmi2");
    return kHas;
  }
#endif
  return isa == Isa::Any;
}

using Bench = void (*)(benchmark::State&, const Values&);

struct Candidate {
  std::string_view kernel;
  std::string_view backend;
  Bench bench;
  Isa isa;
  bool segmenter;
};

std::vector<Candidate> Candidates() {
  return {
#if defined(__x86_64__)
    {"graphemes", "serial", BmSegments<sz_utf8_graphemes_serial>, Isa::Any,
     true},
    {"graphemes", "haswell", BmSegments<sz_utf8_graphemes_haswell>, Isa::Any,
     true},
    {"graphemes", "icelake", BmSegments<sz_utf8_graphemes_icelake>,
     Isa::Icelake, true},
    {"sentences", "serial", BmSegments<sz_utf8_sentences_serial>, Isa::Any,
     true},
    {"sentences", "haswell", BmSegments<sz_utf8_sentences_haswell>, Isa::Any,
     true},
    {"sentences", "icelake", BmSegments<sz_utf8_sentences_icelake>,
     Isa::Icelake, true},
    {"newlines", "serial", BmSegments<sz_utf8_newlines_serial>, Isa::Any, true},
    {"newlines", "haswell", BmSegments<sz_utf8_newlines_haswell>, Isa::Any,
     true},
    {"newlines", "icelake", BmSegments<sz_utf8_newlines_icelake>, Isa::Icelake,
     true},
    {"whitespace", "serial", BmSegments<sz_utf8_whitespaces_serial>, Isa::Any,
     true},
    {"whitespace", "haswell", BmSegments<sz_utf8_whitespaces_haswell>, Isa::Any,
     true},
    {"whitespace", "icelake", BmSegments<sz_utf8_whitespaces_icelake>,
     Isa::Icelake, true},
    {"whitespace", "ours", BmNonSpaceRuns, Isa::Any, true},
    {"alnum", "serial", BmSegments<sz_utf8_delimiters_serial>, Isa::Any, true},
    {"alnum", "haswell", BmSegments<sz_utf8_delimiters_haswell>, Isa::Any,
     true},
    {"alnum", "icelake", BmSegments<sz_utf8_delimiters_icelake>, Isa::Icelake,
     true},
    {"alnum", "ours", BmAlnumRuns, Isa::Any, true},
    {"fold", "serial", BmFold<sz_utf8_uncased_fold_serial>, Isa::Any, false},
    {"fold", "haswell", BmFold<sz_utf8_uncased_fold_haswell>, Isa::Any, false},
    {"fold", "icelake", BmFold<sz_utf8_uncased_fold_icelake>, Isa::Icelake,
     false},
    {"norm", "serial", BmNorm<sz_utf8_norm_serial>, Isa::Any, false},
    {"norm", "haswell", BmNorm<sz_utf8_norm_haswell>, Isa::Any, false},
    {"norm", "skylake", BmNorm<sz_utf8_norm_skylake>, Isa::Skylake, false},
    {"norm", "icelake", BmNorm<sz_utf8_norm_icelake>, Isa::Icelake, false},
    {"quick_check", "serial", BmQuickCheck<sz_utf8_find_denormalized_serial>,
     Isa::Any, false},
    {"quick_check", "haswell", BmQuickCheck<sz_utf8_find_denormalized_haswell>,
     Isa::Any, false},
    {"quick_check", "skylake", BmQuickCheck<sz_utf8_find_denormalized_skylake>,
     Isa::Skylake, false},
    {"quick_check", "icelake", BmQuickCheck<sz_utf8_find_denormalized_icelake>,
     Isa::Icelake, false},
    {"quick_check", "ours", BmQuickCheckOurs, Isa::Any, false},
#elif defined(__aarch64__)
    {"graphemes", "serial", BmSegments<sz_utf8_graphemes_serial>, Isa::Any,
     true},
    {"graphemes", "neon", BmSegments<sz_utf8_graphemes_neon>, Isa::Any, true},
    {"sentences", "serial", BmSegments<sz_utf8_sentences_serial>, Isa::Any,
     true},
    {"sentences", "neon", BmSegments<sz_utf8_sentences_neon>, Isa::Any, true},
    {"newlines", "serial", BmSegments<sz_utf8_newlines_serial>, Isa::Any, true},
    {"newlines", "neon", BmSegments<sz_utf8_newlines_neon>, Isa::Any, true},
    {"whitespace", "serial", BmSegments<sz_utf8_whitespaces_serial>, Isa::Any,
     true},
    {"whitespace", "neon", BmSegments<sz_utf8_whitespaces_neon>, Isa::Any,
     true},
    {"whitespace", "ours", BmNonSpaceRuns, Isa::Any, true},
    {"alnum", "serial", BmSegments<sz_utf8_delimiters_serial>, Isa::Any, true},
    {"alnum", "neon", BmSegments<sz_utf8_delimiters_neon>, Isa::Any, true},
    {"alnum", "ours", BmAlnumRuns, Isa::Any, true},
    {"fold", "serial", BmFold<sz_utf8_uncased_fold_serial>, Isa::Any, false},
    {"fold", "neon", BmFold<sz_utf8_uncased_fold_neon>, Isa::Any, false},
    {"norm", "serial", BmNorm<sz_utf8_norm_serial>, Isa::Any, false},
    {"norm", "neon", BmNorm<sz_utf8_norm_neon>, Isa::Any, false},
    {"quick_check", "serial", BmQuickCheck<sz_utf8_find_denormalized_serial>,
     Isa::Any, false},
    {"quick_check", "neon", BmQuickCheck<sz_utf8_find_denormalized_neon>,
     Isa::Any, false},
    {"quick_check", "ours", BmQuickCheckOurs, Isa::Any, false},
#endif
  };
}

void BmCandidate(benchmark::State& state, Candidate candidate,
                 const Values* values) {
  if (!Supported(candidate.isa)) {
    state.SkipWithError("CPU lacks the instruction set of this backend");
    return;
  }
  candidate.bench(state, *values);
}

void Register() {
  constexpr Shape kShapes[2][2] = {{Shape::Tokens, Shape::Documents},
                                   {Shape::Phrases, Shape::Documents}};
  for (const auto& corpus : Corpora()) {
    for (const auto& candidate : Candidates()) {
      for (const auto shape : kShapes[candidate.segmenter]) {
        benchmark::RegisterBenchmark(
          std::string{candidate.kernel} + "/" + std::string{candidate.backend} +
            "/" + std::string{corpus.name} + "_" +
            std::string{ShapeName(shape)},
          BmCandidate, candidate, &corpus.Get(shape));
      }
    }
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
