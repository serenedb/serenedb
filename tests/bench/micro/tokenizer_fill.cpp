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

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/analysis/batch/token_batch.hpp"
#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/path_hierarchy_tokenizer.hpp"
#include "iresearch/analysis/pattern_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/sparse_ngram_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/tokenizers.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/analysis/wordnet_synonyms_tokenizer.hpp"
#include "iresearch/index/inverter/columnar_flush.hpp"
#include "iresearch/index/inverter/fields_inverter.hpp"

namespace {

using namespace irs;
using namespace irs::analysis;

constexpr size_t kValues = 4096;

const std::vector<std::string>& WordCorpus() {
  static const auto corpus = [] {
    const char* roots[] = {"Running",  "Consideration", "Fox",
                           "Jumping",  "Database",      "Tokenizer",
                           "Grateful", "Performance"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      out.push_back(std::string{roots[i % 8]} + std::to_string(i % 97));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& TextCorpus() {
  static const auto corpus = [] {
    const char* vocab[] = {"the",     "quick", "brown",   "fox",   "jumps",
                           "over",    "lazy",  "dog",     "while", "seven",
                           "wizards", "brew",  "potions", "under", "moonlight",
                           "and",     "watch", "distant", "ships", "sail"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 15; ++w) {
        v += vocab[(i * 7 + w * 3) % 20];
        v += ' ';
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& CsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199) + ",";
      }
      v.pop_back();
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& ColonCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199) + "::";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& PathCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t d = 0; d < 8; ++d) {
        v += "/dir" + std::to_string((i + d * 13) % 211);
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& SynonymCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      if (i % 2 == 0) {
        out.push_back("syn" + std::to_string(i % 64) + "a");
      } else {
        out.push_back("unknown" + std::to_string(i));
      }
    }
    return out;
  }();
  return corpus;
}

std::string SolrSynonymsText() {
  std::string text;
  for (size_t i = 0; i < 64; ++i) {
    const auto n = std::to_string(i);
    text += "syn" + n + "a, syn" + n + "b, syn" + n + "c\n";
  }
  return text;
}

std::string WordnetSynonymsText() {
  std::string text;
  for (size_t i = 0; i < 64; ++i) {
    const auto id = std::to_string(100000000 + i);
    const auto n = std::to_string(i);
    text += "s(" + id + ",1,'syn" + n + "a',a,1,0).\n";
    text += "s(" + id + ",2,'syn" + n + "b',a,1,0).\n";
    text += "s(" + id + ",3,'syn" + n + "c',a,1,0).\n";
  }
  return text;
}

class BenchSink final : public TokenConsumer {
 public:
  explicit BenchSink(TokenLayout l) : layout{l}, writer{*this} {}

  TokenLayout layout;

  void Consume(TokenBatch& batch, std::span<const DocRun>) final {
    uint64_t h = 0;
    for (uint32_t i = 0; i < batch.count; ++i) {
      const auto& t = batch.terms[i];
      h ^= reinterpret_cast<uintptr_t>(t.GetData()) + t.GetSize();
    }
    benchmark::DoNotOptimize(h);
    consumed += batch.count;
  }

  TokenWriter writer;
  size_t consumed = 0;
};

using Factory = std::function<Tokenizer::ptr()>;
using CorpusFn = const std::vector<std::string>& (*)();

void BM_Fill(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = make();
  const auto& corpus = corpus_fn();
  BenchSink sink{TokenLayout::Terms};
  for (auto _ : state) {
    for (const auto& v : corpus) {
      if (!stream->Fill(v, sink.writer, sink.layout)) {
        continue;
      }
    }
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(
    static_cast<double>(sink.consumed), benchmark::Counter::kIsRate);
}

void BM_FillColumn(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = make();
  const auto& corpus = corpus_fn();
  std::vector<duckdb::string_t> vals;
  std::vector<doc_id_t> docs;
  vals.reserve(corpus.size());
  docs.reserve(corpus.size());
  for (size_t i = 0; i < corpus.size(); ++i) {
    vals.emplace_back(corpus[i].data(),
                      static_cast<uint32_t>(corpus[i].size()));
    docs.push_back(static_cast<doc_id_t>(i + 1));
  }
  BenchSink sink{TokenLayout::Terms};
  for (auto _ : state) {
    stream->Fill(vals, docs, sink.writer, sink.layout);
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(
    static_cast<double>(sink.consumed), benchmark::Counter::kIsRate);
}

// Shared consume stage replicating the production resolver contract: byte
// batches resolve term-by-term. Noinline so codegen is identical across
// pipelines.
[[gnu::noinline]] uint64_t ConsumeBatch(TermDictionary& dict, TokenBatch& buf) {
  uint64_t h = 0;
  for (uint32_t i = 0; i < buf.count; ++i) {
    h ^= dict.Resolve(buf.terms[i], TermDictionary::TermHash(buf.terms[i]));
  }
  return h;
}

// Full-pipeline modes: tokens end as dictionary ids, either fused at emit
// (dict-carrying sink) or resolved at flush from the byte batch (the old
// two-pass pipeline).
void BM_FillResolve(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = make();
  const auto& corpus = corpus_fn();
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::None);
  size_t consumed = 0;
  struct ResolveSink final : TokenConsumer {
    ResolveSink(TermDictionary& d, size_t& c) : dict(d), c(c), writer{*this} {}
    void Consume(TokenBatch& batch, std::span<const DocRun>) final {
      benchmark::DoNotOptimize(ConsumeBatch(dict, batch));
      c += batch.count;
    }
    TermDictionary& dict;
    size_t& c;
    TokenWriter writer;
  } sink{field->Dictionary(), consumed};
  for (auto _ : state) {
    for (const auto& v : corpus) {
      if (!stream->Fill(v, sink.writer, TokenLayout::Terms)) {
        continue;
      }
    }
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(static_cast<double>(consumed),
                                                  benchmark::Counter::kIsRate);
}

Tokenizer::ptr MakeKeyword() { return StringTokenizer::Make({}); }

Tokenizer::ptr MakeNorm() {
  NormalizingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  opts.case_convert = Case::Lower;
  opts.accent = false;
  return NormalizingTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeCollation() {
  CollationTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("de");
  return CollationTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeStem() {
  StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  return StemmingTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeStopwords() {
  return StopwordsTokenizer::Make({.mask = {"the", "and", "over", "under"}});
}

Tokenizer::ptr MakeDelimiter() { return DelimitedTokenizer::Make({","}); }

Tokenizer::ptr MakeMultiDelimiter() {
  MultiDelimitedTokenizer::Options opts;
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(","), 1);
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(";"), 1);
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>("|"), 1);
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakePattern() {
  return PatternTokenizer::Make({.pattern = "\\s+", .group = -1});
}

Tokenizer::ptr MakePatternLiteralImpl(bool force_regex) {
  auto stream = PatternTokenizer::Make({.pattern = "::", .group = -1});
  static_cast<PatternTokenizer*>(stream.get())->ForceRegexPath(force_regex);
  return stream;
}
Tokenizer::ptr MakePatternLiteral() { return MakePatternLiteralImpl(false); }
Tokenizer::ptr MakePatternLiteralRegex() {
  return MakePatternLiteralImpl(true);
}

Tokenizer::ptr MakePathHierarchy() { return PathHierarchyTokenizer::Make({}); }

Tokenizer::ptr MakeNgram(NGramTokenizerBase::InputType input) {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 3;
  opts.max_gram = 3;
  opts.preserve_original = false;
  opts.stream_bytes_type = input;
  return NGramTokenizerBase::Make(std::move(opts));
}

Tokenizer::ptr MakeNgramBinary() {
  return MakeNgram(NGramTokenizerBase::InputType::Binary);
}

Tokenizer::ptr MakeNgramUtf8() {
  return MakeNgram(NGramTokenizerBase::InputType::UTF8);
}

Tokenizer::ptr MakeSparseNgram() { return SparseNGramTokenizer::Make({}); }

Tokenizer::ptr MakeWildcard() { return WildcardAnalyzer::Make({}); }

Tokenizer::ptr MakeSegmentation() { return SegmentationTokenizer::Make({}); }

Tokenizer::ptr MakeTextEn() {
  TextTokenizer::Options o;
  o.locale = icu::Locale::createFromName("en_US.UTF-8");
  o.explicit_stopwords = {"the", "and", "of", "a"};
  o.explicit_stopwords_set = true;
  return TextTokenizer::Make(std::move(o));
}

Tokenizer::ptr MakeTextEnUnicode() {
  auto stream = MakeTextEn();
  static_cast<TextTokenizer*>(stream.get())->ForceUnicodePath(true);
  return stream;
}

Tokenizer::ptr MakeSolrSynonyms() {
  return SolrSynonymsTokenizer::Make({.synonyms_text = SolrSynonymsText()});
}

Tokenizer::ptr MakePipelineT2() {
  PipelineTokenizer::Options popts;
  auto add = [&](TokenizerConfig cfg) {
    popts.children.push_back(std::make_unique<TokenizerConfig>(std::move(cfg)));
  };
  {
    TokenizerConfig c;
    c.config = DelimitedTokenizer::Options{.delimiter = ","};
    add(std::move(c));
  }
  {
    TokenizerConfig c;
    StopwordsTokenizer::Options s;
    s.mask = {"field1", "field3", "field5", "field7"};
    c.config = std::move(s);
    add(std::move(c));
  }
  TokenizerConfig cfg;
  cfg.config = std::move(popts);
  return CreateTokenizer(std::move(cfg));
}

Tokenizer::ptr MakePipelineT2Generic() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  StopwordsTokenizer::Options s;
  s.mask = {"field1", "field3", "field5", "field7"};
  subs.push_back(StopwordsTokenizer::Make(std::move(s)));
  auto pipe = std::make_unique<PipelineTokenizer>(std::move(subs));
  pipe->ForceGenericPath(true);
  return pipe;
}

std::string PipelineSynText() {
  std::string text;
  for (size_t i = 0; i < 16; ++i) {
    const auto n = std::to_string(i * 12 + 2);
    text += "field" + n + " => f" + n + "a, f" + n + "b\n";
  }
  return text;
}

Tokenizer::ptr MakePipelineT2SynImpl(bool force_generic) {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  StopwordsTokenizer::Options s;
  s.mask = {"field1", "field3", "field5", "field7"};
  subs.push_back(StopwordsTokenizer::Make(std::move(s)));
  subs.push_back(
    SolrSynonymsTokenizer::Make({.synonyms_text = PipelineSynText()}));
  auto pipe = std::make_unique<PipelineTokenizer>(std::move(subs));
  pipe->ForceGenericPath(force_generic);
  return pipe;
}

Tokenizer::ptr MakePipelineT2RewriteImpl(bool with_stem, bool force_generic) {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(DelimitedTokenizer::Make({","}));
  {
    NormalizingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    o.case_convert = Case::Lower;
    o.accent = false;
    subs.push_back(NormalizingTokenizer::Make(std::move(o)));
  }
  if (with_stem) {
    StemmingTokenizer::Options o;
    o.locale = icu::Locale::createFromName("en");
    subs.push_back(StemmingTokenizer::Make(std::move(o)));
  }
  auto pipe = std::make_unique<PipelineTokenizer>(std::move(subs));
  pipe->ForceGenericPath(force_generic);
  return pipe;
}
Tokenizer::ptr MakePipelineT2Norm() {
  return MakePipelineT2RewriteImpl(false, false);
}
Tokenizer::ptr MakePipelineT2NormGeneric() {
  return MakePipelineT2RewriteImpl(false, true);
}
Tokenizer::ptr MakePipelineT2Stem() {
  return MakePipelineT2RewriteImpl(true, false);
}
Tokenizer::ptr MakePipelineT2StemGeneric() {
  return MakePipelineT2RewriteImpl(true, true);
}

Tokenizer::ptr MakePipelineT2Syn() { return MakePipelineT2SynImpl(false); }

Tokenizer::ptr MakePipelineT2SynGeneric() {
  return MakePipelineT2SynImpl(true);
}

Tokenizer::ptr MakeWordnetSynonyms() {
  return WordnetSynonymsTokenizer::Make(
    {.synonyms_text = WordnetSynonymsText()});
}

#define TOKENIZER_BENCH(name, factory, corpus)               \
  BENCHMARK_CAPTURE(BM_Fill, name, &factory, &corpus)        \
    ->Unit(benchmark::kMillisecond);                         \
  BENCHMARK_CAPTURE(BM_FillColumn, name, &factory, &corpus)  \
    ->Unit(benchmark::kMillisecond);                         \
  BENCHMARK_CAPTURE(BM_FillResolve, name, &factory, &corpus) \
    ->Unit(benchmark::kMillisecond)

TOKENIZER_BENCH(keyword, MakeKeyword, WordCorpus);
TOKENIZER_BENCH(norm, MakeNorm, WordCorpus);
TOKENIZER_BENCH(collation, MakeCollation, WordCorpus);
TOKENIZER_BENCH(stem, MakeStem, WordCorpus);
TOKENIZER_BENCH(stopwords, MakeStopwords, WordCorpus);
TOKENIZER_BENCH(delimiter, MakeDelimiter, CsvCorpus);
TOKENIZER_BENCH(multi_delimiter, MakeMultiDelimiter, CsvCorpus);
TOKENIZER_BENCH(pattern, MakePattern, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, pattern_literal, &MakePatternLiteral, &ColonCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_literal_regex, &MakePatternLiteralRegex,
                  &ColonCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(path_hierarchy, MakePathHierarchy, PathCorpus);
TOKENIZER_BENCH(ngram_binary, MakeNgramBinary, TextCorpus);
TOKENIZER_BENCH(ngram_utf8, MakeNgramUtf8, TextCorpus);
TOKENIZER_BENCH(sparse_ngram, MakeSparseNgram, TextCorpus);
TOKENIZER_BENCH(wildcard, MakeWildcard, TextCorpus);
TOKENIZER_BENCH(segmentation, MakeSegmentation, TextCorpus);
TOKENIZER_BENCH(text_en, MakeTextEn, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, text_en_unicode, &MakeTextEnUnicode, &TextCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(solr_synonyms, MakeSolrSynonyms, SynonymCorpus);
TOKENIZER_BENCH(pipeline_t2, MakePipelineT2, CsvCorpus);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2_generic, &MakePipelineT2Generic,
                  &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2syn, &MakePipelineT2Syn, &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2syn_generic, &MakePipelineT2SynGeneric,
                  &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2norm, &MakePipelineT2Norm, &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2norm_generic, &MakePipelineT2NormGeneric,
                  &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2stem, &MakePipelineT2Stem, &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pipeline_t2stem_generic, &MakePipelineT2StemGeneric,
                  &CsvCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(wordnet_synonyms, MakeWordnetSynonyms, SynonymCorpus);

}  // namespace

BENCHMARK_MAIN();
