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

#include <cstring>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/analysis/collation_tokenizer.hpp"
#include "iresearch/analysis/delimited_tokenizer.hpp"
#include "iresearch/analysis/geo_analyzer.hpp"
#include "iresearch/analysis/minhash_tokenizer.hpp"
#include "iresearch/analysis/multi_delimited_tokenizer.hpp"
#include "iresearch/analysis/ngram_tokenizer.hpp"
#include "iresearch/analysis/normalizing_tokenizer.hpp"
#include "iresearch/analysis/path_hierarchy_tokenizer.hpp"
#include "iresearch/analysis/pattern_tokenizer.hpp"
#include "iresearch/analysis/pipeline_tokenizer.hpp"
#include "iresearch/analysis/segmentation_tokenizer.hpp"
#include "iresearch/analysis/shingle_tokenizer.hpp"
#include "iresearch/analysis/solr_synonyms_tokenizer.hpp"
#include "iresearch/analysis/sparse_ngram_tokenizer.hpp"
#include "iresearch/analysis/stemming_tokenizer.hpp"
#include "iresearch/analysis/stopwords_tokenizer.hpp"
#include "iresearch/analysis/keyword_tokenizer.hpp"
#include "iresearch/analysis/text_tokenizer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/union_tokenizer.hpp"
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

const std::vector<std::string>& TextCorpus();

// TextCorpus with a non-ascii sentinel word appended to every value: the
// same maker takes the unicode path by input selection.
const std::vector<std::string>& TextUnicodeCorpus() {
  static const auto corpus = [] {
    auto out = TextCorpus();
    for (auto& v : out) {
      v += " \xCF\x89\xCF\x89\xCF\x89";
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

// English-like comma-separated tokens with MIXED lengths (1-12): CsvCorpus'
// "fieldNNN" tokens are all 6-8 bytes, which makes size-class branches in
// term-view construction predict perfectly; this corpus is the honest arm
// for anything with a size-dependent branch.
const std::vector<std::string>& MixedCsvCorpus() {
  static const auto corpus = [] {
    constexpr uint32_t kLens[] = {1, 2, 2, 3, 3, 3, 4,  4,  5,  5,
                                  6, 6, 7, 7, 8, 9, 10, 11, 12, 3};
    std::mt19937_64 rng{11};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 25; ++f) {
        // Random draw, NOT a periodic index: periodic length sequences are
        // learned by branch predictors and understate misprediction costs.
        const auto len = kLens[rng() % std::size(kLens)];
        for (uint32_t j = 0; j < len; ++j) {
          v += static_cast<char>('a' + rng() % 26);
        }
        v += ',';
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

const std::vector<std::string>& SepCsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199) + ", ";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& MultiSepCsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "field" + std::to_string((i + f * 11) % 199);
        v += (f & 1) ? "; " : ", ";
      }
      v.resize(v.size() - 2);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// One long (15B) separator with a rare first byte: boyer-moore's classic
// friendly regime (skip grows with needle length).
const std::vector<std::string>& LongSepCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 5; ++f) {
        v += "payload_field_value_" + std::to_string((i + f * 11) % 199);
        v += "|====SPLIT====|";
      }
      v.resize(v.size() - 15);
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// memchr worst case / boyer-moore best case: needle a^15|b over pure 'a'
// runs -- every position is a first-byte candidate with a deep failing
// verify.
const std::vector<std::string>& HostileRunCorpus() {
  static const auto corpus = [] {
    const std::string run(48, 'a');
    const std::string needle = std::string(15, 'a') + "b";
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v = run + needle + run + needle + run;
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// Markup-ish text: '<' is frequent, most tags are NOT delimiters (failing
// verifies), several are. Stresses candidate-dense scanning for both the
// one-string and the 16-delimiter sets.
const std::vector<std::string>& MarkupCorpus() {
  static const auto corpus = [] {
    const char* words[] = {"lorem", "ipsum", "dolor", "sit",
                           "amet",  "sed",   "elit"};
    const char* tags[] = {"<div>", "<span>",     "<b>",  "</b>",
                          "<i>",   "</i>",       "<td>", "<tr>",
                          "<br>",  "</section>", "<em>", "<li>"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 20; ++w) {
        v += words[(i * 7 + w * 3) % 7];
        v += tags[(i * 5 + w * 11) % 12];
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// The multi-string scan's worst construction: 24 delimiters all sharing '<',
// 6-byte shared prefixes within each family, and a corpus where most tags
// are near-misses -- every candidate position pays deep failing verifies.
const std::vector<std::string>& HardTagCorpus() {
  static const auto corpus = [] {
    const char* words[] = {"data", "text", "node", "item", "cell"};
    const char* near[] = {"</sect99>",  "</sectXY>", "</longer>", "</longXY>",
                          "<metadata>", "<metaXY>",  "</secret>", "</sect>"};
    const char* hits[] = {"</sect03>", "</long05>", "<meta02>", "</sect11>"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t w = 0; w < 16; ++w) {
        v += words[(i + w) % 5];
        if ((i * 3 + w) % 4 == 0) {
          v += hits[(i + w) % 4];
        } else {
          v += near[(i * 7 + w) % 8];
        }
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

// Prose with eight mixed separators; " - " and " | " start with a space,
// which is everywhere -- dense first-byte candidates with failing verifies.
const std::vector<std::string>& MixedSepProseCorpus() {
  static const auto corpus = [] {
    const char* seps[] = {", ", "; ", ": ", " - ", " | ", "\t", "--", "\r\n"};
    const char* words[] = {"alpha", "beta gamma", "delta", "epsilon zeta",
                           "eta",   "theta iota", "kappa"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += words[(i * 7 + f * 3) % 7];
        v += seps[(i + f * 5) % 8];
      }
      out.push_back(std::move(v));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoJsonPointCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const double lng = -180.0 + 360.0 * double((i * 131) % 1000) / 1000.0;
      const double lat = -85.0 + 170.0 * double((i * 173) % 997) / 997.0;
      out.push_back(R"({"type":"Point","coordinates":[)" + std::to_string(lng) +
                    "," + std::to_string(lat) + "]}");
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoWkbPointCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const double lng = -180.0 + 360.0 * double((i * 131) % 1000) / 1000.0;
      const double lat = -85.0 + 170.0 * double((i * 173) % 997) / 997.0;
      std::string wkb;
      const uint8_t le = 1;
      const uint32_t type = 1;
      wkb.append(reinterpret_cast<const char*>(&le), 1);
      wkb.append(reinterpret_cast<const char*>(&type), 4);
      wkb.append(reinterpret_cast<const char*>(&lng), 8);
      wkb.append(reinterpret_cast<const char*>(&lat), 8);
      out.push_back(std::move(wkb));
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoPointArrayCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      const double lng = -180.0 + 360.0 * double((i * 131) % 1000) / 1000.0;
      const double lat = -85.0 + 170.0 * double((i * 173) % 997) / 997.0;
      out.push_back("[" + std::to_string(lat) + ", " + std::to_string(lng) +
                    "]");
    }
    return out;
  }();
  return corpus;
}

const std::vector<std::string>& GeoJsonShapeCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    const size_t n = kValues / 8;
    out.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      const double lng = -170.0 + 340.0 * double((i * 131) % 499) / 499.0;
      const double lat = -80.0 + 160.0 * double((i * 173) % 503) / 503.0;
      const double d = 0.05 + 0.001 * double(i % 40);
      const auto p = [](double x, double y) {
        return "[" + std::to_string(x) + "," + std::to_string(y) + "]";
      };
      out.push_back(R"({"type":"Polygon","coordinates":[[)" +
                    p(lng - d, lat - d) + "," + p(lng + d, lat - d) + "," +
                    p(lng + d, lat + d) + "," + p(lng - d, lat + d) + "," +
                    p(lng - d, lat - d) + "]]}");
    }
    return out;
  }();
  return corpus;
}

// Like CsvCorpus but with long (>12B, non-inline) tokens, so a producer emits
// string_t views into the value that the accumulator would otherwise copy --
// the case value-view retention targets.
const std::vector<std::string>& LongCsvCorpus() {
  static const auto corpus = [] {
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      std::string v;
      for (size_t f = 0; f < 15; ++f) {
        v += "longfieldtokenvalue_" + std::to_string((i + f * 11) % 199) +
             "_suffix,";
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
  explicit BenchSink(TokenLayout l) : layout{l} { writer.Bind(*this, this); }

  TokenLayout layout;

  // Branch-free per-term touch: folding the raw 16-byte handle keeps every
  // emitted slot observable without GetData's per-token IsInlined branch,
  // which inflated gram-heavy arms by 10-21% of their runtime.
  void Consume(TokenBatch& batch, DocRuns) final {
    uint64_t h = 0;
    for (uint32_t i = 0; i < batch.count; ++i) {
      uint64_t lo;
      uint64_t hi;
      std::memcpy(&lo, &batch.terms[i], sizeof lo);
      std::memcpy(&hi, reinterpret_cast<const char*>(&batch.terms[i]) + 8,
                  sizeof hi);
      h ^= lo + hi;
    }
    benchmark::DoNotOptimize(h);
    consumed += batch.count;
  }

  TokenSink writer;
  size_t consumed = 0;
};

using Factory = std::function<Tokenizer::ptr()>;
using CorpusFn = const std::vector<std::string>& (*)();

std::vector<duckdb::string_t> AsValues(const std::vector<std::string>& corpus) {
  std::vector<duckdb::string_t> values;
  values.reserve(corpus.size());
  for (const auto& v : corpus) {
    values.emplace_back(v.c_str(), static_cast<uint32_t>(v.size()));
  }
  return values;
}

void BM_Fill(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = make();
  const auto values = AsValues(corpus_fn());
  BenchSink sink{TokenLayout::Terms};
  for (auto _ : state) {
    for (const auto& v : values) {
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
    h ^= dict.Insert(buf.terms[i]);
  }
  return h;
}

// Full-pipeline modes: tokens end as dictionary ids, either fused at emit
// (dict-carrying sink) or resolved at flush from the byte batch (the old
// two-pass pipeline).
void BM_FillResolve(benchmark::State& state, Factory make, CorpusFn corpus_fn) {
  auto stream = make();
  const auto values = AsValues(corpus_fn());
  auto mem = InverterMemory::Default();
  FieldsInverter inv{mem};
  auto* field = inv.Emplace(1, IndexFeatures::None);
  size_t consumed = 0;
  struct ResolveSink final : TokenConsumer {
    ResolveSink(TermDictionary& d, size_t& c) : dict(d), c(c) {
      writer.Bind(*this, this);
    }
    void Consume(TokenBatch& batch, DocRuns) final {
      benchmark::DoNotOptimize(ConsumeBatch(dict, batch));
      c += batch.count;
    }
    TermDictionary& dict;
    size_t& c;
    TokenSink writer;
  } sink{field->Dictionary(), consumed};
  for (auto _ : state) {
    for (const auto& v : values) {
      if (!stream->Fill(v, sink.writer, TokenLayout::Terms)) {
        continue;
      }
    }
    sink.writer.Finish();
  }
  state.counters["tokens/s"] = benchmark::Counter(static_cast<double>(consumed),
                                                  benchmark::Counter::kIsRate);
}

Tokenizer::ptr MakeKeyword() { return KeywordTokenizer::Make({}); }

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

Tokenizer::ptr MakeGeoJsonPoint() {
  return GeoJsonAnalyzer::Make({.type = GeoJsonAnalyzer::Type::Point,
                                .coding = GeoJsonAnalyzer::Coding::S2Point});
}

Tokenizer::ptr MakeGeoJsonShape() {
  return GeoJsonAnalyzer::Make({.type = GeoJsonAnalyzer::Type::Shape,
                                .coding = GeoJsonAnalyzer::Coding::Source});
}

Tokenizer::ptr MakeGeoJsonPointWkb() {
  auto stream = MakeGeoJsonPoint();
  static_cast<GeoJsonAnalyzer*>(stream.get())->SetWkbInput(true);
  return stream;
}

Tokenizer::ptr MakeGeoPoint() { return GeoPointAnalyzer::Make({}); }

Tokenizer::ptr MakeGeoPointWkb() {
  auto stream = GeoPointAnalyzer::Make({});
  static_cast<GeoPointAnalyzer*>(stream.get())->SetWkbInput(true);
  return stream;
}

Tokenizer::ptr MakeStem() {
  StemmingTokenizer::Options opts;
  opts.locale = icu::Locale::createFromName("en");
  return StemmingTokenizer::Make(std::move(opts));
}

// English-like single words, one per value: realistic (first byte, length)
// collision pressure against an English stopword set -- the regime where the
// negative prefilter earns or loses its keep.
const std::vector<std::string>& LowerWordCorpus() {
  static const auto corpus = [] {
    const char* words[] = {
      "the",     "quick", "brown",     "fox",    "jumps",    "over",
      "lazy",    "dog",   "while",     "seven",  "wizards",  "brew",
      "potions", "under", "moonlight", "and",    "watch",    "distant",
      "ships",   "sail",  "running",   "jumped", "mountain", "river",
      "stone",   "cloud", "wind",      "fire",   "water",    "earth",
      "this",    "those"};
    std::vector<std::string> out;
    out.reserve(kValues);
    for (size_t i = 0; i < kValues; ++i) {
      out.push_back(words[(i * 7) % std::size(words)]);
    }
    return out;
  }();
  return corpus;
}

Tokenizer::ptr MakeStopwordsEn() {
  return StopwordsTokenizer::Make(
    {.mask = {"the",   "and",  "over", "under", "a",    "an",    "as",
              "at",    "be",   "by",   "for",   "from", "has",   "he",
              "in",    "is",   "it",   "its",   "of",   "on",    "that",
              "to",    "was",  "were", "will",  "with", "this",  "but",
              "they",  "have", "had",  "what",  "when", "where", "who",
              "which", "why",  "how",  "all",   "each"}});
}

Tokenizer::ptr MakeStopwords() {
  return StopwordsTokenizer::Make({.mask = {"the", "and", "over", "under"}});
}

Tokenizer::ptr MakeDelimiter() { return DelimitedTokenizer::Make({","}); }

Tokenizer::ptr MakeUnion2() {
  std::vector<Tokenizer::ptr> subs;
  subs.push_back(MakeDelimiter());
  subs.push_back(KeywordTokenizer::Make({}));
  return std::make_unique<UnionTokenizer>(std::move(subs));
}

Tokenizer::ptr MakeShingle() {
  ShingleTokenizer::Options opts;
  opts.min_shingle_size = 2;
  opts.max_shingle_size = 4;
  return std::make_unique<ShingleTokenizer>(MakeDelimiter(), std::move(opts));
}

Tokenizer::ptr MakeMinHash() {
  return std::make_unique<MinHashTokenizer>(MakeDelimiter(), 8);
}

Tokenizer::ptr MakeMultiDelimiterStr() {
  MultiDelimitedTokenizer::Options opts;
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(", "), 2);
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterStrings() {
  MultiDelimitedTokenizer::Options opts;
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(", "), 2);
  opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>("; "), 2);
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterFrom(std::initializer_list<const char*> ds) {
  MultiDelimitedTokenizer::Options opts;
  for (const char* d : ds) {
    opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(d),
                                 std::strlen(d));
  }
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterStrLong() {
  return MakeMultiDelimiterFrom({"|====SPLIT====|"});
}

Tokenizer::ptr MakeMultiDelimiterStrHostile() {
  return MakeMultiDelimiterFrom({"aaaaaaaaaaaaaaab"});
}

Tokenizer::ptr MakeMultiDelimiterStrTag() {
  return MakeMultiDelimiterFrom({"</section>"});
}

Tokenizer::ptr MakeMultiDelimiterTags() {
  return MakeMultiDelimiterFrom({"</b>", "</i>", "<br>", "<td>", "<tr>", "<em>",
                                 "<li>", "<ul>", "<ol>", "<hr>", "</p>", "</a>",
                                 "</s>", "</u>", "</h2>", "</h3>"});
}

Tokenizer::ptr MakeMultiDelimiterTagsHard() {
  MultiDelimitedTokenizer::Options opts;
  const auto add = [&](const std::string& s) {
    opts.delimiters.emplace_back(reinterpret_cast<const byte_type*>(s.data()),
                                 s.size());
  };
  const auto pad2 = [](int k) {
    return (k < 10 ? "0" : "") + std::to_string(k);
  };
  for (int k = 1; k <= 12; ++k) {
    add("</sect" + pad2(k) + ">");
  }
  for (int k = 1; k <= 8; ++k) {
    add("</long" + pad2(k) + ">");
  }
  for (int k = 1; k <= 4; ++k) {
    add("<meta" + pad2(k) + ">");
  }
  return MultiDelimitedTokenizer::Make(std::move(opts));
}

Tokenizer::ptr MakeMultiDelimiterMixed8() {
  return MakeMultiDelimiterFrom(
    {", ", "; ", ": ", " - ", " | ", "\t", "--", "\r\n"});
}

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

Tokenizer::ptr MakePathHierarchyReverse() {
  return PathHierarchyTokenizer::Make({.reverse = true});
}

Tokenizer::ptr MakePathHierarchyReplace() {
  return PathHierarchyTokenizer::Make({.replacement = "__"});
}

Tokenizer::ptr MakePathHierarchyReverseReplace() {
  return PathHierarchyTokenizer::Make({.replacement = "__", .reverse = true});
}

Tokenizer::ptr MakeNgram(NGramTokenizerBase::InputType input) {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 3;
  opts.max_gram = 3;
  opts.preserve_original = false;
  opts.stream_bytes_type = input;
  return NGramTokenizerBase::Make(std::move(opts));
}

Tokenizer::ptr MakeNgramVariable() {
  NGramTokenizerBase::Options opts;
  opts.min_gram = 2;
  opts.max_gram = 4;
  opts.preserve_original = false;
  opts.stream_bytes_type = NGramTokenizerBase::InputType::Binary;
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
TOKENIZER_BENCH(geojson_point, MakeGeoJsonPoint, GeoJsonPointCorpus);
TOKENIZER_BENCH(geojson_point_wkb, MakeGeoJsonPointWkb, GeoWkbPointCorpus);
TOKENIZER_BENCH(geojson_shape, MakeGeoJsonShape, GeoJsonShapeCorpus);
TOKENIZER_BENCH(geopoint, MakeGeoPoint, GeoPointArrayCorpus);
TOKENIZER_BENCH(geopoint_wkb, MakeGeoPointWkb, GeoWkbPointCorpus);
TOKENIZER_BENCH(stem, MakeStem, WordCorpus);
TOKENIZER_BENCH(stopwords, MakeStopwords, WordCorpus);
TOKENIZER_BENCH(stopwords_en, MakeStopwordsEn, LowerWordCorpus);
TOKENIZER_BENCH(delimiter, MakeDelimiter, CsvCorpus);
TOKENIZER_BENCH(minhash, MakeMinHash, CsvCorpus);
TOKENIZER_BENCH(shingle, MakeShingle, CsvCorpus);
TOKENIZER_BENCH(union_2, MakeUnion2, CsvCorpus);
TOKENIZER_BENCH(delimiter_mixed, MakeDelimiter, MixedCsvCorpus);
TOKENIZER_BENCH(multi_delimiter, MakeMultiDelimiter, CsvCorpus);
TOKENIZER_BENCH(multi_delimiter_mixed, MakeMultiDelimiter, MixedCsvCorpus);
TOKENIZER_BENCH(multi_delimiter_str, MakeMultiDelimiterStr, SepCsvCorpus);
TOKENIZER_BENCH(multi_delimiter_strings, MakeMultiDelimiterStrings,
                MultiSepCsvCorpus);
TOKENIZER_BENCH(multi_delimiter_str_long, MakeMultiDelimiterStrLong,
                LongSepCorpus);
TOKENIZER_BENCH(multi_delimiter_str_hostile, MakeMultiDelimiterStrHostile,
                HostileRunCorpus);
TOKENIZER_BENCH(multi_delimiter_str_tag, MakeMultiDelimiterStrTag,
                MarkupCorpus);
TOKENIZER_BENCH(multi_delimiter_tags, MakeMultiDelimiterTags, MarkupCorpus);
TOKENIZER_BENCH(multi_delimiter_tags_hard, MakeMultiDelimiterTagsHard,
                HardTagCorpus);
TOKENIZER_BENCH(multi_delimiter_mixed8, MakeMultiDelimiterMixed8,
                MixedSepProseCorpus);
TOKENIZER_BENCH(pattern, MakePattern, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, pattern_literal, &MakePatternLiteral, &ColonCorpus)
  ->Unit(benchmark::kMillisecond);
BENCHMARK_CAPTURE(BM_Fill, pattern_literal_regex, &MakePatternLiteralRegex,
                  &ColonCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(path_hierarchy, MakePathHierarchy, PathCorpus);
TOKENIZER_BENCH(path_hierarchy_reverse, MakePathHierarchyReverse, PathCorpus);
TOKENIZER_BENCH(path_hierarchy_replace, MakePathHierarchyReplace, PathCorpus);
TOKENIZER_BENCH(path_hierarchy_reverse_replace, MakePathHierarchyReverseReplace,
                PathCorpus);
TOKENIZER_BENCH(ngram_binary, MakeNgramBinary, TextCorpus);
TOKENIZER_BENCH(ngram_variable, MakeNgramVariable, TextCorpus);
TOKENIZER_BENCH(ngram_utf8, MakeNgramUtf8, TextCorpus);
TOKENIZER_BENCH(sparse_ngram, MakeSparseNgram, TextCorpus);
TOKENIZER_BENCH(wildcard, MakeWildcard, TextCorpus);
TOKENIZER_BENCH(segmentation, MakeSegmentation, TextCorpus);
TOKENIZER_BENCH(text_en, MakeTextEn, TextCorpus);
BENCHMARK_CAPTURE(BM_Fill, text_en_unicode, &MakeTextEn, &TextUnicodeCorpus)
  ->Unit(benchmark::kMillisecond);
TOKENIZER_BENCH(solr_synonyms, MakeSolrSynonyms, SynonymCorpus);
TOKENIZER_BENCH(pipeline_t2, MakePipelineT2, CsvCorpus);
TOKENIZER_BENCH(pipeline_t2_generic_long, MakePipelineT2Generic, LongCsvCorpus);
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
