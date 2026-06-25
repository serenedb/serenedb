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
////////////////////////////////////////////////////////////////////////////////

// Micro-benchmark comparing phrase-search strategies on the SAME corpus:
//
//   * classic   -- positional irs::ByPhrase over a Freq+Pos text field
//                  (positions intersected per query: the status-quo path).
//   * shingleA  -- irs::ByPhraseNgram over a Freq-only word-shingle field:
//                  AND of the phrase's shingle terms -> candidates -> verify
//                  contiguity against a stored token blob (no positions).
//   * shingleB  -- irs::ByPhraseNgram over a Freq+Pos word-shingle field:
//                  a positional phrase over the shingle terms, exact by
//                  construction (no verification, no store fetch).
//
// For each strategy it times whole-result iteration of three phrase shapes:
// a 2-word content phrase, a 3-word content phrase, and a stopword-heavy
// phrase (the case shingles are meant to win -- a single rare bigram term
// instead of two enormous stopword posting-list intersections). It also prints
// each index's on-disk size once, while building the shared corpus.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <duckdb/main/config.hpp>
#include <duckdb/main/database.hpp>
#include <filesystem>
#include <iostream>
#include <memory>
#include <random>
#include <span>
#include <string>
#include <vector>

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_ngram_filter.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

using namespace irs;

// --- minimal, gtest-free columnstore wiring (mirrors test_cs_helpers) -------

duckdb::DatabaseInstance& CsDb() {
  static std::unique_ptr<duckdb::DuckDB> kDb = [] {
    duckdb::DBConfig cfg;
    cfg.options.access_mode = duckdb::AccessMode::AUTOMATIC;
    return std::make_unique<duckdb::DuckDB>(":memory:", &cfg);
  }();
  return *kDb->instance;
}

IndexWriterOptions WriterOptions() {
  IndexWriterOptions opts;
  opts.db = &CsDb();
  opts.reader_options.db = &CsDb();
  return opts;
}

IndexReaderOptions ReaderOptions() {
  IndexReaderOptions opts;
  opts.db = &CsDb();
  return opts;
}

void AppendBlob(columnstore::ColumnWriter& cw, doc_id_t doc, bytes_view payload) {
  duckdb::Vector v{duckdb::LogicalType::BLOB, /*capacity=*/1};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
  slots[0] = duckdb::StringVector::AddStringOrBlob(
    v, reinterpret_cast<const char*>(payload.data()), payload.size());
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
  cw.Append(row, v, /*count=*/1);
}

constexpr field_id kStoreId = 1;

// --- whitespace base tokenizer ----------------------------------------------

class WhitespaceTokenizer final
  : public analysis::TypedAnalyzer<WhitespaceTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "bench_whitespace";
  }

  Attribute* GetMutable(TypeInfo::type_id type) noexcept final {
    if (type == Type<TermAttr>::id()) {
      return &_term;
    }
    if (type == Type<IncAttr>::id()) {
      return &_inc;
    }
    return nullptr;
  }

  bool reset(std::string_view data) final {
    _data = data;
    _pos = 0;
    return true;
  }

  bool next() final {
    while (_pos < _data.size() && _data[_pos] == ' ') {
      ++_pos;
    }
    if (_pos >= _data.size()) {
      return false;
    }
    const auto start = _pos;
    while (_pos < _data.size() && _data[_pos] != ' ') {
      ++_pos;
    }
    _term.value = ViewCast<byte_type>(_data.substr(start, _pos - start));
    _inc.value = 1;
    return true;
  }

 private:
  std::string_view _data;
  size_t _pos{0};
  TermAttr _term;
  IncAttr _inc;
};

analysis::ShingleAnalyzer::Options ShingleOpts() {
  return {
    .min_shingle_size = 2,
    .max_shingle_size = 2,
    .output_unigrams = true,
  };
}

// The shingle analyzer's ctor takes the (built) base analyzer separately from
// the rest of its options; the bench always wraps a whitespace tokenizer.
std::unique_ptr<analysis::ShingleAnalyzer> MakeShingle(
  analysis::ShingleAnalyzer::Options opts) {
  return std::make_unique<analysis::ShingleAnalyzer>(
    std::make_unique<WhitespaceTokenizer>(), std::move(opts));
}

// Wider shingles (min=2,max=3): any phrase of 2 OR 3 words is a single exact
// term lookup -- no positions, no verification, no store fetch -- and 4+ word
// phrases AND far more selective trigram candidates before verifying.
analysis::ShingleAnalyzer::Options ShingleOpts3() {
  return {
    .min_shingle_size = 2,
    .max_shingle_size = 3,
    .output_unigrams = true,
  };
}

analysis::ShingleAnalyzer::Options ShingleOpts4() {
  return {
    .min_shingle_size = 2,
    .max_shingle_size = 4,
    .output_unigrams = true,
  };
}

// Adaptive width escalation: dense bigrams everywhere; trigrams only over
// windows containing a frequent word -- width bought exactly where bigram
// selectivity is poor.
analysis::ShingleAnalyzer::Options ShingleOptsAdaptive() {
  auto opts = ShingleOpts3();
  for (auto* w : {"the", "of", "and"}) {
    opts.frequent_words.push_back(
      bstring{ViewCast<byte_type>(std::string_view{w})});
  }
  return opts;
}

// --- corpus ------------------------------------------------------------------

// Zipf-distributed vocabulary (s = 1.07 over 10k words): rank 0-2 are the
// "stopwords" the/of/and (~24% of tokens combined), the long tail is rare --
// the shape real text has, unlike a uniform word soup. Content phrases do not
// arise from independent sampling (a specific rare bigram would have ~zero
// expected occurrences), so known collocations are PLANTED at deterministic
// rates, modelling how real phrases repeat:
//   "quick brown fox jumps"  in every 1000th doc (~200 docs)
//   "quick brown"            in every  100th doc (~2000 docs)
//   "brown fox"              in every  400th doc (~500 docs)
std::vector<std::string> MakeCorpus(size_t num_docs, size_t words_per_doc) {
  constexpr size_t kVocabSize = 10000;
  std::vector<double> cum(kVocabSize);
  double acc = 0.0;
  for (size_t r = 0; r < kVocabSize; ++r) {
    acc += 1.0 / std::pow(static_cast<double>(r + 1), 1.07);
    cum[r] = acc;
  }
  auto word_at = [](size_t rank) -> std::string {
    switch (rank) {
      case 0:
        return "the";
      case 1:
        return "of";
      case 2:
        return "and";
      default:
        return "w" + std::to_string(rank);
    }
  };
  std::mt19937 rng{42};
  std::uniform_real_distribution<double> uni{0.0, acc};
  std::vector<std::string> docs;
  docs.reserve(num_docs);
  std::vector<std::string> tokens(words_per_doc);
  for (size_t d = 0; d < num_docs; ++d) {
    for (size_t w = 0; w < words_per_doc; ++w) {
      const auto it = std::lower_bound(cum.begin(), cum.end(), uni(rng));
      tokens[w] = word_at(static_cast<size_t>(it - cum.begin()));
    }
    if (d % 1000 == 7) {
      tokens[5] = "quick";
      tokens[6] = "brown";
      tokens[7] = "fox";
      tokens[8] = "jumps";
    }
    if (d % 100 == 3) {
      tokens[11] = "quick";
      tokens[12] = "brown";
    }
    if (d % 400 == 9) {
      tokens[17] = "brown";
      tokens[18] = "fox";
    }
    std::string doc;
    for (size_t w = 0; w < words_per_doc; ++w) {
      if (w != 0) {
        doc.push_back(' ');
      }
      doc.append(tokens[w]);
    }
    docs.push_back(std::move(doc));
  }
  return docs;
}

// --- index fields ------------------------------------------------------------

struct PlainField {
  std::string_view Name() const { return "body"; }
  Tokenizer& GetTokens() const {
    tokenizer.reset(value);
    return tokenizer;
  }
  bool Write(DataOutput&) const { return true; }
  IndexFeatures GetIndexFeatures() const noexcept {
    return IndexFeatures::Freq | IndexFeatures::Pos;
  }
  mutable WhitespaceTokenizer tokenizer;
  std::string_view value;
};

struct ShingleField {
  std::string_view Name() const { return "body"; }
  Tokenizer& GetTokens() const {
    analyzer->reset(value);
    return *analyzer;
  }
  bool Write(DataOutput& out) const {
    const auto* store = get<StoreAttr>(*analyzer);
    if (store && !store->value.empty()) {
      out.WriteBytes(store->value.data(), store->value.size());
    }
    return true;
  }
  IndexFeatures GetIndexFeatures() const noexcept { return features; }
  mutable analysis::ShingleAnalyzer* analyzer{};
  std::string_view value;
  IndexFeatures features{IndexFeatures::Freq};
};

uintmax_t DirSize(const std::filesystem::path& p) {
  uintmax_t total = 0;
  for (const auto& e : std::filesystem::recursive_directory_iterator{p}) {
    if (e.is_regular_file()) {
      total += e.file_size();
    }
  }
  return total;
}

struct StoreOut final : DataOutput {
  bstring* b;
  void WriteByte(byte_type x) final { b->push_back(x); }
  void WriteBytes(const byte_type* x, size_t n) final { b->append(x, n); }
};

// Build a single-segment index of `docs` over `field`, persisting the store
// column when `with_store`. Returns the opened reader; reports the dir size.
template<typename Field>
DirectoryReader BuildIndex(std::unique_ptr<MMapDirectory>& dir,
                           const std::filesystem::path& path, Field field,
                           const std::vector<std::string>& docs, bool with_store,
                           const char* label) {
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  dir = std::make_unique<MMapDirectory>(path);
  auto codec = formats::Get("1_5simd");
  auto writer = IndexWriter::Make(*dir, codec, kOmCreate, WriterOptions());
  auto ctx = writer->GetBatch();
  for (const auto& body : docs) {
    field.value = body;
    auto doc = ctx.Insert();
    doc.Insert(field);
    if (with_store) {
      auto* cs = doc.Columnstore();
      auto& cw = cs->OpenColumn(kStoreId, duckdb::LogicalType::BLOB);
      bstring buf;
      StoreOut out{};
      out.b = &buf;
      field.Write(out);
      AppendBlob(cw, doc.DocId(), {buf.data(), buf.size()});
    }
  }
  ctx.Commit();
  writer->RefreshCommit();
  std::cerr << "[index] " << label << ": " << DirSize(path) << " bytes\n";
  return DirectoryReader{*dir, codec, ReaderOptions()};
}

// Whole shared state, built exactly once on first access (function-local
// static): google-benchmark instantiates a fresh fixture per registration, so
// per-fixture SetUp would rebuild the indexes for every benchmark.
struct Corpus {
  std::unique_ptr<MMapDirectory> classic_dir, a_dir, a3_dir, a4_dir, af3_dir,
    b_dir;
  DirectoryReader classic_reader, a_reader, a3_reader, a4_reader, af3_reader,
    b_reader;
  std::unique_ptr<analysis::ShingleAnalyzer> query_a, query_a3, query_a4,
    query_af3, query_b;
};

const Corpus& GetCorpus() {
  static const Corpus corpus = [] {
    Corpus c;
    const auto docs = MakeCorpus(/*num_docs=*/200000, /*words_per_doc=*/32);
    const auto tmp = std::filesystem::temp_directory_path();

    c.classic_reader = BuildIndex(c.classic_dir, tmp / "sdb-bench-phrase-classic",
                                  PlainField{}, docs, false, "classic (Freq+Pos)");

    auto idx_a = MakeShingle(ShingleOpts());
    ShingleField fa;
    fa.analyzer = idx_a.get();
    fa.features = IndexFeatures::Freq;
    c.a_reader = BuildIndex(c.a_dir, tmp / "sdb-bench-phrase-shingleA", fa, docs,
                            true, "shingleA (Freq + store)");

    auto idx_a3 = MakeShingle(ShingleOpts3());
    ShingleField fa3;
    fa3.analyzer = idx_a3.get();
    fa3.features = IndexFeatures::Freq;
    c.a3_reader = BuildIndex(c.a3_dir, tmp / "sdb-bench-phrase-shingleA3", fa3,
                             docs, true, "shingleA3 (Freq + store, max=3)");

    auto idx_a4 = MakeShingle(ShingleOpts4());
    ShingleField fa4;
    fa4.analyzer = idx_a4.get();
    fa4.features = IndexFeatures::Freq;
    c.a4_reader = BuildIndex(c.a4_dir, tmp / "sdb-bench-phrase-shingleA4", fa4,
                             docs, true, "shingleA4 (Freq + store, max=4)");

    auto idx_b = MakeShingle(ShingleOpts());
    ShingleField fb;
    fb.analyzer = idx_b.get();
    fb.features = IndexFeatures::Freq | IndexFeatures::Pos;
    c.b_reader = BuildIndex(c.b_dir, tmp / "sdb-bench-phrase-shingleB", fb, docs,
                            false, "shingleB (Freq+Pos)");

    auto idx_af3 = MakeShingle(ShingleOptsAdaptive());
    ShingleField faf3;
    faf3.analyzer = idx_af3.get();
    faf3.features = IndexFeatures::Freq;
    c.af3_reader =
      BuildIndex(c.af3_dir, tmp / "sdb-bench-phrase-shingleAF3", faf3, docs,
                 true, "shingleAF3 (Freq + store, adaptive max=3)");

    c.query_a = MakeShingle(ShingleOpts());
    c.query_a3 = MakeShingle(ShingleOpts3());
    c.query_a4 = MakeShingle(ShingleOpts4());
    c.query_af3 = MakeShingle(ShingleOptsAdaptive());
    c.query_b = MakeShingle(ShingleOpts());
    return c;
  }();
  return corpus;
}

// --- query construction ------------------------------------------------------

ByPhrase MakeClassic(std::span<const std::string_view> words) {
  ByPhrase f;
  *f.mutable_field() = "body";
  auto* opts = f.mutable_options();
  for (auto w : words) {
    opts->push_back<ByTermOptions>().term = ViewCast<byte_type>(w);
  }
  return f;
}

ByPhraseNgram MakeNgram(std::string_view phrase,
                        analysis::ShingleAnalyzer& analyzer) {
  ByPhraseNgram f;
  *f.mutable_field() = "body";
  *f.mutable_options() = ByPhraseNgramOptions{phrase, analyzer};
  f.mutable_options()->store_field_id = kStoreId;
  return f;
}

size_t RunFilter(const DirectoryReader& reader, const Filter& f) {
  auto prepared = f.prepare({.index = *reader});
  size_t hits = 0;
  for (const auto& segment : *reader) {
    auto docs = prepared->execute({.segment = segment});
    while (docs->next()) {
      ++hits;
    }
  }
  return hits;
}

// --- benchmarks --------------------------------------------------------------

void BM_classic(benchmark::State& s, std::span<const std::string_view> words) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.classic_reader, MakeClassic(words)));
  }
}

void BM_shingleA(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.a_reader, MakeNgram(text, *c.query_a)));
  }
}

void BM_shingleA3(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(
      RunFilter(c.a3_reader, MakeNgram(text, *c.query_a3)));
  }
}

void BM_shingleA4(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(
      RunFilter(c.a4_reader, MakeNgram(text, *c.query_a4)));
  }
}

void BM_shingleB(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.b_reader, MakeNgram(text, *c.query_b)));
  }
}

void BM_shingleAF3(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(
      RunFilter(c.af3_reader, MakeNgram(text, *c.query_af3)));
  }
}

constexpr std::string_view kContent2[] = {"quick", "brown"};
constexpr std::string_view kContent3[] = {"quick", "brown", "fox"};
constexpr std::string_view kStop3[] = {"the", "of", "the"};
constexpr std::string_view kContent4[] = {"quick", "brown", "fox", "jumps"};

}  // namespace

BENCHMARK_CAPTURE(BM_classic, content2,
                  std::span<const std::string_view>{kContent2});
BENCHMARK_CAPTURE(BM_classic, content3,
                  std::span<const std::string_view>{kContent3});
BENCHMARK_CAPTURE(BM_classic, stop3, std::span<const std::string_view>{kStop3});
BENCHMARK_CAPTURE(BM_classic, content4,
                  std::span<const std::string_view>{kContent4});

BENCHMARK_CAPTURE(BM_shingleA, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleA, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleA, stop3, "the of the");
BENCHMARK_CAPTURE(BM_shingleA, content4, "quick brown fox jumps");

BENCHMARK_CAPTURE(BM_shingleA3, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleA3, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleA3, stop3, "the of the");
BENCHMARK_CAPTURE(BM_shingleA3, content4, "quick brown fox jumps");

BENCHMARK_CAPTURE(BM_shingleA4, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleA4, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleA4, stop3, "the of the");
BENCHMARK_CAPTURE(BM_shingleA4, content4, "quick brown fox jumps");

BENCHMARK_CAPTURE(BM_shingleB, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleB, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleB, stop3, "the of the");
BENCHMARK_CAPTURE(BM_shingleB, content4, "quick brown fox jumps");

BENCHMARK_CAPTURE(BM_shingleAF3, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleAF3, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleAF3, stop3, "the of the");
BENCHMARK_CAPTURE(BM_shingleAF3, content4, "quick brown fox jumps");

int main(int argc, char** argv) {
  irs::formats::Init();

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
