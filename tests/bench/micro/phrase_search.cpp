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
#include "iresearch/analysis/analyzers.hpp"
#include "iresearch/analysis/shingle_analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/columnstore/column_writer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/phrase_filter.hpp"
#include "iresearch/search/phrase_ngram_filter.hpp"
#include "iresearch/search/scorers.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/compression.hpp"
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
    .base_analyzer = std::make_unique<WhitespaceTokenizer>(),
    .min_shingle_size = 2,
    .max_shingle_size = 2,
    .output_unigrams = true,
  };
}

// A frequent-words-bounded variant: only bigrams involving a stopword are
// indexed (SeekStorm economy). Shrinks the shingle vocabulary; the query side
// falls back to unigrams across rare-only spans.
analysis::ShingleAnalyzer::Options ShingleOptsBounded() {
  auto opts = ShingleOpts();
  for (auto* w : {"the", "of", "and", "a", "to", "in"}) {
    opts.frequent_words.push_back(bstring{ViewCast<byte_type>(std::string_view{w})});
  }
  return opts;
}

// --- corpus ------------------------------------------------------------------

// Vocabulary mixing frequent "stopwords" (front) with content words.
constexpr std::string_view kVocab[] = {
  "the", "the", "the", "of",   "of",    "and", "a",    "to",
  "in",  "quick", "brown", "fox", "lazy", "dog", "jumps", "over",
  "cat", "hat",  "runs",  "red",
};

std::vector<std::string> MakeCorpus(size_t num_docs, size_t words_per_doc) {
  std::mt19937 rng{42};
  std::uniform_int_distribution<size_t> pick{0, std::size(kVocab) - 1};
  std::vector<std::string> docs;
  docs.reserve(num_docs);
  for (size_t d = 0; d < num_docs; ++d) {
    std::string doc;
    for (size_t w = 0; w < words_per_doc; ++w) {
      if (w != 0) {
        doc.push_back(' ');
      }
      doc.append(kVocab[pick(rng)]);
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
  std::unique_ptr<MMapDirectory> classic_dir, a_dir, b_dir, bf_dir;
  DirectoryReader classic_reader, a_reader, b_reader, bf_reader;
  std::unique_ptr<analysis::ShingleAnalyzer> query_a, query_b, query_bf;
};

const Corpus& GetCorpus() {
  static const Corpus corpus = [] {
    Corpus c;
    const auto docs = MakeCorpus(/*num_docs=*/200000, /*words_per_doc=*/32);
    const auto tmp = std::filesystem::temp_directory_path();

    c.classic_reader = BuildIndex(c.classic_dir, tmp / "sdb-bench-phrase-classic",
                                  PlainField{}, docs, false, "classic (Freq+Pos)");

    auto idx_a = std::make_unique<analysis::ShingleAnalyzer>(ShingleOpts());
    ShingleField fa;
    fa.analyzer = idx_a.get();
    fa.features = IndexFeatures::Freq;
    c.a_reader = BuildIndex(c.a_dir, tmp / "sdb-bench-phrase-shingleA", fa, docs,
                            true, "shingleA (Freq + store)");

    auto idx_b = std::make_unique<analysis::ShingleAnalyzer>(ShingleOpts());
    ShingleField fb;
    fb.analyzer = idx_b.get();
    fb.features = IndexFeatures::Freq | IndexFeatures::Pos;
    c.b_reader = BuildIndex(c.b_dir, tmp / "sdb-bench-phrase-shingleB", fb, docs,
                            false, "shingleB (Freq+Pos)");

    auto idx_bf = std::make_unique<analysis::ShingleAnalyzer>(ShingleOptsBounded());
    ShingleField fbf;
    fbf.analyzer = idx_bf.get();
    fbf.features = IndexFeatures::Freq | IndexFeatures::Pos;
    c.bf_reader =
      BuildIndex(c.bf_dir, tmp / "sdb-bench-phrase-shingleBF", fbf, docs, false,
                 "shingleB+freqwords (Freq+Pos, bounded vocab)");

    c.query_a = std::make_unique<analysis::ShingleAnalyzer>(ShingleOpts());
    c.query_b = std::make_unique<analysis::ShingleAnalyzer>(ShingleOpts());
    c.query_bf = std::make_unique<analysis::ShingleAnalyzer>(ShingleOptsBounded());
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

void BM_shingleB(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.b_reader, MakeNgram(text, *c.query_b)));
  }
}

void BM_shingleBF(benchmark::State& s, std::string_view text) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.bf_reader, MakeNgram(text, *c.query_bf)));
  }
}

constexpr std::string_view kContent2[] = {"quick", "brown"};
constexpr std::string_view kContent3[] = {"quick", "brown", "fox"};
constexpr std::string_view kStop3[] = {"the", "of", "the"};

}  // namespace

BENCHMARK_CAPTURE(BM_classic, content2,
                  std::span<const std::string_view>{kContent2});
BENCHMARK_CAPTURE(BM_classic, content3,
                  std::span<const std::string_view>{kContent3});
BENCHMARK_CAPTURE(BM_classic, stop3, std::span<const std::string_view>{kStop3});

BENCHMARK_CAPTURE(BM_shingleA, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleA, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleA, stop3, "the of the");

BENCHMARK_CAPTURE(BM_shingleB, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleB, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleB, stop3, "the of the");

BENCHMARK_CAPTURE(BM_shingleBF, content2, "quick brown");
BENCHMARK_CAPTURE(BM_shingleBF, content3, "quick brown fox");
BENCHMARK_CAPTURE(BM_shingleBF, stop3, "the of the");

int main(int argc, char** argv) {
  irs::analysis::analyzers::Init();
  irs::formats::Init();
  irs::scorers::Init();
  irs::compression::Init();

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
