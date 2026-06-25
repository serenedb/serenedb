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

// Micro-benchmark: regex search accelerated by a char n-gram index
// (irs::ByRegexpNgram) vs the automaton regex (irs::ByRegexp) over a whole-word
// term dictionary. Both run the same regexes over the same corpus.
//
//   * automaton -- ByRegexp intersects the regex automaton with the field's
//                  term dictionary (every distinct word). No prefilter: cost
//                  grows with the vocabulary it must walk.
//   * ngram     -- ByRegexpNgram extracts required char n-grams from the regex
//                  (RE2 Prefilter), intersects their postings for candidates,
//                  then RE2-verifies survivors' stored tokens.

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
#include <string>
#include <vector>

#include "iresearch/analysis/analyzer.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/analysis/tokenizer_config.hpp"
#include "iresearch/analysis/wildcard_analyzer.hpp"
#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_writer.hpp"
#include "iresearch/formats/formats.hpp"
#include "iresearch/index/directory_reader.hpp"
#include "iresearch/index/index_writer.hpp"
#include "iresearch/search/regexp_filter.hpp"
#include "iresearch/search/regexp_ngram_filter.hpp"
#include "iresearch/search/wildcard_filter.hpp"
#include "iresearch/search/wildcard_ngram_filter.hpp"
#include "iresearch/store/mmap_directory.hpp"
#include "iresearch/utils/string.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace {

using namespace irs;

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

void AppendBlob(ColumnWriter& cw, doc_id_t doc, bytes_view payload) {
  duckdb::Vector v{duckdb::LogicalType::BLOB, /*capacity=*/1};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
  slots[0] = duckdb::StringVector::AddStringOrBlob(
    v, reinterpret_cast<const char*>(payload.data()), payload.size());
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  const uint64_t row = static_cast<uint64_t>(doc) - doc_limits::min();
  cw.Append(row, v, /*count=*/1);
}

constexpr field_id kStoreId = 1;
constexpr field_id kBodyId = 2;

class WhitespaceTokenizer final
  : public analysis::TypedAnalyzer<WhitespaceTokenizer> {
 public:
  static constexpr std::string_view type_name() noexcept {
    return "bench_regex_whitespace";
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

// A vocabulary of distinct words: ByRegexp's automaton must walk this term
// dictionary, so we make it sizeable to expose the difference.
std::vector<std::string> MakeVocab(size_t n) {
  std::mt19937 rng{7};
  std::uniform_int_distribution<int> len{3, 9};
  std::uniform_int_distribution<int> ch{'a', 'z'};
  std::vector<std::string> v;
  v.reserve(n + 4);
  // A few known words the benchmark queries hit.
  for (auto* w : {"quick", "brown", "quack", "brawn"}) {
    v.emplace_back(w);
  }
  for (size_t i = 0; i < n; ++i) {
    std::string w;
    const int k = len(rng);
    for (int j = 0; j < k; ++j) {
      w.push_back(static_cast<char>(ch(rng)));
    }
    v.push_back(std::move(w));
  }
  return v;
}

std::vector<std::string> MakeCorpus(size_t num_docs, size_t words_per_doc,
                                    const std::vector<std::string>& vocab) {
  std::mt19937 rng{99};
  std::uniform_int_distribution<size_t> pick{0, vocab.size() - 1};
  std::vector<std::string> docs;
  docs.reserve(num_docs);
  for (size_t d = 0; d < num_docs; ++d) {
    std::string doc;
    for (size_t w = 0; w < words_per_doc; ++w) {
      if (w != 0) {
        doc.push_back(' ');
      }
      doc.append(vocab[pick(rng)]);
    }
    docs.push_back(std::move(doc));
  }
  return docs;
}

// Whole-word field (term = word) for the automaton ByRegexp.
struct KeywordField {
  field_id Id() const { return kBodyId; }
  Tokenizer& GetTokens() const {
    tokenizer.reset(value);
    return tokenizer;
  }
  bool Write(DataOutput&) const { return true; }
  IndexFeatures GetIndexFeatures() const noexcept { return IndexFeatures::Freq; }
  mutable WhitespaceTokenizer tokenizer;
  std::string_view value;
};

// Char-ngram field (+ stored tokens) for ByRegexpNgram.
struct WildcardField {
  field_id Id() const { return kBodyId; }
  Tokenizer& GetTokens() const {
    analyzer->reset(value);
    return *analyzer;
  }
  bool Write(DataOutput& out) const {
    const auto* store = get<StoreAttr>(*analyzer);
    if (store && !store->value.empty()) {
      out.WriteData(store->value.data(), store->value.size());
    }
    return true;
  }
  IndexFeatures GetIndexFeatures() const noexcept { return IndexFeatures::Freq; }
  mutable analysis::WildcardAnalyzer* analyzer{};
  std::string_view value;
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
  void WriteData(const byte_type* x, uint64_t n) final { b->append(x, n); }
};

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
      auto* cs = doc.GetColWriter();
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

struct Corpus {
  std::unique_ptr<MMapDirectory> kw_dir, wc_dir;
  DirectoryReader kw_reader, wc_reader;
  std::unique_ptr<analysis::WildcardAnalyzer> query_analyzer;
};

const Corpus& GetCorpus() {
  static const Corpus corpus = [] {
    Corpus c;
    const auto vocab = MakeVocab(/*distinct=*/4000);
    const auto docs = MakeCorpus(/*num_docs=*/100000, /*words_per_doc=*/16, vocab);
    const auto tmp = std::filesystem::temp_directory_path();

    c.kw_reader = BuildIndex(c.kw_dir, tmp / "sdb-bench-regex-keyword",
                             KeywordField{}, docs, false, "automaton (term dict)");

    auto idx = std::make_unique<analysis::WildcardAnalyzer>(
      std::make_unique<WhitespaceTokenizer>(), /*ngram_size=*/3);
    WildcardField wf;
    wf.analyzer = idx.get();
    c.wc_reader = BuildIndex(c.wc_dir, tmp / "sdb-bench-regex-ngram", wf, docs,
                             true, "ngram (+ store)");

    c.query_analyzer = std::make_unique<analysis::WildcardAnalyzer>(
      std::make_unique<WhitespaceTokenizer>(), /*ngram_size=*/3);
    return c;
  }();
  return corpus;
}

size_t RunFilter(const DirectoryReader& reader, const Filter& f) {
  auto collector = f.MakeCollector(nullptr);
  std::vector<QueryBuilder::ptr> prepared;
  for (const auto& sub : *reader) {
    prepared.emplace_back(f.PrepareSegment(sub, {.collector = collector.get()}));
  }
  const auto stats = collector->Finish(IResourceManager::gNoop);
  size_t hits = 0;
  size_t seg = 0;
  for ([[maybe_unused]] const auto& sub : *reader) {
    auto& p = prepared[seg++];
    if (!p) {
      continue;
    }
    auto docs = p->Execute({}, stats);
    while (!doc_limits::eof(docs->advance())) {
      ++hits;
    }
  }
  return hits;
}

ByRegexp MakeAutomaton(std::string_view pattern) {
  ByRegexp f;
  *f.mutable_field_id() = kBodyId;
  f.mutable_options()->pattern.assign(ViewCast<byte_type>(pattern));
  return f;
}

ByRegexpNgram MakeNgram(std::string_view pattern,
                        analysis::WildcardAnalyzer& analyzer) {
  ByRegexpNgram f;
  *f.mutable_field_id() = kBodyId;
  *f.mutable_options() = ByRegexpNgramOptions{pattern, analyzer};
  f.mutable_options()->store_field_id = kStoreId;
  return f;
}

// LIKE pattern (% / _) -> automaton ByWildcard over the whole-word column.
ByWildcard MakeWildcardAuto(std::string_view pattern) {
  ByWildcard f;
  *f.mutable_field_id() = kBodyId;
  f.mutable_options()->term.assign(ViewCast<byte_type>(pattern));
  return f;
}

// LIKE pattern -> ngram-accelerated ByWildcardNgram over the char-ngram column.
ByWildcardNgram MakeWildcardNgram(std::string_view pattern,
                                  analysis::WildcardAnalyzer& analyzer) {
  ByWildcardNgram f;
  *f.mutable_field_id() = kBodyId;
  *f.mutable_options() =
    ByWildcardNgramOptions{pattern, analyzer, /*has_positions=*/false};
  f.mutable_options()->store_field_id = kStoreId;
  return f;
}

void BM_automaton(benchmark::State& s, std::string_view pattern) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.kw_reader, MakeAutomaton(pattern)));
  }
}

void BM_ngram(benchmark::State& s, std::string_view pattern) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(
      RunFilter(c.wc_reader, MakeNgram(pattern, *c.query_analyzer)));
  }
}

void BM_wildcard_auto(benchmark::State& s, std::string_view pattern) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.kw_reader, MakeWildcardAuto(pattern)));
  }
}

void BM_wildcard_ngram(benchmark::State& s, std::string_view pattern) {
  const auto& c = GetCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(
      RunFilter(c.wc_reader, MakeWildcardNgram(pattern, *c.query_analyzer)));
  }
}

// --- The other end of the spectrum: a HUGE, mostly-distinct vocabulary, where
// an automaton must walk the whole term dictionary for an infix pattern but the
// n-gram prefilter narrows to the few docs that contain the literal. Each token
// is a random 6-char string; ~1/500 embed "quick" as an infix so the infix
// patterns match a small fraction. ~1.4M distinct terms.
std::vector<std::string> MakeLargeCorpus(size_t num_docs, size_t words_per_doc) {
  std::mt19937 rng{123};
  std::uniform_int_distribution<int> ch{'a', 'z'};
  std::uniform_int_distribution<int> embed{0, 499};
  std::vector<std::string> docs;
  docs.reserve(num_docs);
  for (size_t d = 0; d < num_docs; ++d) {
    std::string doc;
    for (size_t w = 0; w < words_per_doc; ++w) {
      if (w != 0) {
        doc.push_back(' ');
      }
      std::string tok;
      for (int j = 0; j < 6; ++j) {
        tok.push_back(static_cast<char>(ch(rng)));
      }
      if (embed(rng) == 0) {
        tok.insert(3, "quick");  // infix: "xxxquickyyy"
      }
      doc += tok;
    }
    docs.push_back(std::move(doc));
  }
  return docs;
}

struct LargeCorpus {
  std::unique_ptr<MMapDirectory> kw_dir, wc_dir;
  DirectoryReader kw_reader, wc_reader;
  std::unique_ptr<analysis::WildcardAnalyzer> query_analyzer;
};

const LargeCorpus& GetLargeCorpus() {
  static const LargeCorpus corpus = [] {
    LargeCorpus c;
    const auto docs = MakeLargeCorpus(/*num_docs=*/180000, /*words_per_doc=*/8);
    const auto tmp = std::filesystem::temp_directory_path();

    c.kw_reader = BuildIndex(c.kw_dir, tmp / "sdb-bench-regexL-keyword",
                             KeywordField{}, docs, false,
                             "LARGE automaton (term dict)");
    auto idx = std::make_unique<analysis::WildcardAnalyzer>(
      std::make_unique<WhitespaceTokenizer>(), /*ngram_size=*/3);
    WildcardField wf;
    wf.analyzer = idx.get();
    c.wc_reader = BuildIndex(c.wc_dir, tmp / "sdb-bench-regexL-ngram", wf, docs,
                             true, "LARGE ngram (+ store)");
    c.query_analyzer = std::make_unique<analysis::WildcardAnalyzer>(
      std::make_unique<WhitespaceTokenizer>(), /*ngram_size=*/3);
    // Parity sanity (hit counts must match between the two paths).
    std::cerr << "[parity] .*quick.* auto=" << RunFilter(c.kw_reader, MakeAutomaton(".*quick.*"))
              << " ngram=" << RunFilter(c.wc_reader, MakeNgram(".*quick.*", *c.query_analyzer))
              << "  %quick% auto=" << RunFilter(c.kw_reader, MakeWildcardAuto("%quick%"))
              << " ngram=" << RunFilter(c.wc_reader, MakeWildcardNgram("%quick%", *c.query_analyzer))
              << "\n";
    return c;
  }();
  return corpus;
}

void BM_automaton_infix(benchmark::State& s, std::string_view re) {
  const auto& c = GetLargeCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.kw_reader, MakeAutomaton(re)));
  }
}
void BM_ngram_infix(benchmark::State& s, std::string_view re) {
  const auto& c = GetLargeCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.wc_reader, MakeNgram(re, *c.query_analyzer)));
  }
}
void BM_wildcard_auto_infix(benchmark::State& s, std::string_view like) {
  const auto& c = GetLargeCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(RunFilter(c.kw_reader, MakeWildcardAuto(like)));
  }
}
void BM_wildcard_ngram_infix(benchmark::State& s, std::string_view like) {
  const auto& c = GetLargeCorpus();
  for (auto _ : s) {
    benchmark::DoNotOptimize(
      RunFilter(c.wc_reader, MakeWildcardNgram(like, *c.query_analyzer)));
  }
}

}  // namespace

// Literal-anchored patterns the n-gram prefilter accelerates well.
BENCHMARK_CAPTURE(BM_automaton, literal, "quick");
BENCHMARK_CAPTURE(BM_ngram, literal, "quick");
BENCHMARK_CAPTURE(BM_automaton, metachar, "qu.ck");
BENCHMARK_CAPTURE(BM_ngram, metachar, "qu.ck");
BENCHMARK_CAPTURE(BM_automaton, alternation, "quick|brown");
BENCHMARK_CAPTURE(BM_ngram, alternation, "quick|brown");
BENCHMARK_CAPTURE(BM_automaton, prefix, "bro.*");
BENCHMARK_CAPTURE(BM_ngram, prefix, "bro.*");

// Equivalent LIKE patterns (regex /quick/ ~ LIKE 'quick'; /qu.ck/ ~ 'qu_ck';
// /bro.*/ ~ 'bro%'), both the automaton (ByWildcard) and ngram (ByWildcardNgram)
// options, so the four filters can be compared on matching work.
BENCHMARK_CAPTURE(BM_wildcard_auto, literal, "quick");
BENCHMARK_CAPTURE(BM_wildcard_ngram, literal, "quick");
BENCHMARK_CAPTURE(BM_wildcard_auto, metachar, "qu_ck");
BENCHMARK_CAPTURE(BM_wildcard_ngram, metachar, "qu_ck");
BENCHMARK_CAPTURE(BM_wildcard_auto, prefix, "bro%");
BENCHMARK_CAPTURE(BM_wildcard_ngram, prefix, "bro%");

// Large-vocabulary INFIX (the n-gram filter's home turf): automaton must scan
// ~1.4M terms; the n-gram prefilter narrows to the docs that contain "quick".
BENCHMARK_CAPTURE(BM_automaton_infix, large, ".*quick.*");
BENCHMARK_CAPTURE(BM_ngram_infix, large, ".*quick.*");
BENCHMARK_CAPTURE(BM_wildcard_auto_infix, large, "%quick%");
BENCHMARK_CAPTURE(BM_wildcard_ngram_infix, large, "%quick%");

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
