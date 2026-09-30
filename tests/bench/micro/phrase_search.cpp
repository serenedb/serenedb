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

#include <absl/strings/str_split.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector/string_vector.hpp>
#include <filesystem>
#include <iresearch/analysis/shingle_tokenizer.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/detail/phrase_verify.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/shingle_phrase.hpp>
#include <iresearch/search/filters/term_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "insert_field.hpp"

namespace {

constexpr irs::field_id kStoreId = 1;
constexpr irs::field_id kBodyId = 2;

class WhitespaceTokenizer final
  : public irs::analysis::TypedTokenizer<WhitespaceTokenizer> {
 public:
  irs::TokenTraits Traits() const noexcept final { return {}; }

  static constexpr std::string_view type_name() noexcept {
    return "bench_whitespace";
  }

  template<irs::TokenLayout L>
  bool DoFill(duckdb::string_t raw, irs::TokenSink& sink) {
    const std::string_view data{raw.GetData(), raw.GetSize()};
    for (const auto word : absl::StrSplit(data, ' ', absl::SkipEmpty())) {
      sink.Emit<L>(irs::MakeTermView(word));
    }
    return true;
  }
};

std::unique_ptr<irs::analysis::ShingleTokenizer> MakeShingles(
  uint32_t min, uint32_t max, bool frequent = false) {
  irs::analysis::ShingleTokenizer::Options options{
    .min_shingle_size = min,
    .max_shingle_size = max,
  };
  if (frequent) {
    for (const std::string_view word : {"the", "of", "and"}) {
      options.frequent_words.emplace_back(irs::ViewCast<irs::byte_type>(word));
    }
  }
  return std::make_unique<irs::analysis::ShingleTokenizer>(
    std::make_unique<WhitespaceTokenizer>(), std::move(options));
}

std::vector<std::string> MakeCorpus(size_t num_docs, size_t words_per_doc) {
  constexpr size_t kVocabSize = 10000;
  std::vector<double> cum(kVocabSize);
  double acc = 0.0;
  for (size_t r = 0; r < kVocabSize; ++r) {
    acc += 1.0 / std::pow(static_cast<double>(r + 1), 1.07);
    cum[r] = acc;
  }
  const auto word_at = [](size_t rank) -> std::string {
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
    for (auto& token : tokens) {
      const auto it = std::lower_bound(cum.begin(), cum.end(), uni(rng));
      token = word_at(static_cast<size_t>(it - cum.begin()));
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
    if (d % 50 == 1) {
      tokens[21] = "quick";
      tokens[24] = "brown";
      tokens[27] = "fox";
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

struct BenchField {
  irs::field_id Id() const { return kBodyId; }
  irs::analysis::Tokenizer& GetTokens() const { return *tokenizer; }
  std::string_view Value() const noexcept { return value; }
  irs::IndexFeatures GetIndexFeatures() const noexcept { return features; }

  irs::analysis::Tokenizer* tokenizer{};
  std::string_view value;
  irs::IndexFeatures features{};
};

void AppendText(irs::ColumnWriter& cw, irs::doc_id_t doc,
                std::string_view text) {
  duckdb::Vector v{duckdb::LogicalType::VARCHAR, 1};
  auto* slots = duckdb::FlatVector::GetDataMutable<duckdb::string_t>(v);
  slots[0] = duckdb::StringVector::AddString(v, text.data(), text.size());
  duckdb::FlatVector::ValidityMutable(v).SetAllValid(1);
  cw.Append(static_cast<uint64_t>(doc) - irs::doc_limits::min(), v, 1);
}

irs::StoredText BodyText() {
  return {.column = kStoreId,
          .tokenizer = [] { return std::make_unique<WhitespaceTokenizer>(); }};
}

uintmax_t DirSize(const std::filesystem::path& path) {
  uintmax_t total = 0;
  for (const auto& e : std::filesystem::recursive_directory_iterator{path}) {
    if (e.is_regular_file()) {
      total += e.file_size();
    }
  }
  return total;
}

struct Strategy {
  const char* name;
  uint32_t max_gram = 0;
  bool frequent = false;
  bool positions = false;
  bool verified = false;
};

constexpr Strategy kUnigrams{.name = "unigrams"};

constexpr Strategy kStrategies[] = {
  {.name = "positions", .positions = true},
  {.name = "verified", .verified = true},
  {.name = "shingle2", .max_gram = 2, .verified = true},
  {.name = "shingle3", .max_gram = 3, .verified = true},
  {.name = "shingle4", .max_gram = 4, .verified = true},
  {.name = "shingle3f", .max_gram = 3, .frequent = true, .verified = true},
  {.name = "shingle2pos", .max_gram = 2, .positions = true},
  {.name = "shingle3pos", .max_gram = 3, .positions = true},
  {.name = "shingle4pos", .max_gram = 4, .positions = true},
  {.name = "shingle3fpos", .max_gram = 3, .frequent = true, .positions = true},
};

struct Index {
  const Strategy* strategy;
  std::unique_ptr<irs::MMapDirectory> dir;
  std::unique_ptr<irs::analysis::Tokenizer> tokenizer;
  irs::DirectoryReader reader;
};

std::unique_ptr<irs::analysis::Tokenizer> MakeTokenizer(
  const Strategy& strategy) {
  if (strategy.max_gram == 0) {
    return std::make_unique<WhitespaceTokenizer>();
  }
  return MakeShingles(2, strategy.max_gram, strategy.frequent);
}

Index BuildIndex(const std::vector<std::string>& docs,
                 const Strategy& strategy) {
  const auto path =
    std::filesystem::temp_directory_path() / "sdb-bench-phrase" / strategy.name;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  Index index{.strategy = &strategy,
              .dir = std::make_unique<irs::MMapDirectory>(path),
              .tokenizer = MakeTokenizer(strategy)};

  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions writer_opts;
  writer_opts.db = db;
  writer_opts.reader_options.db = db;
  auto writer =
    irs::IndexWriter::Make(*index.dir, irs::kOmCreate, std::move(writer_opts));

  BenchField field{
    .tokenizer = index.tokenizer.get(),
    .features = strategy.positions
                  ? irs::IndexFeatures::Freq | irs::IndexFeatures::Pos
                  : irs::IndexFeatures::Freq,
  };
  auto batch = writer->GetBatch();
  for (const auto& body : docs) {
    field.value = body;
    auto doc = batch.Insert();
    tests::InsertField(doc, field);
    if (strategy.verified) {
      AppendText(
        doc.GetColWriter()->OpenColumn(kStoreId, duckdb::LogicalType::VARCHAR),
        doc.DocId(), body);
    }
  }
  batch.Commit();
  writer->RefreshCommit();
  std::fprintf(stderr, "[index] %-12s %12ju bytes\n", strategy.name,
               DirSize(path));

  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  index.reader = irs::DirectoryReader{*index.dir, reader_opts};
  return index;
}

struct Corpus {
  std::vector<std::string> docs;
  std::vector<Index> indexes;
};

const Corpus& GetCorpus() {
  static const Corpus corpus = [] {
    Corpus c;
    c.docs = MakeCorpus(200000, 32);
    uintmax_t text = 0;
    for (const auto& doc : c.docs) {
      text += doc.size();
    }
    std::fprintf(stderr, "[corpus] %-12s %12ju bytes\n", "text", text);
    BuildIndex(c.docs, kUnigrams);
    for (const auto& strategy : kStrategies) {
      c.indexes.push_back(BuildIndex(c.docs, strategy));
    }
    return c;
  }();
  return corpus;
}

template<typename Filter, typename Options>
irs::Filter::ptr MakeFilter(Options&& options) {
  auto filter = std::make_unique<Filter>();
  *filter->mutable_field_id() = kBodyId;
  *filter->mutable_options() = std::forward<Options>(options);
  return filter;
}

irs::Filter::ptr MakePhrase(const Index& index, std::string_view text) {
  irs::ByPhraseOptions phrase;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    phrase.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(word);
  }
  const auto& strategy = *index.strategy;
  const auto body = BodyText();
  const auto* stored = strategy.verified ? &body : nullptr;
  if (strategy.max_gram == 0) {
    if (stored) {
      phrase.set_verifier(std::make_shared<irs::PhraseVerifier>(*stored));
    }
    return MakeFilter<irs::ByPhrase>(std::move(phrase));
  }
  auto plan = irs::PlanShinglePhrase(
    irs::utils::downCast<irs::analysis::ShingleTokenizer>(*index.tokenizer),
    phrase, strategy.positions, stored);
  switch (plan.kind) {
    case irs::ShinglePhrasePlan::Kind::None:
      return nullptr;
    case irs::ShinglePhrasePlan::Kind::Term:
      return MakeFilter<irs::ByTerm>(irs::ByTermOptions{std::move(plan.term)});
    case irs::ShinglePhrasePlan::Kind::Phrase:
      return MakeFilter<irs::ByPhrase>(std::move(plan.phrase));
  }
  return nullptr;
}

uint64_t Count(const irs::DirectoryReader& reader, const irs::Filter& filter) {
  uint64_t hits = 0;
  for (const auto& segment : reader) {
    auto query = filter.PrepareSegment(segment, {});
    if (!query) {
      continue;
    }
    if (auto plan = irs::count::MakeRoot(*query)) {
      hits += plan->Run(irs::doc_limits::min(), irs::doc_limits::eof());
    }
  }
  return hits;
}

double Scored(const irs::DirectoryReader& reader, const irs::Filter& filter,
              uint64_t& hits) {
  static const irs::BM25 kScorer;
  irs::StatsArena stats{duckdb::Allocator::DefaultAllocator()};
  irs::PreparedCollector collector{filter, kScorer, stats, 1};
  std::vector<irs::QueryBuilder::ptr> queries;
  for (const auto& segment : reader) {
    queries.emplace_back(
      filter.PrepareSegment(segment, {.collector = collector.Get()}));
  }
  collector.Finish();
  double total = 0;
  for (const auto& query : queries) {
    if (!query || irs::QueryBuilder::IsEmpty(*query)) {
      continue;
    }
    irs::ColumnArgsFetcher fetcher;
    auto node = query->PlanLead({.scorer = &kScorer, .fetcher = &fetcher});
    if (!node) {
      continue;
    }
    auto score = node->PrepareScore();
    for (auto doc = node->Next(); !irs::doc_limits::eof(doc);
         doc = node->Next()) {
      node->FetchScoreArgs(0);
      fetcher.Fetch(doc);
      irs::score_t value{};
      score.Score(&value, 1);
      total += value;
      ++hits;
    }
  }
  return total;
}

void BenchCount(benchmark::State& state, size_t strategy,
                std::string_view text) {
  const auto& index = GetCorpus().indexes[strategy];
  uint64_t hits = 0;
  for (auto _ : state) {
    const auto filter = MakePhrase(index, text);
    hits = filter ? Count(index.reader, *filter) : 0;
    benchmark::DoNotOptimize(hits);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

void BenchScored(benchmark::State& state, size_t strategy,
                 std::string_view text) {
  const auto& index = GetCorpus().indexes[strategy];
  uint64_t hits = 0;
  for (auto _ : state) {
    const auto filter = MakePhrase(index, text);
    hits = 0;
    benchmark::DoNotOptimize(filter ? Scored(index.reader, *filter, hits) : 0);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

void BenchScan(benchmark::State& state, std::string_view text) {
  const auto& corpus = GetCorpus();
  irs::ByPhraseOptions phrase;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    phrase.push_back<irs::ByTermOptions>().term =
      irs::ViewCast<irs::byte_type>(word);
  }
  const irs::PhraseVerifyKernel kernel{phrase, {}};
  WhitespaceTokenizer tokenizer;
  irs::ValueAnalyzer analyzer;
  irs::ValueTokens<irs::TokenLayout::TermsPos> tokens{tokenizer.Traits()};
  irs::PhraseDocTokens doc;
  irs::PhraseVerifyScratch scratch;
  irs::PhraseVerdict verdict;
  uint64_t hits = 0;
  for (auto _ : state) {
    hits = 0;
    for (const auto& body : corpus.docs) {
      analyzer.Analyze(
        tokenizer,
        duckdb::string_t{body.data(), static_cast<uint32_t>(body.size())},
        tokens);
      doc.Clear();
      const auto terms = tokens.terms();
      const auto positions = tokens.pos();
      for (size_t i = 0; i != terms.size(); ++i) {
        doc.Push(irs::AsBytesView(terms[i]), positions[i]);
      }
      hits += kernel.Match(doc, false, scratch, verdict);
    }
    benchmark::DoNotOptimize(hits);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

constexpr std::pair<const char*, std::string_view> kQueries[] = {
  {"content2", "quick brown"},
  {"content3", "quick brown fox"},
  {"stop3", "the of the"},
  {"content4", "quick brown fox jumps"},
};

void RegisterAll() {
  for (const auto& [query_name, text] : kQueries) {
    benchmark::RegisterBenchmark(
      (std::string{"Count/scan/"} + query_name).c_str(),
      [text](benchmark::State& state) { BenchScan(state, text); });
  }
  for (size_t strategy = 0; strategy != std::size(kStrategies); ++strategy) {
    for (const auto& [query_name, text] : kQueries) {
      const auto suffix =
        std::string{kStrategies[strategy].name} + "/" + query_name;
      benchmark::RegisterBenchmark(("Count/" + suffix).c_str(),
                                   [strategy, text](benchmark::State& state) {
                                     BenchCount(state, strategy, text);
                                   });
      benchmark::RegisterBenchmark(("Scored/" + suffix).c_str(),
                                   [strategy, text](benchmark::State& state) {
                                     BenchScored(state, strategy, text);
                                   });
    }
  }
}

}  // namespace

int main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  RegisterAll();
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
