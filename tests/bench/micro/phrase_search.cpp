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
#include <optional>
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

std::shared_ptr<const irs::PhraseTokenSourceFactory> StoredText() {
  return std::make_shared<irs::StoredValueSourceFactory>(
    kStoreId, [] { return std::make_shared<WhitespaceTokenizer>(); });
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

struct Index {
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::DirectoryReader reader;
  std::unique_ptr<irs::analysis::Tokenizer> tokenizer;
  bool positions = false;
};

Index BuildIndex(const std::vector<std::string>& docs, const char* name,
                 std::unique_ptr<irs::analysis::Tokenizer> tokenizer,
                 bool positions, bool store) {
  const auto path =
    std::filesystem::temp_directory_path() / "sdb-bench-phrase" / name;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  Index index{.dir = std::make_unique<irs::MMapDirectory>(path),
              .tokenizer = std::move(tokenizer),
              .positions = positions};

  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions writer_opts;
  writer_opts.db = db;
  writer_opts.reader_options.db = db;
  auto writer =
    irs::IndexWriter::Make(*index.dir, irs::kOmCreate, std::move(writer_opts));

  BenchField field{
    .tokenizer = index.tokenizer.get(),
    .features = positions ? irs::IndexFeatures::Freq | irs::IndexFeatures::Pos
                          : irs::IndexFeatures::Freq,
  };
  auto batch = writer->GetBatch();
  for (const auto& body : docs) {
    field.value = body;
    auto doc = batch.Insert();
    tests::InsertField(doc, field);
    if (store) {
      AppendText(
        doc.GetColWriter()->OpenColumn(kStoreId, duckdb::LogicalType::VARCHAR),
        doc.DocId(), body);
    }
  }
  batch.Commit();
  writer->RefreshCommit();
  std::fprintf(stderr, "[index] %-10s %12ju bytes\n", name, DirSize(path));

  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  index.reader = irs::DirectoryReader{*index.dir, reader_opts};
  return index;
}

enum class Strategy : uint8_t {
  Positions,
  Verified,
  Shingle2,
  Shingle3,
  Shingle4,
  Shingle2Pos,
  Shingle3Frequent,
  Shingle3Pos,
  Shingle4Pos,
  Shingle3FrequentPos,
};

struct Corpus {
  std::vector<std::string> docs;
  std::vector<Index> indexes;

  const Index& Of(Strategy s) const { return indexes[static_cast<size_t>(s)]; }
};

const Corpus& GetCorpus() {
  static const Corpus corpus = [] {
    Corpus c;
    c.docs = MakeCorpus(200000, 32);
    const auto& docs = c.docs;
    uintmax_t text = 0;
    for (const auto& doc : docs) {
      text += doc.size();
    }
    std::fprintf(stderr, "[corpus] %-10s %12ju bytes\n", "text", text);
    c.indexes.push_back(BuildIndex(
      docs, "positions", std::make_unique<WhitespaceTokenizer>(), true, false));
    c.indexes.push_back(BuildIndex(
      docs, "verified", std::make_unique<WhitespaceTokenizer>(), false, true));
    c.indexes.push_back(
      BuildIndex(docs, "shingle2", MakeShingles(2, 2), false, true));
    c.indexes.push_back(
      BuildIndex(docs, "shingle3", MakeShingles(2, 3), false, true));
    c.indexes.push_back(
      BuildIndex(docs, "shingle4", MakeShingles(2, 4), false, true));
    c.indexes.push_back(
      BuildIndex(docs, "shingle2pos", MakeShingles(2, 2), true, false));
    c.indexes.push_back(
      BuildIndex(docs, "shingle3f", MakeShingles(2, 3, true), false, true));
    c.indexes.push_back(
      BuildIndex(docs, "shingle3pos", MakeShingles(2, 3), true, false));
    c.indexes.push_back(
      BuildIndex(docs, "shingle4pos", MakeShingles(2, 4), true, false));
    c.indexes.push_back(
      BuildIndex(docs, "shingle3fpos", MakeShingles(2, 3, true), true, false));
    c.indexes.push_back(BuildIndex(
      docs, "unigrams", std::make_unique<WhitespaceTokenizer>(), false, false));
    return c;
  }();
  return corpus;
}

irs::Filter::ptr MakePhrase(const Index& index, Strategy strategy,
                            std::string_view text) {
  std::vector<irs::bytes_view> tokens;
  std::vector<irs::PosAttr::value_t> positions;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    tokens.push_back(irs::ViewCast<irs::byte_type>(word));
    positions.push_back(static_cast<irs::PosAttr::value_t>(positions.size()));
  }
  const auto source = StoredText();
  if (strategy == Strategy::Positions || strategy == Strategy::Verified) {
    auto filter = std::make_unique<irs::ByPhrase>();
    *filter->mutable_field_id() = kBodyId;
    auto* options = filter->mutable_options();
    for (const auto token : tokens) {
      options->push_back<irs::ByTermOptions>().term = token;
    }
    if (strategy != Strategy::Positions) {
      options->set_verifier(std::make_shared<irs::PhraseVerifier>(source));
    }
    return filter;
  }
  auto plan = irs::PlanShinglePhrase(
    irs::utils::downCast<irs::analysis::ShingleTokenizer>(*index.tokenizer),
    tokens, positions, index.positions, index.positions ? nullptr : source);
  if (plan.kind == irs::ShinglePhrasePlan::Kind::Term) {
    auto filter = std::make_unique<irs::ByTerm>();
    *filter->mutable_field_id() = kBodyId;
    filter->mutable_options()->term = std::move(plan.term);
    return filter;
  }
  if (plan.kind == irs::ShinglePhrasePlan::Kind::Phrase) {
    auto filter = std::make_unique<irs::ByPhrase>();
    *filter->mutable_field_id() = kBodyId;
    *filter->mutable_options() = std::move(plan.phrase);
    return filter;
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

void BenchCount(benchmark::State& state, Strategy strategy,
                std::string_view text) {
  const auto& index = GetCorpus().Of(strategy);
  uint64_t hits = 0;
  for (auto _ : state) {
    const auto filter = MakePhrase(index, strategy, text);
    hits = filter ? Count(index.reader, *filter) : 0;
    benchmark::DoNotOptimize(hits);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

void BenchScored(benchmark::State& state, Strategy strategy,
                 std::string_view text) {
  const auto& index = GetCorpus().Of(strategy);
  uint64_t hits = 0;
  for (auto _ : state) {
    const auto filter = MakePhrase(index, strategy, text);
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

constexpr std::pair<const char*, Strategy> kStrategies[] = {
  {"positions", Strategy::Positions},
  {"verified", Strategy::Verified},
  {"shingle2", Strategy::Shingle2},
  {"shingle3", Strategy::Shingle3},
  {"shingle4", Strategy::Shingle4},
  {"shingle2pos", Strategy::Shingle2Pos},
  {"shingle3f", Strategy::Shingle3Frequent},
  {"shingle3pos", Strategy::Shingle3Pos},
  {"shingle4pos", Strategy::Shingle4Pos},
  {"shingle3fpos", Strategy::Shingle3FrequentPos},
};

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
  for (const auto& [strategy_name, strategy] : kStrategies) {
    for (const auto& [query_name, text] : kQueries) {
      const auto suffix = std::string{strategy_name} + "/" + query_name;
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
