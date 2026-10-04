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

#include <absl/strings/numbers.h>
#include <absl/strings/str_split.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <filesystem>
#include <iresearch/analysis/shingle_tokenizer.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/analysis/tokenizer_config.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/filters/shingle_phrase.hpp>
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
};

constexpr Strategy kUnigrams{.name = "unigrams"};

constexpr Strategy kStrategies[] = {
  {.name = "positions", .positions = true},
  {.name = "shingle2", .max_gram = 2},
  {.name = "shingle3", .max_gram = 3},
  {.name = "shingle4", .max_gram = 4},
  {.name = "shingle3f", .max_gram = 3, .frequent = true},
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

const std::vector<std::string>& Docs() {
  static const auto docs = [] {
    auto docs = MakeCorpus(200000, 32);
    uintmax_t text = 0;
    for (const auto& doc : docs) {
      text += doc.size();
    }
    std::fprintf(stderr, "[corpus] %-12s %12ju bytes\n", "text", text);
    BuildIndex(docs, kUnigrams);
    return docs;
  }();
  return docs;
}

const Index& IndexOf(size_t strategy) {
  static std::vector<std::unique_ptr<Index>> indexes(std::size(kStrategies));
  auto& index = indexes[strategy];
  if (!index) {
    index = std::make_unique<Index>(BuildIndex(Docs(), kStrategies[strategy]));
  }
  return *index;
}

irs::ByPhraseOptions ParsePhrase(std::string_view text) {
  irs::ByPhraseOptions phrase;
  irs::PosAttr::value_t offs_min = 1;
  irs::PosAttr::value_t offs_max = 1;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    if (word.starts_with('+')) {
      const std::pair<std::string_view, std::string_view> range =
        absl::StrSplit(word.substr(1), '-');
      [[maybe_unused]] const bool parsed =
        absl::SimpleAtoi(range.first, &offs_min) &&
        absl::SimpleAtoi(range.second, &offs_max);
      SDB_ASSERT(parsed);
      continue;
    }
    if (word.starts_with('[')) {
      const std::pair<std::string_view, std::string_view> bounds =
        absl::StrSplit(word.substr(1, word.size() - 2), ',');
      auto& range =
        phrase.push_back<irs::ByRangeOptions>(offs_min, offs_max).range;
      range.min = irs::ViewCast<irs::byte_type>(bounds.first);
      range.max = irs::ViewCast<irs::byte_type>(bounds.second);
      range.min_type = irs::BoundType::Inclusive;
      range.max_type = irs::BoundType::Inclusive;
    } else if (const auto tilde = word.rfind('~');
               tilde != std::string_view::npos) {
      auto& fuzzy =
        phrase.push_back<irs::ByEditDistanceOptions>(offs_min, offs_max);
      fuzzy.term = irs::ViewCast<irs::byte_type>(word.substr(0, tilde));
      uint32_t distance = 1;
      [[maybe_unused]] const bool parsed =
        absl::SimpleAtoi(word.substr(tilde + 1), &distance);
      SDB_ASSERT(parsed);
      fuzzy.max_distance = static_cast<irs::byte_type>(distance);
    } else if (word.contains('%')) {
      phrase.push_back<irs::ByWildcardOptions>(offs_min, offs_max) =
        irs::ByWildcardOptions{irs::ViewCast<irs::byte_type>(word)};
    } else if (word.ends_with('*')) {
      phrase.push_back<irs::ByPrefixOptions>(offs_min, offs_max).term =
        irs::ViewCast<irs::byte_type>(word.substr(0, word.size() - 1));
    } else {
      phrase.push_back<irs::ByTermOptions>(offs_min, offs_max).term =
        irs::ViewCast<irs::byte_type>(word);
    }
    offs_min = offs_max = 1;
  }
  return phrase;
}

irs::Filter::ptr MakeLoweredPhrase(irs::ByPhraseOptions&& phrase) {
  auto node = std::make_unique<irs::ByPhrase>();
  *node->mutable_field_id() = kBodyId;
  *node->mutable_options() = std::move(phrase);
  irs::Filter::ptr filter = std::move(node);
  irs::Optimize(filter);
  return filter;
}

irs::Filter::ptr MakePhrase(const Index& index, std::string_view text,
                            bool cover) {
  auto phrase = ParsePhrase(text);
  const auto& strategy = *index.strategy;
  if (strategy.max_gram == 0) {
    return MakeLoweredPhrase(std::move(phrase));
  }
  const auto& shingles =
    irs::utils::downCast<irs::analysis::ShingleTokenizer>(*index.tokenizer);
  if (cover) {
    if (auto plan =
          irs::PlanShinglePhrase(shingles, phrase, strategy.positions)) {
      return MakeLoweredPhrase(std::move(*plan));
    }
  }
  if (!strategy.positions) {
    return nullptr;
  }
  phrase.set_word_separator(shingles.Separator());
  return MakeLoweredPhrase(std::move(phrase));
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

void BenchCount(benchmark::State& state, size_t strategy, std::string_view text,
                bool cover) {
  const auto& index = IndexOf(strategy);
  uint64_t hits = 0;
  for (auto _ : state) {
    const auto filter = MakePhrase(index, text, cover);
    hits = filter ? Count(index.reader, *filter) : 0;
    benchmark::DoNotOptimize(hits);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

void BenchScored(benchmark::State& state, size_t strategy,
                 std::string_view text, bool cover) {
  const auto& index = IndexOf(strategy);
  uint64_t hits = 0;
  for (auto _ : state) {
    const auto filter = MakePhrase(index, text, cover);
    hits = 0;
    benchmark::DoNotOptimize(filter ? Scored(index.reader, *filter, hits) : 0);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

void BenchScan(benchmark::State& state, std::string_view text) {
  const auto& docs = Docs();
  const std::vector<std::string_view> words =
    absl::StrSplit(text, ' ', absl::SkipEmpty());
  WhitespaceTokenizer tokenizer;
  irs::ValueAnalyzer analyzer;
  irs::ValueTokens<irs::TokenLayout::Terms> tokens;
  const auto same = [](const duckdb::string_t& term, std::string_view word) {
    return std::string_view{term.GetData(), term.GetSize()} == word;
  };
  uint64_t hits = 0;
  for (auto _ : state) {
    hits = 0;
    for (const auto& body : docs) {
      analyzer.Analyze(
        tokenizer,
        duckdb::string_t{body.data(), static_cast<uint32_t>(body.size())},
        tokens);
      const auto terms = tokens.terms();
      hits += std::search(terms.begin(), terms.end(), words.begin(),
                          words.end(), same) != terms.end();
    }
    benchmark::DoNotOptimize(hits);
  }
  state.counters["hits"] = static_cast<double>(hits);
}

bool Answers(const Strategy& strategy, std::string_view text) {
  if (strategy.max_gram == 0 || strategy.positions) {
    return true;
  }
  const auto tokenizer = MakeTokenizer(strategy);
  return irs::PlanShinglePhrase(
           irs::utils::downCast<irs::analysis::ShingleTokenizer>(*tokenizer),
           ParsePhrase(text), false)
    .has_value();
}

struct Query {
  const char* name;
  std::string_view text;
  bool complex = false;
};

constexpr Query kQueries[] = {
  {.name = "content2", .text = "quick brown"},
  {.name = "content3", .text = "quick brown fox"},
  {.name = "stop3", .text = "the of the"},
  {.name = "content4", .text = "quick brown fox jumps"},
  {.name = "prefix3", .text = "quick brown fo*", .complex = true},
  {.name = "interval4", .text = "quick brown +2-3 jumps", .complex = true},
  {.name = "stopprefix3", .text = "the of th*", .complex = true},
  {.name = "suffix3", .text = "quick brown %ox", .complex = true},
  {.name = "infix3", .text = "quick brown f%x", .complex = true},
  {.name = "range3", .text = "quick brown [fo,fp]", .complex = true},
  {.name = "fuzzy3", .text = "quick brown fax~2", .complex = true},
};

void Register(std::string name, size_t strategy, std::string_view text,
              bool cover) {
  benchmark::RegisterBenchmark(
    ("Count/" + name).c_str(),
    [=](benchmark::State& state) { BenchCount(state, strategy, text, cover); });
  benchmark::RegisterBenchmark(("Scored/" + name).c_str(),
                               [=](benchmark::State& state) {
                                 BenchScored(state, strategy, text, cover);
                               });
}

void RegisterAll() {
  for (const auto& query : kQueries) {
    if (query.complex) {
      continue;
    }
    const auto text = query.text;
    benchmark::RegisterBenchmark(
      (std::string{"Count/scan/"} + query.name).c_str(),
      [text](benchmark::State& state) { BenchScan(state, text); });
  }
  for (size_t strategy = 0; strategy != std::size(kStrategies); ++strategy) {
    const auto& spec = kStrategies[strategy];
    const bool shingles = spec.max_gram != 0;
    for (const auto& query : kQueries) {
      if (!Answers(spec, query.text)) {
        continue;
      }
      const auto suffix = std::string{"/"} + query.name;
      Register(spec.name + suffix, strategy, query.text, true);
      if (query.complex && shingles) {
        Register(spec.name + std::string{"-flat"} + suffix, strategy,
                 query.text, false);
      }
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
