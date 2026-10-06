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
#include <linux/perf_event.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <filesystem>
#include <fstream>
#include <iresearch/analysis/pipeline_tokenizer.hpp>
#include <iresearch/analysis/split_by_non_alpha_tokenizer.hpp>
#include <iresearch/analysis/stemming_tokenizer.hpp>
#include <iresearch/analysis/text_tokenizer.hpp>
#include <iresearch/analysis/token_sinks.hpp>
#include <iresearch/analysis/tokenizer.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_writer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/count/make.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/detail/token_phrase.hpp>
#include <iresearch/search/filters/filter_optimizer.hpp>
#include <iresearch/search/filters/phrase_filter.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "insert_field.hpp"

namespace {

constexpr irs::field_id kStoreId = 1;
constexpr irs::field_id kPlainId = 2;
constexpr irs::field_id kPositionalId = 3;
constexpr size_t kSegmentDocs = 10000;

class Instructions {
 public:
  Instructions() {
    perf_event_attr attr{};
    attr.type = PERF_TYPE_HARDWARE;
    attr.size = sizeof(attr);
    attr.config = PERF_COUNT_HW_INSTRUCTIONS;
    attr.exclude_kernel = 1;
    attr.exclude_hv = 1;
    _fd = static_cast<int>(syscall(__NR_perf_event_open, &attr, 0, -1, -1, 0));
  }

  ~Instructions() {
    if (_fd >= 0) {
      close(_fd);
    }
  }

  uint64_t Read() const {
    uint64_t value = 0;
    if (_fd < 0 || read(_fd, &value, sizeof(value)) != sizeof(value)) {
      return 0;
    }
    return value;
  }

 private:
  int _fd = -1;
};

const std::vector<std::string>& Docs() {
  static const auto docs = [] {
    const char* path = std::getenv("PHRASE_CORPUS");
    const char* limit_env = std::getenv("PHRASE_DOCS");
    const size_t limit =
      limit_env ? std::strtoull(limit_env, nullptr, 10) : 100000;
    std::vector<std::string> out;
    out.reserve(limit);
    std::ifstream in{path ? path
                          : "/mnt/data/searchbench/wiki_small/corpus.json"};
    std::string line;
    constexpr std::string_view kKey = "\"text\": \"";
    while (out.size() < limit && std::getline(in, line)) {
      const auto begin = line.find(kKey);
      const auto end = line.rfind("\"}");
      if (begin == std::string::npos || end == std::string::npos ||
          end < begin + kKey.size()) {
        continue;
      }
      out.emplace_back(
        line.substr(begin + kKey.size(), end - begin - kKey.size()));
    }
    std::fprintf(stderr, "[corpus] %zu docs\n", out.size());
    return out;
  }();
  return docs;
}

irs::analysis::Tokenizer::ptr MakeNonAlpha() {
  return irs::analysis::SplitByNonAlphaTokenizer::Make(
    {.case_convert = irs::Case::Lower});
}

irs::analysis::Tokenizer::ptr MakeText() {
  return irs::analysis::TextTokenizer::Make({});
}

irs::analysis::Tokenizer::ptr MakeStem() {
  std::vector<irs::analysis::Tokenizer::ptr> children;
  children.push_back(irs::analysis::TextTokenizer::Make({}));
  children.push_back(irs::analysis::StemmingTokenizer::Make(
    {.locale = duckdb::text::Locale::FromName("en")}));
  return std::make_unique<irs::analysis::PipelineTokenizer>(
    std::move(children));
}

struct Dictionary {
  const char* name;
  irs::analysis::Tokenizer::ptr (*make)();
};

constexpr Dictionary kDictionaries[] = {
  {"nonalpha", &MakeNonAlpha},
  {"text", &MakeText},
  {"stem", &MakeStem},
};

struct Field {
  irs::field_id Id() const { return id; }
  irs::analysis::Tokenizer& GetTokens() const { return *tokenizer; }
  std::string_view Value() const noexcept { return value; }
  irs::IndexFeatures GetIndexFeatures() const noexcept { return features; }

  irs::analysis::Tokenizer* tokenizer{};
  std::string_view value;
  irs::field_id id{};
  irs::IndexFeatures features{};
};

struct Index {
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::DirectoryReader reader;
};

const Index& IndexOf(size_t dictionary) {
  static std::vector<std::unique_ptr<Index>> indexes(std::size(kDictionaries));
  auto& index = indexes[dictionary];
  if (index) {
    return *index;
  }
  const auto& docs = Docs();
  const auto& spec = kDictionaries[dictionary];
  const auto path =
    std::filesystem::temp_directory_path() / "sdb-bench-recheck" / spec.name;
  std::filesystem::remove_all(path);
  std::filesystem::create_directories(path);
  index = std::make_unique<Index>();
  index->dir = std::make_unique<irs::MMapDirectory>(path);
  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions writer_opts;
  writer_opts.db = db;
  writer_opts.reader_options.db = db;
  auto writer =
    irs::IndexWriter::Make(*index->dir, irs::kOmCreate, std::move(writer_opts));
  auto plain_tokens = spec.make();
  auto positional_tokens = spec.make();
  Field plain{.tokenizer = plain_tokens.get(),
              .id = kPlainId,
              .features = irs::IndexFeatures::Freq};
  Field positional{
    .tokenizer = positional_tokens.get(),
    .id = kPositionalId,
    .features = irs::IndexFeatures::Freq | irs::IndexFeatures::Pos};
  duckdb::Vector one{duckdb::LogicalType::VARCHAR, 1};
  for (size_t begin = 0; begin < docs.size(); begin += kSegmentDocs) {
    auto batch = writer->GetBatch();
    const auto end = std::min(docs.size(), begin + kSegmentDocs);
    for (size_t i = begin; i < end; ++i) {
      plain.value = docs[i];
      positional.value = docs[i];
      auto doc = batch.Insert();
      tests::InsertField(doc, plain);
      tests::InsertField(doc, positional);
      auto& column =
        doc.GetColWriter()->OpenColumn(kStoreId, duckdb::LogicalType::VARCHAR);
      duckdb::FlatVector::GetDataMutable<duckdb::string_t>(one)[0] =
        duckdb::string_t{docs[i].data(), static_cast<uint32_t>(docs[i].size())};
      column.Append(doc.DocId() - irs::doc_limits::min(), one, 1);
    }
    batch.Commit();
    writer->RefreshCommit();
  }
  irs::IndexReaderOptions reader_opts;
  reader_opts.db = db;
  index->reader = irs::DirectoryReader{*index->dir, reader_opts};
  std::fprintf(stderr, "[index] %-8s %zu segments\n", spec.name,
               index->reader.size());
  return *index;
}

std::string Analyze(irs::analysis::Tokenizer& tokenizer,
                    std::string_view word) {
  irs::ValueAnalyzer analyzer;
  irs::ValueTokens<irs::TokenLayout::Terms> tokens;
  analyzer.Analyze(
    tokenizer,
    duckdb::string_t{word.data(), static_cast<uint32_t>(word.size())}, tokens);
  if (tokens.terms().empty()) {
    return std::string{word};
  }
  const auto& term = tokens.terms().front();
  return std::string{term.GetData(), term.GetSize()};
}

irs::ByPhraseOptions Parse(std::string_view text,
                           irs::analysis::Tokenizer& tokenizer) {
  irs::ByPhraseOptions phrase;
  uint32_t min = 1;
  uint32_t max = 1;
  for (const auto word : absl::StrSplit(text, ' ', absl::SkipEmpty())) {
    if (word == "?") {
      ++min;
      ++max;
      continue;
    }
    if (word.starts_with('{')) {
      const auto comma = word.find(',');
      min += static_cast<uint32_t>(std::strtoul(
        std::string{word.substr(1, comma - 1)}.c_str(), nullptr, 10));
      max += static_cast<uint32_t>(std::strtoul(
        std::string{word.substr(comma + 1, word.size() - comma - 2)}.c_str(),
        nullptr, 10));
      continue;
    }
    if (phrase.empty()) {
      min = max = 0;
    }
    if (word.starts_with('%')) {
      phrase.push_back<irs::ByWildcardOptions>(min, max) =
        irs::ByWildcardOptions{irs::ViewCast<irs::byte_type>(word)};
    } else if (word.ends_with('*')) {
      phrase.push_back<irs::ByPrefixOptions>(min, max).term =
        irs::ViewCast<irs::byte_type>(word.substr(0, word.size() - 1));
    } else {
      const auto term = Analyze(tokenizer, word);
      phrase.push_back<irs::ByTermOptions>(min, max).term =
        irs::ViewCast<irs::byte_type>(std::string_view{term});
    }
    min = max = 1;
  }
  return phrase;
}

struct Mode {
  const char* name;
  bool positional;
  std::optional<irs::PhraseMatch> match;
};

constexpr Mode kModes[] = {
  {"positional", true, std::nullopt},
  {"auto", false, std::nullopt},
  {"automaton", false, irs::PhraseMatch::Automaton},
  {"lists", false, irs::PhraseMatch::Positions},
};

irs::Filter::ptr MakeFilter(size_t dictionary,
                            const irs::ByPhraseOptions& phrase,
                            const Mode& mode) {
  auto filter = std::make_unique<irs::ByPhrase>();
  *filter->mutable_field_id() = mode.positional ? kPositionalId : kPlainId;
  *filter->mutable_options() = phrase;
  if (!mode.positional) {
    auto tokens = std::make_shared<irs::PhraseTokens>();
    tokens->text = {.columns = {kStoreId},
                    .types = {duckdb::LogicalType::VARCHAR}};
    tokens->tokenizer = [dictionary] {
      return std::shared_ptr<irs::analysis::Tokenizer>{
        kDictionaries[dictionary].make()};
    };
    tokens->match = mode.match;
    filter->mutable_options()->set_tokens(std::move(tokens));
  }
  irs::Filter::ptr root = std::move(filter);
  irs::Optimize(root);
  return root;
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

struct Case {
  size_t dictionary;
  std::string text;
  irs::ByPhraseOptions phrase;
};

std::vector<std::unique_ptr<Case>>& Cases() {
  static std::vector<std::unique_ptr<Case>> cases;
  return cases;
}

void Check(const Case& c) {
  const auto& index = IndexOf(c.dictionary);
  std::optional<std::pair<uint64_t, double>> expected;
  for (const auto& mode : kModes) {
    const auto filter = MakeFilter(c.dictionary, c.phrase, mode);
    uint64_t hits = 0;
    const auto score = Scored(index.reader, *filter, hits);
    const auto count = Count(index.reader, *filter);
    if (count != hits) {
      std::fprintf(stderr, "[mismatch] %s '%s' %s: count %lu scored %lu\n",
                   kDictionaries[c.dictionary].name, c.text.c_str(), mode.name,
                   count, hits);
    }
    if (!expected) {
      expected.emplace(hits, score);
      continue;
    }
    if (expected->first != hits ||
        std::abs(expected->second - score) > 1e-3 * (1 + expected->second)) {
      std::fprintf(stderr,
                   "[mismatch] %s '%s' %s: hits %lu vs %lu, score %f vs %f\n",
                   kDictionaries[c.dictionary].name, c.text.c_str(), mode.name,
                   hits, expected->first, score, expected->second);
    }
  }
}

void Measure(benchmark::State& state, auto&& body) {
  Instructions counter;
  uint64_t hits = 0;
  const auto before = counter.Read();
  for (auto _ : state) {
    hits = body();
    benchmark::DoNotOptimize(hits);
  }
  const auto after = counter.Read();
  state.counters["instructions"] = static_cast<double>(after - before) /
                                   static_cast<double>(state.iterations());
  state.counters["hits"] = static_cast<double>(hits);
}

void RegisterAll() {
  constexpr std::string_view kPhrases[] = {
    "of the",
    "united states",
    "one of the most",
    "to be or not to be",
    "the united states of america",
    "the population was at the census",
    "united ? of",
    "the {0,10} of",
    "in {0,2} united {0,1} of",
    "united st*",
    "the s*",
    "the %tion of",
    "the the",
    "of the of the",
    "the {0,20} of",
    "the {0,10} the {0,10} the",
    "of {0,5} the {0,5} of {0,5} the",
    "in {0,10} the {0,10} of {0,10} the",
  };
  auto& cases = Cases();
  for (size_t d = 0; d < std::size(kDictionaries); ++d) {
    auto tokenizer = kDictionaries[d].make();
    for (const auto text : kPhrases) {
      cases.push_back(
        std::make_unique<Case>(Case{.dictionary = d,
                                    .text = std::string{text},
                                    .phrase = Parse(text, *tokenizer)}));
    }
  }
  for (const auto& c : cases) {
    Check(*c);
    std::string label{c->text};
    std::replace(label.begin(), label.end(), ' ', '_');
    const auto prefix =
      std::string{kDictionaries[c->dictionary].name} + "/" + label;
    for (const auto& mode : kModes) {
      benchmark::RegisterBenchmark(
        (prefix + "/" + mode.name + "/count").c_str(),
        [c = c.get(), &mode](benchmark::State& state) {
          const auto& index = IndexOf(c->dictionary);
          const auto filter = MakeFilter(c->dictionary, c->phrase, mode);
          Measure(state, [&] { return Count(index.reader, *filter); });
        })
        ->Unit(benchmark::kMillisecond);
      benchmark::RegisterBenchmark(
        (prefix + "/" + mode.name + "/scored").c_str(),
        [c = c.get(), &mode](benchmark::State& state) {
          const auto& index = IndexOf(c->dictionary);
          const auto filter = MakeFilter(c->dictionary, c->phrase, mode);
          Measure(state, [&] {
            uint64_t hits = 0;
            benchmark::DoNotOptimize(Scored(index.reader, *filter, hits));
            return hits;
          });
        })
        ->Unit(benchmark::kMillisecond);
    }
  }
}

}  // namespace

static int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  irs::InitOptimizeRules();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  RegisterAll();
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}

[[maybe_unused]] static const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);
