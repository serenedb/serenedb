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

#include <algorithm>
#include <cstdint>
#include <duckdb/common/allocator.hpp>
#include <filesystem>
#include <iresearch/analysis/delimited_tokenizer.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/detail/column_collector.hpp>
#include <iresearch/search/detail/stats_arena.hpp>
#include <iresearch/search/detail/window.hpp>
#include <iresearch/search/filters/boolean_filter.hpp>
#include <iresearch/search/filters/filter.hpp>
#include <iresearch/search/hits/root.hpp>
#include <iresearch/search/scorers/bm25.hpp>
#include <iresearch/store/mmap_directory.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <iresearch/utils/string.hpp>
#include <iresearch/utils/type_limits.hpp>
#include <map>
#include <memory>
#include <string>
#include <string_view>

#include "insert_field.hpp"

namespace {

constexpr irs::field_id kBodyId = 1;
constexpr irs::field_id kNormBase = 16;

struct BodyField {
  irs::field_id Id() const noexcept { return kBodyId; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq | irs::IndexFeatures::Norm;
  }

  bool Write(irs::DataOutput&) const { return true; }

  std::string value;
  mutable irs::analysis::DelimitedTokenizer stream{" "};
};

std::string MakeBody(size_t i) {
  std::string body = "w";
  const auto add = [&](bool on, std::string_view term) {
    if (on) {
      body += ' ';
      body += term;
    }
  };
  add(i % 4096 != 17, "a");
  add(i % 2 == 0, "b");
  add(i % 8 == 1, "c");
  add(i % 16 == 3, "d");
  add(i % 256 == 5, "e");
  add(i % 512 == 9, "f");
  for (auto fill = static_cast<uint32_t>(i) * 2654435761u >> 29; fill != 0;
       --fill) {
    body += " x";
  }
  return body;
}

struct Index {
  std::filesystem::path path;
  std::unique_ptr<irs::MMapDirectory> dir;
  irs::DirectoryReader reader;
};

const Index& IndexOf(size_t docs) {
  static std::map<size_t, Index> gCache;
  auto [it, added] = gCache.try_emplace(docs);
  auto& index = it->second;
  if (!added) {
    return index;
  }
  index.path = std::filesystem::temp_directory_path() /
               ("serenedb-bench-hits-window-" + std::to_string(docs));
  std::filesystem::remove_all(index.path);
  std::filesystem::create_directories(index.path);
  index.dir = std::make_unique<irs::MMapDirectory>(index.path);

  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions opts;
  opts.db = db;
  opts.reader_options.db = db;
  opts.column_options = [](irs::field_id) -> irs::ColumnOptions { return {}; };
  opts.norm_column_id = [](irs::field_id id) { return id + kNormBase; };
  auto writer =
    irs::IndexWriter::Make(*index.dir, irs::kOmCreate, std::move(opts));
  {
    auto trx = writer->GetBatch();
    BodyField field;
    for (size_t i = 0; i != docs; ++i) {
      field.value = MakeBody(i);
      auto doc = trx.Insert();
      tests::InsertField(doc, field);
    }
    trx.Commit();
  }
  writer->RefreshCommit();
  index.reader =
    irs::DirectoryReader{*index.dir, irs::IndexReaderOptions{.db = db}};
  return index;
}

irs::Filter::ptr Either(std::string_view lhs, std::string_view rhs) {
  auto filter = std::make_unique<irs::BooleanFilter>();
  for (const auto term : {lhs, rhs}) {
    filter->Add(
      irs::TermClause{
        .field = kBodyId,
        .term = irs::bstring{irs::ViewCast<irs::byte_type>(term)},
      },
      irs::Occur::Should);
  }
  filter->SetMinShouldMatch(1);
  return filter;
}

void BmScan(benchmark::State& state, std::string_view lhs, std::string_view rhs,
            irs::doc_id_t take) {
  static const irs::BM25 kScorer;
  const auto& index = IndexOf(static_cast<size_t>(state.range(0)));
  const auto& segment = index.reader[0];
  const auto filter = Either(lhs, rhs);
  irs::StatsArena stats{duckdb::Allocator::DefaultAllocator()};
  irs::PreparedCollector collector{*filter, kScorer, stats, 1};
  const auto query =
    filter->PrepareSegment(segment, {.collector = collector.Get()});
  collector.Finish();

  constexpr uint32_t kCapacity = irs::doc_limits::kMinCapacity;
  irs::SlackBuf<irs::doc_id_t, kCapacity, irs::doc_limits::kDocsSlack> docs;
  irs::SlackBuf<irs::score_t, kCapacity, irs::doc_limits::kScoresSlack> scores;
  const auto end =
    irs::doc_limits::min() + static_cast<irs::doc_id_t>(segment.docs_count());
  uint64_t hits = 0;
  for (auto _ : state) {
    irs::ColumnArgsFetcher fetcher;
    auto plan = query->PlanScored({.scorer = kScorer, .fetcher = fetcher});
    for (auto window = irs::doc_limits::min(); window < end;
         window += irs::detail::kWindowDocs) {
      const auto stop = std::min<irs::doc_id_t>(window + take, end);
      for (auto min = window; min < stop; min += kCapacity) {
        hits += plan->Run(min, std::min<irs::doc_id_t>(min + kCapacity, stop),
                          docs.data(), scores.data());
        benchmark::DoNotOptimize(scores.data());
      }
    }
  }
  state.SetItemsProcessed(state.iterations() *
                          static_cast<int64_t>(segment.docs_count()));
  state.counters["hits"] = benchmark::Counter(
    static_cast<double>(hits) / static_cast<double>(state.iterations()));
}

void Sizes(benchmark::internal::Benchmark* b) {
  b->Arg(1 << 20)->Unit(benchmark::kMicrosecond);
}

constexpr irs::doc_id_t kWhole = irs::detail::kWindowDocs;
constexpr irs::doc_id_t kHead = 256;

BENCHMARK_CAPTURE(BmScan, dense_whole, "a", "b", kWhole)->Apply(Sizes);
BENCHMARK_CAPTURE(BmScan, dense_head, "a", "b", kHead)->Apply(Sizes);
BENCHMARK_CAPTURE(BmScan, medium_whole, "c", "d", kWhole)->Apply(Sizes);
BENCHMARK_CAPTURE(BmScan, medium_head, "c", "d", kHead)->Apply(Sizes);
BENCHMARK_CAPTURE(BmScan, sparse_whole, "e", "f", kWhole)->Apply(Sizes);
BENCHMARK_CAPTURE(BmScan, sparse_head, "e", "f", kHead)->Apply(Sizes);

int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
