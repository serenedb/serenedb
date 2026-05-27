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

// Sweep benchmark for IndexWriter Stage 1 of PrepareFlush — the per-segment
// loop that applies removal QueryContexts to already committed segments.
//
// Goal: figure out at which (segments, docs_per_segment, removal_contexts,
// selectivity, threads) configurations the executor branch beats the sync
// branch. Use the data to design a heuristic inside PrepareFlush.
//
// Series (use --benchmark_filter to run individual ones):
//   BmStage1_Segments       — sweep committed_segments
//   BmStage1_SegmentSize    — sweep docs_per_segment
//   BmStage1_Contexts       — sweep removal_contexts
//   BmStage1_Selectivity    — sweep selectivity_pct
//   BmStage1_Skewed         — one fat segment + many small ones
//
// Each row args:
//   range(0): committed_segments
//   range(1): docs_per_segment
//   range(2): removal_contexts
//   range(3): selectivity_pct  (1..100)
//   range(4): threads          (0 = sync, otherwise FairThreadPool size)
//
// Use --benchmark_min_time=Nx (fixed iterations), not =Ns. Each iteration has
// expensive PauseTiming() setup, so wall-clock time / iteration ≫ measured
// time / iteration. Time-based min_time loops forever on small configs.
//
// Recommended:
//   ./bin/serenedb-bench-micro-index_writer_executor \
//       --benchmark_min_time=5x \
//       --benchmark_out=stage1.json --benchmark_out_format=json
//
// Caveat — what is being measured.
// `Commit()` includes Stage 1 (the parallelizable per-segment query loop) and
// Stage 2+ (per-segment FlushIndexSegment writes for partially-deleted
// segments). The executor only parallelizes Stage 1. When `deleted_ratio` is
// close to 1.0 the writer takes the "full segment masked" shortcut and skips
// FlushIndexSegment, so the measurement is closer to pure Stage 1; with
// `deleted_ratio` well below 1.0 a constant per-segment serial write cost is
// included. Use the deleted_ratio counter to identify each regime.

#include <benchmark/benchmark.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/term_filter.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>
#include <yaclib/exe/submit.hpp>
#include <yaclib/runtime/fair_thread_pool.hpp>

namespace {

constexpr std::string_view kFormatName = "1_5simd";
constexpr std::string_view kFieldName = "value";

using ThreadPoolPtr = yaclib::IntrusivePtr<yaclib::FairThreadPool>;

class StringField {
 public:
  explicit StringField(std::string_view name) noexcept : _name{name} {}

  std::string_view Name() const noexcept { return _name; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::None;
  }

  irs::Tokenizer& GetTokens() const {
    _stream.reset(_value);
    return _stream;
  }

  bool Write(irs::DataOutput&) const { return false; }

  void Value(std::string_view value) noexcept { _value = value; }

 private:
  std::string_view _name;
  std::string_view _value;
  mutable irs::StringTokenizer _stream;
};

irs::Format::ptr GetFormat() {
  static std::once_flag once;
  std::call_once(once, [] { irs::formats::Init(); });

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    std::fprintf(stderr,
                 "index_writer_executor bench: format '%.*s' not found\n",
                 static_cast<int>(kFormatName.size()), kFormatName.data());
    std::abort();
  }
  return format;
}

irs::IndexWriterOptions MakeWriterOptions(uint32_t docs_per_segment,
                                          const yaclib::IExecutorPtr& executor,
                                          size_t executor_parallelism = 0) {
  irs::IndexWriterOptions options;
  options.lock_repository = false;
  options.segment_docs_max = docs_per_segment;
  options.executor = executor;
  options.executor_parallelism = executor_parallelism;
  return options;
}

// Number of distinct terms in the index. Each term matches ~1/N of docs in
// every segment. Returned value drives the per-filter selectivity.
size_t IndexedTermsForSelectivity(int64_t selectivity_pct) {
  if (selectivity_pct <= 0) {
    return 1;
  }
  if (selectivity_pct >= 100) {
    return 1;
  }
  // ceil(100 / sel)
  return static_cast<size_t>((100 + selectivity_pct - 1) / selectivity_pct);
}

std::vector<std::string> MakeTerms(size_t term_count) {
  std::vector<std::string> terms;
  terms.reserve(term_count);

  for (size_t i = 0; i != term_count; ++i) {
    terms.emplace_back("bucket_" + std::to_string(i));
  }

  return terms;
}

irs::Filter::ptr MakeTermFilter(std::string_view term) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = kFieldName;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return filter;
}

// Insert `docs_count` docs and Commit() — produces ceil(docs_count /
// segment_docs_max) sealed segments. The writer's segment_docs_max must be
// already configured (either via initial options or `writer.Options(...)`).
void InsertAndCommit(irs::IndexWriter& writer, int64_t docs_count,
                     const std::vector<std::string>& terms) {
  StringField field{kFieldName};
  for (int64_t i = 0; i != docs_count; ++i) {
    field.Value(terms[static_cast<size_t>(i) % terms.size()]);
    auto trx = writer.GetBatch();
    auto doc = trx.Insert();
    const auto ok = doc.Insert(field);
    if (!ok) {
      std::fprintf(stderr,
                   "index_writer_executor bench: document insert failed\n");
      std::abort();
    }
  }
  const auto committed = writer.Commit();
  benchmark::DoNotOptimize(committed);
}

// Build N uniform-sized segments, each of docs_per_segment.
void BuildCommittedSegments(irs::IndexWriter& writer, int64_t segment_count,
                            int64_t docs_per_segment,
                            const std::vector<std::string>& terms) {
  InsertAndCommit(writer, segment_count * docs_per_segment, terms);
}

// Build (small_count + 1) segments — one "fat" of fat_docs followed by
// small_count small ones of small_docs each. Uses runtime Options() to
// change segment_docs_max between batches.
void BuildCommittedSegmentsSkewed(irs::IndexWriter& writer, int64_t small_count,
                                  int64_t small_docs, int64_t fat_docs,
                                  const std::vector<std::string>& terms) {
  // Step 1: one fat segment.
  irs::SegmentOptions fat_opts;
  fat_opts.segment_docs_max = static_cast<uint32_t>(fat_docs);
  writer.Options(fat_opts);
  InsertAndCommit(writer, fat_docs, terms);

  // Step 2: many small segments.
  irs::SegmentOptions small_opts;
  small_opts.segment_docs_max = static_cast<uint32_t>(small_docs);
  writer.Options(small_opts);
  InsertAndCommit(writer, small_count * small_docs, terms);
}

// Multi-fat variant: `fat_count` segments of `fat_docs` followed by
// `small_count` segments of `small_docs`. Used to test stage 1 batching on
// loads with several hot spots, not just one.
void BuildCommittedSegmentsMultiSkewed(irs::IndexWriter& writer,
                                       int64_t fat_count, int64_t fat_docs,
                                       int64_t small_count, int64_t small_docs,
                                       const std::vector<std::string>& terms) {
  irs::SegmentOptions fat_opts;
  fat_opts.segment_docs_max = static_cast<uint32_t>(fat_docs);
  writer.Options(fat_opts);
  for (int64_t i = 0; i != fat_count; ++i) {
    InsertAndCommit(writer, fat_docs, terms);
  }

  irs::SegmentOptions small_opts;
  small_opts.segment_docs_max = static_cast<uint32_t>(small_docs);
  writer.Options(small_opts);
  InsertAndCommit(writer, small_count * small_docs, terms);
}

void AddRemovalQueryContexts(irs::IndexWriter& writer, int64_t removal_contexts,
                             const std::vector<std::string>& terms) {
  std::vector<irs::IndexWriter::Transaction> transactions;
  transactions.reserve(static_cast<size_t>(removal_contexts));

  // Keep transactions alive until all Remove calls are registered. This forces
  // one active SegmentContext per transaction, so the next Commit() observes
  // removal_contexts entries in ctx->segments during PrepareFlush().
  for (int64_t i = 0; i != removal_contexts; ++i) {
    auto& trx = transactions.emplace_back(writer.GetBatch());
    trx.Remove(MakeTermFilter(terms[static_cast<size_t>(i) % terms.size()]));
  }

  for (auto& trx : transactions) {
    benchmark::DoNotOptimize(trx.Commit());
  }
}

struct Workload {
  std::unique_ptr<irs::MemoryDirectory> dir;
  irs::IndexWriter::ptr writer;
  int64_t total_docs = 0;
  int64_t segment_count = 0;
  int64_t removed_docs_total = 0;
};

ThreadPoolPtr MakeExecutor(int64_t threads) {
  if (threads > 0) {
    return yaclib::MakeFairThreadPool(static_cast<uint64_t>(threads));
  }
  return nullptr;
}

void StopExecutor(ThreadPoolPtr& executor) {
  if (executor) {
    executor->Stop();
    executor->Wait();
    executor = nullptr;
  }
}

// For uniform segments: each segment has docs_per_segment docs distributed
// uniformly across `num_terms`. Each of the first min(contexts, num_terms)
// distinct filters matches ~ docs_per_segment / num_terms docs. Contexts that
// reuse a term still iterate the same matches.
int64_t EstimatedRemovedDocsUniform(int64_t segment_count,
                                    int64_t docs_per_segment,
                                    int64_t removal_contexts,
                                    size_t num_terms) {
  // Per-segment: distinct removed docs ≈ min(contexts, num_terms) buckets,
  // each of size docs_per_segment / num_terms.
  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  const auto per_segment =
    distinct * (docs_per_segment / static_cast<int64_t>(num_terms));
  return segment_count * per_segment;
}

void SetCommonCounters(benchmark::State& state, const Workload& workload,
                       int64_t removal_contexts, size_t num_terms,
                       int64_t threads) {
  state.SetItemsProcessed(state.iterations() * workload.removed_docs_total);
  state.counters["mode"] = static_cast<double>(threads);
  state.counters["segments"] = static_cast<double>(workload.segment_count);
  state.counters["total_live_docs"] = static_cast<double>(workload.total_docs);
  state.counters["avg_docs_per_segment"] =
    workload.segment_count ? static_cast<double>(workload.total_docs) /
                               static_cast<double>(workload.segment_count)
                           : 0.0;
  state.counters["removal_contexts"] = static_cast<double>(removal_contexts);
  state.counters["num_terms"] = static_cast<double>(num_terms);
  state.counters["removed_docs_total"] =
    static_cast<double>(workload.removed_docs_total);
  state.counters["deleted_ratio"] =
    workload.total_docs ? static_cast<double>(workload.removed_docs_total) /
                            static_cast<double>(workload.total_docs)
                        : 0.0;

  if (threads == 0) {
    state.SetLabel("sync");
  } else {
    state.SetLabel("exec/" + std::to_string(threads));
  }
}

Workload PrepareUniformWorkload(int64_t segments, int64_t docs_per_segment,
                                int64_t removal_contexts, size_t num_terms,
                                const yaclib::IExecutorPtr& executor,
                                size_t parallelism) {
  Workload workload;
  auto terms = MakeTerms(num_terms);

  workload.dir = std::make_unique<irs::MemoryDirectory>();
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(docs_per_segment), executor,
                      parallelism));

  BuildCommittedSegments(*workload.writer, segments, docs_per_segment, terms);
  AddRemovalQueryContexts(*workload.writer, removal_contexts, terms);

  workload.total_docs = segments * docs_per_segment;
  workload.segment_count = segments;
  workload.removed_docs_total = EstimatedRemovedDocsUniform(
    segments, docs_per_segment, removal_contexts, num_terms);

  return workload;
}

template <typename PrepareFn>
void RunBenchLoop(benchmark::State& state, PrepareFn prepare) {
  for (auto _ : state) {
    state.PauseTiming();
    auto workload = prepare();
    state.ResumeTiming();

    benchmark::DoNotOptimize(workload.writer->Commit());

    state.PauseTiming();
    workload.writer.reset();
    workload.dir.reset();
    state.ResumeTiming();
  }
}

// ---------------------------- Uniform sweep ----------------------------

void BmStage1Uniform(benchmark::State& state) {
  const auto segments = state.range(0);
  const auto docs_per_segment = state.range(1);
  const auto removal_contexts = state.range(2);
  const auto selectivity_pct = state.range(3);
  const auto threads = state.range(4);

  const auto num_terms = IndexedTermsForSelectivity(selectivity_pct);
  auto executor = MakeExecutor(threads);

  auto prepare = [&] {
    return PrepareUniformWorkload(segments, docs_per_segment, removal_contexts,
                                  num_terms, executor,
                                  static_cast<size_t>(threads));
  };
  RunBenchLoop(state, prepare);
  StopExecutor(executor);

  Workload reference;
  reference.total_docs = segments * docs_per_segment;
  reference.segment_count = segments;
  reference.removed_docs_total = EstimatedRemovedDocsUniform(
    segments, docs_per_segment, removal_contexts, num_terms);
  SetCommonCounters(state, reference, removal_contexts, num_terms, threads);
}

constexpr std::array<int64_t, 4> kThreadsAll = {0, 2, 4, 8};

void AddUniformArgs(benchmark::internal::Benchmark* b, int64_t segments,
                    int64_t docs, int64_t contexts, int64_t sel_pct) {
  for (auto threads : kThreadsAll) {
    b->Args({segments, docs, contexts, sel_pct, threads});
  }
}

// Series 1: vary committed_segments. N=2 is the smallest value that activates
// the executor (n_active >= 2). N=1 stays for sync baseline.
void RegisterSegmentsSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kDocsPerSegment = 4096;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto segments : {1, 2, 4, 16, 64, 256}) {
    AddUniformArgs(b, segments, kDocsPerSegment, kContexts, kSelectivityPct);
  }
}

// Series 2: vary docs_per_segment. Extended down to docs=4 to cover the
// W*Q ≈ 8k..524k range for crossover discovery.
void RegisterSegmentSizeSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kSegments = 64;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto docs : {4, 16, 64, 256, 1024, 4096, 16384, 65536}) {
    AddUniformArgs(b, kSegments, docs, kContexts, kSelectivityPct);
  }
}

// Series 3: vary removal_contexts.
void RegisterContextsSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kSegments = 64;
  constexpr int64_t kDocsPerSegment = 4096;
  constexpr int64_t kSelectivityPct = 10;
  for (auto contexts : {1, 8, 32, 128}) {
    AddUniformArgs(b, kSegments, kDocsPerSegment, contexts, kSelectivityPct);
  }
}

// Series 4: vary selectivity.
void RegisterSelectivitySeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kSegments = 64;
  constexpr int64_t kDocsPerSegment = 4096;
  constexpr int64_t kContexts = 32;
  for (auto sel : {1, 10, 50}) {
    AddUniformArgs(b, kSegments, kDocsPerSegment, kContexts, sel);
  }
}

BENCHMARK(BmStage1Uniform)
  ->Name("BmStage1_Segments")
  ->Apply(RegisterSegmentsSeries)
  ->ArgNames({"segments", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

BENCHMARK(BmStage1Uniform)
  ->Name("BmStage1_SegmentSize")
  ->Apply(RegisterSegmentSizeSeries)
  ->ArgNames({"segments", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

BENCHMARK(BmStage1Uniform)
  ->Name("BmStage1_Contexts")
  ->Apply(RegisterContextsSeries)
  ->ArgNames({"segments", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

BENCHMARK(BmStage1Uniform)
  ->Name("BmStage1_Selectivity")
  ->Apply(RegisterSelectivitySeries)
  ->ArgNames({"segments", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

// ---------------------------- Skewed sweep ----------------------------
//
// Args:
//   range(0): small_count   (number of small segments; total = small_count+1)
//   range(1): small_docs    (docs per small segment)
//   range(2): fat_docs      (docs in the single fat segment)
//   range(3): removal_contexts
//   range(4): selectivity_pct
//   range(5): threads

Workload PrepareSkewedWorkload(int64_t small_count, int64_t small_docs,
                               int64_t fat_docs, int64_t removal_contexts,
                               size_t num_terms,
                               const yaclib::IExecutorPtr& executor,
                               size_t parallelism) {
  Workload workload;
  auto terms = MakeTerms(num_terms);

  workload.dir = std::make_unique<irs::MemoryDirectory>();
  // Start with a permissive limit; switched inside the builder.
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(fat_docs), executor, parallelism));

  BuildCommittedSegmentsSkewed(*workload.writer, small_count, small_docs,
                               fat_docs, terms);
  AddRemovalQueryContexts(*workload.writer, removal_contexts, terms);

  workload.segment_count = small_count + 1;
  workload.total_docs = fat_docs + small_count * small_docs;
  // Removed estimate: distinct buckets times per-segment matches summed.
  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  const auto fat_matches =
    distinct * (fat_docs / static_cast<int64_t>(num_terms));
  const auto small_matches =
    distinct * (small_docs / static_cast<int64_t>(num_terms));
  workload.removed_docs_total = fat_matches + small_count * small_matches;
  return workload;
}

void BmStage1Skewed(benchmark::State& state) {
  const auto small_count = state.range(0);
  const auto small_docs = state.range(1);
  const auto fat_docs = state.range(2);
  const auto removal_contexts = state.range(3);
  const auto selectivity_pct = state.range(4);
  const auto threads = state.range(5);

  const auto num_terms = IndexedTermsForSelectivity(selectivity_pct);
  auto executor = MakeExecutor(threads);

  auto prepare = [&] {
    return PrepareSkewedWorkload(small_count, small_docs, fat_docs,
                                 removal_contexts, num_terms, executor,
                                 static_cast<size_t>(threads));
  };
  RunBenchLoop(state, prepare);
  StopExecutor(executor);

  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  Workload reference;
  reference.segment_count = small_count + 1;
  reference.total_docs = fat_docs + small_count * small_docs;
  reference.removed_docs_total =
    distinct * (fat_docs / static_cast<int64_t>(num_terms)) +
    small_count * distinct * (small_docs / static_cast<int64_t>(num_terms));
  SetCommonCounters(state, reference, removal_contexts, num_terms, threads);
}

void RegisterSkewedSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kSmallDocs = 512;
  constexpr int64_t kFatDocs = 32768;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto small_count : {8, 32, 128}) {
    for (auto threads : {0, 4, 8}) {
      b->Args({small_count, kSmallDocs, kFatDocs, kContexts, kSelectivityPct,
               threads});
    }
  }
}

BENCHMARK(BmStage1Skewed)
  ->Name("BmStage1_Skewed")
  ->Apply(RegisterSkewedSeries)
  ->ArgNames({"small_count", "small_docs", "fat_docs", "contexts", "sel_pct",
              "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

// -------------------------- Heavy skewed workloads -------------------------
//
// Heavier and more uneven loads — kept as a regression catcher for the
// stage 1 batching path. Used previously to compare CountBased against
// SizeAwareGreedy (LPT) and SizeAwareContiguous variants; size-aware
// strategies did not beat CountBased on average (see
// stage1_executor_history.md), and were removed. The shapes here remain
// because they exercise the corners (single huge segment, multiple
// hotspots, dense fat) and would catch regressions in any future change to
// the dispatch.
//
// Args:
//   range(0): fat_count
//   range(1): fat_docs
//   range(2): small_count
//   range(3): small_docs
//   range(4): removal_contexts
//   range(5): selectivity_pct
//   range(6): threads             (0 = sync, otherwise executor)

Workload PrepareMultiSkewedWorkload(int64_t fat_count, int64_t fat_docs,
                                    int64_t small_count, int64_t small_docs,
                                    int64_t removal_contexts, size_t num_terms,
                                    const yaclib::IExecutorPtr& executor,
                                    size_t parallelism) {
  Workload workload;
  auto terms = MakeTerms(num_terms);

  workload.dir = std::make_unique<irs::MemoryDirectory>();
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(fat_docs), executor, parallelism));

  BuildCommittedSegmentsMultiSkewed(*workload.writer, fat_count, fat_docs,
                                    small_count, small_docs, terms);
  AddRemovalQueryContexts(*workload.writer, removal_contexts, terms);

  workload.segment_count = fat_count + small_count;
  workload.total_docs = fat_count * fat_docs + small_count * small_docs;
  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  const auto fat_matches =
    distinct * (fat_docs / static_cast<int64_t>(num_terms));
  const auto small_matches =
    distinct * (small_docs / static_cast<int64_t>(num_terms));
  workload.removed_docs_total =
    fat_count * fat_matches + small_count * small_matches;
  return workload;
}

void BmStage1HeavySkewed(benchmark::State& state) {
  const auto fat_count = state.range(0);
  const auto fat_docs = state.range(1);
  const auto small_count = state.range(2);
  const auto small_docs = state.range(3);
  const auto removal_contexts = state.range(4);
  const auto selectivity_pct = state.range(5);
  const auto threads = state.range(6);

  const auto num_terms = IndexedTermsForSelectivity(selectivity_pct);
  auto executor = MakeExecutor(threads);

  auto prepare = [&] {
    return PrepareMultiSkewedWorkload(fat_count, fat_docs, small_count,
                                      small_docs, removal_contexts, num_terms,
                                      executor, static_cast<size_t>(threads));
  };
  RunBenchLoop(state, prepare);
  StopExecutor(executor);

  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  Workload reference;
  reference.segment_count = fat_count + small_count;
  reference.total_docs = fat_count * fat_docs + small_count * small_docs;
  reference.removed_docs_total =
    fat_count * (distinct * (fat_docs / static_cast<int64_t>(num_terms))) +
    small_count * (distinct * (small_docs / static_cast<int64_t>(num_terms)));
  SetCommonCounters(state, reference, removal_contexts, num_terms, threads);
  state.counters["fat_count"] = static_cast<double>(fat_count);
  state.counters["fat_docs"] = static_cast<double>(fat_docs);
}

void RegisterHeavySkewedSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  // Each tuple: (fat_count, fat_docs, small_count, small_docs).
  // All configs sized so total docs ~ 0.5..1.5M and N is well above any T
  // we test, to guarantee executor runs.
  const std::vector<std::array<int64_t, 4>> shapes = {
    {1, 262144, 256, 1024},  // single fat, moderate skew (N=257)
    {1, 524288, 512, 512},   // single fat, extreme skew  (N=513)
    {4, 65536, 256, 1024},   // few fat, balanced totals  (N=260)
    {16, 16384, 256, 1024},  // many fat, mid-skew        (N=272)
    {16, 16384, 48, 1024},   // dense fat                 (N=64)
  };
  for (const auto& s : shapes) {
    for (auto threads : {0, 4, 8, 16}) {  // 0 = sync baseline
      b->Args({s[0], s[1], s[2], s[3], kContexts, kSelectivityPct, threads});
    }
  }
}

BENCHMARK(BmStage1HeavySkewed)
  ->Name("BmStage1_HeavySkewed")
  ->Apply(RegisterHeavySkewedSeries)
  ->ArgNames({"fat_count", "fat_docs", "small_count", "small_docs", "contexts",
              "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
