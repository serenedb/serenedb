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

// Sweep benchmark for IndexWriter Stage 1 of PrepareFlush -- the per-segment
// loop that applies removal QueryContexts to already committed segments.
//
// Goal: figure out at which (segments, docs_per_segment, removal_contexts,
// selectivity, threads) configurations the executor branch beats the sync
// branch.
//
// Series (use --benchmark_filter to run individual ones):
//   BmStage1_Segments       -- sweep committed_segments
//   BmStage1_SegmentSize    -- sweep docs_per_segment
//   BmStage1_Contexts       -- sweep removal_contexts
//   BmStage1_Selectivity    -- sweep selectivity_pct
//   BmStage1_Skewed         -- one fat segment + many small ones
//   BmStage1_HeavySkewed    -- several fat segments + many small ones
//
// Use --benchmark_min_time=Nx (fixed iterations), not =Ns. Each iteration has
// expensive PauseTiming() setup, so wall-clock time / iteration >> measured
// time / iteration. Time-based min_time loops forever on small configs.
//
// Recommended:
//   ./bin/serenedb-bench-micro-iresearch-index_writer_stage1 \
//       --benchmark_min_time=5x \
//       --benchmark_out=stage1.json --benchmark_out_format=json

#include <algorithm>
#include <array>

#include "bench_common.hpp"

namespace {

using namespace irbench;

// Build (small_count + 1) segments -- one "fat" of fat_docs followed by
// small_count small ones of small_docs each. Uses runtime Options() to change
// segment_docs_max between batches.
void BuildCommittedSegmentsSkewed(irs::IndexWriter& writer, int64_t small_count,
                                  int64_t small_docs, int64_t fat_docs,
                                  const std::vector<std::string>& terms) {
  irs::SegmentOptions fat_opts;
  fat_opts.segment_docs_max = static_cast<uint32_t>(fat_docs);
  writer.Options(fat_opts);
  InsertAndCommit(writer, fat_docs, terms);

  irs::SegmentOptions small_opts;
  small_opts.segment_docs_max = static_cast<uint32_t>(small_docs);
  writer.Options(small_opts);
  InsertAndCommit(writer, small_count * small_docs, terms);
}

// Multi-fat variant: `fat_count` segments of `fat_docs` followed by
// `small_count` segments of `small_docs`.
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

// For uniform segments: each segment has docs_per_segment docs distributed
// uniformly across `num_terms`. Each of the first min(contexts, num_terms)
// distinct filters matches ~ docs_per_segment / num_terms docs.
int64_t EstimatedRemovedDocsUniform(int64_t segment_count,
                                    int64_t docs_per_segment,
                                    int64_t removal_contexts,
                                    size_t num_terms) {
  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  const auto per_segment =
    distinct * (docs_per_segment / static_cast<int64_t>(num_terms));
  return segment_count * per_segment;
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

void RegisterSegmentsSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kDocsPerSegment = 4096;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto segments : {1, 2, 4, 16, 64, 256}) {
    AddUniformArgs(b, segments, kDocsPerSegment, kContexts, kSelectivityPct);
  }
}

void RegisterSegmentSizeSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kSegments = 64;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto docs : {4, 16, 64, 256, 1024, 4096, 16384, 65536}) {
    AddUniformArgs(b, kSegments, docs, kContexts, kSelectivityPct);
  }
}

void RegisterContextsSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kSegments = 64;
  constexpr int64_t kDocsPerSegment = 4096;
  constexpr int64_t kSelectivityPct = 10;
  for (auto contexts : {1, 8, 32, 128}) {
    AddUniformArgs(b, kSegments, kDocsPerSegment, contexts, kSelectivityPct);
  }
}

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

Workload PrepareSkewedWorkload(int64_t small_count, int64_t small_docs,
                               int64_t fat_docs, int64_t removal_contexts,
                               size_t num_terms,
                               const yaclib::IExecutorPtr& executor,
                               size_t parallelism) {
  Workload workload;
  auto terms = MakeTerms(num_terms);

  workload.dir = std::make_unique<irs::MemoryDirectory>();
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(fat_docs), executor, parallelism));

  BuildCommittedSegmentsSkewed(*workload.writer, small_count, small_docs,
                               fat_docs, terms);
  AddRemovalQueryContexts(*workload.writer, removal_contexts, terms);

  workload.segment_count = small_count + 1;
  workload.total_docs = fat_docs + small_count * small_docs;
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
