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

// Sweep benchmark for IndexWriter Stage 2 of PrepareFlush -- the per-import loop
// that applies removal QueryContexts to imported segments (the parallelizable
// kernel). The workload registers `imports` imported segments via Import() and
// `removal_contexts` removal queries, then measures the final Commit().
//
// On startup the binary first runs a deterministic sync-vs-executor equivalence
// check (parallelism 0 vs 8 on gate-crossing workloads) and aborts on mismatch
// -- this is the correctness gate for the parallel Stage 2 dispatch, which the
// small functional unit tests never reach (they stay below kMinWork).
//
// Series:
//   BmStage2_Imports      -- sweep imports
//   BmStage2_ImportSize   -- sweep docs_per_import
//   BmStage2_Contexts     -- sweep removal_contexts
//   BmStage2_Selectivity  -- sweep selectivity_pct
//
// Use --benchmark_min_time=Nx (fixed iterations), not =Ns.
//   ./bin/serenedb-bench-micro-iresearch-index_writer_stage2 \
//       --benchmark_min_time=5x \
//       --benchmark_out=stage2.json --benchmark_out_format=json

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iresearch/index/directory_reader.hpp>
#include <utility>
#include <vector>

#include "bench_common.hpp"

namespace {

using namespace irbench;

// Build `imports` imported segments (each a copy of one `docs_per_import`-doc
// source index) plus `removal_contexts` removal queries on the target writer.
// Import() runs synchronously, so the source index can be released immediately.
Workload PrepareImportWorkload(int64_t imports, int64_t docs_per_import,
                               int64_t removal_contexts, size_t num_terms,
                               const yaclib::IExecutorPtr& executor,
                               size_t parallelism) {
  auto terms = MakeTerms(num_terms);

  irs::MemoryDirectory src_dir;
  auto src_writer = irs::IndexWriter::Make(
    src_dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(docs_per_import), nullptr, 0));
  InsertAndCommit(*src_writer, docs_per_import, terms);
  auto src_reader = src_writer->GetSnapshot();

  Workload workload;
  workload.dir = std::make_unique<irs::MemoryDirectory>();
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(docs_per_import), executor,
                      parallelism));

  for (int64_t i = 0; i != imports; ++i) {
    if (!workload.writer->Import(src_reader)) {
      std::fprintf(stderr, "index_writer_stage2 bench: Import failed\n");
      std::abort();
    }
  }
  AddRemovalQueryContexts(*workload.writer, removal_contexts, terms);

  workload.segment_count = imports;
  workload.total_docs = imports * docs_per_import;
  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  workload.removed_docs_total =
    imports * distinct * (docs_per_import / static_cast<int64_t>(num_terms));
  return workload;
}

// ------------------------------ Equivalence ------------------------------

struct Fingerprint {
  size_t segments = 0;
  uint64_t live_docs = 0;
  uint64_t docs = 0;
  std::vector<std::pair<uint64_t, uint64_t>> per_segment;  // (docs, live_docs)

  bool operator==(const Fingerprint& o) const = default;
};

Fingerprint SnapshotFingerprint(irs::IndexWriter& writer) {
  auto reader = writer.GetSnapshot();
  Fingerprint fp;
  fp.segments = reader.size();
  fp.live_docs = reader.live_docs_count();
  fp.docs = reader.docs_count();
  for (const auto& sub : reader) {
    fp.per_segment.emplace_back(sub.docs_count(), sub.live_docs_count());
  }
  std::sort(fp.per_segment.begin(), fp.per_segment.end());
  return fp;
}

Fingerprint CommitAndFingerprint(int64_t imports, int64_t docs,
                                 int64_t contexts, size_t num_terms,
                                 const yaclib::IExecutorPtr& executor,
                                 size_t parallelism) {
  auto workload = PrepareImportWorkload(imports, docs, contexts, num_terms,
                                        executor, parallelism);
  benchmark::DoNotOptimize(workload.writer->Commit());
  return SnapshotFingerprint(*workload.writer);
}

// Same deterministic workload committed with parallelism 0 and 8 must yield a
// byte-identical index. Workloads are sized to cross the Stage 2 kMinWork gate
// so the executor branch is actually taken.
void VerifyStage2Equivalence() {
  struct Cfg {
    int64_t imports;
    int64_t docs;
    int64_t contexts;
    int64_t sel_pct;
  };
  constexpr std::array<Cfg, 4> kCfgs = {{
    {4, 4096, 32, 10},
    {8, 2048, 16, 5},
    {3, 8192, 64, 25},
    {16, 1024, 8, 50},
  }};

  for (const auto& c : kCfgs) {
    const auto num_terms = IndexedTermsForSelectivity(c.sel_pct);
    const auto sync =
      CommitAndFingerprint(c.imports, c.docs, c.contexts, num_terms, nullptr, 0);

    auto executor = MakeExecutor(8);
    const auto par = CommitAndFingerprint(c.imports, c.docs, c.contexts,
                                          num_terms, executor, 8);
    StopExecutor(executor);

    if (!(sync == par)) {
      std::fprintf(stderr,
                   "Stage2 equivalence FAILED (imports=%lld docs=%lld "
                   "contexts=%lld sel=%lld): sync{seg=%zu,live=%llu} != "
                   "exec{seg=%zu,live=%llu}\n",
                   static_cast<long long>(c.imports),
                   static_cast<long long>(c.docs),
                   static_cast<long long>(c.contexts),
                   static_cast<long long>(c.sel_pct), sync.segments,
                   static_cast<unsigned long long>(sync.live_docs), par.segments,
                   static_cast<unsigned long long>(par.live_docs));
      std::abort();
    }
  }
  std::fprintf(stderr, "Stage2 equivalence OK (sync == executor)\n");
}

// -------------------------------- Sweep ----------------------------------

void BmStage2Import(benchmark::State& state) {
  const auto imports = state.range(0);
  const auto docs_per_import = state.range(1);
  const auto removal_contexts = state.range(2);
  const auto selectivity_pct = state.range(3);
  const auto threads = state.range(4);

  const auto num_terms = IndexedTermsForSelectivity(selectivity_pct);
  auto executor = MakeExecutor(threads);

  auto prepare = [&] {
    return PrepareImportWorkload(imports, docs_per_import, removal_contexts,
                                 num_terms, executor,
                                 static_cast<size_t>(threads));
  };
  RunBenchLoop(state, prepare);
  StopExecutor(executor);

  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  Workload reference;
  reference.segment_count = imports;
  reference.total_docs = imports * docs_per_import;
  reference.removed_docs_total =
    imports * distinct * (docs_per_import / static_cast<int64_t>(num_terms));
  SetCommonCounters(state, reference, removal_contexts, num_terms, threads);
  state.counters["imports"] = static_cast<double>(imports);
}

constexpr std::array<int64_t, 4> kThreadsAll = {0, 2, 4, 8};

void AddImportArgs(benchmark::internal::Benchmark* b, int64_t imports,
                   int64_t docs, int64_t contexts, int64_t sel_pct) {
  for (auto threads : kThreadsAll) {
    b->Args({imports, docs, contexts, sel_pct, threads});
  }
}

// imports=1 stays for the sync baseline; >=2 activates the executor.
void RegisterImportsSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kDocsPerImport = 4096;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto imports : {1, 2, 4, 16, 64, 256}) {
    AddImportArgs(b, imports, kDocsPerImport, kContexts, kSelectivityPct);
  }
}

void RegisterImportSizeSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kImports = 64;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  for (auto docs : {4, 16, 64, 256, 1024, 4096, 16384}) {
    AddImportArgs(b, kImports, docs, kContexts, kSelectivityPct);
  }
}

void RegisterContextsSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kImports = 64;
  constexpr int64_t kDocsPerImport = 4096;
  constexpr int64_t kSelectivityPct = 10;
  for (auto contexts : {1, 8, 32, 128}) {
    AddImportArgs(b, kImports, kDocsPerImport, contexts, kSelectivityPct);
  }
}

void RegisterSelectivitySeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kImports = 64;
  constexpr int64_t kDocsPerImport = 4096;
  constexpr int64_t kContexts = 32;
  for (auto sel : {1, 10, 50}) {
    AddImportArgs(b, kImports, kDocsPerImport, kContexts, sel);
  }
}

BENCHMARK(BmStage2Import)
  ->Name("BmStage2_Imports")
  ->Apply(RegisterImportsSeries)
  ->ArgNames({"imports", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

BENCHMARK(BmStage2Import)
  ->Name("BmStage2_ImportSize")
  ->Apply(RegisterImportSizeSeries)
  ->ArgNames({"imports", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

BENCHMARK(BmStage2Import)
  ->Name("BmStage2_Contexts")
  ->Apply(RegisterContextsSeries)
  ->ArgNames({"imports", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

BENCHMARK(BmStage2Import)
  ->Name("BmStage2_Selectivity")
  ->Apply(RegisterSelectivitySeries)
  ->ArgNames({"imports", "docs", "contexts", "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

}  // namespace

int main(int argc, char** argv) {
  VerifyStage2Equivalence();

  benchmark::Initialize(&argc, argv);
  if (benchmark::ReportUnrecognizedArguments(argc, argv)) {
    return 1;
  }
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  return 0;
}
