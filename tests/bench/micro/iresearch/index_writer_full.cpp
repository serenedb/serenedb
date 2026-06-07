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

// End-to-end PrepareFlush benchmark: a single measured Commit() that exercises
// every stage at once --
//   Stage 1: removals over pre-existing committed segments,
//   Stage 2: imported segments,
//   Stage 0 + Stage 3: pending (uncommitted) buffered docs flushed into new
//            segments,
//   removal queries hitting all of the above.
// Used to see the net effect of the executor on a realistic mixed commit.
//
//   ./bin/serenedb-bench-micro-iresearch-index_writer_full \
//       --benchmark_min_time=5x \
//       --benchmark_out=full.json --benchmark_out_format=json

#include <algorithm>
#include <array>
#include <cstdint>

#include "bench_common.hpp"

namespace {

using namespace irbench;

// Insert `docs` documents via committed transactions but WITHOUT a writer
// Commit(), so they remain pending and are flushed by the next Commit()
// (Stage 0) and turned into new segments (Stage 3).
void InsertPending(irs::IndexWriter& writer, int64_t docs,
                   const std::vector<std::string>& terms) {
  StringField field{kFieldName};
  for (int64_t i = 0; i != docs; ++i) {
    field.Value(terms[static_cast<size_t>(i) % terms.size()]);
    auto trx = writer.GetBatch();
    auto doc = trx.Insert();
    const auto ok = doc.Insert(field);
    if (!ok) {
      std::fprintf(stderr, "index_writer_full bench: pending insert failed\n");
      std::abort();
    }
    benchmark::DoNotOptimize(trx.Commit());
  }
}

// committed_segments x docs_per_unit committed segments (Stage 1),
// `imports` imported segments of docs_per_unit each (Stage 2),
// `pending_docs` buffered docs (Stage 0 + Stage 3), then removal queries.
Workload PrepareFullWorkload(int64_t committed_segments, int64_t imports,
                             int64_t pending_docs, int64_t docs_per_unit,
                             int64_t removal_contexts, size_t num_terms,
                             const yaclib::IExecutorPtr& executor,
                             size_t parallelism) {
  auto terms = MakeTerms(num_terms);

  Workload workload;
  workload.dir = std::make_unique<irs::MemoryDirectory>();
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(docs_per_unit), executor,
                      parallelism));

  // Stage 1 surface: committed segments.
  BuildCommittedSegments(*workload.writer, committed_segments, docs_per_unit,
                         terms);

  // Stage 2 surface: imported segments (Import runs synchronously here).
  if (imports != 0) {
    irs::MemoryDirectory src_dir;
    auto src_writer = irs::IndexWriter::Make(
      src_dir, GetFormat(), irs::kOmCreate,
      MakeWriterOptions(static_cast<uint32_t>(docs_per_unit), nullptr, 0));
    InsertAndCommit(*src_writer, docs_per_unit, terms);
    auto src_reader = src_writer->GetSnapshot();
    for (int64_t i = 0; i != imports; ++i) {
      if (!workload.writer->Import(src_reader)) {
        std::fprintf(stderr, "index_writer_full bench: Import failed\n");
        std::abort();
      }
    }
  }

  // Stage 0 + Stage 3 surface: pending buffered docs.
  InsertPending(*workload.writer, pending_docs, terms);

  // Removal queries hitting all of the above.
  AddRemovalQueryContexts(*workload.writer, removal_contexts, terms);

  workload.segment_count = committed_segments + imports;
  workload.total_docs =
    (committed_segments + imports) * docs_per_unit + pending_docs;
  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  workload.removed_docs_total =
    (committed_segments + imports) * distinct *
    (docs_per_unit / static_cast<int64_t>(num_terms));
  return workload;
}

void BmFull(benchmark::State& state) {
  const auto committed = state.range(0);
  const auto imports = state.range(1);
  const auto pending_docs = state.range(2);
  const auto docs_per_unit = state.range(3);
  const auto removal_contexts = state.range(4);
  const auto selectivity_pct = state.range(5);
  const auto threads = state.range(6);

  const auto num_terms = IndexedTermsForSelectivity(selectivity_pct);
  auto executor = MakeExecutor(threads);

  auto prepare = [&] {
    return PrepareFullWorkload(committed, imports, pending_docs, docs_per_unit,
                               removal_contexts, num_terms, executor,
                               static_cast<size_t>(threads));
  };
  RunBenchLoop(state, prepare);
  StopExecutor(executor);

  const auto distinct =
    std::min<int64_t>(removal_contexts, static_cast<int64_t>(num_terms));
  Workload reference;
  reference.segment_count = committed + imports;
  reference.total_docs = (committed + imports) * docs_per_unit + pending_docs;
  reference.removed_docs_total =
    (committed + imports) * distinct *
    (docs_per_unit / static_cast<int64_t>(num_terms));
  SetCommonCounters(state, reference, removal_contexts, num_terms, threads);
  state.counters["committed"] = static_cast<double>(committed);
  state.counters["imports"] = static_cast<double>(imports);
  state.counters["pending_docs"] = static_cast<double>(pending_docs);
}

void RegisterFullSeries(benchmark::internal::Benchmark* b) {
  constexpr int64_t kDocsPerUnit = 4096;
  constexpr int64_t kContexts = 32;
  constexpr int64_t kSelectivityPct = 10;
  // Each tuple: (committed_segments, imports, pending_docs).
  const std::array<std::array<int64_t, 3>, 4> shapes = {{
    {16, 16, 65536},   // balanced mix
    {64, 8, 16384},    // commit-heavy
    {8, 64, 16384},    // import-heavy
    {8, 8, 262144},    // pending/flush-heavy
  }};
  for (const auto& s : shapes) {
    for (auto threads : {0, 2, 4, 8}) {
      b->Args({s[0], s[1], s[2], kDocsPerUnit, kContexts, kSelectivityPct,
               threads});
    }
  }
}

BENCHMARK(BmFull)
  ->Name("BmFull")
  ->Apply(RegisterFullSeries)
  ->ArgNames({"committed", "imports", "pending_docs", "docs", "contexts",
              "sel_pct", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime();

}  // namespace

BENCHMARK_MAIN();
