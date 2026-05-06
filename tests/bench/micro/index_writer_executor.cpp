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

// Measures IndexWriter Commit() with and without an executor on a Stage 1-heavy
// workload: many already committed segments plus many removal query contexts.

#include <benchmark/benchmark.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <iresearch/analysis/tokenizers.hpp>
#include <iresearch/formats/formats.hpp>
#include <iresearch/index/index_features.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/search/all_filter.hpp>
#include <iresearch/store/data_output.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <memory>
#include <mutex>
#include <string_view>
#include <vector>
#include <yaclib/runtime/fair_thread_pool.hpp>

namespace {

constexpr std::string_view kFormatName = "1_5simd";
constexpr std::string_view kFieldName = "value";

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

irs::IndexWriterOptions MakeWriterOptions(
  uint32_t docs_per_segment, const yaclib::IExecutorPtr& executor) {
  irs::IndexWriterOptions options;
  options.lock_repository = false;
  options.segment_docs_max = docs_per_segment;
  options.executor = executor;
  return options;
}

void BuildCommittedSegments(irs::IndexWriter& writer, int64_t segment_count,
                            int64_t docs_per_segment) {
  StringField field{kFieldName};
  field.Value("x");

  const auto docs_count = segment_count * docs_per_segment;
  for (int64_t i = 0; i != docs_count; ++i) {
    auto trx = writer.GetBatch();
    auto doc = trx.Insert();
    const auto ok = doc.Insert<irs::Action::INDEX>(field);
    if (!ok) {
      std::fprintf(stderr,
                   "index_writer_executor bench: document insert failed\n");
      std::abort();
    }
  }

  const auto committed = writer.Commit();
  benchmark::DoNotOptimize(committed);
}

void AddRemovalQueryContexts(irs::IndexWriter& writer, int64_t query_contexts) {
  std::vector<irs::IndexWriter::Transaction> transactions;
  transactions.reserve(static_cast<size_t>(query_contexts));

  for (int64_t i = 0; i != query_contexts; ++i) {
    auto& trx = transactions.emplace_back(writer.GetBatch());
    irs::Filter::ptr filter = std::make_unique<irs::All>();
    trx.Remove(std::move(filter));
  }

  for (auto& trx : transactions) {
    benchmark::DoNotOptimize(trx.Commit());
  }
}

struct Workload {
  std::unique_ptr<irs::MemoryDirectory> dir;
  irs::IndexWriter::ptr writer;
};

Workload PrepareWorkload(int64_t segment_count, int64_t query_contexts,
                         int64_t docs_per_segment,
                         const yaclib::IExecutorPtr& executor) {
  Workload workload;

  workload.dir = std::make_unique<irs::MemoryDirectory>();
  workload.writer = irs::IndexWriter::Make(
    *workload.dir, GetFormat(), irs::kOmCreate,
    MakeWriterOptions(static_cast<uint32_t>(docs_per_segment), executor));

  BuildCommittedSegments(*workload.writer, segment_count, docs_per_segment);
  AddRemovalQueryContexts(*workload.writer, query_contexts);

  return workload;
}

void CleanupWorkload(Workload& workload) {
  workload.writer.reset();
  workload.dir.reset();
}

struct SharedExecutor {
  yaclib::IntrusivePtr<yaclib::FairThreadPool> pool;
  yaclib::IExecutorPtr executor;
};

SharedExecutor MakeExecutor(int64_t threads) {
  SharedExecutor shared;

  if (threads != 0) {
    shared.pool =
      yaclib::MakeFairThreadPool(static_cast<std::uint64_t>(threads));
    shared.executor = shared.pool;
  }

  return shared;
}

void StopExecutor(SharedExecutor& shared) {
  if (shared.pool) {
    shared.pool->Stop();
    shared.pool->Wait();
    shared.executor = nullptr;
    shared.pool = nullptr;
  }
}

void SetCounters(benchmark::State& state, int64_t segment_count,
                 int64_t query_contexts, int64_t docs_per_segment,
                 int64_t threads, int64_t batch_size) {
  state.SetItemsProcessed(state.iterations() * segment_count * query_contexts *
                          docs_per_segment);
  state.counters["segments"] = static_cast<double>(segment_count);
  state.counters["queries"] = static_cast<double>(query_contexts);
  state.counters["docs_per_segment"] = static_cast<double>(docs_per_segment);
  state.counters["threads"] = static_cast<double>(threads);
  state.counters["batch_size"] = static_cast<double>(batch_size);
}

void BmCommitAllRemovals(benchmark::State& state, bool use_executor) {
  const auto segment_count = state.range(0);
  const auto query_contexts = state.range(1);
  const auto docs_per_segment = state.range(2);
  const auto threads = use_executor ? state.range(3) : 0;

  auto shared_executor = MakeExecutor(threads);

  for (auto _ : state) {
    state.PauseTiming();
    auto workload = PrepareWorkload(segment_count, query_contexts,
                                    docs_per_segment, shared_executor.executor);
    state.ResumeTiming();

    const auto modified = workload.writer->Commit();
    benchmark::DoNotOptimize(modified);

    state.PauseTiming();
    CleanupWorkload(workload);
    state.ResumeTiming();
  }

  StopExecutor(shared_executor);
  SetCounters(state, segment_count, query_contexts, docs_per_segment, threads,
              1);
}

void BmCommitAllRemovalsBatch(benchmark::State& state, bool use_executor) {
  const auto segment_count = state.range(0);
  const auto query_contexts = state.range(1);
  const auto docs_per_segment = state.range(2);
  const auto threads = use_executor ? state.range(3) : 0;
  const auto batch_size = state.range(4);

  auto shared_executor = MakeExecutor(threads);

  while (state.KeepRunningBatch(batch_size)) {
    state.PauseTiming();
    std::vector<Workload> workloads;
    workloads.reserve(static_cast<size_t>(batch_size));
    for (int64_t i = 0; i != batch_size; ++i) {
      workloads.emplace_back(PrepareWorkload(segment_count, query_contexts,
                                             docs_per_segment,
                                             shared_executor.executor));
    }
    state.ResumeTiming();

    for (auto& workload : workloads) {
      const auto modified = workload.writer->Commit();
      benchmark::DoNotOptimize(modified);
    }

    state.PauseTiming();
    for (auto& workload : workloads) {
      CleanupWorkload(workload);
    }
    state.ResumeTiming();
  }

  StopExecutor(shared_executor);
  SetCounters(state, segment_count, query_contexts, docs_per_segment, threads,
              batch_size);
}

void BmNoExecutor(benchmark::State& state) {
  BmCommitAllRemovals(state, false);
}

void BmFairThreadPool(benchmark::State& state) {
  BmCommitAllRemovals(state, true);
}

void BmNoExecutorBatch(benchmark::State& state) {
  BmCommitAllRemovalsBatch(state, false);
}

void BmFairThreadPoolBatch(benchmark::State& state) {
  BmCommitAllRemovalsBatch(state, true);
}

BENCHMARK(BmNoExecutor)
  ->Args({16, 4, 256, 0})
  ->Args({32, 8, 256, 0})
  ->Args({64, 16, 256, 0})
  ->UseRealTime()
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BmFairThreadPool)
  ->Args({16, 4, 256, 8})
  ->Args({32, 8, 256, 8})
  ->Args({64, 16, 256, 8})
  ->UseRealTime()
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BmNoExecutorBatch)
  ->Args({16, 4, 256, 0, 8})
  ->Args({32, 8, 256, 0, 8})
  ->Args({64, 16, 256, 0, 4})
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BmFairThreadPoolBatch)
  ->Args({16, 4, 256, 8, 8})
  ->Args({32, 8, 256, 8, 8})
  ->Args({64, 16, 256, 8, 4})
  ->Unit(benchmark::kMillisecond);

}  // namespace

BENCHMARK_MAIN();
