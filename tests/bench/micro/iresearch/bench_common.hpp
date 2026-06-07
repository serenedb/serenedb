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

// Shared helpers for the IndexWriter executor micro-benchmarks. Each stage has
// its own translation unit / executable (index_writer_stage1, _stage2, _full)
// and includes this header for the workload-building and measurement plumbing.

#pragma once

#include <benchmark/benchmark.h>

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
#include <yaclib/runtime/fair_thread_pool.hpp>

namespace irbench {

inline constexpr std::string_view kFormatName = "1_5simd";
inline constexpr std::string_view kFieldName = "value";

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

inline irs::Format::ptr GetFormat() {
  static std::once_flag once;
  std::call_once(once, [] { irs::formats::Init(); });

  auto format = irs::formats::Get(std::string{kFormatName});
  if (!format) {
    std::fprintf(stderr, "iresearch bench: format '%.*s' not found\n",
                 static_cast<int>(kFormatName.size()), kFormatName.data());
    std::abort();
  }
  return format;
}

inline irs::IndexWriterOptions MakeWriterOptions(
  uint32_t docs_per_segment, const yaclib::IExecutorPtr& executor,
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
inline size_t IndexedTermsForSelectivity(int64_t selectivity_pct) {
  if (selectivity_pct <= 0) {
    return 1;
  }
  if (selectivity_pct >= 100) {
    return 1;
  }
  // ceil(100 / sel)
  return static_cast<size_t>((100 + selectivity_pct - 1) / selectivity_pct);
}

inline std::vector<std::string> MakeTerms(size_t term_count) {
  std::vector<std::string> terms;
  terms.reserve(term_count);

  for (size_t i = 0; i != term_count; ++i) {
    terms.emplace_back("bucket_" + std::to_string(i));
  }

  return terms;
}

inline irs::Filter::ptr MakeTermFilter(std::string_view term) {
  auto filter = std::make_unique<irs::ByTerm>();
  *filter->mutable_field() = kFieldName;
  filter->mutable_options()->term = irs::ViewCast<irs::byte_type>(term);
  return filter;
}

// Insert `docs_count` docs and Commit() -- produces ceil(docs_count /
// segment_docs_max) sealed segments. The writer's segment_docs_max must be
// already configured (either via initial options or `writer.Options(...)`).
inline void InsertAndCommit(irs::IndexWriter& writer, int64_t docs_count,
                            const std::vector<std::string>& terms) {
  StringField field{kFieldName};
  for (int64_t i = 0; i != docs_count; ++i) {
    field.Value(terms[static_cast<size_t>(i) % terms.size()]);
    auto trx = writer.GetBatch();
    auto doc = trx.Insert();
    const auto ok = doc.Insert(field);
    if (!ok) {
      std::fprintf(stderr, "iresearch bench: document insert failed\n");
      std::abort();
    }
  }
  const auto committed = writer.Commit();
  benchmark::DoNotOptimize(committed);
}

// Build N uniform-sized segments, each of docs_per_segment.
inline void BuildCommittedSegments(irs::IndexWriter& writer,
                                   int64_t segment_count,
                                   int64_t docs_per_segment,
                                   const std::vector<std::string>& terms) {
  InsertAndCommit(writer, segment_count * docs_per_segment, terms);
}

inline void AddRemovalQueryContexts(irs::IndexWriter& writer,
                                    int64_t removal_contexts,
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

inline ThreadPoolPtr MakeExecutor(int64_t threads) {
  if (threads > 0) {
    return yaclib::MakeFairThreadPool(static_cast<uint64_t>(threads));
  }
  return nullptr;
}

inline void StopExecutor(ThreadPoolPtr& executor) {
  if (executor) {
    executor->Stop();
    executor->Wait();
    executor = nullptr;
  }
}

inline void SetCommonCounters(benchmark::State& state, const Workload& workload,
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

// Each iteration rebuilds the workload under PauseTiming() and measures only the
// final Commit(). Use --benchmark_min_time=Nx (fixed iterations), not =Ns: the
// per-iteration setup is expensive, so time-based min_time loops forever on
// small configs.
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

}  // namespace irbench
