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

#include <absl/strings/str_cat.h>
#include <benchmark/benchmark.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdlib>
#include <duckdb/common/enums/order_type.hpp>
#include <duckdb/common/sorting/presorted_merge.hpp>
#include <duckdb/common/sorting/sort.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/execution/execution_context.hpp>
#include <duckdb/main/client_context.hpp>
#include <duckdb/main/connection.hpp>
#include <duckdb/main/query_result.hpp>
#include <duckdb/main/query_result_stream.hpp>
#include <duckdb/parallel/interrupt.hpp>
#include <duckdb/parallel/thread_context.hpp>
#include <duckdb/planner/bound_result_modifier.hpp>
#include <duckdb/planner/expression/bound_reference_expression.hpp>
#include <fstream>
#include <iresearch/analysis/keyword_tokenizer.hpp>
#include <iresearch/formats/column/col_reader.hpp>
#include <iresearch/formats/column/col_writer.hpp>
#include <iresearch/formats/column/column_reader.hpp>
#include <iresearch/formats/column/read_context.hpp>
#include <iresearch/index/directory_reader.hpp>
#include <iresearch/index/index_writer.hpp>
#include <iresearch/index/merge/key_merge.hpp>
#include <iresearch/index/merge/merge_plan.hpp>
#include <iresearch/index/merge/merge_schedule.hpp>
#include <iresearch/index/merge/sort_spec.hpp>
#include <iresearch/index/merge_writer.hpp>
#include <iresearch/store/memory_directory.hpp>
#include <iresearch/utils/async.hpp>
#include <iresearch/utils/duckdb_engine.hpp>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "insert_field.hpp"
#include "search/compaction/pipeline_merge_executor.h"

extern "C" int JemallocControl(const char* name, void* old_value,
                               size_t* old_size, void* new_value,
                               size_t new_size) __asm__("mallctl")
  __attribute__((weak));

namespace {

constexpr irs::field_id kIdField = 1;
constexpr irs::field_id kServiceField = 2;
constexpr irs::field_id kKey = 100;
constexpr irs::field_id kValue = 101;
constexpr irs::field_id kMessage = 102;
constexpr size_t kSegments = 8;
constexpr size_t kRowsPerSegment = 250000;

enum class Layout : int64_t {
  Disjoint,
  Interleaved,
  Random,
};

struct KeywordField {
  irs::field_id Id() const noexcept { return id; }

  irs::analysis::Tokenizer& GetTokens() const { return stream; }

  std::string_view Value() const noexcept { return value; }

  irs::IndexFeatures GetIndexFeatures() const noexcept {
    return irs::IndexFeatures::Freq;
  }

  bool Write(irs::DataOutput&) const { return true; }

  irs::field_id id{irs::field_limits::invalid()};
  std::string_view value;
  mutable irs::KeywordTokenizer stream;
};

void Append(irs::IndexWriter::Document& doc, irs::field_id id,
            const duckdb::LogicalType& type, const duckdb::Value& value) {
  auto& cw = doc.GetColWriter()->OpenColumn(id, type);
  duckdb::Vector v{type, 1};
  v.SetValue(0, value);
  duckdb::FlatVector::SetSize(v, 1);
  cw.Append(static_cast<uint64_t>(doc.DocId()) - irs::doc_limits::min(), v, 1);
}

std::vector<int64_t> KeysOf(Layout layout, size_t segment) {
  std::vector<int64_t> keys(kRowsPerSegment);
  std::mt19937_64 rng{segment + 1};
  for (size_t i = 0; i != kRowsPerSegment; ++i) {
    switch (layout) {
      case Layout::Disjoint:
        keys[i] = static_cast<int64_t>(segment * kRowsPerSegment + i);
        break;
      case Layout::Interleaved:
        keys[i] = static_cast<int64_t>(i * kSegments + segment);
        break;
      case Layout::Random:
        keys[i] = static_cast<int64_t>(rng() >> 16);
        break;
    }
  }
  std::ranges::sort(keys);
  return keys;
}

struct Inputs {
  irs::MemoryDirectory dir;
  irs::DirectoryReader reader;
};

std::map<Layout, std::unique_ptr<Inputs>>& Cache() {
  static std::map<Layout, std::unique_ptr<Inputs>> gCache;
  return gCache;
}

const Inputs& InputsOf(Layout layout) {
  auto& cached = Cache()[layout];
  if (cached) {
    return *cached;
  }
  cached = std::make_unique<Inputs>();
  auto* db = &irs::DuckDBEngine::Instance().instance();
  irs::IndexWriterOptions options;
  options.db = db;
  options.reader_options.db = db;
  options.segment_memory_max = std::numeric_limits<size_t>::max();
  options.column_options = [](irs::field_id) -> irs::ColumnOptions {
    return {};
  };
  auto writer =
    irs::IndexWriter::Make(cached->dir, irs::kOmCreate, std::move(options));
  static constexpr std::array<std::string_view, 10> kServices{
    "cart",    "checkout", "frontend", "shipping", "currency",
    "payment", "email",    "ads",      "quote",    "recommendation"};
  size_t next = 0;
  for (size_t s = 0; s != kSegments; ++s) {
    const auto keys = KeysOf(layout, s);
    auto trx = writer->GetBatch();
    for (const auto key : keys) {
      auto doc = trx.Insert();
      const auto id = absl::StrCat("doc-", next++);
      KeywordField id_field{.id = kIdField, .value = id};
      tests::InsertField(doc, id_field);
      const auto service = kServices[static_cast<size_t>(key) % 10];
      KeywordField service_field{.id = kServiceField, .value = service};
      tests::InsertField(doc, service_field);
      Append(doc, kKey, duckdb::LogicalType::BIGINT,
             duckdb::Value::BIGINT(key));
      Append(doc, kValue, duckdb::LogicalType::DOUBLE,
             duckdb::Value::DOUBLE(static_cast<double>(key % 100003) / 7.0));
      Append(doc, kMessage, duckdb::LogicalType::VARCHAR,
             duckdb::Value{absl::StrCat(service, " handled request ",
                                        key % 9973, " in ", key % 997, "ms")});
    }
    trx.Commit();
    writer->RefreshCommit();
  }
  cached->reader = writer->GetSnapshot();
  return *cached;
}

constexpr std::array<std::string_view, 9> kUnitNames{
  "norms", "column",   "train", "copy", "footer",
  "ann",   "postings", "index", "order"};
constexpr size_t kOrderNanos = kUnitNames.size() - 1;

class ThreadedExecutor final : public irs::MergeExecutor {
 public:
  explicit ThreadedExecutor(size_t threads) noexcept : _threads{threads} {}

  size_t Threads() const noexcept final { return _threads; }

  uint64_t Nanos(size_t kind) const noexcept {
    return _nanos[kind].load(std::memory_order_relaxed);
  }

  bool Run(irs::MergePlan& plan) final {
    if (plan.OrderPending()) {
      const auto begin = std::chrono::steady_clock::now();
      const bool ordered = plan.RunOrder();
      _nanos[kOrderNanos].fetch_add(
        static_cast<uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(
            std::chrono::steady_clock::now() - begin)
            .count()),
        std::memory_order_relaxed);
      if (!ordered) {
        return false;
      }
    }
    irs::MergeSchedule schedule{plan};
    std::mutex mutex;
    std::condition_variable wake;
    auto work = [&] {
      std::unique_lock lock{mutex};
      while (true) {
        std::optional<size_t> unit;
        wake.wait(lock, [&] {
          return schedule.Done() || (unit = schedule.Claim()).has_value();
        });
        if (!unit) {
          return;
        }
        lock.unlock();
        const auto begin = std::chrono::steady_clock::now();
        const bool ok = plan.RunUnit(*unit);
        const auto nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - begin);
        _nanos[static_cast<size_t>(plan.Units()[*unit].kind)].fetch_add(
          static_cast<uint64_t>(nanos.count()), std::memory_order_relaxed);
        lock.lock();
        schedule.Finish(*unit, ok);
        wake.notify_all();
      }
    };
    std::vector<std::thread> helpers;
    for (size_t i = 1; i < _threads; ++i) {
      helpers.emplace_back(work);
    }
    work();
    for (auto& helper : helpers) {
      helper.join();
    }
    return schedule.Ok();
  }

 private:
  size_t _threads;
  std::array<std::atomic<uint64_t>, kUnitNames.size()> _nanos{};
};

uint64_t StatusKb(std::string_view key) {
  std::ifstream status{"/proc/self/status"};
  std::string line;
  while (std::getline(status, line)) {
    if (line.starts_with(key)) {
      return std::stoull(line.substr(key.size()));
    }
  }
  return 0;
}

uint64_t ResetPeakKb() {
  if (JemallocControl != nullptr) {
    JemallocControl("thread.tcache.flush", nullptr, nullptr, nullptr, 0);
    JemallocControl("arena.4096.purge", nullptr, nullptr, nullptr, 0);
  }
  std::ofstream{"/proc/self/clear_refs"} << "5";
  return StatusKb("VmRSS:");
}

void BM_Merge(benchmark::State& state) {
  const auto layout = static_cast<Layout>(state.range(0));
  const bool sorted = state.range(1) != 0;
  const auto threads = static_cast<size_t>(state.range(2));
  const bool pipeline = state.range(3) != 0;
  const auto& inputs = InputsOf(layout);
  uint64_t peak_kb = 0;
  uint64_t prepare_nanos = 0;
  irs::FunctionFieldOptions field_options{
    [](irs::field_id) { return irs::ColumnOptions{}; },
    [next = std::make_shared<std::atomic<irs::field_id>>(1000)](irs::field_id) {
      return next->fetch_add(1);
    },
    DEFAULT_ROW_GROUP_SIZE};
  const irs::SortSpec spec{{{.column = kKey}}};
  ThreadedExecutor threaded{threads};
  sdb::search::PipelineMergeExecutor piped{
    irs::DuckDBEngine::Instance().instance(), nullptr, threads};
  irs::MergeExecutor& executor =
    pipeline ? static_cast<irs::MergeExecutor&>(piped) : threaded;
  for (auto _ : state) {
    state.PauseTiming();
    const auto base_kb = ResetPeakKb();
    state.ResumeTiming();
    irs::MemoryDirectory out;
    irs::SegmentMeta meta;
    const irs::SegmentWriterOptions options{
      .db = &irs::DuckDBEngine::Instance().instance(),
      .field_options = &field_options,
      .sort = sorted ? &spec : nullptr,
    };
    irs::MergeWriter merger{out, options};
    merger.Reset(inputs.reader.begin(), inputs.reader.end());
    if (!irs::GetReady(merger.Flush(meta, {}, nullptr, &executor))) {
      state.SkipWithError("merge failed");
      return;
    }
    benchmark::DoNotOptimize(meta.byte_size);
    state.PauseTiming();
    peak_kb = std::max(peak_kb, StatusKb("VmHWM:") - base_kb);
    prepare_nanos += merger.Profile().Nanos(irs::MergeStep::Prepare);
    state.ResumeTiming();
  }
  const auto per_merge = [&](uint64_t nanos) {
    return static_cast<double>(nanos) / 1e6 /
           static_cast<double>(state.iterations());
  };
  state.SetItemsProcessed(state.iterations() * kSegments * kRowsPerSegment);
  state.counters["peak_mb"] = static_cast<double>(peak_kb) / 1024;
  state.counters["prepare"] = per_merge(prepare_nanos);
  for (size_t kind = 0; kind != kUnitNames.size(); ++kind) {
    if (const auto nanos = threaded.Nanos(kind); nanos != 0) {
      state.counters[std::string{kUnitNames[kind]}] = per_merge(nanos);
    }
  }
}

BENCHMARK(BM_Merge)
  ->ArgsProduct({{0, 1, 2}, {0, 1}, {1, 4}, {0, 1}})
  ->ArgNames({"layout", "sorted", "threads", "pipeline"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime()
  ->Iterations(3);

std::string_view KeyExpression(Layout layout) {
  switch (layout) {
    case Layout::Disjoint:
      return "s * 250000 + i";
    case Layout::Interleaved:
      return "i * 8 + s";
    case Layout::Random:
      return "(hash(s * 1000003 + i) >> 16)::BIGINT";
  }
  return {};
}

void BM_DuckDBOrder(benchmark::State& state) {
  const auto layout = static_cast<Layout>(state.range(0));
  const bool sorted = state.range(1) != 0;
  duckdb::Connection connection{irs::DuckDBEngine::Instance().instance()};
  const auto table = absl::StrCat("rows_", state.range(0));
  for (const auto& statement :
       {absl::StrCat("SET threads = ", state.range(2)),
        absl::StrCat(
          "CREATE TABLE IF NOT EXISTS ", table,
          " AS SELECT key, (key % 100003) / 7.0 AS value, "
          "['cart', 'checkout', 'frontend', 'shipping', 'currency', "
          "'payment', 'email', 'ads', 'quote', 'recommendation'][key % 10 + 1]"
          " || ' handled request ' || (key % 9973) || ' in ' || (key % 997)"
          " || 'ms' AS message, (s << 32) | i AS rid FROM (SELECT s, i, ",
          KeyExpression(layout),
          " AS key FROM range(8) r1(s), range(250000) r2(i)) ORDER BY s, "
          "key")}) {
    if (const auto result = connection.Query(statement); result->HasError()) {
      state.SkipWithError(result->GetError());
      return;
    }
  }
  const auto query = absl::StrCat("SELECT key, value, message, rid FROM ",
                                  table, sorted ? " ORDER BY key" : "");
  uint64_t peak_kb = 0;
  for (auto _ : state) {
    state.PauseTiming();
    const auto base_kb = ResetPeakKb();
    state.ResumeTiming();
    auto handle = connection.Submit(query);
    if (handle->HasError()) {
      state.SkipWithError(handle->GetError());
      return;
    }
    duckdb::QueryResultStream<> stream{std::move(handle)};
    uint64_t rows = 0;
    while (auto chunk = stream.Fetch()) {
      rows += chunk->size();
    }
    if (rows != kSegments * kRowsPerSegment) {
      state.SkipWithError("the query lost rows");
      return;
    }
    state.PauseTiming();
    peak_kb = std::max(peak_kb, StatusKb("VmHWM:") - base_kb);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * kSegments * kRowsPerSegment);
  state.counters["peak_mb"] = static_cast<double>(peak_kb) / 1024;
}

BENCHMARK(BM_DuckDBOrder)
  ->ArgsProduct({{0, 1, 2}, {0, 1}, {1, 4}})
  ->ArgNames({"layout", "sorted", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime()
  ->Iterations(3);

void BM_OtelOrder(benchmark::State& state) {
  const char* parquet = std::getenv("SDB_BENCH_OTEL_PARQUET");
  if (parquet == nullptr) {
    state.SkipWithError("SDB_BENCH_OTEL_PARQUET is not set");
    return;
  }
  duckdb::Connection connection{irs::DuckDBEngine::Instance().instance()};
  for (const auto& statement :
       {absl::StrCat("SET threads = ", state.range(1)),
        absl::StrCat("CREATE TABLE IF NOT EXISTS otel_keys AS SELECT "
                     "file_row_number // 625000 AS seg, file_row_number AS rn, "
                     "ServiceName AS svc, Timestamp AS ts FROM read_parquet('",
                     parquet, "', file_row_number = true)")}) {
    if (const auto result = connection.Query(statement); result->HasError()) {
      state.SkipWithError(result->GetError());
      return;
    }
  }
  const auto* query =
    state.range(0) == 0
      ? "SELECT seg, rn FROM otel_keys ORDER BY svc, ts, seg, rn"
      : "SELECT seg, rn FROM otel_keys ORDER BY ts, seg, rn";
  uint64_t peak_kb = 0;
  uint64_t rows = 0;
  for (auto _ : state) {
    state.PauseTiming();
    const auto base_kb = ResetPeakKb();
    state.ResumeTiming();
    auto handle = connection.Submit(query);
    if (handle->HasError()) {
      state.SkipWithError(handle->GetError());
      return;
    }
    duckdb::QueryResultStream<> stream{std::move(handle)};
    rows = 0;
    while (auto chunk = stream.Fetch()) {
      rows += chunk->size();
    }
    state.PauseTiming();
    peak_kb = std::max(peak_kb, StatusKb("VmHWM:") - base_kb);
    state.ResumeTiming();
  }
  state.SetItemsProcessed(state.iterations() * rows);
  state.counters["peak_mb"] = static_cast<double>(peak_kb) / 1024;
}

BENCHMARK(BM_OtelOrder)
  ->ArgsProduct({{0, 1}, {1, 4}})
  ->ArgNames({"key", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime()
  ->Iterations(3);

class OtelInput final : public duckdb::PresortedMergeInput {
 public:
  explicit OtelInput(bool composite) : _composite{composite} {}

  duckdb::idx_t Count() const override { return ts.size(); }

  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader()
    const override {
    return duckdb::make_uniq<Reader>(*this);
  }

  std::vector<duckdb::string_t> svc;
  std::vector<int64_t> ts;

 private:
  class Reader final : public duckdb::PresortedMergeReader {
   public:
    explicit Reader(const OtelInput& input) : _input{input} {}

    void Read(duckdb::idx_t begin, duckdb::idx_t count,
              duckdb::DataChunk& keys) override {
      duckdb::idx_t column = 0;
      if (_input._composite) {
        std::copy_n(
          _input.svc.data() + begin, count,
          duckdb::FlatVector::GetDataMutable<duckdb::string_t>(keys.data[0]));
        duckdb::FlatVector::SetSize(keys.data[0], count);
        column = 1;
      }
      std::copy_n(
        _input.ts.data() + begin, count,
        duckdb::FlatVector::GetDataMutable<int64_t>(keys.data[column]));
      duckdb::FlatVector::SetSize(keys.data[column], count);
      keys.SetCardinality(count);
    }

   private:
    const OtelInput& _input;
  };

  bool _composite;
};

struct OtelInputs {
  std::vector<duckdb::shared_ptr<OtelInput>> inputs;
  std::map<std::string, std::unique_ptr<std::string>> names;
};

const OtelInputs& OtelInputsOf(bool composite, std::string_view parquet) {
  static std::map<bool, std::unique_ptr<OtelInputs>> gCache;
  auto& cached = gCache[composite];
  if (cached) {
    return *cached;
  }
  cached = std::make_unique<OtelInputs>();
  duckdb::Connection connection{irs::DuckDBEngine::Instance().instance()};
  connection.Query(
    absl::StrCat("CREATE TABLE IF NOT EXISTS otel_keys AS SELECT "
                 "file_row_number // 625000 AS seg, file_row_number AS rn, "
                 "ServiceName AS svc, Timestamp AS ts FROM read_parquet('",
                 parquet, "', file_row_number = true)"));
  duckdb::QueryResultStream<> stream{connection.Submit(
    composite ? "SELECT seg, svc, ts FROM otel_keys ORDER BY seg, svc, ts"
              : "SELECT seg, svc, ts FROM otel_keys ORDER BY seg, ts")};
  while (auto chunk = stream.Fetch()) {
    duckdb::UnifiedVectorFormat segs;
    duckdb::UnifiedVectorFormat names;
    duckdb::UnifiedVectorFormat stamps;
    chunk->data[0].ToUnifiedFormat(chunk->size(), segs);
    chunk->data[1].ToUnifiedFormat(chunk->size(), names);
    chunk->data[2].ToUnifiedFormat(chunk->size(), stamps);
    for (duckdb::idx_t i = 0; i != chunk->size(); ++i) {
      const auto seg =
        static_cast<size_t>(duckdb::UnifiedVectorFormat::GetData<int64_t>(
          segs)[segs.sel->get_index(i)]);
      while (cached->inputs.size() <= seg) {
        cached->inputs.push_back(duckdb::make_shared_ptr<OtelInput>(composite));
      }
      auto& input = *cached->inputs[seg];
      const auto& name = duckdb::UnifiedVectorFormat::GetData<duckdb::string_t>(
        names)[names.sel->get_index(i)];
      auto& stored = cached->names[name.GetString()];
      if (!stored) {
        stored = std::make_unique<std::string>(name.GetString());
      }
      input.svc.emplace_back(stored->data(),
                             static_cast<uint32_t>(stored->size()));
      input.ts.push_back(duckdb::UnifiedVectorFormat::GetData<int64_t>(
        stamps)[stamps.sel->get_index(i)]);
    }
  }
  return *cached;
}

void BM_PresortedMerge(benchmark::State& state) {
  const char* parquet = std::getenv("SDB_BENCH_OTEL_PARQUET");
  if (parquet == nullptr) {
    state.SkipWithError("SDB_BENCH_OTEL_PARQUET is not set");
    return;
  }
  const bool composite = state.range(0) == 0;
  const auto threads = static_cast<size_t>(state.range(1));
  const auto& data = OtelInputsOf(composite, parquet);
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
  if (composite) {
    types.push_back(duckdb::LogicalType::VARCHAR);
    modifiers.emplace_back(duckdb::OrderType::ASCENDING,
                           duckdb::OrderByNullType::NULLS_LAST);
  }
  types.push_back(duckdb::LogicalType::TIMESTAMP_TZ);
  modifiers.emplace_back(duckdb::OrderType::ASCENDING,
                         duckdb::OrderByNullType::NULLS_LAST);
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> inputs{
    data.inputs.begin(), data.inputs.end()};
  uint64_t peak_kb = 0;
  size_t runs = 0;
  size_t partitions = 0;
  for (auto _ : state) {
    state.PauseTiming();
    const auto base_kb = ResetPeakKb();
    state.ResumeTiming();
    duckdb::PresortedMerge merge{types, modifiers, inputs};
    merge.Partition(threads > 1 ? threads * 4 : 1);
    partitions = merge.PartitionCount();
    std::atomic<size_t> next{0};
    std::atomic<size_t> total{0};
    std::atomic<bool> failed{false};
    auto work = [&] {
      duckdb::vector<duckdb::PresortedRun> part;
      for (auto p = next++; p < partitions; p = next++) {
        part.clear();
        duckdb::optional_idx bad;
        if (!merge.Merge(p, part, [] { return true; }, bad)) {
          failed = true;
          return;
        }
        total += part.size();
      }
    };
    std::vector<std::thread> helpers;
    for (size_t i = 1; i < threads; ++i) {
      helpers.emplace_back(work);
    }
    work();
    for (auto& helper : helpers) {
      helper.join();
    }
    if (failed) {
      state.SkipWithError("the merge failed");
      return;
    }
    runs = total;
    state.PauseTiming();
    peak_kb = std::max(peak_kb, StatusKb("VmHWM:") - base_kb);
    state.ResumeTiming();
  }
  state.counters["runs"] = static_cast<double>(runs);
  state.counters["partitions"] = static_cast<double>(partitions);
  state.counters["peak_mb"] = static_cast<double>(peak_kb) / 1024;
}

BENCHMARK(BM_PresortedMerge)
  ->ArgsProduct({{0, 1}, {1, 4}})
  ->ArgNames({"key", "threads"})
  ->Unit(benchmark::kMillisecond)
  ->UseRealTime()
  ->Iterations(3);

using Clock = std::chrono::steady_clock;

double Millis(Clock::duration elapsed, int64_t iterations) {
  return std::chrono::duration<double, std::milli>(elapsed).count() /
         static_cast<double>(iterations);
}

void BM_ReadKeys(benchmark::State& state) {
  const auto& inputs = InputsOf(static_cast<Layout>(state.range(0)));
  for (auto _ : state) {
    for (const auto& segment : inputs.reader) {
      const auto* columns = segment.GetColReader();
      const auto* column = columns->Column(kKey);
      auto scan =
        column->InitScan(std::make_shared<irs::ReadContext>(*columns));
      irs::ColumnReader::VectorScratch scratch{column->Type()};
      for (uint64_t row = 0; row < column->RowCount();
           row += STANDARD_VECTOR_SIZE) {
        auto& values = scratch.Reset();
        column->Scan(
          scan, values,
          std::min<uint64_t>(column->RowCount() - row, STANDARD_VECTOR_SIZE));
        benchmark::DoNotOptimize(&values);
      }
    }
  }
}

BENCHMARK(BM_ReadKeys)
  ->DenseRange(0, 2)
  ->ArgName("layout")
  ->Unit(benchmark::kMillisecond)
  ->Iterations(5);

void BM_KeyOrder(benchmark::State& state) {
  const auto& inputs = InputsOf(static_cast<Layout>(state.range(0)));
  const irs::SortSpec spec{{{.column = kKey}}};
  uint64_t prepare_nanos = 0;
  uint64_t merge_nanos = 0;
  uint64_t finish_nanos = 0;
  for (auto _ : state) {
    irs::MergeDocMap doc_map{{irs::IResourceManager::gNoop}};
    for (const auto& segment : inputs.reader) {
      doc_map.emplace_back(&segment, irs::IResourceManager::gNoop);
    }
    irs::KeyOrder order{doc_map, spec};
    const auto begin = std::chrono::steady_clock::now();
    if (!order.Prepare(1)) {
      state.SkipWithError("prepare failed");
      return;
    }
    const auto prepared = std::chrono::steady_clock::now();
    if (!order.Merge([] { return true; })) {
      state.SkipWithError("merge failed");
      return;
    }
    const auto merged = std::chrono::steady_clock::now();
    if (!order.Finish()) {
      state.SkipWithError("finish failed");
      return;
    }
    const auto finished = std::chrono::steady_clock::now();
    const auto nanos = [](auto from, auto to) {
      return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(to - from)
          .count());
    };
    prepare_nanos += nanos(begin, prepared);
    merge_nanos += nanos(prepared, merged);
    finish_nanos += nanos(merged, finished);
  }
  const auto per_iteration = [&](uint64_t nanos) {
    return static_cast<double>(nanos) / 1e6 /
           static_cast<double>(state.iterations());
  };
  state.counters["prepare"] = per_iteration(prepare_nanos);
  state.counters["merge"] = per_iteration(merge_nanos);
  state.counters["finish"] = per_iteration(finish_nanos);
}

BENCHMARK(BM_KeyOrder)
  ->DenseRange(0, 2)
  ->ArgName("layout")
  ->Unit(benchmark::kMillisecond)
  ->Iterations(10);

std::vector<std::vector<int64_t>> AllKeys(Layout layout) {
  std::vector<std::vector<int64_t>> keys;
  for (size_t s = 0; s != kSegments; ++s) {
    keys.emplace_back(KeysOf(layout, s));
  }
  return keys;
}

void BM_DuckDBSortPhases(benchmark::State& state) {
  const auto keys = AllKeys(static_cast<Layout>(state.range(0)));
  duckdb::Connection connection{irs::DuckDBEngine::Instance().instance()};
  auto& context = *connection.context;
  const duckdb::vector<duckdb::LogicalType> types{duckdb::LogicalType::BIGINT,
                                                  duckdb::LogicalType::UBIGINT};
  Clock::duration sink{};
  Clock::duration combine{};
  Clock::duration finalize{};
  Clock::duration merge{};
  for (auto _ : state) {
    duckdb::vector<duckdb::BoundOrderByNode> orders;
    for (duckdb::storage_t k = 0; k != 2; ++k) {
      orders.emplace_back(
        duckdb::OrderType::ASCENDING, duckdb::OrderByNullType::NULLS_LAST,
        duckdb::make_uniq<duckdb::BoundReferenceExpression>(types[k], k));
    }
    std::unique_ptr<duckdb::Sort> sort;
    context.RunFunctionInTransaction([&] {
      sort = std::make_unique<duckdb::Sort>(context, orders, types,
                                            duckdb::vector<duckdb::idx_t>{1});
    });
    auto global = sort->GetGlobalSinkState(context);
    duckdb::InterruptState interrupt;
    duckdb::DataChunk chunk;
    chunk.Initialize(context, types);
    for (size_t s = 0; s != kSegments; ++s) {
      duckdb::ThreadContext thread{context};
      duckdb::ExecutionContext execution{context, thread, nullptr};
      auto local = sort->GetLocalSinkState(execution);
      for (size_t first = 0; first < kRowsPerSegment;
           first += STANDARD_VECTOR_SIZE) {
        const auto n =
          std::min<size_t>(STANDARD_VECTOR_SIZE, kRowsPerSegment - first);
        chunk.Reset();
        auto* key = duckdb::FlatVector::GetDataMutable<int64_t>(chunk.data[0]);
        auto* payload =
          duckdb::FlatVector::GetDataMutable<uint64_t>(chunk.data[1]);
        for (size_t i = 0; i != n; ++i) {
          key[i] = keys[s][first + i];
          payload[i] = (uint64_t{s} << 32) | (first + i);
        }
        duckdb::FlatVector::SetSize(chunk.data[0], n);
        duckdb::FlatVector::SetSize(chunk.data[1], n);
        chunk.SetChildCardinality(n);
        const auto begin = Clock::now();
        duckdb::OperatorSinkInput input{*global, *local, interrupt};
        sort->Sink(execution, chunk, input);
        sink += Clock::now() - begin;
      }
      const auto begin = Clock::now();
      duckdb::OperatorSinkCombineInput input{*global, *local, interrupt};
      sort->Combine(execution, input);
      combine += Clock::now() - begin;
    }
    auto begin = Clock::now();
    duckdb::OperatorSinkFinalizeInput finalize_input{*global, interrupt};
    sort->Finalize(context, finalize_input);
    auto source = sort->GetGlobalSourceState(context, *global);
    finalize += Clock::now() - begin;
    begin = Clock::now();
    duckdb::ThreadContext thread{context};
    duckdb::ExecutionContext execution{context, thread, nullptr};
    auto local = sort->GetLocalSourceState(execution, *source);
    duckdb::DataChunk out;
    out.Initialize(context, {duckdb::LogicalType::UBIGINT});
    uint64_t rows = 0;
    while (true) {
      out.Reset();
      duckdb::OperatorSourceInput input{*source, *local, interrupt};
      if (sort->GetData(execution, out, input) ==
            duckdb::SourceResultType::FINISHED &&
          out.size() == 0) {
        break;
      }
      rows += out.size();
    }
    merge += Clock::now() - begin;
    if (rows != kSegments * kRowsPerSegment) {
      state.SkipWithError("the sort lost rows");
      return;
    }
  }
  state.counters["sink"] = Millis(sink, state.iterations());
  state.counters["combine"] = Millis(combine, state.iterations());
  state.counters["finalize"] = Millis(finalize, state.iterations());
  state.counters["merge"] = Millis(merge, state.iterations());
}

BENCHMARK(BM_DuckDBSortPhases)
  ->DenseRange(0, 2)
  ->ArgName("layout")
  ->Unit(benchmark::kMillisecond)
  ->Iterations(5);

void BM_KWayMerge(benchmark::State& state) {
  const auto keys = AllKeys(static_cast<Layout>(state.range(0)));
  std::vector<uint8_t> order(kSegments * kRowsPerSegment);
  std::vector<std::vector<uint32_t>> id_maps(
    kSegments, std::vector<uint32_t>(kRowsPerSegment));
  for (auto _ : state) {
    std::array<size_t, kSegments> at{};
    uint32_t row = 0;
    while (true) {
      size_t best = kSegments;
      size_t rival = kSegments;
      for (size_t i = 0; i != kSegments; ++i) {
        if (at[i] == kRowsPerSegment) {
          continue;
        }
        if (best == kSegments || keys[i][at[i]] < keys[best][at[best]]) {
          rival = best;
          best = i;
        } else if (rival == kSegments ||
                   keys[i][at[i]] < keys[rival][at[rival]]) {
          rival = i;
        }
      }
      if (best == kSegments) {
        break;
      }
      const auto& run = keys[best];
      auto& pos = at[best];
      const auto limit = rival == kSegments
                           ? std::numeric_limits<int64_t>::max()
                           : keys[rival][at[rival]];
      while (pos != kRowsPerSegment &&
             (run[pos] < limit || (run[pos] == limit && best < rival))) {
        order[row] = static_cast<uint8_t>(best);
        id_maps[best][pos] = row;
        ++row;
        ++pos;
      }
    }
    benchmark::DoNotOptimize(order.data());
    benchmark::DoNotOptimize(id_maps.data());
  }
}

BENCHMARK(BM_KWayMerge)
  ->DenseRange(0, 2)
  ->ArgName("layout")
  ->Unit(benchmark::kMillisecond)
  ->Iterations(5);

int Main(int argc, char** argv) {
  irs::DuckDBEngine::Instance().Initialize();
  benchmark::Initialize(&argc, argv);
  benchmark::RunSpecifiedBenchmarks();
  benchmark::Shutdown();
  Cache().clear();
  irs::DuckDBEngine::Instance().Shutdown();
  return 0;
}

[[maybe_unused]] const bool kMain =
  sdb::bench::AddMain(SDB_BENCH_MODULE, &Main);

}  // namespace
