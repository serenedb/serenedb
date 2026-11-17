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

#include "connector/copy_from_batches.h"

#include <absl/container/btree_map.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb/catalog/catalog.hpp>
#include <duckdb/catalog/catalog_entry/table_catalog_entry.hpp>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/async_memory_governor.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/async_result.hpp>
#include <duckdb/parallel/scan_read_ahead.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <duckdb/planner/binder.hpp>
#include <duckdb/planner/expression_binder/constant_binder.hpp>
#include <exception>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "formats/copy_decoder.h"
#include "pg/connection_context.h"
#include "pg/copy_batch_builder.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kCopyStdinPath = "/dev/stdin";
constexpr uint64_t kCopyFileRangeBytes = 8 << 20;

pg::CopyBatchFormat BatchFormatOf(const pg::CopyInTarget& target) {
  if (target.binary) {
    return pg::CopyBatchFormat::Binary;
  }
  return target.csv ? pg::CopyBatchFormat::Csv : pg::CopyBatchFormat::Text;
}

formats::CsvDialect CsvDialectOf(const pg::CopyInTarget& target) {
  return {target.delim, target.quote, target.escape};
}

class CopyFileReader {
 public:
  CopyFileReader(duckdb::ClientContext& context,
                 std::unique_ptr<duckdb::FileHandle> file,
                 const pg::CopyInTarget& target)
    : _file{std::move(file)},
      _builder{BatchFormatOf(target), pg::kCopyBatchBytes,
               !target.binary && !target.csv, CsvDialectOf(target)},
      _batches{duckdb::TaskScheduler::QueryThreads(context) *
               pg::kCopyBatchesPerThread},
      _memory{context},
      _fixed_budget{duckdb::TaskScheduler::QueryThreads(context) *
                    pg::kCopyBatchesPerThread * pg::kCopyBatchBytes},
      _executor{context, duckdb::TaskSchedulerType::ASYNC},
      _chunk(pg::kCopyBatchBytes, '\0') {
    if (target.encoding != formats::TextEncoding::Utf8) {
      _decoder.emplace(target.encoding);
    }
    UpdateBudget();
  }

  ~CopyFileReader() {
    _ended.store(true, std::memory_order_release);
    _executor.CancelAndDrain();
  }

  pg::CopyInBatches& Batches() noexcept { return _batches; }

  void Resume() {
    if (_ended.load(std::memory_order_acquire) || !HasRoom() ||
        _reading.exchange(true, std::memory_order_acq_rel)) {
      return;
    }
    _executor.ScheduleTask(duckdb::make_uniq<ReadTask>(_executor, *this));
  }

 private:
  class ReadTask final : public duckdb::BaseExecutorTask {
   public:
    ReadTask(duckdb::TaskExecutor& executor, CopyFileReader& reader)
      : BaseExecutorTask{executor}, _reader{reader} {}

    void ExecuteTask() final { _reader.Read(); }

   private:
    CopyFileReader& _reader;
  };

  bool HasRoom() const {
    const size_t budget =
      std::max(_budget.load(std::memory_order_relaxed), pg::kCopyBatchBytes);
    return _batches.RetainedBytes() < budget;
  }

  void UpdateBudget() {
    const size_t budget =
      _memory.IsActive() ? _memory.BackpressureBudget() : _fixed_budget;
    _budget.store(budget, std::memory_order_relaxed);
  }

  bool KeepReading() {
    return !_ended.load(std::memory_order_acquire) && HasRoom() &&
           !_reading.exchange(true, std::memory_order_acq_rel);
  }

  void Read() {
    do {
      ReadWhileRoom();
      _reading.store(false, std::memory_order_release);
    } while (KeepReading());
  }

  void ReadWhileRoom() {
    try {
      while (!_ended.load(std::memory_order_acquire) && HasRoom()) {
        ReadBatch();
      }
      _batches.Flush();
    } catch (...) {
      _ended.store(true, std::memory_order_release);
      _batches.Fail(std::current_exception());
    }
  }

  void ReadBatch() {
    while (!_builder.Ready()) {
      const auto got =
        static_cast<size_t>(_file->Read(_chunk.data(), _chunk.size()));
      if (got == 0) {
        FinishFile();
        return;
      }
      const std::string_view bytes{_chunk.data(), got};
      if (_decoder) {
        _decoded.clear();
        _decoder->Decode(bytes, _decoded);
        _builder.Feed(_decoded, false);
      } else {
        _builder.Feed(bytes, false);
      }
    }
    Push(_builder.Take());
  }

  void FinishFile() {
    if (_decoder) {
      _decoder->Finish();
    }
    _builder.Finish();
    while (_builder.HasRows()) {
      Push(_builder.Take());
    }
    _ended.store(true, std::memory_order_release);
    _batches.Finish();
  }

  void Push(std::unique_ptr<pg::CopyBatch> batch) {
    _batches.Push(std::move(batch), {});
    _memory.UpdateReservation(_batches.RetainedBytes());
    UpdateBudget();
  }

  std::unique_ptr<duckdb::FileHandle> _file;
  pg::CopyBatchBuilder _builder;
  pg::CopyInBatches _batches;
  duckdb::ManagedAsyncMemoryGovernor _memory;
  const size_t _fixed_budget;
  std::atomic<size_t> _budget{0};
  std::atomic_bool _reading{false};
  std::atomic_bool _ended{false};
  duckdb::TaskExecutor _executor;
  std::string _chunk;
  std::optional<formats::Utf8Decoder> _decoder;
  std::string _decoded;
};

enum class CopyFromSource : uint8_t {
  Batches,
  Ranges,
};

using Guard = duckdb::annotated_lock_guard<duckdb::annotated_mutex>;
using Lock = duckdb::annotated_unique_lock<duckdb::annotated_mutex>;

void Wake(const duckdb::vector<duckdb::InterruptState>& waiters) {
  for (const auto& waiter : waiters) {
    waiter.Callback();
  }
}

class CopyLines {
 public:
  CopyLines(ConnectionContext& connection, const pg::CopyInTarget& target)
    : _connection{connection}, _target{target} {}

  void Finish(uint64_t seq, uint64_t lines,
              std::vector<formats::CopySkippedRow> skipped) {
    duckdb::vector<duckdb::InterruptState> waiters;
    {
      Guard guard{_waiters.lock};
      _done.try_emplace(seq, Unit{lines, std::move(skipped)});
      if (!Advance()) {
        return;
      }
      waiters = _waiters.TakeBlockedTasks();
    }
    Wake(waiters);
  }

  std::optional<uint64_t> Locate(duckdb::TableFunctionInput& input,
                                 uint64_t seq, uint64_t line) {
    Lock lock{_waiters.lock};
    if (seq < _failed) {
      _failed = seq;
      auto waiters = _waiters.TakeBlockedTasks();
      lock.unlock();
      Wake(waiters);
      lock.lock();
    }
    for (;;) {
      if (_failed < seq) {
        return 0;
      }
      if (_next == seq) {
        return _lines + line;
      }
      if (pg::CopyInBatches::CanPark(input) &&
          _waiters.BlockTask(*input.interrupt_state)) {
        input.async_result = duckdb::AsyncResultType::BLOCKED;
        return std::nullopt;
      }
      auto signal = duckdb::make_shared_ptr<duckdb::InterruptDoneSignalState>();
      _waiters.BlockTask(duckdb::InterruptState{
        duckdb::weak_ptr<duckdb::InterruptDoneSignalState>{signal}});
      lock.unlock();
      signal->Await();
      lock.lock();
    }
  }

 private:
  struct Unit {
    uint64_t lines = 0;
    std::vector<formats::CopySkippedRow> skipped;
  };

  bool Advance() DUCKDB_REQUIRES(_waiters.lock) {
    bool advanced = false;
    for (auto it = _done.begin(); it != _done.end() && it->first == _next;
         it = _done.erase(it)) {
      for (const auto& row : it->second.skipped) {
        const auto line = _lines + row.line;
        _connection.AddNotice(SQL_ERROR_DATA(
          ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
          ERR_MSG("skipping row due to data type incompatibility at line ",
                  line, " for column \"", _target.names[row.column], "\": \"",
                  row.value, "\"")));
      }
      _lines += it->second.lines;
      ++_next;
      advanced = true;
    }
    return advanced;
  }

  ConnectionContext& _connection;
  const pg::CopyInTarget& _target;
  duckdb::StateWithBlockableTasks _waiters;
  absl::btree_map<uint64_t, Unit> _done DUCKDB_GUARDED_BY(_waiters.lock);
  uint64_t _next DUCKDB_GUARDED_BY(_waiters.lock) = 0;
  uint64_t _lines DUCKDB_GUARDED_BY(_waiters.lock) = 0;
  uint64_t _failed DUCKDB_GUARDED_BY(_waiters.lock) =
    std::numeric_limits<uint64_t>::max();
};

struct CopyFromGlobalState : public duckdb::GlobalTableFunctionState {
  CopyFromGlobalState(duckdb::ClientContext& context, CopyFromSource source,
                      pg::CopyInTarget target, bool ordered)
    : source{source},
      target{std::move(target)},
      ordered{ordered},
      lines{ConnectionContext::Get(context), this->target} {}

  CopyFromSource source;
  pg::CopyInTarget target;
  bool ordered;
  formats::CopyTextEnd end;
  std::atomic<uint64_t> rejected{0};
  CopyLines lines;
  std::atomic<uint32_t> workers{0};
  std::atomic_bool summarized{false};
};

struct CopyFromLocalState : public duckdb::LocalTableFunctionState {
  std::optional<formats::CopyDecoder> parser;
  duckdb::idx_t batch_index = 0;
  bool finished = false;
};

formats::CopyDecoder& StartWorker(duckdb::ClientContext& context,
                                  CopyFromGlobalState& g,
                                  CopyFromLocalState& l) {
  if (!l.parser) {
    l.parser.emplace(context, g.target, g.end, g.rejected);
    g.workers.fetch_add(1, std::memory_order_relaxed);
  }
  return *l.parser;
}

void FinishWorker(duckdb::ClientContext& context, CopyFromGlobalState& g,
                  CopyFromLocalState& l) {
  if (std::exchange(l.finished, true) ||
      g.workers.fetch_sub(1, std::memory_order_acq_rel) != 1 ||
      g.summarized.exchange(true, std::memory_order_acq_rel)) {
    return;
  }
  const auto rejected = g.rejected.load(std::memory_order_relaxed);
  if (rejected == 0 || g.target.log_verbosity == pg::CopyLogVerbosity::Silent) {
    return;
  }
  ConnectionContext::Get(context).AddNotice(
    SQL_ERROR_DATA(ERR_CODE(ERRCODE_SUCCESSFUL_COMPLETION),
                   ERR_MSG(rejected, rejected == 1 ? " row was" : " rows were",
                           " skipped due to data type incompatibility")));
}

void FinishUnit(CopyFromGlobalState& g, formats::CopyDecoder& parser) {
  g.lines.Finish(parser.Seq(), parser.Lines(), parser.TakeSkipped());
  parser.Stop();
}

void LocateError(CopyFromGlobalState& g, duckdb::TableFunctionInput& input,
                 const formats::CopyDecoder& parser) {
  if (const auto line =
        g.lines.Locate(input, parser.Seq(), parser.FailedLine());
      line && *line != 0) {
    parser.ThrowError(*line);
  }
}

struct BatchesGlobalState final : public CopyFromGlobalState {
  BatchesGlobalState(duckdb::ClientContext& context, pg::CopyInBatches& queue,
                     pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{context, CopyFromSource::Batches, std::move(target),
                          ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      queue{queue} {}

  BatchesGlobalState(duckdb::ClientContext& context,
                     std::unique_ptr<duckdb::FileHandle> file,
                     pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{context, CopyFromSource::Batches, std::move(target),
                          ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{std::in_place, context, std::move(file), this->target},
      queue{reader->Batches()} {
    reader->Resume();
  }

  duckdb::idx_t MaxThreads() const final { return max_threads; }

  duckdb::idx_t max_threads;
  std::optional<CopyFileReader> reader;
  pg::CopyInBatches& queue;
};

struct BatchesLocalState final : public CopyFromLocalState {
  std::vector<pg::CopyBatchLease> run;
  size_t next = 0;
};

struct RangeJobState {
  uint64_t range = 0;
  std::string buffer;
};

using RangeJob = duckdb::ScanReadAheadJobWrapper<RangeJobState>;

struct RangesGlobalState final : public CopyFromGlobalState {
  RangesGlobalState(duckdb::ClientContext& context,
                    std::unique_ptr<duckdb::FileHandle> file,
                    pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{context, CopyFromSource::Ranges, std::move(target),
                          ordered},
      file{std::move(file)},
      size{this->file->GetFileSize()},
      range_count{(size + kCopyFileRangeBytes - 1) / kCopyFileRangeBytes},
      read_ahead{duckdb::ScanReadAhead::Create(context)} {
    if (this->target.csv) {
      parity = std::make_unique<std::atomic<int8_t>[]>(range_count);
      for (uint64_t range = 0; range < range_count; ++range) {
        parity[range].store(-1, std::memory_order_relaxed);
      }
    }
  }

  duckdb::idx_t MaxThreads() const final {
    return std::max<uint64_t>(range_count, 1);
  }

  std::unique_ptr<duckdb::FileHandle> file;
  uint64_t size;
  uint64_t range_count;
  std::atomic<uint64_t> next{0};
  duckdb::unique_ptr<duckdb::ScanReadAhead> read_ahead;
  std::unique_ptr<std::atomic<int8_t>[]> parity;
};

struct RangesLocalState final : public CopyFromLocalState {
  pg::CopyBatch batch;
  duckdb::unique_ptr<RangeJob> job;
};

duckdb::OperatorPartitionData CopyFromPartitionData(
  duckdb::ClientContext&, duckdb::TableFunctionGetPartitionInput& input) {
  if (input.partition_info.RequiresPartitionColumns()) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_INTERNAL_ERROR),
                    ERR_MSG("COPY FROM: partition columns are not supported"));
  }
  return duckdb::OperatorPartitionData{
    input.local_state->Cast<CopyFromLocalState>().batch_index};
}

void ScanBatches(duckdb::ClientContext& context,
                 duckdb::TableFunctionInput& input, duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<BatchesGlobalState>();
  auto& l = input.local_state->Cast<BatchesLocalState>();
  auto& parser = StartWorker(context, g, l);
  if (parser.Failed()) {
    output.SetChildCardinality(0);
    LocateError(g, input, parser);
    return;
  }
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    if (!parser.Active()) {
      if (l.next == l.run.size()) {
        l.run.clear();
        l.next = 0;
        if (row != 0) {
          if (g.ordered || !g.queue.TryTake(1, l.run)) {
            break;
          }
        } else if (const auto take = g.queue.Take(input, 1, l.run);
                   take != pg::CopyTake::Batches) {
          if (take == pg::CopyTake::End) {
            FinishWorker(context, g, l);
          }
          break;
        }
        l.batch_index = l.run.front()->seq;
      }
      parser.Start(*l.run[l.next++]);
    }
    row = parser.Parse(output, row);
    if (parser.Failed()) {
      output.SetChildCardinality(0);
      LocateError(g, input, parser);
      return;
    }
    if (parser.Exhausted()) {
      FinishUnit(g, parser);
      l.run[l.next - 1].Reset();
      if (g.reader) {
        g.reader->Resume();
      }
    }
  }
  output.SetChildCardinality(row);
}

size_t ReadAt(RangesGlobalState& g, std::string& buffer, uint64_t location,
              size_t bytes) {
  bytes = std::min<uint64_t>(bytes, g.size - std::min(location, g.size));
  const size_t old = buffer.size();
  buffer.resize(old + bytes);
  if (bytes != 0) {
    g.file->Read(buffer.data() + old, bytes, location);
  }
  return bytes;
}

size_t FindCut(std::string_view bytes, size_t from) {
  for (auto pos = bytes.find('\n', from); pos != std::string_view::npos;
       pos = bytes.find('\n', pos + 1)) {
    if (pos == 0 || bytes[pos - 1] != '\\') {
      return pos;
    }
  }
  return std::string_view::npos;
}

size_t SkipBlankLines(std::string_view bytes, size_t cut) {
  for (;;) {
    if (cut + 1 < bytes.size() && bytes[cut + 1] == '\n') {
      ++cut;
    } else if (cut + 2 < bytes.size() && bytes[cut + 1] == '\r' &&
               bytes[cut + 2] == '\n') {
      cut += 2;
    } else {
      return cut;
    }
  }
}

bool BlankRunOpen(std::string_view bytes, size_t cut) {
  return cut + 1 == bytes.size() ||
         (cut + 2 == bytes.size() && bytes[cut + 1] == '\r');
}

uint64_t RangeFrom(uint64_t range) {
  const uint64_t begin = range * kCopyFileRangeBytes;
  return begin == 0 ? 0 : begin - 2;
}

uint64_t RangeBegin(uint64_t range) { return range * kCopyFileRangeBytes; }

uint64_t RangeEnd(const RangesGlobalState& g, uint64_t range) {
  return std::min<uint64_t>((range + 1) * kCopyFileRangeBytes, g.size);
}

void StoreParity(RangesGlobalState& g, uint64_t range, std::string_view bytes) {
  g.parity[range].store(formats::CsvQuoteParity(bytes, g.target.quote) ? 1 : 0,
                        std::memory_order_release);
}

void ReadRange(RangesGlobalState& g, RangeJobState& job) {
  const uint64_t end = RangeEnd(g, job.range);
  const uint64_t from = RangeFrom(job.range);
  job.buffer.clear();
  ReadAt(g, job.buffer, from, end - from);
  if (g.parity) {
    StoreParity(
      g, job.range,
      std::string_view{job.buffer}.substr(RangeBegin(job.range) - from));
  }
}

bool CsvStartsQuoted(RangesGlobalState& g, uint64_t range) {
  bool quoted = false;
  std::string scratch;
  for (uint64_t earlier = 0; earlier < range; ++earlier) {
    auto parity = g.parity[earlier].load(std::memory_order_acquire);
    if (parity < 0) {
      scratch.clear();
      ReadAt(g, scratch, RangeBegin(earlier),
             RangeEnd(g, earlier) - RangeBegin(earlier));
      StoreParity(g, earlier, scratch);
      parity = g.parity[earlier].load(std::memory_order_acquire);
    }
    quoted ^= parity != 0;
  }
  return quoted;
}

bool CutCsvRange(RangesGlobalState& g, RangeJobState& job,
                 pg::CopyBatch& batch) {
  auto& buffer = job.buffer;
  if (buffer.empty()) {
    return false;
  }
  const auto dialect = CsvDialectOf(g.target);
  const uint64_t from = RangeFrom(job.range);
  const size_t begin = RangeBegin(job.range) - from;
  const size_t end = RangeEnd(g, job.range) - from;
  const bool quoted = CsvStartsQuoted(g, job.range);
  size_t first = begin;
  if (job.range != 0 && (quoted || buffer[begin - 1] != '\n')) {
    formats::CsvQuoteState state{.in_quote = quoted};
    const auto cut = formats::CsvRecordEnd(
      std::string_view{buffer}.substr(0, end), begin, dialect, state);
    if (cut == std::string_view::npos || cut + 1 >= end) {
      return false;
    }
    first = cut + 1;
  }
  formats::CsvQuoteState state{
    .in_quote =
      quoted != (g.parity[job.range].load(std::memory_order_acquire) != 0),
  };
  size_t stop = end;
  if (state.in_quote || buffer[end - 1] != '\n') {
    for (size_t scanned = end;;) {
      const auto cut = formats::CsvRecordEnd(buffer, scanned, dialect, state);
      if (cut != std::string_view::npos) {
        stop = cut + 1;
        break;
      }
      scanned = buffer.size();
      if (ReadAt(g, buffer, from + scanned, pg::kCopyBatchBytes) == 0) {
        stop = buffer.size();
        break;
      }
    }
  }
  batch.segments.clear();
  batch.bytes = 0;
  batch.seq = job.range;
  batch.AddView(std::string_view{buffer}.substr(first, stop - first));
  return true;
}

class RangeReadTask final : public duckdb::AsyncTask {
 public:
  RangeReadTask(RangesGlobalState& g, RangeJobState& job) : _g{g}, _job{job} {}

  void Execute() final { ReadRange(_g, _job); }

  duckdb::idx_t GetIOSize() const final { return kCopyFileRangeBytes; }

 private:
  RangesGlobalState& _g;
  RangeJobState& _job;
};

bool CutRange(RangesGlobalState& g, RangeJobState& job, pg::CopyBatch& batch) {
  const uint64_t from = RangeFrom(job.range);
  auto& buffer = job.buffer;
  if (buffer.empty()) {
    return false;
  }
  size_t first = 0;
  if (job.range != 0) {
    auto cut = FindCut(buffer, 1);
    if (cut == std::string::npos) {
      return false;
    }
    cut = SkipBlankLines(buffer, cut);
    if (BlankRunOpen(buffer, cut)) {
      return false;
    }
    first = cut + 1;
  }
  auto last = FindCut(buffer, buffer.size() - 1);
  while (last == std::string::npos) {
    const auto scanned = buffer.size();
    if (ReadAt(g, buffer, from + scanned, pg::kCopyBatchBytes) == 0) {
      last = scanned - 1;
      break;
    }
    last = FindCut(buffer, scanned);
  }
  for (last = SkipBlankLines(buffer, last);
       BlankRunOpen(buffer, last) &&
       ReadAt(g, buffer, from + buffer.size(), pg::kCopyBatchBytes) != 0;
       last = SkipBlankLines(buffer, last)) {
  }
  batch.segments.clear();
  batch.bytes = 0;
  batch.seq = job.range;
  batch.AddView(std::string_view{buffer}.substr(first, last + 1 - first));
  return true;
}

duckdb::unique_ptr<RangeJob> ClaimRange(RangesGlobalState& g) {
  const auto range = g.next.fetch_add(1, std::memory_order_relaxed);
  if (range >= g.range_count) {
    return nullptr;
  }
  auto job = duckdb::make_uniq<RangeJob>();
  job->range = range;
  job->batch_index = range;
  return job;
}

bool AcquireRange(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input, RangesGlobalState& g,
                  RangesLocalState& l) {
  if (!g.read_ahead) {
    l.job = ClaimRange(g);
    if (l.job) {
      ReadRange(g, *l.job);
    }
    return static_cast<bool>(l.job);
  }
  duckdb::unique_ptr<duckdb::ScanReadAheadJob> claimed;
  const auto acquired = g.read_ahead->AcquireJob(
    context, input,
    [&](duckdb::vector<duckdb::unique_ptr<duckdb::AsyncTask>>& io_tasks)
      -> duckdb::unique_ptr<duckdb::ScanReadAheadJob> {
      auto job = ClaimRange(g);
      if (job) {
        io_tasks.emplace_back(duckdb::make_uniq<RangeReadTask>(g, *job));
      }
      return job;
    },
    claimed);
  if (acquired == duckdb::ScanReadAheadAcquire::EXHAUSTED) {
    return false;
  }
  l.job = duckdb::unique_ptr_cast<duckdb::ScanReadAheadJob, RangeJob>(
    std::move(claimed));
  return acquired == duckdb::ScanReadAheadAcquire::ACQUIRED;
}

bool NextRange(duckdb::ClientContext& context,
               duckdb::TableFunctionInput& input, RangesGlobalState& g,
               RangesLocalState& l) {
  for (;;) {
    if (!l.job) {
      if (!AcquireRange(context, input, g, l)) {
        return false;
      }
    } else if (l.job->io_completion) {
      g.read_ahead->WaitForJob(*l.job);
    }
    if (g.target.csv ? CutCsvRange(g, *l.job, l.batch)
                     : CutRange(g, *l.job, l.batch)) {
      l.batch_index = l.job->batch_index;
      return true;
    }
    g.lines.Finish(l.job->range, 0, {});
    l.job.reset();
  }
}

void ScanRanges(duckdb::ClientContext& context,
                duckdb::TableFunctionInput& input, duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<RangesGlobalState>();
  auto& l = input.local_state->Cast<RangesLocalState>();
  auto& parser = StartWorker(context, g, l);
  if (parser.Failed()) {
    output.SetChildCardinality(0);
    LocateError(g, input, parser);
    return;
  }
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    if (!parser.Active()) {
      if (row != 0) {
        break;
      }
      if (!NextRange(context, input, g, l)) {
        if (input.async_result.GetResultType() !=
            duckdb::AsyncResultType::BLOCKED) {
          FinishWorker(context, g, l);
        }
        break;
      }
      parser.Start(l.batch);
    }
    row = parser.Parse(output, row);
    if (parser.Failed()) {
      output.SetChildCardinality(0);
      LocateError(g, input, parser);
      return;
    }
    if (parser.Exhausted()) {
      FinishUnit(g, parser);
      l.job.reset();
    }
  }
  output.SetChildCardinality(row);
}

}  // namespace

bool BindCopyFrom(duckdb::ClientContext& context,
                  duckdb::CopyFromFunctionBindInput& input) {
  const auto table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    context, input.info.GetQualifiedName(),
    duckdb::OnEntryNotFound::RETURN_NULL);
  const bool ordered =
    table && table->IsDuckTable() &&
    duckdb::Settings::Get<duckdb::PreserveInsertionOrderSetting>(context);
  if (ordered) {
    input.tf.get_partition_data = CopyFromPartitionData;
  } else {
    input.tf.get_partition_data = nullptr;
    input.tf.order_preservation_type = duckdb::OrderPreservationType::NO_ORDER;
  }
  return ordered;
}

std::shared_ptr<const pg::CopyDefaults> BindCopyDefaults(
  duckdb::ClientContext& context, const duckdb::CopyInfo& info,
  std::span<const duckdb::Identifier> names) {
  auto& table = duckdb::Catalog::GetEntry<duckdb::TableCatalogEntry>(
    context, info.GetQualifiedName());
  auto binder =
    duckdb::Binder::CreateTableBinder(context, table.ParentSchema(context));
  auto defaults = std::make_shared<pg::CopyDefaults>();
  defaults->reserve(names.size());
  for (const auto& name : names) {
    const auto& column = table.GetColumn(name);
    if (!column.HasDefaultValue()) {
      defaults->emplace_back();
      continue;
    }
    auto expr = column.DefaultValue().Copy();
    duckdb::ConstantBinder default_binder{*binder, context, "DEFAULT value"};
    default_binder.target_type = column.Type();
    defaults->push_back(default_binder.Bind(expr));
  }
  return defaults;
}

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitCopyFromGlobal(
  duckdb::ClientContext& context, const std::string& path,
  pg::CopyInTarget target, bool ordered,
  const duckdb::FileCompressionType& compression) {
  if (auto* queue =
        ConnectionContext::Get(context).GetSideChannel<pg::CopyInBatches>();
      queue && path == kCopyStdinPath) {
    return duckdb::make_uniq<BatchesGlobalState>(context, *queue,
                                                 std::move(target), ordered);
  }
  const auto flags =
    (target.binary ? duckdb::FileFlags::FILE_FLAGS_READ
                   : duckdb::FileFlags::FILE_FLAGS_READ |
                       duckdb::FileFlags::FILE_FLAGS_PARALLEL_ACCESS) |
    compression;
  auto file = duckdb::FileSystem::GetFileSystem(context).OpenFile(path, flags);
  if (target.binary || !file->CanSeek() ||
      target.encoding != formats::TextEncoding::Utf8 ||
      (target.csv && target.escape != target.quote)) {
    return duckdb::make_uniq<BatchesGlobalState>(context, std::move(file),
                                                 std::move(target), ordered);
  }
  return duckdb::make_uniq<RangesGlobalState>(context, std::move(file),
                                              std::move(target), ordered);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitCopyFromLocal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState* global) {
  if (global->Cast<CopyFromGlobalState>().source == CopyFromSource::Ranges) {
    return duckdb::make_uniq<RangesLocalState>();
  }
  return duckdb::make_uniq<BatchesLocalState>();
}

void ScanCopyFrom(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input,
                  duckdb::DataChunk& output) {
  if (input.global_state->Cast<CopyFromGlobalState>().source ==
      CopyFromSource::Ranges) {
    ScanRanges(context, input, output);
  } else {
    ScanBatches(context, input, output);
  }
}

}  // namespace sdb::connector
