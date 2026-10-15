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
#include <exception>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <memory>
#include <optional>
#include <string_view>
#include <utility>
#include <vector>

#include "connector/copy_batch_parse.h"
#include "connector/duckdb_client_state.h"
#include "pg/connection_context.h"
#include "pg/copy_batch_builder.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kCopyStdinPath = "/dev/stdin";
constexpr size_t kOrderedRun = 16;
constexpr uint64_t kCopyFileRangeBytes = 8 << 20;

pg::CopyInBatches& RequireCopyInBatches(duckdb::ClientContext& context,
                                        bool binary) {
  const std::string_view format = binary ? "binary" : "text";
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (!state) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT ", format,
                            ") is only valid on a server-side PostgreSQL wire "
                            "connection"));
  }
  auto* batches =
    state->GetConnectionContext().GetSideChannel<pg::CopyInBatches>();
  if (!batches) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT ", format,
                            ") requires a PG wire connection (transport not "
                            "attached)"));
  }
  return *batches;
}

class CopyFileReader {
 public:
  CopyFileReader(duckdb::ClientContext& context,
                 std::unique_ptr<duckdb::FileHandle> file, bool binary)
    : _file{std::move(file)},
      _builder{binary ? pg::CopyBatchFormat::Binary
                      : pg::CopyBatchFormat::Text},
      _memory{context},
      _fixed_budget{duckdb::TaskScheduler::QueryThreads(context) * kOrderedRun *
                    pg::kCopyBatchBytes},
      _executor{context, duckdb::TaskSchedulerType::ASYNC},
      _chunk(pg::kCopyBatchBytes, '\0') {
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
      _builder.Feed({_chunk.data(), got}, false);
    }
    Push(_builder.Take());
  }

  void FinishFile() {
    _builder.Finish();
    if (_builder.HasRows()) {
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
};

enum class CopyFromSource : uint8_t {
  Batches,
  Ranges,
};

struct CopyFromGlobalState : public duckdb::GlobalTableFunctionState {
  CopyFromGlobalState(CopyFromSource source, pg::CopyInTarget target,
                      bool ordered)
    : source{source}, target{std::move(target)}, ordered{ordered} {}

  CopyFromSource source;
  pg::CopyInTarget target;
  bool ordered;
};

struct CopyFromLocalState : public duckdb::LocalTableFunctionState {
  std::optional<CopyBatchParser> parser;
  duckdb::idx_t batch_index = 0;
};

struct BatchesGlobalState final : public CopyFromGlobalState {
  BatchesGlobalState(duckdb::ClientContext& context, pg::CopyInBatches& queue,
                     pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{CopyFromSource::Batches, std::move(target), ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      queue{queue} {}

  BatchesGlobalState(duckdb::ClientContext& context,
                     std::unique_ptr<duckdb::FileHandle> file,
                     pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{CopyFromSource::Batches, std::move(target), ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{std::in_place, context, std::move(file), this->target.binary},
      queue{reader->Batches()} {
    reader->Resume();
  }

  duckdb::idx_t MaxThreads() const final { return max_threads; }

  duckdb::idx_t max_threads;
  std::optional<CopyFileReader> reader;
  pg::CopyInBatches& queue;
};

struct BatchesLocalState final : public CopyFromLocalState {
  std::vector<pg::CopyBatch*> run;
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
    : CopyFromGlobalState{CopyFromSource::Ranges, std::move(target), ordered},
      file{std::move(file)},
      size{this->file->GetFileSize()},
      range_count{(size + kCopyFileRangeBytes - 1) / kCopyFileRangeBytes},
      read_ahead{duckdb::ScanReadAhead::Create(context)} {}

  duckdb::idx_t MaxThreads() const final {
    return std::max<uint64_t>(range_count, 1);
  }

  std::unique_ptr<duckdb::FileHandle> file;
  uint64_t size;
  uint64_t range_count;
  std::atomic<uint64_t> next{0};
  duckdb::unique_ptr<duckdb::ScanReadAhead> read_ahead;
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
  if (!l.parser) {
    l.parser.emplace(context, g.target);
  }
  auto& parser = *l.parser;
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
        } else if (g.queue.Take(input, g.ordered ? kOrderedRun : 1, l.run) !=
                   pg::CopyTake::Batches) {
          break;
        }
        l.batch_index = l.run.front()->seq;
      }
      parser.Start(*l.run[l.next++]);
    }
    row = parser.Parse(output, row);
    if (parser.Exhausted()) {
      g.queue.Done(parser.Seq());
      parser.Stop();
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

uint64_t RangeFrom(uint64_t range) {
  const uint64_t begin = range * kCopyFileRangeBytes;
  return begin == 0 ? 0 : begin - 2;
}

void ReadRange(RangesGlobalState& g, RangeJobState& job) {
  const uint64_t end =
    std::min<uint64_t>((job.range + 1) * kCopyFileRangeBytes, g.size);
  const uint64_t from = RangeFrom(job.range);
  job.buffer.clear();
  ReadAt(g, job.buffer, from, end - from);
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
    const auto cut = FindCut(buffer, 1);
    if (cut == std::string::npos || cut + 1 == buffer.size()) {
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
    if (CutRange(g, *l.job, l.batch)) {
      l.batch_index = l.job->batch_index;
      return true;
    }
    l.job.reset();
  }
}

void ScanRanges(duckdb::ClientContext& context,
                duckdb::TableFunctionInput& input, duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<RangesGlobalState>();
  auto& l = input.local_state->Cast<RangesLocalState>();
  if (!l.parser) {
    l.parser.emplace(context, g.target);
  }
  auto& parser = *l.parser;
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    if (!parser.Active()) {
      if (row != 0 || !NextRange(context, input, g, l)) {
        break;
      }
      parser.Start(l.batch);
    }
    row = parser.Parse(output, row);
    if (parser.Exhausted()) {
      parser.Stop();
      l.job.reset();
    }
  }
  output.SetChildCardinality(row);
}

}  // namespace

bool BindCopyFrom(duckdb::ClientContext& context,
                  duckdb::CopyFromFunctionBindInput& input, bool binary) {
  if (input.info.file_path == kCopyStdinPath) {
    RequireCopyInBatches(context, binary);
  }
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

duckdb::unique_ptr<duckdb::GlobalTableFunctionState> InitCopyFromGlobal(
  duckdb::ClientContext& context, const std::string& path,
  pg::CopyInTarget target, bool ordered) {
  if (path == kCopyStdinPath) {
    auto& queue = RequireCopyInBatches(context, target.binary);
    return duckdb::make_uniq<BatchesGlobalState>(context, queue,
                                                 std::move(target), ordered);
  }
  const auto flags = target.binary
                       ? duckdb::FileFlags::FILE_FLAGS_READ
                       : duckdb::FileFlags::FILE_FLAGS_READ |
                           duckdb::FileFlags::FILE_FLAGS_PARALLEL_ACCESS;
  auto file = duckdb::FileSystem::GetFileSystem(context).OpenFile(path, flags);
  if (target.binary || !file->CanSeek()) {
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
