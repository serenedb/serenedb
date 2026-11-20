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
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/serializer/async_memory_governor.hpp>
#include <duckdb/formats/copy_from.hpp>
#include <duckdb/parallel/async_result.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "formats/binary_copy_decoder.h"
#include "pg/connection_context.h"
#include "pg/copy_batch_builder.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kCopyStdinPath = "/dev/stdin";

class CopyFileReader {
 public:
  CopyFileReader(duckdb::ClientContext& context,
                 std::unique_ptr<duckdb::FileHandle> file)
    : _file{std::move(file)},
      _batches{duckdb::TaskScheduler::QueryThreads(context) *
               pg::kCopyBatchesPerThread},
      _memory{context},
      _fixed_budget{duckdb::TaskScheduler::QueryThreads(context) *
                    pg::kCopyBatchesPerThread * pg::kCopyBatchBytes},
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
  pg::CopyBatchBuilder _builder{pg::CopyBatchFormat::Binary,
                                pg::kCopyBatchBytes};
  pg::CopyInBatches _batches;
  duckdb::ManagedAsyncMemoryGovernor _memory;
  const size_t _fixed_budget;
  std::atomic<size_t> _budget{0};
  std::atomic_bool _reading{false};
  std::atomic_bool _ended{false};
  duckdb::TaskExecutor _executor;
  std::string _chunk;
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
  void Finish(uint64_t seq, uint64_t lines) {
    duckdb::vector<duckdb::InterruptState> waiters;
    {
      Guard guard{_waiters.lock};
      _done.try_emplace(seq, lines);
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
  bool Advance() DUCKDB_REQUIRES(_waiters.lock) {
    bool advanced = false;
    for (auto it = _done.begin(); it != _done.end() && it->first == _next;
         it = _done.erase(it)) {
      _lines += it->second;
      ++_next;
      advanced = true;
    }
    return advanced;
  }

  duckdb::StateWithBlockableTasks _waiters;
  absl::btree_map<uint64_t, uint64_t> _done DUCKDB_GUARDED_BY(_waiters.lock);
  uint64_t _next DUCKDB_GUARDED_BY(_waiters.lock) = 0;
  uint64_t _lines DUCKDB_GUARDED_BY(_waiters.lock) = 0;
  uint64_t _failed DUCKDB_GUARDED_BY(_waiters.lock) =
    std::numeric_limits<uint64_t>::max();
};

struct CopyFromGlobalState final : public duckdb::GlobalTableFunctionState {
  CopyFromGlobalState(duckdb::ClientContext& context, pg::CopyInBatches& queue,
                      pg::CopyInTarget target, bool ordered)
    : target{std::move(target)},
      ordered{ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      queue{queue} {}

  CopyFromGlobalState(duckdb::ClientContext& context,
                      std::unique_ptr<duckdb::FileHandle> file,
                      pg::CopyInTarget target, bool ordered)
    : target{std::move(target)},
      ordered{ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{std::in_place, context, std::move(file)},
      queue{reader->Batches()} {
    reader->Resume();
  }

  duckdb::idx_t MaxThreads() const final { return max_threads; }

  pg::CopyInTarget target;
  bool ordered;
  duckdb::idx_t max_threads;
  CopyLines lines;
  std::optional<CopyFileReader> reader;
  pg::CopyInBatches& queue;
};

struct CopyFromLocalState final : public duckdb::LocalTableFunctionState {
  std::optional<formats::BinaryCopyDecoder> parser;
  std::vector<pg::CopyBatchLease> run;
  size_t next = 0;
  duckdb::idx_t batch_index = 0;
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

void LocateError(CopyFromGlobalState& g, duckdb::TableFunctionInput& input,
                 const formats::BinaryCopyDecoder& parser) {
  if (const auto line =
        g.lines.Locate(input, parser.Seq(), parser.FailedLine());
      line && *line != 0) {
    parser.ThrowError(*line);
  }
}

}  // namespace

bool BindCopyFrom(duckdb::ClientContext& context,
                  duckdb::CopyFromFunctionBindInput& input) {
  const bool ordered = duckdb::CopyFromKeepsOrder(context, input.info);
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
  if (auto* queue =
        ConnectionContext::Get(context).GetSideChannel<pg::CopyInBatches>();
      queue && path == kCopyStdinPath) {
    return duckdb::make_uniq<CopyFromGlobalState>(context, *queue,
                                                  std::move(target), ordered);
  }
  auto file = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    path, duckdb::FileFlags::FILE_FLAGS_READ |
            duckdb::FileCompressionType::AUTO_DETECT);
  return duckdb::make_uniq<CopyFromGlobalState>(context, std::move(file),
                                                std::move(target), ordered);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitCopyFromLocal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState*) {
  return duckdb::make_uniq<CopyFromLocalState>();
}

void ScanCopyFrom(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input,
                  duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<CopyFromGlobalState>();
  auto& l = input.local_state->Cast<CopyFromLocalState>();
  if (!l.parser) {
    l.parser.emplace(context, g.target);
  }
  auto& parser = *l.parser;
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
        } else if (g.queue.Take(input, 1, l.run) != pg::CopyTake::Batches) {
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
      g.lines.Finish(parser.Seq(), parser.Lines());
      parser.Stop();
      l.run[l.next - 1].Reset();
      if (g.reader) {
        g.reader->Resume();
      }
    }
  }
  output.SetChildCardinality(row);
}

}  // namespace sdb::connector
