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

#include <absl/base/internal/endian.h>
#include <absl/container/btree_map.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/async_memory_governor.hpp>
#include <duckdb/formats/copy_from.hpp>
#include <duckdb/formats/row_input.hpp>
#include <duckdb/parallel/async_result.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <duckdb/parallel/task_scheduler.hpp>
#include <iresearch/utils/pg/errcodes.hpp>
#include <iresearch/utils/pg/sql_exception_macro.hpp>
#include <limits>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "formats/binary_copy_decoder.h"
#include "pg/connection_context.h"
#include "pg/copy_batch_builder.h"
#include "pg/copy_binary.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kCopyStdinPath = "/dev/stdin";

constexpr size_t kPlanProbeRows = 4;
constexpr size_t kPlanGroupsPerThread = 4;
constexpr size_t kPlanGroupMinBytes = size_t{1} << 20;
constexpr size_t kPlanPrefetchBytes = size_t{1} << 22;

int32_t BinaryWidth(const duckdb::LogicalType& type) {
  if (type.HasAlias()) {
    return -1;
  }
  switch (type.id()) {
    using enum duckdb::LogicalTypeId;
    case BOOLEAN:
      return 1;
    case SMALLINT:
      return 2;
    case INTEGER:
    case DATE:
    case FLOAT:
      return 4;
    case BIGINT:
    case DOUBLE:
    case TIMESTAMP:
    case TIMESTAMP_TZ:
      return 8;
    case UUID:
      return 16;
    default:
      return -1;
  }
}

bool PlausibleRows(std::string_view bytes, size_t pos,
                   std::span<const int32_t> widths) {
  for (size_t row = 0; row < kPlanProbeRows; ++row) {
    if (bytes.size() - pos < sizeof(int16_t)) {
      return false;
    }
    const auto fields =
      static_cast<int16_t>(absl::big_endian::Load16(bytes.data() + pos));
    if (fields == pg::kPgCopyTrailer && bytes.size() - pos == sizeof(int16_t)) {
      return row != 0;
    }
    if (fields < 0 || static_cast<size_t>(fields) != widths.size()) {
      return false;
    }
    pos += sizeof(int16_t);
    for (const auto width : widths) {
      if (bytes.size() - pos < sizeof(int32_t)) {
        return false;
      }
      const auto length =
        static_cast<int32_t>(absl::big_endian::Load32(bytes.data() + pos));
      pos += sizeof(int32_t);
      if (length == pg::kPgCopyNull) {
        continue;
      }
      if (length < 0 || (width >= 0 && length != width) ||
          bytes.size() - pos < static_cast<size_t>(length)) {
        return false;
      }
      pos += static_cast<size_t>(length);
    }
  }
  return true;
}

size_t FindBinaryRow(std::string_view bytes, size_t from, size_t limit,
                     std::span<const int32_t> widths) {
  const auto columns = static_cast<uint16_t>(widths.size());
  for (auto pos = from; pos < limit && bytes.size() - pos >= sizeof(int16_t);
       ++pos) {
    if (absl::big_endian::Load16(bytes.data() + pos) == columns &&
        PlausibleRows(bytes, pos, widths)) {
      return pos;
    }
  }
  return std::string_view::npos;
}

struct BinaryWalk {
  size_t start = std::string_view::npos;
  size_t end = std::string_view::npos;
  std::vector<size_t> cuts;
};

void WalkCuts(std::string_view bytes, size_t pos, size_t limit,
              BinaryWalk& walk) {
  walk.start = pos;
  walk.cuts.clear();
  while (pos < limit) {
    const auto next = pg::WalkBinaryRows(
      bytes, pos, std::min(limit, pos + pg::kCopyBatchBytes));
    if (next == pos) {
      break;
    }
    walk.cuts.push_back(next);
    pos = next;
  }
  walk.end = pos;
}

class CopyFileReader {
 public:
  CopyFileReader(duckdb::ClientContext& context,
                 std::unique_ptr<duckdb::FileHandle> file,
                 std::span<const duckdb::LogicalType> types)
    : _file{std::move(file)},
      _batches{duckdb::TaskScheduler::QueryThreads(context) *
               pg::kCopyBatchesPerThread},
      _memory{context},
      _fixed_budget{duckdb::TaskScheduler::QueryThreads(context) *
                    pg::kCopyBatchesPerThread * pg::kCopyBatchBytes},
      _executor{context, duckdb::TaskSchedulerType::ASYNC},
      _chunk(pg::kCopyBatchBytes, '\0') {
    UpdateBudget();
    Plan(context, types);
  }

  CopyFileReader(duckdb::ClientContext& context, pg::CopyInBatches& source)
    : _source{&source},
      _batches{duckdb::TaskScheduler::QueryThreads(context) *
               pg::kCopyBatchesPerThread},
      _memory{context},
      _fixed_budget{duckdb::TaskScheduler::QueryThreads(context) *
                    pg::kCopyBatchesPerThread * pg::kCopyBatchBytes},
      _executor{context, duckdb::TaskSchedulerType::ASYNC} {
    UpdateBudget();
  }

  ~CopyFileReader() {
    _ended.store(true, std::memory_order_release);
    if (_source) {
      _source->Nudge();
    }
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

  void Plan(duckdb::ClientContext& context,
            std::span<const duckdb::LogicalType> types) {
    if (_file->GetFileCompressionType() !=
        duckdb::FileCompressionType::UNCOMPRESSED) {
      return;
    }
    const auto size = static_cast<size_t>(_file->GetFileSize());
    auto mapping = duckdb::MapForReading(context, _file->GetPath(),
                                         _file->OnDiskFile(), size, 0);
    if (!mapping || size < pg::kPgCopyHeader.size()) {
      return;
    }
    const std::string_view bytes{
      reinterpret_cast<const char*>(mapping->GetData(0, size)), size};
    const auto extension = static_cast<int32_t>(
      absl::big_endian::Load32(bytes.data() + pg::kPgCopyHeader.size() - 4));
    if (extension < 0 ||
        size - pg::kPgCopyHeader.size() < static_cast<size_t>(extension)) {
      return;
    }
    const auto begin =
      pg::kPgCopyHeader.size() + static_cast<size_t>(extension);
    mapping->PrefetchMissing(0, std::min(size, kPlanPrefetchBytes));
    _builder.Feed(bytes.substr(0, begin), true);
    _mapping = duckdb::shared_ptr<duckdb::MemoryMappedFile>(std::move(mapping));
    _mapped = bytes;
    _file->ReadInPlace(context, size, 0);

    std::vector<int32_t> widths;
    widths.reserve(types.size());
    for (const auto& type : types) {
      widths.push_back(BinaryWidth(type));
    }
    const auto data_size = size - begin;
    const auto groups = std::clamp<size_t>(
      data_size / kPlanGroupMinBytes, 1,
      duckdb::TaskScheduler::QueryThreads(context) * kPlanGroupsPerThread);
    const auto group_bytes = (data_size + groups - 1) / groups;
    const auto group_end = [&](size_t group) {
      return std::min(size, begin + (group + 1) * group_bytes);
    };
    std::vector<BinaryWalk> walks(groups);
    duckdb::TaskExecutor executor{context};
    for (size_t group = 0; group < groups; ++group) {
      executor.ScheduleTask(
        [&, group] {
          const auto from = begin + group * group_bytes;
          const auto to = group_end(group);
          _mapping->PrefetchMissing(from, to - from);
          const auto start =
            group == 0 ? begin : FindBinaryRow(bytes, from, to, widths);
          if (start != std::string_view::npos) {
            WalkCuts(bytes, start, to, walks[group]);
          }
        },
        "binary copy plan");
    }
    executor.WorkOnTasks();

    auto at = begin;
    for (size_t group = 0; group < groups; ++group) {
      const auto to = group_end(group);
      if (at >= to) {
        continue;
      }
      auto& walk = walks[group];
      if (walk.start != at) {
        WalkCuts(bytes, at, to, walk);
      }
      for (const auto cut : walk.cuts) {
        _planned.emplace_back(at, cut);
        at = cut;
      }
      if (at < to) {
        break;
      }
    }
    _tail = at;
  }

  void ReadMapped() {
    if (_next < _planned.size()) {
      const auto [from, to] = _planned[_next++];
      auto batch = std::make_unique<pg::CopyBatch>();
      batch->AddView(_mapped.substr(from, to - from));
      batch->owner = std::make_shared<duckdb::MappedRange>(_mapping, from, to);
      Push(std::move(batch));
      return;
    }
    _builder.Feed(_mapped.substr(_tail), true);
    FinishFile();
  }

  void ReadStream() {
    if (_builder.HasRows() && !_source->Pending()) {
      PushBuilt();
      return;
    }
    auto lease = _source->Next();
    if (!lease) {
      if (_source->Ended()) {
        FinishFile();
      }
      return;
    }
    bool viewed = false;
    for (const auto segment : lease->segments) {
      viewed |= _builder.Feed(segment, true);
    }
    if (viewed) {
      _fed.push_back(std::move(lease));
    }
    if (_builder.Ready()) {
      PushBuilt();
    }
  }

  void ReadBatch() {
    if (_mapping) {
      ReadMapped();
      return;
    }
    if (_source) {
      ReadStream();
      return;
    }
    while (!_builder.Ready()) {
      const auto got =
        static_cast<size_t>(_file->Read(_chunk.data(), _chunk.size()));
      if (got == 0) {
        FinishFile();
        return;
      }
      _builder.Feed({_chunk.data(), got}, false);
    }
    PushBuilt();
  }

  void PushBuilt() {
    auto batch = _builder.Take();
    if (!_fed.empty()) {
      batch->owner = std::make_shared<Leases>(std::exchange(_fed, {}));
    }
    Push(std::move(batch));
  }

  void FinishFile() {
    _builder.Finish();
    while (_builder.HasRows()) {
      PushBuilt();
    }
    _ended.store(true, std::memory_order_release);
    _batches.Finish();
  }

  void Push(std::unique_ptr<pg::CopyBatch> batch) {
    _batches.Push(std::move(batch), {});
    _memory.UpdateReservation(_batches.RetainedBytes());
    UpdateBudget();
  }

  using Leases = std::vector<pg::CopyBatchLease>;

  std::unique_ptr<duckdb::FileHandle> _file;
  pg::CopyInBatches* _source = nullptr;
  Leases _fed;
  duckdb::shared_ptr<duckdb::MemoryMappedFile> _mapping;
  std::string_view _mapped;
  std::vector<std::pair<size_t, size_t>> _planned;
  size_t _next = 0;
  size_t _tail = 0;
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
  CopyFromGlobalState(duckdb::ClientContext& context, pg::CopyInBatches& source,
                      pg::CopyInTarget target, bool ordered)
    : target{std::move(target)},
      ordered{ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{std::in_place, context, source},
      queue{reader->Batches()} {
    reader->Resume();
  }

  CopyFromGlobalState(duckdb::ClientContext& context,
                      std::unique_ptr<duckdb::FileHandle> file,
                      pg::CopyInTarget target, bool ordered)
    : target{std::move(target)},
      ordered{ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{std::in_place, context, std::move(file), this->target.types},
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
