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
#include <absl/synchronization/mutex.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <duckdb/common/file_system.hpp>
#include <duckdb/common/memory_mapped_file.hpp>
#include <duckdb/common/serializer/async_memory_governor.hpp>
#include <duckdb/formats/copy_from.hpp>
#include <duckdb/formats/range_lines.hpp>
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

constexpr size_t kProbeRows = 4;
constexpr size_t kPieceBytes = size_t{1} << 22;
constexpr size_t kAheadBytesPerThread = size_t{1} << 22;

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
  for (size_t row = 0; row < kProbeRows; ++row) {
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
    pos = pg::BinaryRowEnd(bytes, pos, widths);
    if (pos == std::string_view::npos) {
      return false;
    }
  }
  return true;
}

size_t FindBinaryRow(std::string_view bytes, std::span<const int32_t> widths) {
  const auto columns = static_cast<uint16_t>(widths.size());
  for (size_t pos = 0; bytes.size() - pos >= sizeof(int16_t); ++pos) {
    if (absl::big_endian::Load16(bytes.data() + pos) == columns &&
        PlausibleRows(bytes, pos, widths)) {
      return pos;
    }
  }
  return std::string_view::npos;
}

size_t RowsAfterHeader(std::string_view bytes) {
  if (bytes.size() < pg::kPgCopyHeader.size()) {
    return std::string_view::npos;
  }
  const auto extension = pg::BinaryCopyHeaderExtension(bytes);
  if (bytes.size() - pg::kPgCopyHeader.size() < extension) {
    return std::string_view::npos;
  }
  return pg::kPgCopyHeader.size() + extension;
}

struct BinaryPiece {
  std::string_view bytes;
  std::shared_ptr<void> owner;
  size_t start = std::string_view::npos;
  std::vector<size_t> cuts;
  std::exception_ptr error;
  bool ready = false;
};

void WalkCuts(BinaryPiece& piece) {
  const auto bytes = piece.bytes;
  auto pos = piece.start;
  while (pos < bytes.size()) {
    const auto next = pg::WalkBinaryRows(
      bytes, pos, std::min(bytes.size(), pos + pg::kCopyBatchBytes));
    if (next == pos) {
      break;
    }
    piece.cuts.push_back(next);
    pos = next;
  }
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
    Split(context, types);
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
    {
      absl::MutexLock lock{&_piece_lock};
    }
    _executor.CancelAndDrain();
    if (_loader) {
      _loader->CancelAndDrain();
    }
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

  void Split(duckdb::ClientContext& context,
             std::span<const duckdb::LogicalType> types) {
    const auto size = static_cast<size_t>(_file->GetFileSize());
    const auto& path = _file->GetPath();
    const auto compression = _file->GetFileCompressionType();
    if (compression.IsCompressed()) {
      _frames = duckdb::PiecedRowSource::Open(context, path, compression,
                                              _file->OnDiskFile(), size);
      if (!_frames) {
        return;
      }
      for (const auto& span : _frames->Pieces()) {
        _bounds.push_back(static_cast<size_t>(span.output_offset));
      }
      _bounds.push_back(static_cast<size_t>(_frames->Size()));
    } else {
      auto mapping =
        duckdb::MapForReading(context, path, _file->OnDiskFile(), size, 0);
      if (!mapping) {
        return;
      }
      _mapping =
        duckdb::shared_ptr<duckdb::MemoryMappedFile>(std::move(mapping));
      _mapped = {reinterpret_cast<const char*>(_mapping->GetData(0, size)),
                 size};
      for (size_t at = 0; at < size; at += kPieceBytes) {
        _bounds.push_back(at);
      }
      _bounds.push_back(size);
    }
    _pieces = std::vector<BinaryPiece>(_bounds.size() - 1);
    _ahead_limit =
      duckdb::TaskScheduler::QueryThreads(context) * kAheadBytesPerThread;
    _widths.reserve(types.size());
    for (const auto& type : types) {
      _widths.push_back(BinaryWidth(type));
    }
    _loader.emplace(context);
    _file->ReadInPlace(context, size, 0);
  }

  void ScheduleLoads() {
    while (_scheduled < _pieces.size() &&
           (_scheduled == _next || _ahead < _ahead_limit)) {
      const auto index = _scheduled++;
      _ahead += _bounds[index + 1] - _bounds[index];
      _loader->ScheduleTask([this, index] { Load(index); },
                            "binary copy piece");
    }
  }

  void Load(size_t index) {
    auto& piece = _pieces[index];
    try {
      const auto from = _bounds[index];
      const auto size = _bounds[index + 1] - from;
      if (_mapping) {
        _mapping->PrefetchMissing(from, size);
        piece.bytes = _mapped.substr(from, size);
        piece.owner =
          std::make_shared<duckdb::MappedRange>(_mapping, from, from + size);
      } else {
        auto bytes = std::make_shared_for_overwrite<char[]>(size);
        const auto read = _frames->Read(bytes.get(), size, from);
        piece.bytes = {bytes.get(), static_cast<size_t>(read)};
        piece.owner = std::move(bytes);
      }
      piece.start = index == 0 ? RowsAfterHeader(piece.bytes)
                               : FindBinaryRow(piece.bytes, _widths);
      if (piece.start != std::string_view::npos) {
        WalkCuts(piece);
      }
    } catch (...) {
      piece.error = std::current_exception();
    }
    absl::MutexLock lock{&_piece_lock};
    piece.ready = true;
  }

  void ReadPiece() {
    ScheduleLoads();
    auto& piece = _pieces[_next];
    const auto loaded = [&] {
      return piece.ready || _ended.load(std::memory_order_acquire);
    };
    {
      duckdb::TaskScheduler::BlockingScope blocking;
      absl::MutexLock lock{&_piece_lock};
      _piece_lock.Await(absl::Condition(&loaded));
      if (!piece.ready) {
        return;
      }
    }
    if (piece.error) {
      std::rethrow_exception(piece.error);
    }
    FeedPiece(piece);
    _ahead -= _bounds[_next + 1] - _bounds[_next];
    piece = {};
    if (++_next == _pieces.size()) {
      FinishFile();
    }
  }

  void FeedPiece(const BinaryPiece& piece) {
    const auto bytes = piece.bytes;
    size_t at = 0;
    if (piece.start != std::string_view::npos) {
      at = piece.start;
      Keep(piece.owner, _builder.Feed(bytes.substr(0, at), true));
      if (_builder.BetweenRows()) {
        for (const auto cut : piece.cuts) {
          Keep(piece.owner, _builder.AddRows(bytes.substr(at, cut - at), true));
          at = cut;
          if (_builder.Ready()) {
            PushBuilt();
          }
        }
      }
    }
    Keep(piece.owner, _builder.Feed(bytes.substr(at), true));
    if (_builder.Ready()) {
      PushBuilt();
    }
  }

  void Keep(const std::shared_ptr<void>& owner, bool viewed) {
    if (viewed && (_fed.empty() || _fed.back() != owner)) {
      _fed.push_back(owner);
    }
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
      _fed.push_back(std::make_shared<pg::CopyBatchLease>(std::move(lease)));
    }
    if (_builder.Ready()) {
      PushBuilt();
    }
  }

  void ReadBatch() {
    if (!_pieces.empty()) {
      ReadPiece();
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
    if (_fed.size() == 1) {
      batch->owner = std::move(_fed.front());
    } else if (!_fed.empty()) {
      batch->owner = std::make_shared<Owners>(std::move(_fed));
    }
    _fed.clear();
    Push(std::move(batch));
  }

  void FinishFile() {
    while (_builder.HasRows()) {
      PushBuilt();
    }
    _builder.Finish();
    _ended.store(true, std::memory_order_release);
    _batches.Finish();
  }

  void Push(std::unique_ptr<pg::CopyBatch> batch) {
    _batches.Push(std::move(batch), {});
    _memory.UpdateReservation(_batches.RetainedBytes());
    UpdateBudget();
  }

  using Owners = std::vector<std::shared_ptr<void>>;

  std::unique_ptr<duckdb::FileHandle> _file;
  pg::CopyInBatches* _source = nullptr;
  Owners _fed;
  duckdb::shared_ptr<duckdb::MemoryMappedFile> _mapping;
  duckdb::shared_ptr<duckdb::PiecedRowSource> _frames;
  std::string_view _mapped;
  std::vector<size_t> _bounds;
  std::vector<BinaryPiece> _pieces;
  std::vector<int32_t> _widths;
  size_t _next = 0;
  size_t _scheduled = 0;
  size_t _ahead = 0;
  size_t _ahead_limit = 0;
  absl::Mutex _piece_lock;
  pg::CopyBatchBuilder _builder{pg::CopyBatchFormat::Binary,
                                pg::kCopyBatchBytes};
  pg::CopyInBatches _batches;
  duckdb::ManagedAsyncMemoryGovernor _memory;
  const size_t _fixed_budget;
  std::atomic<size_t> _budget{0};
  std::atomic_bool _reading{false};
  std::atomic_bool _ended{false};
  duckdb::TaskExecutor _executor;
  std::optional<duckdb::TaskExecutor> _loader;
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
      if (!_lines.Add(seq, lines)) {
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
      if (_lines.Known(seq)) {
        return _lines.Before(seq) + line;
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
  duckdb::StateWithBlockableTasks _waiters;
  duckdb::RangeLines _lines DUCKDB_GUARDED_BY(_waiters.lock);
  uint64_t _failed DUCKDB_GUARDED_BY(_waiters.lock) =
    std::numeric_limits<uint64_t>::max();
};

struct CopyFromGlobalState final : public duckdb::GlobalTableFunctionState {
  CopyFromGlobalState(duckdb::ClientContext& context, pg::CopyInBatches& source,
                      pg::CopyInTarget target, bool ordered)
    : target{std::move(target)},
      ordered{ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{context, source},
      queue{reader.Batches()} {
    reader.Resume();
  }

  CopyFromGlobalState(duckdb::ClientContext& context,
                      std::unique_ptr<duckdb::FileHandle> file,
                      pg::CopyInTarget target, bool ordered)
    : target{std::move(target)},
      ordered{ordered},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      reader{context, std::move(file), this->target.types},
      queue{reader.Batches()} {
    reader.Resume();
  }

  duckdb::idx_t MaxThreads() const final { return max_threads; }

  pg::CopyInTarget target;
  bool ordered;
  duckdb::idx_t max_threads;
  CopyLines lines;
  CopyFileReader reader;
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

bool TakeBatch(CopyFromGlobalState& g, duckdb::TableFunctionInput& input,
               CopyFromLocalState& l) {
  try {
    return g.queue.Take(input, 1, l.run) == pg::CopyTake::Batches;
  } catch (...) {
    const auto error = std::current_exception();
    if (const auto line = g.lines.Locate(input, g.queue.FailedAt(), 1);
        line && *line != 0) {
      std::rethrow_exception(error);
    }
    return false;
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
        } else if (!TakeBatch(g, input, l)) {
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
      g.reader.Resume();
    }
  }
  output.SetChildCardinality(row);
}

}  // namespace sdb::connector
