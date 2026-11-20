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
#include <duckdb/formats/row_ranges.hpp>
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/async_result.hpp>
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
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "formats/copy_decoder.h"
#include "pg/connection_context.h"
#include "pg/copy_batch_builder.h"

namespace sdb::connector {
namespace {

constexpr std::string_view kCopyStdinPath = "/dev/stdin";
constexpr uint64_t kRangeMinBytes = 1 << 20;
constexpr uint64_t kTextRangeMaxBytes = 8 << 20;
constexpr uint64_t kCsvRangeMaxBytes = 2 << 20;

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
  Rows,
};

class FileRowSource final : public duckdb::RowSource {
 public:
  FileRowSource(duckdb::FileHandle& file, uint64_t size)
    : _file{file}, _size{size} {}

  duckdb::idx_t Read(char* data, duckdb::idx_t count,
                     duckdb::idx_t offset) const final {
    const auto available = offset < _size ? std::min(count, _size - offset) : 0;
    if (available != 0) {
      _file.Read(data, available, offset);
    }
    return available;
  }

 private:
  duckdb::FileHandle& _file;
  const uint64_t _size;
};

char FileLineBreak(duckdb::FileHandle& file, const pg::CopyInTarget& target) {
  constexpr uint64_t kProbeBytes = uint64_t{64} << 10;
  const uint64_t size = file.GetFileSize();
  const formats::CsvDialect csv{target.delim, target.quote, target.escape};
  std::string head;
  for (uint64_t want = kProbeBytes;; want *= 2) {
    head.resize(std::min(want, size));
    if (!head.empty()) {
      file.Read(head.data(), head.size(), 0);
    }
    const auto found =
      formats::DetectLineBreak(head, target.csv, csv, head.size() == size);
    if (found != '\0') {
      return found;
    }
  }
}

uint64_t RangeBytes(duckdb::ClientContext& context,
                    const pg::CopyInTarget& target, uint64_t size) {
  const uint64_t threads =
    std::max<uint64_t>(duckdb::TaskScheduler::QueryThreads(context), 1);
  return std::clamp<uint64_t>(
    size / (threads * 4), kRangeMinBytes,
    target.csv ? kCsvRangeMaxBytes : kTextRangeMaxBytes);
}

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

struct RowsGlobalState final : public CopyFromGlobalState {
  RowsGlobalState(duckdb::ClientContext& context,
                  std::unique_ptr<duckdb::FileHandle> file_p,
                  pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{context, CopyFromSource::Rows, std::move(target),
                          ordered},
      file{std::move(file_p)},
      size{file->GetFileSize()},
      source{*file, size},
      lexer{formats::RowDialectOf(this->target),
            this->target.types.size(),
            std::numeric_limits<duckdb::idx_t>::max(),
            true,
            false,
            true},
      ranges{lexer, source, 0, size, RangeBytes(context, this->target, size),
             true} {
    ranges.path = file->GetPath();
  }

  duckdb::idx_t MaxThreads() const final { return ranges.RangeCount(); }

  std::unique_ptr<duckdb::FileHandle> file;
  uint64_t size;
  FileRowSource source;
  duckdb::RowLexer lexer;
  duckdb::RowRanges ranges;
};

struct RowsLocalState final : public CopyFromLocalState {
  std::optional<duckdb::RowRangeReader> reader;
  duckdb::idx_t range = 0;
  bool scanning = false;
  bool decoding = false;
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

void ScanRows(duckdb::ClientContext& context, duckdb::TableFunctionInput& input,
              duckdb::DataChunk& output) {
  auto& g = input.global_state->Cast<RowsGlobalState>();
  auto& l = input.local_state->Cast<RowsLocalState>();
  auto& parser = StartWorker(context, g, l);
  if (parser.Failed()) {
    output.SetChildCardinality(0);
    LocateError(g, input, parser);
    return;
  }
  if (!l.reader) {
    l.reader.emplace(g.ranges);
  }
  auto& reader = *l.reader;
  duckdb::idx_t row = 0;
  while (row < STANDARD_VECTOR_SIZE) {
    if (!l.scanning) {
      if (row != 0) {
        break;
      }
      if (!g.ranges.Claim(l.range)) {
        FinishWorker(context, g, l);
        break;
      }
      reader.Start(l.range, true, false, false);
      l.batch_index = l.range;
      l.scanning = true;
      l.decoding = false;
    }
    const auto result = reader.Advance(STANDARD_VECTOR_SIZE - row);
    if (result == duckdb::RowReadResult::ROWS ||
        result == duckdb::RowReadResult::FLUSH) {
      if (!std::exchange(l.decoding, true)) {
        parser.StartLexed(l.range);
      }
      const auto rows =
        std::min<duckdb::idx_t>(reader.Ready(), STANDARD_VECTOR_SIZE - row);
      row = parser.ParseLexed(output, row, reader, rows);
      reader.Emit(rows);
      if (parser.Failed()) {
        output.SetChildCardinality(0);
        LocateError(g, input, parser);
        return;
      }
      continue;
    }
    if (l.decoding) {
      FinishUnit(g, parser);
    } else {
      g.lines.Finish(l.range, 0, {});
    }
    g.ranges.Finish(l.range);
    l.scanning = false;
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
  if (target.binary || target.types.empty() || !file->CanSeek() ||
      target.encoding != formats::TextEncoding::Utf8) {
    return duckdb::make_uniq<BatchesGlobalState>(context, std::move(file),
                                                 std::move(target), ordered);
  }
  target.line_break = FileLineBreak(*file, target);
  return duckdb::make_uniq<RowsGlobalState>(context, std::move(file),
                                            std::move(target), ordered);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitCopyFromLocal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState* global) {
  if (global->Cast<CopyFromGlobalState>().source == CopyFromSource::Rows) {
    return duckdb::make_uniq<RowsLocalState>();
  }
  return duckdb::make_uniq<BatchesLocalState>();
}

void ScanCopyFrom(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input,
                  duckdb::DataChunk& output) {
  if (input.global_state->Cast<CopyFromGlobalState>().source ==
      CopyFromSource::Rows) {
    ScanRows(context, input, output);
  } else {
    ScanBatches(context, input, output);
  }
}

}  // namespace sdb::connector
