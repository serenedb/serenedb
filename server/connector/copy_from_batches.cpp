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
#include <duckdb/main/settings.hpp>
#include <duckdb/parallel/async_result.hpp>
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
constexpr char kRowSep = '\n';

pg::CopyInBatches& RequireCopyInBatches(duckdb::ClientContext& context,
                                        bool binary) {
  const std::string_view format = binary ? "binary" : "text";
  auto* state =
    context.registered_state->Get<SereneDBClientState>(kSereneDBClientStateKey)
      .get();
  if (state == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT ", format,
                            ") is only valid on a server-side PostgreSQL wire "
                            "connection"));
  }
  auto* batches =
    state->GetConnectionContext().GetSideChannel<pg::CopyInBatches>();
  if (batches == nullptr) {
    THROW_SQL_ERROR(ERR_CODE(ERRCODE_FEATURE_NOT_SUPPORTED),
                    ERR_MSG("COPY ... FROM STDIN (FORMAT ", format,
                            ") requires a PG wire connection (transport not "
                            "attached)"));
  }
  return *batches;
}

class CopyFileFeeder {
 public:
  CopyFileFeeder(std::unique_ptr<duckdb::FileHandle> file, bool binary)
    : _file{std::move(file)},
      _builder{binary ? pg::CopyBatchFormat::Binary
                      : pg::CopyBatchFormat::Text},
      _chunk(pg::kCopyBatchBytes, '\0') {}

  pg::CopyInBatches& Batches() noexcept { return _batches; }

  void Produce(size_t batches) {
    try {
      while (batches != 0) {
        const auto got =
          static_cast<size_t>(_file->Read(_chunk.data(), _chunk.size()));
        if (got == 0) {
          _builder.Finish();
          if (_builder.HasRows()) {
            _batches.Push(_builder.Take(), {});
          }
          _batches.Finish();
          break;
        }
        _builder.Feed({_chunk.data(), got}, false);
        if (_builder.Ready()) {
          _batches.Push(_builder.Take(), {});
          --batches;
        }
      }
    } catch (...) {
      _batches.Fail(std::current_exception());
    }
    _batches.Produced();
  }

 private:
  std::unique_ptr<duckdb::FileHandle> _file;
  pg::CopyBatchBuilder _builder;
  pg::CopyInBatches _batches;
  std::string _chunk;
};

class CopyFileReadTask final : public duckdb::AsyncTask {
 public:
  CopyFileReadTask(CopyFileFeeder& feeder, size_t batches)
    : _feeder{feeder}, _batches{batches} {}

  void Execute() final { _feeder.Produce(_batches); }

  duckdb::idx_t GetIOSize() const final {
    return _batches * pg::kCopyBatchBytes;
  }

 private:
  CopyFileFeeder& _feeder;
  size_t _batches;
};

struct CopyFromGlobalState : public duckdb::GlobalTableFunctionState {
  CopyFromGlobalState(bool ranges, pg::CopyInTarget target, bool ordered)
    : ranges{ranges}, target{std::move(target)}, ordered{ordered} {}

  bool ranges;
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
    : CopyFromGlobalState{false, std::move(target), ordered},
      queue{queue},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)} {}

  BatchesGlobalState(duckdb::ClientContext& context,
                     std::unique_ptr<duckdb::FileHandle> file,
                     pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{false, std::move(target), ordered},
      feeder{std::in_place, std::move(file), this->target.binary},
      queue{feeder->Batches()},
      max_threads{duckdb::TaskScheduler::QueryThreads(context)},
      read_ahead{max_threads * (ordered ? kOrderedRun : 1)} {}

  duckdb::idx_t MaxThreads() const final { return max_threads; }

  std::optional<CopyFileFeeder> feeder;
  pg::CopyInBatches& queue;
  duckdb::idx_t max_threads;
  size_t read_ahead = 0;
};

struct BatchesLocalState final : public CopyFromLocalState {
  std::vector<pg::CopyBatch*> run;
  size_t next = 0;
};

struct RangesGlobalState final : public CopyFromGlobalState {
  RangesGlobalState(std::unique_ptr<duckdb::FileHandle> file,
                    pg::CopyInTarget target, bool ordered)
    : CopyFromGlobalState{true, std::move(target), ordered},
      file{std::move(file)},
      size{this->file->GetFileSize()},
      ranges{(size + kCopyFileRangeBytes - 1) / kCopyFileRangeBytes} {}

  duckdb::idx_t MaxThreads() const final {
    return std::max<uint64_t>(ranges, 1);
  }

  std::unique_ptr<duckdb::FileHandle> file;
  uint64_t size;
  uint64_t ranges;
  std::atomic<uint64_t> next{0};
};

struct RangesLocalState final : public CopyFromLocalState {
  pg::CopyBatch batch;
  std::string buffer;
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

bool TakeRun(duckdb::TableFunctionInput& input, BatchesGlobalState& g,
             size_t max, std::vector<pg::CopyBatch*>& out) {
  for (;;) {
    switch (g.queue.Take(input, max, out, (g.read_ahead + 1) / 2)) {
      case pg::CopyTake::Batches:
        return true;
      case pg::CopyTake::Produce:
        if (input.results_execution_mode ==
            duckdb::AsyncResultsExecutionMode::TASK_EXECUTOR) {
          duckdb::vector<duckdb::unique_ptr<duckdb::AsyncTask>> tasks;
          tasks.emplace_back(
            duckdb::make_uniq<CopyFileReadTask>(*g.feeder, g.read_ahead));
          input.async_result = duckdb::AsyncResult{
            std::move(tasks), duckdb::TaskSchedulerType::ASYNC};
          return false;
        }
        g.feeder->Produce(g.read_ahead);
        break;
      case pg::CopyTake::End:
      case pg::CopyTake::Blocked:
        return false;
    }
  }
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
        } else if (!TakeRun(input, g, g.ordered ? kOrderedRun : 1, l.run)) {
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

bool CutRange(RangesGlobalState& g, RangesLocalState& l, uint64_t range) {
  const uint64_t begin = range * kCopyFileRangeBytes;
  const uint64_t end = std::min<uint64_t>(begin + kCopyFileRangeBytes, g.size);
  const uint64_t from = begin == 0 ? 0 : begin - 1;
  auto& buffer = l.buffer;
  buffer.clear();
  ReadAt(g, buffer, from, end - from);
  size_t first = 0;
  if (begin != 0) {
    const auto row_sep = buffer.find(kRowSep);
    if (row_sep == std::string::npos || row_sep + 1 == buffer.size()) {
      return false;
    }
    first = row_sep + 1;
  }
  auto last = buffer.find(kRowSep, buffer.size() - 1);
  while (last == std::string::npos) {
    const auto scanned = buffer.size();
    if (ReadAt(g, buffer, from + scanned, pg::kCopyBatchBytes) == 0) {
      last = scanned - 1;
      break;
    }
    last = buffer.find(kRowSep, scanned);
  }
  l.batch.segments.clear();
  l.batch.bytes = 0;
  l.batch.seq = range;
  l.batch.AddView(std::string_view{buffer}.substr(first, last + 1 - first));
  return true;
}

const pg::CopyBatch* NextRange(RangesGlobalState& g, RangesLocalState& l,
                               bool may_claim) {
  while (may_claim) {
    const auto range = g.next.fetch_add(1, std::memory_order_relaxed);
    if (range >= g.ranges) {
      break;
    }
    if (CutRange(g, l, range)) {
      l.batch_index = range;
      return &l.batch;
    }
  }
  return nullptr;
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
      const auto* batch = NextRange(g, l, row == 0 || !g.ordered);
      if (batch == nullptr) {
        break;
      }
      parser.Start(*batch);
    }
    row = parser.Parse(output, row);
    if (parser.Exhausted()) {
      parser.Stop();
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
    table != nullptr && table->IsDuckTable() &&
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
  auto file = duckdb::FileSystem::GetFileSystem(context).OpenFile(
    path, duckdb::FileFlags::FILE_FLAGS_READ);
  if (target.binary || !file->CanSeek()) {
    return duckdb::make_uniq<BatchesGlobalState>(context, std::move(file),
                                                 std::move(target), ordered);
  }
  return duckdb::make_uniq<RangesGlobalState>(std::move(file),
                                              std::move(target), ordered);
}

duckdb::unique_ptr<duckdb::LocalTableFunctionState> InitCopyFromLocal(
  duckdb::ExecutionContext&, duckdb::TableFunctionInitInput&,
  duckdb::GlobalTableFunctionState* global) {
  if (global->Cast<CopyFromGlobalState>().ranges) {
    return duckdb::make_uniq<RangesLocalState>();
  }
  return duckdb::make_uniq<BatchesLocalState>();
}

void ScanCopyFrom(duckdb::ClientContext& context,
                  duckdb::TableFunctionInput& input,
                  duckdb::DataChunk& output) {
  if (input.global_state->Cast<CopyFromGlobalState>().ranges) {
    ScanRanges(context, input, output);
  } else {
    ScanBatches(context, input, output);
  }
}

}  // namespace sdb::connector
