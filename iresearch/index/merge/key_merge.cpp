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

#include "iresearch/index/merge/key_merge.hpp"

#include <algorithm>
#include <atomic>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <duckdb/parallel/task_executor.hpp>
#include <memory>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/docs_mask/docs_mask.hpp"
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/log.hpp"
#include "iresearch/utils/pg/errcodes.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {
namespace {

using KeyColumn = KeyOrder::KeyColumn;

class SegmentKeyReader final : public duckdb::PresortedMergeReader {
 public:
  SegmentKeyReader(const SubReader& reader, std::span<const KeyColumn> keys,
                   duckdb::ObjectCache* cache)
    : _cs{reader.GetColReader()}, _cache{cache} {
    for (const auto& key : keys) {
      auto& column = *_columns.emplace_back(std::make_unique<Column>(key.type));
      column.reader = _cs ? _cs->Column(key.id) : nullptr;
    }
  }

  void Read(duckdb::idx_t begin, duckdb::idx_t count,
            duckdb::DataChunk& keys) final {
    for (size_t c = 0; c != _columns.size(); ++c) {
      auto& column = *_columns[c];
      auto& out = keys.data[c];
      if (!column.reader || begin >= column.reader->RowCount()) {
        out.Reference(duckdb::Value{column.type}, duckdb::count_t{count});
        continue;
      }
      if (count == 1) {
        if (!column.point) {
          column.point =
            std::make_unique<ColumnReader::PointReader>(*_cs, *column.reader);
          column.point->SetCache(_cache);
        }
        column.value.ResetFromCache(column.value_cache);
        column.point->FetchRow(begin, column.value, 0);
        duckdb::FlatVector::SetSize(column.value, 1);
        out.Reference(column.value);
        continue;
      }
      if (!column.scanning) {
        auto ctx = std::make_shared<ReadContext>(*_cs);
        ctx->SetMergeRead(true);
        ctx->SetCache(_cache);
        ctx->SetUnmap(ReadOnceUnmap(column.type));
        column.state = column.reader->InitScan(std::move(ctx));
        column.scanning = true;
      }
      SDB_ASSERT(begin >= column.position);
      if (begin > column.position) {
        column.reader->Skip(column.state, begin - column.position);
      }
      const auto stored =
        std::min<uint64_t>(count, column.reader->RowCount() - begin);
      auto& batch = column.scratch.Reset();
      column.reader->Scan(column.state, batch, stored);
      column.position = begin + stored;
      if (stored == count) {
        out.Reference(batch);
        continue;
      }
      duckdb::Vector padded{column.type, count};
      duckdb::VectorOperations::Copy(batch, padded, stored, 0, 0);
      duckdb::FlatVector::SetSize(padded, count);
      for (auto row = stored; row != count; ++row) {
        duckdb::FlatVector::SetNull(padded, row, true);
      }
      out.Reference(padded);
    }
    keys.SetCardinality(count);
  }

 private:
  struct Column {
    explicit Column(const duckdb::LogicalType& type)
      : type{type},
        scratch{type},
        value_cache{duckdb::Allocator::DefaultAllocator(), type, 1},
        value{value_cache} {}

    duckdb::LogicalType type;
    const ColumnReader* reader = nullptr;
    ColumnReader::ScanState state;
    ColumnReader::VectorScratch scratch;
    std::unique_ptr<ColumnReader::PointReader> point;
    duckdb::VectorCache value_cache;
    duckdb::Vector value;
    uint64_t position = 0;
    bool scanning = false;
  };

  const ColReader* _cs;
  duckdb::ObjectCache* _cache;
  std::vector<std::unique_ptr<Column>> _columns;
};

class SegmentKeys final : public duckdb::PresortedMergeInput {
 public:
  SegmentKeys(const SubReader& reader, std::span<const KeyColumn> keys,
              duckdb::ObjectCache* cache)
    : _reader{reader}, _keys{keys}, _cache{cache} {}

  duckdb::idx_t Count() const final { return _reader.docs_count(); }

  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader() const final {
    return duckdb::make_uniq<SegmentKeyReader>(_reader, _keys, _cache);
  }

 private:
  const SubReader& _reader;
  std::span<const KeyColumn> _keys;
  duckdb::ObjectCache* _cache;
};

template<typename Work>
class PartitionTask final : public duckdb::BaseExecutorTask {
 public:
  PartitionTask(duckdb::TaskExecutor& executor, std::atomic_size_t& next,
                size_t count, const Work& work) noexcept
    : BaseExecutorTask{executor}, _next{&next}, _count{count}, _work{&work} {}

  void ExecuteTask() final {
    for (auto partition = _next->fetch_add(1, std::memory_order_relaxed);
         partition < _count && !executor.HasError();
         partition = _next->fetch_add(1, std::memory_order_relaxed)) {
      (*_work)(partition);
    }
  }

 private:
  std::atomic_size_t* _next;
  size_t _count;
  const Work* _work;
};

template<typename Work>
void ForEachPartition(size_t count, duckdb::TaskScheduler* scheduler,
                      size_t threads, const Work& work) {
  const auto tasks = std::min(threads, count);
  if (!scheduler || tasks < 2) {
    for (size_t partition = 0; partition != count; ++partition) {
      work(partition);
    }
    return;
  }
  std::atomic_size_t next{0};
  duckdb::TaskExecutor executor{
    *scheduler, scheduler->CreateAccount(duckdb::SchedulerClass::FOREGROUND)};
  for (size_t task = 0; task != tasks; ++task) {
    executor.ScheduleTask(
      duckdb::make_uniq<PartitionTask<Work>>(executor, next, count, work));
  }
  executor.WorkOnTasks();
}

}  // namespace

void KeyOrder::PlanParts(const duckdb::PresortedMerge& merge) {
  const auto count = static_cast<size_t>(merge.PartitionCount());
  std::vector<uint64_t> live_in(count);
  for (size_t i = 0; i != _doc_map.size(); ++i) {
    const auto mask = MakeGenericDocsMask(Reader(i));
    for (size_t p = 0; p != count; ++p) {
      const auto first = merge.PartitionBegin(p, i);
      const auto end = merge.PartitionBegin(p + 1, i);
      live_in[p] +=
        end - first -
        mask.CountIn(static_cast<doc_id_t>(first + doc_limits::min()),
                     static_cast<doc_id_t>(end + doc_limits::min()));
    }
  }
  _parts.resize(count);
  uint64_t next = 0;
  for (size_t p = 0; p != count; ++p) {
    _parts[p].next = next;
    next += live_in[p];
    _parts[p].end = next;
    _parts[p].spans.resize(_doc_map.size());
  }
}

void KeyOrder::ConcatOrder() {
  doc_id_t next = doc_limits::min();
  for (size_t i = 0; i != _doc_map.size(); ++i) {
    const auto& reader = Reader(i);
    auto* const id_map = _doc_map[i].remap.id_map.data();
    auto mask = MakeGenericDocsMask(reader);
    _isolated[i] = next - doc_limits::min();
    VisitLive(mask, doc_limits::min(), LiveEnd(reader),
              [&](doc_id_t window, std::span<const uint32_t> live) {
                for (const auto doc : live) {
                  _order->Set(next - doc_limits::min(), i);
                  id_map[window + doc] = next++;
                }
                return true;
              });
  }
  _order->Finish();
}

bool KeyOrder::Prepare(size_t partitions, const MergeProgress& progress,
                       duckdb::TaskScheduler* scheduler, size_t threads) {
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
  for (const auto& key : _spec.keys) {
    for (const auto& input : _doc_map) {
      const auto* cs = input.reader->GetColReader();
      const auto* col = cs ? cs->Column(key.column) : nullptr;
      if (!col) {
        continue;
      }
      _keys.push_back({.id = key.column, .type = col->Type()});
      types.push_back(col->Type());
      modifiers.emplace_back(key.order, key.null_order);
      break;
    }
  }
  uint64_t rows = 0;
  _live.resize(_doc_map.size());
  _isolated.assign(_doc_map.size(), kNotIsolated);
  for (size_t i = 0; i != _doc_map.size(); ++i) {
    auto& input = _doc_map[i];
    const auto& reader = *input.reader;
    _live[i] = reader.live_docs_count();
    rows += _live[i];
    input.remap.id_map.resize(reader.docs_count() + doc_limits::min(),
                              doc_limits::eof());
  }
  _order.emplace(_doc_map.size(), rows, _doc_map.get_allocator().Manager());
  if (_keys.empty() || rows == 0) {
    ConcatOrder();
    return true;
  }
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> inputs;
  for (const auto& input : _doc_map) {
    inputs.push_back(
      duckdb::make_shared_ptr<SegmentKeys>(*input.reader, _keys, _cache));
  }
  duckdb::PresortedMerge merge{std::move(types), std::move(modifiers),
                               std::move(inputs)};
  merge.Partition(partitions, false, scheduler, threads);
  PlanParts(merge);
  std::atomic_bool stopped{false};
  ForEachPartition(merge.PartitionCount(), scheduler, threads,
                   [&](size_t partition) {
                     if (!Expand(merge, partition, progress)) {
                       stopped.store(true, std::memory_order_relaxed);
                     }
                   });
  if (stopped.load(std::memory_order_relaxed)) {
    return false;
  }
  Finish();
  return true;
}

bool KeyOrder::Expand(const duckdb::PresortedMerge& merge, size_t partition,
                      const MergeProgress& progress) {
  const auto state = merge.BeginMerge(partition);
  duckdb::vector<duckdb::PresortedRun> runs;
  while (!state->Done()) {
    runs.clear();
    duckdb::optional_idx unsorted;
    if (!merge.Merge(*state, runs, STANDARD_VECTOR_SIZE, progress, unsorted)) {
      if (unsorted.IsValid()) {
        THROW_SQL_ERROR(
          ERR_CODE(ERRCODE_INDEX_CORRUPTED),
          ERR_MSG("segment ", Reader(unsorted.GetIndex()).Meta().name,
                  " is not sorted by the sort key of its merge"));
      }
      return false;
    }
    Place(merge, partition, runs);
    if (!progress()) {
      return false;
    }
  }
  return true;
}

void KeyOrder::Place(const duckdb::PresortedMerge& merge, size_t partition,
                     std::span<const duckdb::PresortedRun> runs) {
  SDB_ASSERT(partition < _parts.size());
  auto& part = _parts[partition];
  for (const auto& run : runs) {
    SDB_ASSERT(run.input < _doc_map.size());
    const auto input = static_cast<size_t>(run.input);
    SDB_ASSERT(merge.PartitionBegin(partition, input) <= run.begin);
    SDB_ASSERT(run.begin + run.count <=
               merge.PartitionBegin(partition + 1, input));
    auto& remap = _doc_map[input].remap;
    auto& span = part.spans[input];
    const auto begin = static_cast<doc_id_t>(run.begin + doc_limits::min());
    const auto end = static_cast<doc_id_t>(begin + run.count);
    auto mask = MakeGenericDocsMask(*_doc_map[input].reader);
    VisitLive(mask, begin, end,
              [&](doc_id_t window, std::span<const uint32_t> live) {
                for (const auto offset : live) {
                  const auto doc = static_cast<doc_id_t>(window + offset);
                  SDB_ASSERT(remap.id_map[doc] == doc_limits::eof());
                  _order->Set(part.next, input);
                  remap.id_map[doc] =
                    static_cast<doc_id_t>(part.next + doc_limits::min());
                  span.first = std::min(span.first, part.next);
                  span.last = part.next++;
                }
                return true;
              });
  }
}

void KeyOrder::Finish() {
  SDB_IF_FAILURE("irs::KeyOrder::Finish") {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("the order of a sorted merge of ", _doc_map.size(),
              " segments does not hold every live document once"));
  }
  std::vector<Span> spans(_doc_map.size());
  for (const auto& part : _parts) {
    SDB_ASSERT(part.next == part.end);
    for (size_t i = 0; i != spans.size(); ++i) {
      spans[i].first = std::min(spans[i].first, part.spans[i].first);
      spans[i].last = std::max(spans[i].last, part.spans[i].last);
    }
  }
  for (size_t i = 0; i != spans.size(); ++i) {
    if (spans[i].first <= spans[i].last &&
        spans[i].last - spans[i].first + 1 == _live[i]) {
      _isolated[i] = spans[i].first;
    }
  }
  _parts = {};
  _order->Finish();
}

}  // namespace irs
