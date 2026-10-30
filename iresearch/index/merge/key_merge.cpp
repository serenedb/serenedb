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
#include <duckdb/common/enums/order_type.hpp>
#include <duckdb/common/fault_points.hpp>
#include <duckdb/common/types/data_chunk.hpp>
#include <duckdb/common/types/value.hpp>
#include <duckdb/common/types/vector.hpp>
#include <duckdb/common/vector/flat_vector.hpp>
#include <duckdb/common/vector_operations/vector_operations.hpp>
#include <duckdb/function/create_sort_key.hpp>
#include <limits>

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

struct KeyColumn {
  field_id id;
  duckdb::LogicalType type;
};

class SegmentKeyReader final : public duckdb::PresortedMergeReader {
 public:
  SegmentKeyReader(const SubReader& reader, std::span<const KeyColumn> keys)
    : _cs{reader.GetColReader()} {
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
        }
        column.value.ResetFromCache(column.value_cache);
        column.point->FetchRow(begin, column.value, 0);
        duckdb::FlatVector::SetSize(column.value, 1);
        out.Reference(column.value);
        continue;
      }
      if (!column.scanning) {
        column.state =
          column.reader->InitScan(std::make_shared<ReadContext>(*_cs));
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
  std::vector<std::unique_ptr<Column>> _columns;
};

class SegmentKeys final : public duckdb::PresortedMergeInput {
 public:
  SegmentKeys(const SubReader& reader, std::span<const KeyColumn> keys)
    : _reader{reader}, _keys{keys} {}

  duckdb::idx_t Count() const final { return _reader.docs_count(); }

  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader() const final {
    return duckdb::make_uniq<SegmentKeyReader>(_reader, _keys);
  }

 private:
  const SubReader& _reader;
  std::span<const KeyColumn> _keys;
};

}  // namespace

struct KeyOrder::State {
  State(MergeDocMap& doc_map, const SortSpec& spec)
    : doc_map{doc_map}, spec{spec} {}

  const SubReader& Reader(size_t input) const { return *doc_map[input].reader; }

  [[noreturn]] void ThrowIncomplete() const {
    THROW_SQL_ERROR(
      ERR_CODE(ERRCODE_INTERNAL_ERROR),
      ERR_MSG("the order of a sorted merge of ", doc_map.size(),
              " segments does not hold every live document once"));
  }

  void ThrowIfUnsorted(const duckdb::optional_idx& input) const {
    if (input.IsValid()) {
      THROW_SQL_ERROR(
        ERR_CODE(ERRCODE_INDEX_CORRUPTED),
        ERR_MSG("segment '", Reader(input.GetIndex()).Meta().name,
                "' of a sorted merge is not sorted by the merge key"));
    }
  }

  void PlanParts() {
    const auto count = static_cast<size_t>(merge->PartitionCount());
    std::vector<uint64_t> live_in(count);
    for (size_t i = 0; i != doc_map.size(); ++i) {
      const auto mask = MakeGenericDocsMask(Reader(i));
      for (size_t p = 0; p != count; ++p) {
        const auto first = merge->PartitionBegin(p, i);
        const auto end = merge->PartitionBegin(p + 1, i);
        live_in[p] +=
          end - first -
          mask.CountIn(static_cast<doc_id_t>(first + doc_limits::min()),
                       static_cast<doc_id_t>(end + doc_limits::min()));
      }
    }
    parts.resize(count);
    uint64_t next = 0;
    for (size_t p = 0; p != count; ++p) {
      parts[p].next = next;
      next += live_in[p];
      parts[p].end = next;
      parts[p].spans.resize(doc_map.size());
    }
  }

  void ConcatOrder() {
    doc_id_t next = doc_limits::min();
    for (size_t i = 0; i != doc_map.size(); ++i) {
      const auto& reader = Reader(i);
      auto* const id_map = doc_map[i].remap.id_map.data();
      auto mask = MakeGenericDocsMask(reader);
      isolated[i] = next - doc_limits::min();
      VisitLive(mask, doc_limits::min(), LiveEnd(reader),
                [&](doc_id_t window, std::span<const uint32_t> live) {
                  for (const auto doc : live) {
                    order->Set(next - doc_limits::min(), i);
                    id_map[window + doc] = next++;
                  }
                  return true;
                });
    }
    order->Finish();
  }

  struct Span {
    uint64_t first = std::numeric_limits<uint64_t>::max();
    uint64_t last = 0;
  };

  struct Part {
    uint64_t next = 0;
    uint64_t end = 0;
    std::vector<Span> spans;
    bool failed = false;
  };

  MergeDocMap& doc_map;
  const SortSpec& spec;
  std::vector<KeyColumn> keys;
  std::vector<uint64_t> live;
  std::vector<std::optional<uint64_t>> isolated;
  std::optional<MergeOrder> order;
  duckdb::shared_ptr<duckdb::PresortedMerge> merge;
  std::vector<Part> parts;
};

KeyOrder::KeyOrder(MergeDocMap& doc_map, const SortSpec& spec)
  : _state{std::make_unique<State>(doc_map, spec)} {}

KeyOrder::~KeyOrder() = default;

void KeyOrder::Prepare(size_t partitions, duckdb::TaskScheduler* scheduler,
                       size_t threads) {
  auto& s = *_state;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
  for (const auto& key : s.spec.keys) {
    for (const auto& input : s.doc_map) {
      const auto* cs = input.reader->GetColReader();
      const auto* col = cs ? cs->Column(key.column) : nullptr;
      if (!col) {
        continue;
      }
      s.keys.push_back({.id = key.column, .type = col->Type()});
      types.push_back(col->Type());
      modifiers.emplace_back(key.descending ? duckdb::OrderType::DESCENDING
                                            : duckdb::OrderType::ASCENDING,
                             key.nulls_first
                               ? duckdb::OrderByNullType::NULLS_FIRST
                               : duckdb::OrderByNullType::NULLS_LAST);
      break;
    }
  }
  uint64_t rows = 0;
  s.live.resize(s.doc_map.size());
  s.isolated.resize(s.doc_map.size());
  for (size_t i = 0; i != s.doc_map.size(); ++i) {
    auto& input = s.doc_map[i];
    const auto& reader = *input.reader;
    s.live[i] = reader.live_docs_count();
    rows += s.live[i];
    input.remap.id_map.resize(reader.docs_count() + doc_limits::min(),
                              doc_limits::eof());
  }
  s.order.emplace(s.doc_map.size(), rows, s.doc_map.get_allocator().Manager());
  if (s.keys.empty() || rows == 0) {
    s.ConcatOrder();
    return;
  }
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> inputs;
  for (const auto& input : s.doc_map) {
    inputs.push_back(
      duckdb::make_shared_ptr<SegmentKeys>(*input.reader, s.keys));
  }
  s.merge = duckdb::make_shared_ptr<duckdb::PresortedMerge>(
    std::move(types), std::move(modifiers), std::move(inputs));
  if (scheduler) {
    s.merge->Partition(partitions, false, *scheduler, threads);
  } else {
    s.merge->Partition(partitions);
  }
  s.PlanParts();
}

bool KeyOrder::Pending() const noexcept {
  return static_cast<bool>(_state->merge);
}

duckdb::shared_ptr<duckdb::PresortedMerge> KeyOrder::KeyMerge() const noexcept {
  return _state->merge;
}

void KeyOrder::Expand(size_t partition,
                      std::span<const duckdb::PresortedRun> runs) {
  auto& s = *_state;
  SDB_ASSERT(partition < s.parts.size());
  auto& part = s.parts[partition];
  for (const auto& run : runs) {
    if (run.input >= s.doc_map.size()) {
      part.failed = true;
      return;
    }
    const auto input = static_cast<size_t>(run.input);
    const auto first_row = s.merge->PartitionBegin(partition, input);
    const auto end_row = s.merge->PartitionBegin(partition + 1, input);
    if (run.begin < first_row || run.begin > end_row ||
        run.count > end_row - run.begin) {
      part.failed = true;
      return;
    }
    auto& remap = s.doc_map[input].remap;
    auto& span = part.spans[input];
    const auto begin = static_cast<doc_id_t>(run.begin + doc_limits::min());
    const auto end = static_cast<doc_id_t>(begin + run.count);
    auto mask = MakeGenericDocsMask(*s.doc_map[input].reader);
    const bool expanded = VisitLive(
      mask, begin, end, [&](doc_id_t window, std::span<const uint32_t> live) {
        for (const auto offset : live) {
          const auto doc = static_cast<doc_id_t>(window + offset);
          if (remap.id_map[doc] != doc_limits::eof()) {
            return false;
          }
          s.order->Set(part.next, input);
          remap.id_map[doc] =
            static_cast<doc_id_t>(part.next + doc_limits::min());
          span.first = std::min(span.first, part.next);
          span.last = part.next++;
        }
        return true;
      });
    if (!expanded) {
      part.failed = true;
      return;
    }
  }
}

bool KeyOrder::Merge(const std::function<bool()>& proceed) {
  auto& s = *_state;
  SDB_ASSERT(s.merge);
  duckdb::vector<duckdb::PresortedRun> runs;
  for (size_t part = 0; part != s.parts.size(); ++part) {
    const auto state = s.merge->BeginMerge(part);
    while (!state->Done()) {
      runs.clear();
      duckdb::optional_idx unsorted;
      if (!s.merge->Merge(*state, runs, STANDARD_VECTOR_SIZE, proceed,
                          unsorted)) {
        s.ThrowIfUnsorted(unsorted);
        return false;
      }
      Expand(part, runs);
    }
  }
  return true;
}

void KeyOrder::Finish() {
  auto& s = *_state;
  SDB_IF_FAILURE("irs::KeyOrder::Finish") { s.ThrowIncomplete(); }
  std::vector<State::Span> spans(s.doc_map.size());
  for (const auto& part : s.parts) {
    if (part.failed || part.next != part.end) {
      s.ThrowIncomplete();
    }
    for (size_t i = 0; i != spans.size(); ++i) {
      spans[i].first = std::min(spans[i].first, part.spans[i].first);
      spans[i].last = std::max(spans[i].last, part.spans[i].last);
    }
  }
  for (size_t i = 0; i != spans.size(); ++i) {
    if (spans[i].first <= spans[i].last &&
        spans[i].last - spans[i].first + 1 == s.live[i]) {
      s.isolated[i] = spans[i].first;
    }
  }
  s.merge.reset();
  s.parts = {};
  s.order->Finish();
}

std::optional<uint64_t> KeyOrder::IsolatedBegin(size_t input) const noexcept {
  return _state->isolated[input];
}

const MergeOrder& KeyOrder::Order() const noexcept { return *_state->order; }

}  // namespace irs
