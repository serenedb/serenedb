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
#include "iresearch/index/index_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/debugging.hpp"
#include "iresearch/utils/log.hpp"
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
      column.reader = _cs != nullptr ? _cs->Column(key.id) : nullptr;
    }
  }

  void Read(duckdb::idx_t begin, duckdb::idx_t count,
            duckdb::DataChunk& keys) override {
    for (size_t c = 0; c != _columns.size(); ++c) {
      auto& column = *_columns[c];
      auto& out = keys.data[c];
      if (column.reader == nullptr || begin >= column.reader->RowCount()) {
        out.Reference(duckdb::Value{column.type}, duckdb::count_t{count});
        continue;
      }
      if (count == 1) {
        if (!column.point) {
          column.point =
            std::make_unique<ColumnReader::PointReader>(*_cs, *column.reader);
        }
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
      : type{type}, scratch{type}, value{type, 1} {}

    duckdb::LogicalType type;
    const ColumnReader* reader = nullptr;
    ColumnReader::ScanState state;
    ColumnReader::VectorScratch scratch;
    std::unique_ptr<ColumnReader::PointReader> point;
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

  duckdb::idx_t Count() const override { return _reader.docs_count(); }

  duckdb::unique_ptr<duckdb::PresortedMergeReader> CreateReader()
    const override {
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

  bool Incomplete() const {
    SDB_ERROR(IRESEARCH, "The order of a sorted merge of ", doc_map.size(),
              " segments does not hold every live document once");
    return false;
  }

  bool Unsorted(const duckdb::optional_idx& input) const {
    if (input.IsValid()) {
      SDB_ERROR(IRESEARCH, "Segment '", Reader(input.GetIndex()).Meta().name,
                "' of a sorted merge is not sorted by the merge key");
    }
    return false;
  }

  void PlanParts() {
    const auto count = static_cast<size_t>(merge->PartitionCount());
    std::vector<uint64_t> live_in(count);
    for (size_t i = 0; i != doc_map.size(); ++i) {
      for (size_t p = 0; p != count; ++p) {
        live_in[p] +=
          merge->PartitionBegin(p + 1, i) - merge->PartitionBegin(p, i);
      }
      auto mask = Reader(i).MaskedDocs();
      size_t p = 0;
      for (auto doc = mask.Seek(doc_limits::min()); !doc_limits::eof(doc);
           doc = mask.Seek(doc + 1)) {
        while (doc - doc_limits::min() >= merge->PartitionBegin(p + 1, i)) {
          ++p;
        }
        --live_in[p];
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
      auto& id_map = doc_map[i].remap.id_map;
      auto mask = reader.MaskedDocs();
      isolated[i] = next - doc_limits::min();
      const auto end =
        static_cast<doc_id_t>(reader.docs_count() + doc_limits::min());
      for (auto doc = doc_limits::min(); doc != end; ++doc) {
        if (!mask.Contains(doc)) {
          order->Set(next - doc_limits::min(), i);
          id_map[doc] = next++;
        }
      }
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

bool KeyOrder::Prepare(size_t partitions) {
  auto& s = *_state;
  duckdb::vector<duckdb::LogicalType> types;
  duckdb::vector<duckdb::OrderModifiers> modifiers;
  for (const auto& key : s.spec.keys) {
    for (const auto& input : s.doc_map) {
      const auto* cs = input.reader->GetColReader();
      const auto* col = cs != nullptr ? cs->Column(key.column) : nullptr;
      if (col == nullptr) {
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
  try {
    for (size_t i = 0; i != s.doc_map.size(); ++i) {
      auto& input = s.doc_map[i];
      const auto& reader = *input.reader;
      s.live[i] = reader.live_docs_count();
      rows += s.live[i];
      input.remap.id_map.resize(reader.docs_count() + doc_limits::min(),
                                doc_limits::eof());
      if (HasRemovals(reader.Meta())) {
        input.remap.mask = reader.MaskedDocs();
      }
    }
    s.order.emplace(s.doc_map.size(), rows,
                    s.doc_map.get_allocator().Manager());
  } catch (...) {
    SDB_ERROR(IRESEARCH, "Failed to allocate the order of a sorted merge of ",
              s.doc_map.size(), " segments and ", rows, " documents");
    return false;
  }
  if (s.keys.empty() || rows == 0) {
    s.ConcatOrder();
    return true;
  }
  duckdb::vector<duckdb::shared_ptr<duckdb::PresortedMergeInput>> inputs;
  for (const auto& input : s.doc_map) {
    inputs.push_back(
      duckdb::make_shared_ptr<SegmentKeys>(*input.reader, s.keys));
  }
  s.merge = duckdb::make_shared_ptr<duckdb::PresortedMerge>(
    std::move(types), std::move(modifiers), std::move(inputs));
  s.merge->Partition(partitions);
  s.PlanParts();
  return true;
}

bool KeyOrder::Pending() const noexcept { return _state->merge != nullptr; }

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
    for (auto doc = begin; doc != end; ++doc) {
      if (remap.IsMasked(doc)) {
        continue;
      }
      if (remap.id_map[doc] != doc_limits::eof()) {
        part.failed = true;
        return;
      }
      s.order->Set(part.next, input);
      remap.id_map[doc] = static_cast<doc_id_t>(part.next + doc_limits::min());
      span.first = std::min(span.first, part.next);
      span.last = part.next++;
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
        return s.Unsorted(unsorted);
      }
      Expand(part, runs);
    }
  }
  return true;
}

bool KeyOrder::Finish() {
  auto& s = *_state;
  SDB_IF_FAILURE("irs::KeyOrder::Finish") { return s.Incomplete(); }
  std::vector<State::Span> spans(s.doc_map.size());
  for (const auto& part : s.parts) {
    if (part.failed || part.next != part.end) {
      return s.Incomplete();
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
  return true;
}

std::optional<uint64_t> KeyOrder::IsolatedBegin(size_t input) const noexcept {
  return _state->isolated[input];
}

const MergeOrder& KeyOrder::Order() const noexcept { return *_state->order; }

}  // namespace irs
