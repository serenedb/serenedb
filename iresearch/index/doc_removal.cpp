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

#include "iresearch/index/doc_removal.hpp"

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/common/vector/vector_iterator.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <optional>
#include <utility>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/containers/flat_hash_map.hpp"
#include "iresearch/utils/pg/sql_exception_macro.hpp"

namespace irs {
namespace {

enum class Coverage : uint8_t {
  None,
  All,
  Some,
};

std::span<const KeyRange>::iterator RangeEndingAfter(
  std::span<const KeyRange> ranges, int64_t key) {
  return absl::c_upper_bound(
    ranges, key,
    [](int64_t value, const KeyRange& range) { return value < range.end; });
}

Coverage Classify(const duckdb::BaseStatistics& stats,
                  std::span<const KeyRange> ranges) {
  if (!duckdb::NumericStats::HasMinMax(stats)) {
    return Coverage::Some;
  }
  const auto min = duckdb::NumericStats::GetMin<int64_t>(stats);
  const auto max = duckdb::NumericStats::GetMax<int64_t>(stats);
  const auto range = RangeEndingAfter(ranges, min);
  if (range == ranges.end() || range->begin > max) {
    return Coverage::None;
  }
  if (range->begin <= min && max < range->end) {
    return Coverage::All;
  }
  return Coverage::Some;
}

struct KeyColumnScan {
  KeyColumnScan(const ColReader& store, const ColumnReader& column)
    : ctx{store}, scan{column.InitScan(ctx)}, scratch{column.Type()} {}

  ReadContext ctx;
  ColumnReader::ScanState scan;
  ColumnReader::VectorScratch scratch;
};

std::vector<DocRange> CollectKeyRangeDocs(const SubReader& segment,
                                          field_id key_column,
                                          std::span<const KeyRange> ranges) {
  std::vector<DocRange> docs;
  const auto* store = segment.GetColReader();
  const auto* column = store ? store->Column(key_column) : nullptr;
  if (!column) {
    return docs;
  }
  SDB_ASSERT(column->Type().InternalType() == duckdb::PhysicalType::INT64);
  const uint64_t rows = std::min(column->RowCount(), segment.docs_count());
  const auto add = [&](uint64_t begin, uint64_t end) {
    const auto first = static_cast<doc_id_t>(doc_limits::min() + begin);
    const auto last = static_cast<doc_id_t>(doc_limits::min() + end);
    if (!docs.empty() && docs.back().end == first) {
      docs.back().end = last;
    } else {
      docs.emplace_back(first, last);
    }
  };
  auto next = ranges.begin();
  int64_t last = std::numeric_limits<int64_t>::min();
  const auto contains = [&](int64_t key) {
    if (key < last) {
      next = RangeEndingAfter(ranges, key);
    } else if (next != ranges.end() && next->end <= key) {
      next = RangeEndingAfter({next, ranges.end()}, key);
    }
    last = key;
    return next != ranges.end() && next->begin <= key;
  };
  switch (Classify(column->MergedStatistics(), ranges)) {
    case Coverage::None:
      return docs;
    case Coverage::All:
      add(0, rows);
      return docs;
    case Coverage::Some:
      break;
  }
  std::optional<KeyColumnScan> decode;
  uint64_t scanned = 0;
  BlockWindow window;
  for (uint64_t row = 0; row < rows; row = window.end) {
    window = column->Locate(row, window);
    const uint64_t end = std::min<uint64_t>(window.end, rows);
    const auto coverage =
      Classify(column->RowGroupStatistics(window.block), ranges);
    if (coverage == Coverage::All) {
      add(row, end);
    }
    if (coverage != Coverage::Some) {
      continue;
    }
    if (!decode) {
      decode.emplace(*store, *column);
    }
    if (row > scanned) {
      column->Skip(decode->scan, row - scanned);
      scanned = row;
    }
    while (scanned < end) {
      const auto count =
        std::min<uint64_t>(end - scanned, STANDARD_VECTOR_SIZE);
      auto& values = decode->scratch.Reset();
      column->Scan(decode->scan, values, count);
      values.BufferMutable().SetVectorSize(count);
      for (const auto& entry : values.ValidValues<int64_t>()) {
        if (contains(entry.GetValue())) {
          add(scanned + entry.GetIndex(), scanned + entry.GetIndex() + 1);
        }
      }
      scanned += count;
    }
  }
  return docs;
}

}  // namespace

void AppendKeyRanges(std::vector<KeyRange>& ranges,
                     std::span<const int64_t> keys) {
  for (const auto key : keys) {
    if (!ranges.empty() && ranges.back().end == key) {
      ++ranges.back().end;
    } else {
      ranges.emplace_back(key, key + 1);
    }
  }
}

void NormalizeKeyRanges(std::vector<KeyRange>& ranges) {
  const auto by_begin = [](const KeyRange& l, const KeyRange& r) {
    return l.begin < r.begin;
  };
  if (!absl::c_is_sorted(ranges, by_begin)) {
    absl::c_sort(ranges, by_begin);
  }
  size_t merged = 0;
  for (const auto& range : ranges) {
    if (merged != 0 && ranges[merged - 1].end >= range.begin) {
      ranges[merged - 1].end = std::max(ranges[merged - 1].end, range.end);
    } else {
      ranges[merged++] = range;
    }
  }
  ranges.resize(merged);
}

DocRemoval DocRemoval::Build(field_id key_column, std::span<const Row> rows,
                             std::vector<KeyRange> ranges) {
  containers::FlatHashMap<uint32_t, uint32_t> cursors;
  uint32_t* cursor = nullptr;
  uint32_t cursor_segment = kNoSegment;
  const auto cursor_of = [&](uint32_t segment) -> uint32_t& {
    SDB_ASSERT(segment != kNoSegment);
    if (cursor == nullptr || segment != cursor_segment) {
      cursor = &cursors[segment];
      cursor_segment = segment;
    }
    return *cursor;
  };
  for (const auto& row : rows) {
    ++cursor_of(row.segment);
  }

  DocRemoval removal{.key_column = key_column, .ranges = std::move(ranges)};
  removal.segments.reserve(cursors.size());
  for (const auto& [segment, count] : cursors) {
    removal.segments.emplace_back(segment);
  }
  absl::c_sort(removal.segments);
  removal.offsets.reserve(removal.segments.size() + 1);
  uint32_t offset = 0;
  for (const auto segment : removal.segments) {
    removal.offsets.emplace_back(offset);
    offset += std::exchange(cursors[segment], offset);
  }
  removal.offsets.emplace_back(offset);

  removal.docs.resize(rows.size());
  removal.keys.resize(rows.size());
  for (const auto& row : rows) {
    const auto at = cursor_of(row.segment)++;
    removal.docs[at] = row.doc;
    removal.keys[at] = row.key;
  }
  return removal;
}

uint32_t SegmentNumber(std::string_view name) noexcept {
  uint64_t id = 0;
  if (!ParseSegmentId(name, id) || id >= DocRemoval::kNoSegment) {
    return DocRemoval::kNoSegment;
  }
  return static_cast<uint32_t>(id);
}

size_t DocRemoval::Find(uint32_t segment) const noexcept {
  const auto it = absl::c_lower_bound(segments, segment);
  if (it == segments.end() || *it != segment) {
    return segments.size();
  }
  return static_cast<size_t>(it - segments.begin());
}

void DocRemovalResolver::Prepare(std::span<const DocRemoval* const> removals,
                                 std::span<const SubReader* const> live) {
  std::vector<uint32_t> numbers;
  numbers.reserve(live.size());
  for (const auto* segment : live) {
    if (const auto number = SegmentNumber(segment->Meta().name);
        number != DocRemoval::kNoSegment) {
      numbers.emplace_back(number);
    }
  }
  absl::c_sort(numbers);

  for (const auto* removal : removals) {
    bool keyed = !removal->ranges.empty();
    for (size_t group = 0; group < removal->segments.size(); ++group) {
      if (!absl::c_binary_search(numbers, removal->segments[group])) {
        AppendKeyRanges(_ranges, removal->Keys(group));
        keyed = true;
      }
    }
    if (!keyed) {
      continue;
    }
    SDB_ENSURE(_keyed.empty() || removal->key_column == _key_column,
               "removals of one commit resolve keys in columns ", _key_column,
               " and ", removal->key_column);
    _key_column = removal->key_column;
    _ranges.insert(_ranges.end(), removal->ranges.begin(),
                   removal->ranges.end());
    _keyed.insert(removal);
  }
  NormalizeKeyRanges(_ranges);
}

std::span<const doc_id_t> DocRemovalResolver::PositionalDocs(
  const DocRemoval& removal, const SubReader& segment) const {
  const auto number = SegmentNumber(segment.Meta().name);
  if (number == DocRemoval::kNoSegment) {
    return {};
  }
  const auto group = removal.Find(number);
  if (group == removal.segments.size()) {
    return {};
  }
  return removal.Docs(group);
}

std::vector<DocRange> DocRemovalResolver::KeyedDocs(
  const SubReader& segment) const {
  return CollectKeyRangeDocs(segment, _key_column, _ranges);
}

}  // namespace irs
