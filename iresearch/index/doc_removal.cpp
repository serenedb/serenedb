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
#include <duckdb/common/vector/unified_vector_format.hpp>
#include <duckdb/storage/statistics/numeric_stats.hpp>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/file_names.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"

namespace irs {

DocRemoval DocRemoval::Build(field_id key_column, std::vector<Row> rows) {
  for (auto& row : rows) {
    if (row.segment == kNoSegment) {
      row.doc = 0;
    }
  }
  absl::c_sort(rows, [](const Row& lhs, const Row& rhs) {
    if (lhs.segment != rhs.segment) {
      return lhs.segment < rhs.segment;
    }
    if (lhs.doc != rhs.doc) {
      return lhs.doc < rhs.doc;
    }
    return lhs.key < rhs.key;
  });
  rows.erase(std::unique(rows.begin(), rows.end(),
                         [](const Row& lhs, const Row& rhs) {
                           return lhs.segment == rhs.segment &&
                                  lhs.doc == rhs.doc && lhs.key == rhs.key;
                         }),
             rows.end());

  DocRemoval removal{.key_column = key_column};
  removal.docs.reserve(rows.size());
  removal.keys.reserve(rows.size());
  for (const auto& row : rows) {
    if (removal.segments.empty() || removal.segments.back() != row.segment) {
      removal.segments.emplace_back(row.segment);
      removal.offsets.emplace_back(static_cast<uint32_t>(removal.docs.size()));
    }
    removal.docs.emplace_back(row.doc);
    removal.keys.emplace_back(row.key);
  }
  removal.offsets.emplace_back(static_cast<uint32_t>(removal.docs.size()));
  return removal;
}

size_t DocRemoval::Find(uint64_t segment) const noexcept {
  const auto it = absl::c_lower_bound(segments, segment);
  if (it == segments.end() || *it != segment) {
    return segments.size();
  }
  return static_cast<size_t>(it - segments.begin());
}

void DocRemovalResolver::Prepare(std::span<const DocRemoval* const> removals,
                                 std::span<const SubReader* const> live) {
  _fallback.clear();
  _lookups.clear();
  std::vector<uint64_t> numbers;
  numbers.reserve(live.size());
  for (const auto* segment : live) {
    if (const auto number = SegmentNumber(segment->Meta().name)) {
      numbers.emplace_back(*number);
    }
  }
  absl::c_sort(numbers);

  for (const auto* removal : removals) {
    std::vector<int64_t> keys;
    for (size_t group = 0; group < removal->segments.size(); ++group) {
      const auto segment = removal->segments[group];
      if (segment != DocRemoval::kNoSegment &&
          std::binary_search(numbers.begin(), numbers.end(), segment)) {
        continue;
      }
      const auto group_keys = removal->Keys(group);
      keys.insert(keys.end(), group_keys.begin(), group_keys.end());
    }
    if (keys.empty()) {
      continue;
    }
    absl::c_sort(keys);
    keys.erase(std::unique(keys.begin(), keys.end()), keys.end());
    auto* lookup = LookupOf(removal->key_column);
    if (lookup == nullptr) {
      lookup = &_lookups.emplace_back(Lookup{.column = removal->key_column});
    }
    lookup->keys.insert(lookup->keys.end(), keys.begin(), keys.end());
    _fallback.insert_or_assign(removal, std::move(keys));
  }

  for (auto& lookup : _lookups) {
    absl::c_sort(lookup.keys);
    lookup.keys.erase(std::unique(lookup.keys.begin(), lookup.keys.end()),
                      lookup.keys.end());
  }
}

std::span<const doc_id_t> DocRemovalResolver::PositionalDocs(
  const DocRemoval& removal, const SubReader& segment) const {
  const auto number = SegmentNumber(segment.Meta().name);
  if (!number || *number == DocRemoval::kNoSegment) {
    return {};
  }
  const auto group = removal.Find(*number);
  if (group == removal.segments.size()) {
    return {};
  }
  const auto docs = removal.Docs(group);
  const auto end = doc_limits::min() + segment.docs_count();
  return {docs.begin(), std::lower_bound(docs.begin(), docs.end(), end)};
}

std::span<const doc_id_t> DocRemovalResolver::Docs(const DocRemoval& removal,
                                                   const SubReader& segment) {
  const auto positional = PositionalDocs(removal, segment);
  const auto fallback = _fallback.find(&removal);
  if (fallback == _fallback.end()) {
    return positional;
  }
  auto* lookup = LookupOf(removal.key_column);
  SDB_ASSERT(lookup != nullptr);
  const auto hits = Resolve(*lookup, segment);
  if (hits.empty()) {
    return positional;
  }
  _scratch.clear();
  auto hit = hits.begin();
  for (const auto key : fallback->second) {
    hit = std::lower_bound(hit, hits.end(), Hit{key, 0});
    for (; hit != hits.end() && hit->key == key; ++hit) {
      _scratch.emplace_back(hit->doc);
    }
    if (hit == hits.end()) {
      break;
    }
  }
  if (_scratch.empty()) {
    return positional;
  }
  absl::c_sort(_scratch);
  const auto keyed = _scratch.size();
  _scratch.insert(_scratch.end(), positional.begin(), positional.end());
  std::inplace_merge(_scratch.begin(), _scratch.begin() + keyed,
                     _scratch.end());
  _scratch.erase(std::unique(_scratch.begin(), _scratch.end()), _scratch.end());
  return _scratch;
}

DocRemovalResolver::Lookup* DocRemovalResolver::LookupOf(field_id column) {
  const auto it = absl::c_find_if(
    _lookups, [&](const Lookup& lookup) { return lookup.column == column; });
  return it == _lookups.end() ? nullptr : &*it;
}

std::span<const DocRemovalResolver::Hit> DocRemovalResolver::Resolve(
  Lookup& lookup, const SubReader& segment) {
  const auto [it, inserted] =
    lookup.hits.try_emplace(std::string{segment.Meta().name});
  if (inserted) {
    Scan(lookup, segment, it->second);
    absl::c_sort(it->second);
  }
  return it->second;
}

void DocRemovalResolver::Scan(const Lookup& lookup, const SubReader& segment,
                              std::vector<Hit>& hits) {
  const auto* columns = segment.GetColReader();
  const auto* column = columns ? columns->Column(lookup.column) : nullptr;
  if (!column || column->RowCount() == 0) {
    return;
  }
  const std::span<const int64_t> all{lookup.keys};
  const auto overlaps = [&](const duckdb::BaseStatistics& stats) {
    if (!duckdb::NumericStats::HasMinMax(stats)) {
      return all;
    }
    const auto first = std::lower_bound(
      all.begin(), all.end(), duckdb::NumericStats::GetMin<int64_t>(stats));
    const auto last = std::upper_bound(
      first, all.end(), duckdb::NumericStats::GetMax<int64_t>(stats));
    return std::span<const int64_t>{first, last};
  };
  if (overlaps(column->MergedStatistics()).empty()) {
    return;
  }
  ReadContext ctx{*columns};
  auto state = column->InitScan(ctx);
  ColumnReader::VectorScratch scratch{column->Type()};
  const auto blocks = column->DataBlocks();
  const uint64_t total = column->RowCount();
  uint64_t row = 0;
  for (size_t b = 0; b < blocks.size() && row < total; ++b) {
    const uint64_t end =
      b + 1 < blocks.size() ? column->DataBlockFirstRow(b + 1) : total;
    const auto keys = overlaps(blocks[b].statistics);
    if (keys.empty()) {
      column->Skip(state, end - row);
      row = end;
      continue;
    }
    auto cursor = keys.begin();
    while (row < end) {
      const auto take = std::min<uint64_t>(end - row, STANDARD_VECTOR_SIZE);
      auto& values = scratch.Reset();
      column->Scan(state, values, take);
      duckdb::UnifiedVectorFormat format;
      values.ToUnifiedFormat(take, format);
      const auto* data = duckdb::UnifiedVectorFormat::GetData<int64_t>(format);
      for (duckdb::idx_t i = 0; i < take; ++i) {
        const auto idx = format.sel->get_index(i);
        if (!format.validity.RowIsValid(idx)) {
          continue;
        }
        const auto value = data[idx];
        if (value < keys.front() || value > keys.back()) {
          continue;
        }
        if (cursor == keys.end() || *cursor > value) {
          if (cursor == keys.begin() || *std::prev(cursor) < value) {
            continue;
          }
          cursor = std::lower_bound(keys.begin(), cursor, value);
        } else if (*cursor < value) {
          ++cursor;
          if (cursor != keys.end() && *cursor < value) {
            cursor = std::lower_bound(cursor, keys.end(), value);
          }
        }
        if (cursor == keys.end() || *cursor != value) {
          continue;
        }
        hits.emplace_back(value,
                          static_cast<doc_id_t>(row + i) + doc_limits::min());
      }
      row += take;
    }
  }
}

}  // namespace irs
