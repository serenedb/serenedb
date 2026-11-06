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

#pragma once

#include <absl/algorithm/container.h>

#include <algorithm>
#include <duckdb/storage/statistics/numeric_stats.hpp>
#include <span>
#include <vector>

#include "iresearch/formats/column/col_reader.hpp"
#include "iresearch/formats/column/column_reader.hpp"
#include "iresearch/formats/column/read_context.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs {

void SortUnique(std::vector<int64_t>& keys);

std::span<const int64_t> KeysWithin(const duckdb::BaseStatistics& stats,
                                    std::span<const int64_t> keys);

// Hands `remove` every document of `segment` whose key column holds one of
// `keys`, which must be sorted. Block statistics decide what is read, so a key
// set that falls outside a block costs nothing; a removal by key and a probe
// for one therefore agree on which documents a key names, at the same price.
void RemoveKeys(std::span<const int64_t> keys, field_id key_column,
                const SubReader& segment, const auto& remove) {
  const auto* columns = segment.GetColReader();
  const auto* column = columns ? columns->Column(key_column) : nullptr;
  if (!column) {
    return;
  }
  SDB_ASSERT(column->Type().InternalType() == duckdb::PhysicalType::INT64);
  const auto candidates = KeysWithin(column->MergedStatistics(), keys);
  if (candidates.empty()) {
    return;
  }
  ReadContext ctx{*columns};
  auto state = column->InitScan(ctx);
  ColumnReader::VectorScratch scratch{column->Type()};
  uint64_t cursor = 0;
  const auto blocks = column->DataBlocks();
  for (size_t block = 0; block != blocks.size(); ++block) {
    const auto matching = KeysWithin(blocks[block].statistics, candidates);
    if (matching.empty()) {
      continue;
    }
    if (const auto first = column->DataBlockFirstRow(block); first != cursor) {
      column->Skip(state, first - cursor);
      cursor = first;
    }
    for (auto left = blocks[block].tuple_count; left != 0;) {
      const auto take = std::min<uint64_t>(left, STANDARD_VECTOR_SIZE);
      auto& scanned = scratch.Reset();
      column->Scan(state, scanned, take);
      const auto values = scanned.Values<int64_t>();
      for (duckdb::idx_t i = 0; i != take; ++i) {
        const auto value = values[i].GetValue();
        const auto key = absl::c_lower_bound(matching, value);
        if (key != matching.end() && *key == value) {
          remove(*key, static_cast<doc_id_t>(doc_limits::min() + cursor + i));
        }
      }
      cursor += take;
      left -= take;
    }
  }
}

}  // namespace irs
